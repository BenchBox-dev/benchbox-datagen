import gzip
import hashlib
import json
import os
import re
from pathlib import Path

import pytest

import benchbox_datagen

CHECKOUT = os.environ.get("BENCHBOX_CHECKOUT")
pytestmark = pytest.mark.skipif(CHECKOUT is None, reason="BENCHBOX_CHECKOUT is not set")

RANDOM_SEEDS_CHECKED = 500
QUERIES = [str(number) for number in range(1, 23)] + ["15a"]


def clean_sql(sql: str) -> str:
    rowcount = None
    for pattern in (r"where\s+rownum\s*<=\s*(\d+)", r"set\s+rowcount\s+(\d+)", r"\bFIRST\s+(\d+)\b"):
        if match := re.search(pattern, sql, re.IGNORECASE):
            rowcount = int(match.group(1))
            break
    lines = []
    for line in sql.split("\n"):
        line = line.strip()
        if line and not line.startswith("--") and line.lower() not in ("go", ""):
            if line.lower().startswith("set rowcount") or line.lower().startswith("where rownum"):
                continue
            lines.append(line)
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"interval\s+'([^']+)'\s+(day|month|year)\s*\(\d+\)", r"interval '\1' \2", cleaned)
    cleaned = re.sub(r"date\s+'([^']+)'\s*-\s*interval", r"date '\1' - interval", cleaned)
    cleaned = re.sub(r";\s*where\s+rownum\s*<=\s*-?\d+;?\s*$", ";", cleaned, flags=re.IGNORECASE | re.MULTILINE)
    if rowcount and rowcount > 0:
        cleaned = cleaned.rstrip(";").rstrip()
        cleaned = f"{cleaned}\nLIMIT {rowcount};"
    return cleaned


@pytest.fixture(scope="module")
def inputs():
    root = Path(CHECKOUT)
    templates = root / "benchbox/_binaries/tpc-h/templates"
    return {
        "corpus": root / "tests/fixtures/tpc_query_corpus",
        "distributions": (templates / "dists.dss").read_text(encoding="utf-8"),
        "queries": {
            query: (templates / ("variants" if query == "15a" else "queries") / f"{query}.sql").read_text(
                encoding="utf-8"
            )
            for query in QUERIES
        },
    }


def read_records(corpus: Path, stem: str) -> list[dict]:
    records = []
    for shard in sorted(corpus.glob(f"{stem}-*.jsonl.gz")):
        with gzip.open(shard, "rt", encoding="utf-8") as handle:
            records.extend(json.loads(line) for line in handle)
    return records


def generate(inputs, query: str, seed: int | None, scale: float):
    return benchbox_datagen.generate_query(
        "tpch",
        query,
        inputs["queries"][query],
        inputs["distributions"],
        scale,
        seed=seed,
        default_params=seed is None,
    )


def digest(texts: list[str]) -> str:
    sha = hashlib.sha256()
    for query, text in zip(QUERIES, texts):
        sha.update(f"{query}\n{text}\n".encode())
    return sha.hexdigest()


def test_named_cases_match_parameters_and_text_hashes(inputs):
    records = read_records(inputs["corpus"], "tpch_named")
    checked = 0
    for case in records:
        if case.get("s") == "timeout":
            with pytest.raises(ValueError, match="does not terminate"):
                generate(inputs, case["q"], case["seed"], case["sf"])
            continue
        text, params = generate(inputs, case["q"], case["seed"], case["sf"])
        assert params == case["p"], case
        assert hashlib.sha256(clean_sql(text).encode()).hexdigest() == case["h"], case
        checked += 1
    assert checked == len(records) - 2


def test_random_seeds_match_parameters_and_digests(inputs):
    for record in read_records(inputs["corpus"], "tpch_random")[:RANDOM_SEEDS_CHECKED]:
        texts = []
        for query in QUERIES:
            text, params = generate(inputs, query, record["seed"], 1.0)
            assert params == record["p"][query], (record["seed"], query)
            texts.append(clean_sql(text))
        assert digest(texts) == record["h"], record["seed"]


def test_seed_zero_queries_that_never_terminate_are_refused(inputs):
    for query in ("7", "12"):
        with pytest.raises(ValueError, match="does not terminate"):
            generate(inputs, query, 0, 1.0)


def test_a_seed_or_default_parameters_are_required(inputs):
    with pytest.raises(ValueError, match="seed"):
        benchbox_datagen.generate_query(
            "tpch", "6", inputs["queries"]["6"], inputs["distributions"], 1.0, default_params=False
        )


def test_other_benchmarks_are_rejected(inputs):
    with pytest.raises(ValueError, match="not available"):
        benchbox_datagen.generate_query("tpcds", "1", "", "", 1.0, seed=1)

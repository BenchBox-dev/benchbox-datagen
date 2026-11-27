import gzip
import hashlib
import json
import os
from pathlib import Path

import pytest

import benchbox_datagen

CHECKOUT = os.environ.get("BENCHBOX_CHECKOUT")
MULTI_PART = {"14", "23", "24", "39"}
REASON_OFFICIAL = "define REASON= dist(return_reasons, 1, 1);"
REASON_OVERRIDE = 'define REASON= distmember(return_reasons, random(1, rowcount("reason"), uniform), 1);'


@pytest.fixture(scope="module")
def inputs():
    if CHECKOUT is None:
        if os.environ.get("CI"):
            pytest.fail("BENCHBOX_CHECKOUT is required in CI for the TPC-DS corpus tests")
        pytest.skip("BENCHBOX_CHECKOUT is not set")
    root = Path(CHECKOUT)
    tpcds = root / "benchbox/_binaries/tpc-ds"
    return {
        "corpus": root / "tests/fixtures/tpc_query_corpus",
        "templates": tpcds / "templates/query_templates",
        "variants": tpcds / "templates/query_variants",
        "dialect": (tpcds / "templates/query_templates/netezza.tpl").read_text(),
        "idx": (tpcds / "linux-x86_64/tpcds.idx").read_bytes(),
    }


def records(inputs, stem):
    for shard in sorted(inputs["corpus"].glob(f"{stem}-*.jsonl.gz")):
        with gzip.open(shard, "rt") as handle:
            yield from (json.loads(line) for line in handle)


def template(inputs, query, scale):
    base, variant = (query[:-1], query[-1]) if query[-1] in "ab" else (query, None)
    multi_part = base in MULTI_PART and variant is not None
    name = f"query{base}.tpl" if multi_part else f"query{base}{variant or ''}.tpl"
    path = inputs["templates"] / name
    if (variant and not multi_part and (inputs["variants"] / name).exists()) or not path.exists():
        path = inputs["variants"] / name
    text = path.read_text()
    if scale < 1 and name == "query93.tpl":
        text = text.replace(REASON_OFFICIAL, REASON_OVERRIDE, 1)
    return text


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def test_single_template_records_replay(inputs):
    checked = 0
    for record in records(inputs, "tpcds_single"):
        if record.get("s") or checked >= 1500:
            continue
        text, params = benchbox_datagen.generate_query(
            "tpcds",
            record["q"],
            template(inputs, record["q"], record["sf"]),
            inputs["idx"],
            record["sf"],
            seed=record["seed"],
            dialect_text=inputs["dialect"],
        )
        assert digest(text) == record["h"], record["q"]
        assert params == record["p"]
        checked += 1
    assert checked == 1500


def test_explicit_parameter_records_replay(inputs):
    checked = 0
    for record in records(inputs, "tpcds_explicit"):
        text, _ = benchbox_datagen.generate_query(
            "tpcds",
            record["q"],
            template(inputs, record["q"], record["sf"]),
            inputs["idx"],
            record["sf"],
            seed=record["seed"],
            dialect_text=inputs["dialect"],
            parameters=list(record["p"].items()),
        )
        assert digest(text) == record["h"], record["q"]
        checked += 1
    assert checked == 824


def test_streams_follow_the_template_permutation(inputs):
    names = [line for line in (inputs["templates"] / "templates.lst").read_text().splitlines() if line]
    templates = [(name, (inputs["templates"] / name).read_text()) for name in names]
    log, streams = benchbox_datagen.generate_tpcds_streams(
        templates, inputs["dialect"], inputs["idx"], 1.0, seed=42, streams=2
    )
    assert [stream for stream, _ in streams] == [0, 1]
    assert all(len(queries) == 99 for _, queries in streams)
    assert streams[0][1][0][0] != streams[1][1][0][0] or streams[0][1] != streams[1][1]
    assert log.startswith("BEGIN STREAM 0\nTemplate: ")


def test_malformed_inputs_are_errors(inputs):
    good = template(inputs, "1", 1.0)
    for idx in (b"", b"\x00\x00\x00\x05", inputs["idx"][:1000]):
        with pytest.raises(ValueError):
            benchbox_datagen.generate_query("tpcds", "1", good, idx, 1.0, seed=1, dialect_text=inputs["dialect"])
    for broken in ("select [NOPE];", "define X = random(1,;", "select 1", "define A = [A];\nselect [A];"):
        with pytest.raises(ValueError):
            benchbox_datagen.generate_query(
                "tpcds", "1", broken, inputs["idx"], 1.0, seed=1, dialect_text=inputs["dialect"]
            )
    with pytest.raises(ValueError):
        benchbox_datagen.generate_query("tpcds", "1", good, inputs["idx"], 1.0, seed=1)
    with pytest.raises(ValueError):
        benchbox_datagen.generate_query(
            "tpcds", "1", good, inputs["idx"], 1.0, seed=1, dialect_text=inputs["dialect"], parameters=[("NOPE.01", "x")]
        )

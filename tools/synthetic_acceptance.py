import argparse
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import duckdb

import synthetic_configs
from synthetic_configs.base import EXTENDED_SEEDS, SEEDS

CONFIG = None
QUANTILES = [0.01, 0.25, 0.5, 0.75, 0.99]
METRICS = ["null_rate", "distinct_ratio", "mean", "p1", "p25", "p50", "p75", "p99"]
T_99_9_FOUR_DF = 8.610
assert len(SEEDS) == 5
SCALAR_TYPES = {
    "int32": "INTEGER",
    "int64": "BIGINT",
    "string": "VARCHAR",
    "float": "FLOAT",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "date32": "DATE",
    "date32[day]": "DATE",
    "timestamp[us]": "TIMESTAMP",
}
VECTOR_KIND = re.compile(r"fixed_size_list<item: float>\[(\d+)\]$")
QUANTILE_METRICS = {"p1": 0.01, "p25": 0.25, "p50": 0.5, "p75": 0.75, "p99": 0.99}
CDF_MAX_DISTINCT = 5000
COLUMN_BATCH = 4
NOISE_REL_TOL = 1e-9
REJUDGE_ALPHA = 0.001
RANK_SIGMAS = 5.0
EMPTY_RESULT_ALPHA = 0.001
TBL_TYPES = {"int64": "BIGINT", "string": "VARCHAR", "double": "DOUBLE", "date32": "DATE", "bool": "BOOLEAN"}


def tables() -> list[str]:
    return CONFIG.tables


def schemas() -> dict[str, list[list]]:
    return CONFIG.schemas()


def kind_of(table: str, column: str) -> str:
    return {name: kind for name, kind, _ in schemas()[table]}[column]


def tbl_type(kind: str) -> str:
    if kind.startswith("decimal128("):
        return "DECIMAL" + kind.removeprefix("decimal128").replace(" ", "")
    if vector := VECTOR_KIND.match(kind):
        return f"FLOAT[{vector.group(1)}]"
    if kind not in SCALAR_TYPES:
        raise ValueError(f"unsupported Arrow type {kind!r}")
    return SCALAR_TYPES[kind]


def is_text(table: str, column: str) -> bool:
    return kind_of(table, column) == "string"


def column_measure(table: str, column: str) -> str:
    duck_type = tbl_type(kind_of(table, column))
    if duck_type == "VARCHAR":
        return f"length({column})"
    if duck_type == "DATE":
        return f"epoch_us({column}::TIMESTAMP)::DOUBLE"
    if duck_type == "TIMESTAMP":
        return f"epoch_us({column})::DOUBLE"
    if duck_type == "BOOLEAN":
        return f"{column}::INTEGER::DOUBLE"
    if duck_type.startswith("FLOAT["):
        return f"list_aggregate({column}::FLOAT[], 'stddev_pop')::DOUBLE"
    return f"{column}::DOUBLE"


def column_stats(
    con: duckdb.DuckDBPyConnection, source: str, table: str, legacy: bool = False
) -> dict[str, dict]:
    names = [name for name, _, _ in schemas()[table]]
    result = _column_stats(con, source, table, names)
    if legacy and table in CONFIG.shared_domains:
        domain = CONFIG.shared_domains[table]
        shared = f"(SELECT * FROM {source} WHERE {domain.filter})"
        result["columns"].update(_column_stats(con, shared, table, domain.columns)["columns"])
    return result


def _column_stats(con: duckdb.DuckDBPyConnection, source: str, table: str, columns: list[str]) -> dict[str, dict]:
    schema = [column for column in schemas()[table] if column[0] in columns]
    result: dict = {}
    total = 0
    for start in range(0, len(schema), COLUMN_BATCH):
        total, batch = _batch_stats(con, source, table, schema[start : start + COLUMN_BATCH])
        result.update(batch)
    return {"rows": total, "columns": result}


def _batch_stats(con: duckdb.DuckDBPyConnection, source: str, table: str, schema: list[list]) -> tuple[int, dict]:
    selects = []
    for name, _, _ in schema:
        measure = column_measure(table, name)
        selects += [
            f"count({name})",
            f"count(DISTINCT {name})",
            f"avg({measure})",
            f"quantile_disc({measure}, {QUANTILES})",
        ]
    row = con.execute(f"SELECT count(*), {', '.join(selects)} FROM {source}").fetchone()
    total, values = row[0], row[1:]
    result = {}
    for index, (name, _, _) in enumerate(schema):
        present, distinct, mean, quantiles = values[index * 4 : index * 4 + 4]
        entry = {
            "text": is_text(table, name),
            "null_rate": 1 - present / total,
            "distinct": distinct,
            "distinct_ratio": distinct / total,
            "mean": mean,
            **dict(zip(["p1", "p25", "p50", "p75", "p99"], quantiles or [None] * 5)),
        }
        if entry["text"] and distinct <= 64:
            entry["values"] = [r[0] for r in con.execute(f"SELECT DISTINCT {name} FROM {source} ORDER BY 1").fetchall()]
        measure = column_measure(table, name)
        if entry["text"] or distinct <= CDF_MAX_DISTINCT:
            running, cdf = 0, []
            for value, count in con.execute(
                f"SELECT {measure} AS m, count(*) FROM {source} WHERE {name} IS NOT NULL GROUP BY m ORDER BY m"
            ).fetchall():
                running += count
                cdf.append([value, running / present])
            entry["cdf"] = cdf
        result[name] = entry
    return total, result


def table_stats(con: duckdb.DuckDBPyConnection, sources: dict[str, str], legacy: bool = False) -> dict[str, dict]:
    return {table: column_stats(con, sources[table], table, legacy) for table in tables()}


def parquet_sources(directory: Path) -> dict[str, str]:
    return {table: f"read_parquet('{directory / (table + '.parquet')}')" for table in tables()}


def legacy_sources(directory: Path) -> dict[str, str]:
    sources = {}
    header = "true" if CONFIG.legacy_header else "false"
    nullstr = f", nullstr='{CONFIG.legacy_nullstr}'" if CONFIG.legacy_nullstr is not None else ""
    for table in tables():
        columns = ", ".join(f"'{name}': '{tbl_type(kind)}'" for name, kind, _ in schemas()[table])
        path = directory / f"{table}.{CONFIG.legacy_suffix}"
        sources[table] = (
            f"read_csv('{path}', delim='{CONFIG.legacy_delimiter}', header={header}, columns={{{columns}}}{nullstr})"
        )
    return sources


def run_legacy(scale_factor: float, seed: int, workdir: Path, queries_only: bool = False) -> dict:
    out = Path(tempfile.mkdtemp(prefix=f"legacy-{seed}-", dir=workdir))
    try:
        started = time.monotonic()
        CONFIG.legacy_generate(scale_factor, seed, out)
        seconds = time.monotonic() - started
        if queries_only:
            return {"seed": seed, "generate_seconds": seconds, "query_rows": query_row_counts(legacy_sources(out))}
        stats = table_stats(
            duckdb.connect(config={"threads": 4, "memory_limit": CONFIG.stats_memory_limit}), legacy_sources(out), legacy=True
        )
        sources = legacy_sources(out)
        return {
            "seed": seed,
            "generate_seconds": seconds,
            "tables": stats,
            **row_digests(sources),
            "query_rows": query_row_counts(sources),
        }
    finally:
        shutil.rmtree(out, ignore_errors=True)


def row_digests(sources: dict[str, str]) -> dict[str, str]:
    return {
        f"{table}_digest": duckdb.connect().execute(sql.format(source=sources[table])).fetchone()[0]
        for table, sql in CONFIG.row_digests.items()
    }


def cmd_legacy(args: argparse.Namespace) -> None:
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    partial = Path(args.out + ".partial")
    runs = json.loads(partial.read_text())["runs"] if partial.exists() else []
    done = {run["seed"] for run in runs}
    for seed in args.seeds:
        if seed in done:
            continue
        runs.append(run_legacy(args.scale, seed, workdir, args.queries_only))
        partial.write_text(json.dumps({"scale": args.scale, "runs": runs}))
    Path(args.out).write_text(json.dumps({"scale": args.scale, "runs": runs}))
    partial.unlink()
    print(f"legacy sf={args.scale} seeds={args.seeds} -> {args.out}")


def library_write(scale_factor: float, seed: int, directory: Path) -> None:
    writer = os.environ.get("SYNTHETIC_ACCEPT_WRITER")
    if writer:
        subprocess.run([writer, str(scale_factor), str(seed), "parquet", str(directory)], check=True, capture_output=True)
        return
    import benchbox_datagen

    benchbox_datagen.write_tables(CONFIG.name, scale_factor, directory, format="parquet", seed=seed)


def cmd_library(args: argparse.Namespace) -> None:
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    runs = []
    for seed in args.seeds:
        out = Path(tempfile.mkdtemp(prefix=f"library-{seed}-", dir=workdir))
        try:
            library_write(args.scale, seed, out)
            if args.queries_only:
                runs.append({"seed": seed, "query_rows": query_row_counts(parquet_sources(out))})
                continue
            runs.append(
                {
                    "seed": seed,
                    "tables": table_stats(duckdb.connect(), parquet_sources(out)),
                    **row_digests(parquet_sources(out)),
                    "query_rows": query_row_counts(parquet_sources(out)),
                }
            )
        finally:
            shutil.rmtree(out, ignore_errors=True)
    Path(args.out).write_text(json.dumps({"scale": args.scale, "runs": runs}))
    print(f"library sf={args.scale} seeds={args.seeds} -> {args.out}")


def close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)


def constant_match(new: float, value: float, metric: str) -> bool:
    if metric != "mean" and float(value).is_integer():
        return new == value
    return math.isclose(new, value, rel_tol=NOISE_REL_TOL, abs_tol=0.0)


def judge(new: float | None, legacy: list[float | None], metric: str = "") -> str:
    if new is None or any(v is None for v in legacy):
        return "ok" if new is None and all(v is None for v in legacy) else "FAIL"
    low, high = min(legacy), max(legacy)
    if low == high:
        return "ok" if constant_match(new, low, metric) else "FAIL"
    if low <= new <= high or close(new, low) or close(new, high):
        return "ok"
    if len(set(legacy)) == 1:
        return "FAIL"
    spread = statistics.stdev(legacy) * T_99_9_FOUR_DF * math.sqrt(1 + 1 / len(legacy))
    return "pi" if abs(new - statistics.mean(legacy)) <= spread else "FAIL"


def mann_whitney_p(a: list[float], b: list[float]) -> float:
    n1, n2 = len(a), len(b)
    pooled = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    ranks, ties, index = [0.0] * len(pooled), 0.0, 0
    while index < len(pooled):
        end = index
        while end + 1 < len(pooled) and pooled[end + 1][0] == pooled[index][0]:
            end += 1
        for position in range(index, end + 1):
            ranks[position] = (index + end) / 2 + 1
        size = end - index + 1
        ties += size**3 - size
        index = end + 1
    total = n1 + n2
    rank_sum = sum(rank for rank, (_, group) in zip(ranks, pooled) if group == 0)
    u = rank_sum - n1 * (n1 + 1) / 2
    variance = n1 * n2 / 12 * ((total + 1) - ties / (total * (total - 1)))
    if variance <= 0:
        return 1.0
    z = max(0.0, abs(u - n1 * n2 / 2) - 0.5) / math.sqrt(variance)
    return math.erfc(z / math.sqrt(2))


def describe(values: list[float]) -> dict[str, float]:
    return {
        "min": min(values),
        "median": statistics.median(values),
        "mean": statistics.mean(values),
        "max": max(values),
    }


class Rejudge:
    def __init__(self, legacy_runs: list[dict], library_runs: list[dict]):
        self.legacy_runs = legacy_runs
        self.library_runs = library_runs
        self.cache: dict[tuple[str, str, str], dict] = {}

    def values(self, runs: list[dict], side: str) -> list[dict]:
        by_seed = {run["seed"]: run for run in runs if "tables" in run}
        missing = [seed for seed in EXTENDED_SEEDS if seed not in by_seed]
        if missing:
            raise SystemExit(f"{side}: no statistics for extended seeds {missing}")
        return [by_seed[seed] for seed in EXTENDED_SEEDS]

    def judge(self, table: str, column: str, metric: str) -> dict:
        key = (table, column, metric)
        if key not in self.cache:
            legacy_runs = self.values(self.legacy_runs, "legacy")
            library_runs = self.values(self.library_runs, "library")
            legacy = [run["tables"][table]["columns"][column][metric] for run in legacy_runs]
            library = [run["tables"][table]["columns"][column][metric] for run in library_runs]
            if any(v is None for v in legacy + library):
                self.cache[key] = {"p_value": None, "legacy": legacy, "library": library, "ok": False}
            else:
                p_value = mann_whitney_p(legacy, library)
                self.cache[key] = {
                    "p_value": p_value,
                    "legacy": describe(legacy),
                    "library": describe(library),
                    "ok": p_value >= REJUDGE_ALPHA,
                }
        return self.cache[key]


def rank_ok(q: float, legacy_values: list[float], cdf: list[list[float]], rows: int) -> bool:
    tolerance = RANK_SIGMAS * math.sqrt(q * (1 - q) / rows)
    for value in set(legacy_values):
        position = next((i for i, (v, _) in enumerate(cdf) if close(v, value)), None)
        if position is None:
            continue
        upper = cdf[position][1]
        lower = cdf[position - 1][1] if position else 0.0
        if lower - tolerance <= q <= upper + tolerance:
            return True
    return False


def compare_stats(legacy: dict, library_run: dict, rejudge: Rejudge | None = None) -> list[dict]:
    findings = []
    excluded = CONFIG.excluded_columns(legacy, library_run)
    for table in CONFIG.row_digests:
        key = f"{table}_digest"
        digests = {run[key] for run in legacy["runs"]}
        findings.append(
            {
                "table": table,
                "column": "*",
                "metric": "shared_rows_digest",
                "new": library_run[key],
                "legacy": sorted(digests),
                "verdict": "ok" if digests == {library_run[key]} else "FAIL",
            }
        )
    for table in tables():
        for name, entry in library_run["tables"][table]["columns"].items():
            if (table, name) in excluded:
                findings.append(
                    {
                        "table": table,
                        "column": name,
                        "metric": "*",
                        "new": None,
                        "legacy": excluded[(table, name)],
                        "verdict": "excluded",
                    }
                )
                continue
            legacy_entries = [run["tables"][table]["columns"][name] for run in legacy["runs"]]
            shared = CONFIG.shared_domains[table].columns if table in CONFIG.shared_domains else []
            metrics = ["distinct" if m == "distinct_ratio" and name in shared else m for m in METRICS]
            for metric in metrics:
                legacy_values = [e[metric] for e in legacy_entries]
                verdict = judge(entry[metric], legacy_values, metric)
                if verdict == "FAIL" and metric in QUANTILE_METRICS and "cdf" in entry:
                    if rank_ok(QUANTILE_METRICS[metric], legacy_values, entry["cdf"], library_run["tables"][table]["rows"]):
                        verdict = "rank"
                if verdict == "FAIL" and rejudge is not None and entry[metric] is not None:
                    verdict = "mw" if rejudge.judge(table, name, metric)["ok"] else "FAIL"
                findings.append(
                    {
                        "table": table,
                        "column": name,
                        "metric": metric,
                        "new": entry[metric],
                        "legacy": [e[metric] for e in legacy_entries],
                        "verdict": verdict,
                    }
                )
            if "values" in entry:
                same = all(e.get("values") == entry["values"] for e in legacy_entries)
                findings.append(
                    {
                        "table": table,
                        "column": name,
                        "metric": "value_set",
                        "new": entry["values"],
                        "legacy": legacy_entries[0].get("values"),
                        "verdict": "ok" if same else "FAIL",
                    }
                )
    return findings


def fisher_upper_tail(empty_a: int, runs_a: int, empty_b: int, runs_b: int) -> float:
    total, empty = runs_a + runs_b, empty_a + empty_b
    return sum(
        math.comb(empty, k) * math.comb(total - empty, runs_b - k)
        for k in range(empty_b, min(empty, runs_b) + 1)
    ) / math.comb(total, runs_b)


def empty_result_rule(legacy_rows: list[int], library_rows: list[int]) -> dict:
    legacy_empty = sum(1 for n in legacy_rows if n == 0)
    library_empty = sum(1 for n in library_rows if n == 0)
    p_value = fisher_upper_tail(legacy_empty, len(legacy_rows), library_empty, len(library_rows))
    ok = library_empty == 0 if legacy_empty == 0 else p_value >= EMPTY_RESULT_ALPHA
    return {"legacy_empty": legacy_empty, "library_empty": library_empty, "p_value": p_value, "ok": ok}


def seed_rows(base: dict, extra: dict | None, query_id: str, seeds: list[int]) -> list[int]:
    by_seed = {run["seed"]: run["query_rows"][query_id] for run in base["runs"]}
    if extra:
        by_seed.update({run["seed"]: run["query_rows"][query_id] for run in extra["runs"]})
    missing = [seed for seed in seeds if seed not in by_seed]
    if missing:
        raise SystemExit(f"{query_id}: no query row counts for seeds {missing}")
    return [by_seed[seed] for seed in seeds]


def query_rule(legacy: dict, library: dict, legacy_extra: dict | None = None, library_extra: dict | None = None) -> dict[str, dict]:
    result = {}
    for query_id in legacy["runs"][0]["query_rows"]:
        legacy_rows = seed_rows(legacy, None, query_id, SEEDS)
        library_rows = seed_rows(library, None, query_id, SEEDS)
        if any(n == 0 for n in legacy_rows):
            legacy_rows = seed_rows(legacy, legacy_extra, query_id, EXTENDED_SEEDS)
            library_rows = seed_rows(library, library_extra, query_id, EXTENDED_SEEDS)
        result[query_id] = {
            "seeds": len(legacy_rows),
            "legacy": legacy_rows,
            "library": library_rows,
            **empty_result_rule(legacy_rows, library_rows),
        }
    return result


def cmd_compare(args: argparse.Namespace) -> None:
    legacy = json.loads(Path(args.legacy).read_text())
    library = json.loads(Path(args.library).read_text())
    def optional(path: str | None) -> dict | None:
        return json.loads(Path(path).read_text()) if path else None

    legacy_extended, library_extended = optional(args.legacy_extended), optional(args.library_extended)
    rejudge = (
        Rejudge(
            legacy["runs"] + (legacy_extended["runs"] if legacy_extended else []),
            library["runs"] + (library_extended["runs"] if library_extended else []),
        )
        if legacy["scale"] in CONFIG.distribution_scales
        else None
    )
    findings_by_seed = {run["seed"]: compare_stats(legacy, run, rejudge) for run in library["runs"]}
    summary = {"scale": legacy["scale"], "legacy_seeds": [r["seed"] for r in legacy["runs"]], "seeds": {}}
    for seed, findings in findings_by_seed.items():
        counts = {"ok": 0, "pi": 0, "rank": 0, "excluded": 0, "FAIL": 0}
        for finding in findings:
            counts[finding["verdict"]] = counts.get(finding["verdict"], 0) + 1
        summary["seeds"][seed] = {"counts": counts, "details": [f for f in findings if f["verdict"] != "ok"]}
    summary["queries"] = query_rule(legacy, library, optional(args.legacy_queries), optional(args.library_queries))
    for run in legacy["runs"]:
        summary.setdefault("legacy_rows", {}).update({t: run["tables"][t]["rows"] for t in tables()})
    summary["library_rows"] = {t: library["runs"][0]["tables"][t]["rows"] for t in tables()}
    if rejudge and rejudge.cache:
        summary["rejudged"] = [
            {"table": t, "column": c, "metric": m, **result} for (t, c, m), result in sorted(rejudge.cache.items())
        ]
    Path(args.out).write_text(json.dumps(summary, indent=1))
    for seed, data in summary["seeds"].items():
        print(f"sf={legacy['scale']} seed={seed} {data['counts']}")
    bad_queries = [q for q, v in summary["queries"].items() if not v["ok"]]
    print(f"sf={legacy['scale']} queries violating the empty-result rule: {bad_queries}")
    failed = bad_queries or any(d["counts"]["FAIL"] for d in summary["seeds"].values())
    sys.exit(1 if failed else 0)


def query_row_counts(sources: dict[str, str]) -> dict[str, int]:
    con = duckdb.connect()
    for table in tables():
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM {sources[table]}")
    return {query_id: len(con.execute(sql).fetchall()) for query_id, sql in CONFIG.query_texts().items()}


def duckdb_rows(directory: Path, sql: str, con: duckdb.DuckDBPyConnection) -> list[tuple]:
    return [tuple(row) for row in con.execute(sql).fetchall()]


def numeric(value: object) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def sort_key(row: tuple) -> str:
    return repr(tuple(f"{float(v):.6g}" if numeric(v) else v for v in row))


def values_match(a: object, b: object) -> bool:
    if numeric(a) and numeric(b):
        a, b = float(a), float(b)
        return (math.isnan(a) and math.isnan(b)) or math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-9)
    return a == b


def first_difference(duck_rows: list[tuple], df_rows: list[tuple]) -> dict | None:
    if len(duck_rows) != len(df_rows):
        return {"row": None, "column": None, "duckdb": len(duck_rows), "datafusion": len(df_rows)}
    for index, (left, right) in enumerate(zip(sorted(duck_rows, key=sort_key), sorted(df_rows, key=sort_key))):
        if len(left) != len(right):
            return {"row": index, "column": None, "duckdb": repr(left), "datafusion": repr(right)}
        for column, (a, b) in enumerate(zip(left, right)):
            if not values_match(a, b):
                return {"row": index, "column": column, "duckdb": repr(a), "datafusion": repr(b)}
    return None


def orphan_count(con: duckdb.DuckDBPyConnection, child: str, child_col: str, parent: str, parent_col: str) -> int:
    return con.execute(
        f"SELECT count(*) FROM (SELECT {child_col} FROM {child} WHERE {child_col} IS NOT NULL) c "
        f"ANTI JOIN {parent} p ON c.{child_col} = p.{parent_col}"
    ).fetchone()[0]


def verify_tables(scale_factor: float, directory: Path) -> dict:
    con = duckdb.connect()
    sources = parquet_sources(directory)
    result: dict = {"rows": {}, "orphans": {}, "duplicates": {}}
    expected = CONFIG.expected_rows(scale_factor)
    for table in tables():
        result["rows"][table] = {"expected": expected[table], "actual": con.execute(f"SELECT count(*) FROM {sources[table]}").fetchone()[0]}
        keys = ", ".join(CONFIG.primary_keys[table])
        result["duplicates"][table] = con.execute(
            f"SELECT count(*) - count(DISTINCT ({keys})) FROM {sources[table]}"
        ).fetchone()[0]
    for child, child_col, parent, parent_col in CONFIG.foreign_keys:
        result["orphans"][f"{child}.{child_col}->{parent}.{parent_col}"] = orphan_count(
            con, sources[child], child_col, sources[parent], parent_col
        )
    return result


def run_queries(directory: Path) -> dict:
    import datafusion

    queries = CONFIG.query_texts()
    duck = duckdb.connect()
    for table in tables():
        duck.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{directory / (table + '.parquet')}')")
    context = datafusion.SessionContext()
    for table in tables():
        context.register_parquet(table, str(directory / f"{table}.parquet"))
    outcomes = {}
    for query_id, sql in queries.items():
        duck_started = time.monotonic()
        duck_rows = [tuple(row) for row in duck.execute(sql).fetchall()]
        duck_seconds = time.monotonic() - duck_started
        df_started = time.monotonic()
        batches = context.sql(sql).collect()
        df_rows = [tuple(row.values()) for batch in batches for row in batch.to_pylist()]
        df_seconds = time.monotonic() - df_started
        difference = first_difference(duck_rows, df_rows)
        outcomes[query_id] = {
            "duckdb_rows": len(duck_rows),
            "datafusion_rows": len(df_rows),
            "agree": len(duck_rows) == len(df_rows),
            "values_agree": difference is None,
            "first_difference": difference,
            "first_row_null": bool(duck_rows) and all(v is None for v in duck_rows[0]),
            "duckdb_seconds": duck_seconds,
            "datafusion_seconds": df_seconds,
        }
    return outcomes


def cmd_verify(args: argparse.Namespace) -> None:
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    out = Path(tempfile.mkdtemp(prefix=f"verify-{args.scale}-", dir=workdir))
    try:
        started = time.monotonic()
        library_write(args.scale, args.seed, out)
        generate_seconds = time.monotonic() - started
        report = {"scale": args.scale, "seed": args.seed, "generate_seconds": generate_seconds}
        report.update(verify_tables(args.scale, out))
        report["queries"] = run_queries(out)
        Path(args.out).write_text(json.dumps(report, indent=1))
        failed = (
            any(v["expected"] != v["actual"] for v in report["rows"].values())
            or any(report["orphans"].values())
            or any(report["duplicates"].values())
            or any(not q["agree"] for q in report["queries"].values())
        )
        for query_id, q in report["queries"].items():
            if not q["values_agree"]:
                print(f"warning: {query_id} values differ between DuckDB and DataFusion: {q['first_difference']}")
        print(f"verify sf={args.scale} seed={args.seed} failed={failed} -> {args.out}")
        sys.exit(1 if failed else 0)
    finally:
        shutil.rmtree(out, ignore_errors=True)


def load(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def sf_name(scale: float) -> str:
    return f"{scale:g}"


def cmd_report(args: argparse.Namespace) -> None:
    base = Path(args.dir)
    scales = CONFIG.verify_scales
    first, last = SEEDS[0], SEEDS[-1]
    extended_last = EXTENDED_SEEDS[-1]
    seed_range = f"{first}-{last}"
    queries = len(CONFIG.query_texts())
    lines = [f"### Row counts and referential integrity (seed {first})", ""]
    lines += ["| SF | " + " | ".join(tables()) + f" | orphan FKs | duplicate keys | DuckDB = DataFusion on all {queries} |", "|---|" + "---|" * (len(tables()) + 3)]
    for scale in scales:
        verify = load(base / f"verify_{sf_name(scale)}.json")
        if verify is None:
            continue
        counts = " | ".join(
            f"{verify['rows'][t]['actual']:,}{'' if verify['rows'][t]['actual'] == verify['rows'][t]['expected'] else ' (EXPECTED ' + format(verify['rows'][t]['expected'], ',') + ')'}"
            for t in tables()
        )
        agree = all(q["agree"] for q in verify["queries"].values())
        lines.append(
            f"| {sf_name(scale)} | {counts} | {sum(verify['orphans'].values())} | {sum(verify['duplicates'].values())} | {'yes' if agree else 'NO'} |"
        )
    lines += ["", f"### Distribution comparison with {len(SEEDS)} legacy seeds ({seed_range})", ""]
    lines += ["| SF | library seed | in legacy range | in 99.9% prediction interval | rank-equivalent quantile | re-judged on 30 seeds (not different) | excluded | outside |", "|---|---|---|---|---|---|---|---|"]
    details = []
    excluded = {}
    for scale in CONFIG.distribution_scales:
        compare = load(base / f"compare_{sf_name(scale)}.json")
        if compare is None:
            continue
        for seed, data in compare["seeds"].items():
            c = data["counts"]
            lines.append(f"| {sf_name(scale)} | {seed} | {c['ok']} | {c['pi']} | {c['rank']} | {c.get('mw', 0)} | {c['excluded']} | {c['FAIL']} |")
        for finding in compare["seeds"][str(first)]["details"]:
            if finding["verdict"] == "excluded":
                excluded.setdefault((finding["table"], finding["column"]), finding["legacy"])
            else:
                details.append((scale, finding))
    lines += ["", "Excluded columns (domain necessarily changes with the specification row counts; all metrics of the column):", ""]
    lines += ["| table.column | reason |", "|---|---|"]
    lines += [f"| {table}.{column} | {reason} |" for (table, column), reason in sorted(excluded.items())]
    lines += ["", "Metrics for library seed 42 that are not inside the legacy min-max range:", ""]
    lines += ["| SF | table.column | metric | library | legacy (5 seeds) | verdict |", "|---|---|---|---|---|---|"]
    for scale, f in details:
        legacy = f["legacy"]
        shown = ", ".join(f"{v:.6g}" if isinstance(v, (int, float)) else str(v) for v in legacy) if isinstance(legacy, list) else str(legacy)
        new = f"{f['new']:.6g}" if isinstance(f["new"], (int, float)) else str(f["new"])
        lines.append(f"| {sf_name(scale)} | {f['table']}.{f['column']} | {f['metric']} | {new} | {shown} | {f['verdict']} |")
    lines += ["", f"Metrics that failed the 5-seed rule and were re-judged on seeds {first}-{extended_last} per side (two-sided Mann-Whitney U, fails only at p < {REJUDGE_ALPHA}):", ""]
    lines += ["| SF | table.column | metric | legacy min / median / mean / max (30 seeds) | library min / median / mean / max (30 seeds) | p-value | verdict |", "|---|---|---|---|---|---|---|"]
    for scale in CONFIG.distribution_scales:
        compare = load(base / f"compare_{sf_name(scale)}.json")
        for item in (compare or {}).get("rejudged", []):
            shown = lambda d: " / ".join(f"{d[k]:.6g}" for k in ("min", "median", "mean", "max")) if isinstance(d, dict) else str(d)
            p_value = f"{item['p_value']:.3g}" if item["p_value"] is not None else "n/a"
            lines.append(f"| {sf_name(scale)} | {item['table']}.{item['column']} | {item['metric']} | {shown(item['legacy'])} | {shown(item['library'])} | {p_value} | {'not different' if item['ok'] else 'FAIL'} |")
    lines += ["", f"### Empty-result rule (seeds {seed_range} per side; seeds {first}-{extended_last} per side for queries legacy leaves empty on any of {seed_range})", ""]
    lines += ["| SF | queries empty on some legacy seed | rule violations |", "|---|---|---|"]
    for scale in CONFIG.query_scales:
        compare = load(base / f"compare_{sf_name(scale)}.json")
        if compare is None:
            continue
        sometimes = [f"{q} ({v['legacy_empty']} legacy / {v['library_empty']} library empty of {v['seeds']} seeds, p={v['p_value']:.3g})" for q, v in compare["queries"].items() if v["legacy_empty"]]
        bad = [q for q, v in compare["queries"].items() if not v["ok"]]
        lines.append(f"| {sf_name(scale)} | {', '.join(sometimes) or 'none'} | {', '.join(bad) or 'none'} |")
    lines += ["", f"### {CONFIG.name} queries (BenchBox query texts, default parameters)", ""]
    for scale in scales:
        verify = load(base / f"verify_{sf_name(scale)}.json")
        if verify is None:
            continue
        legacy = load(base / f"legacy_{sf_name(scale)}.json")
        library = load(base / f"library_{sf_name(scale)}.json")
        lines += [f"SF {sf_name(scale)}", "", f"| query | expected row counts | legacy seeds {seed_range} | library seeds {seed_range} (DuckDB) | library seed {first} DuckDB | DataFusion | agree |", "|---|---|---|---|---|---|---|"]
        expected = CONFIG.expected_query_rows(scale) if CONFIG.expected_query_rows else {}
        for query_id, outcome in verify["queries"].items():
            key = query_id.removeprefix("Q")
            legacy_rows = [r["query_rows"][query_id] for r in legacy["runs"]] if legacy else "n/a"
            library_rows = [r["query_rows"][query_id] for r in library["runs"]] if library else "n/a"
            lines.append(
                f"| {query_id} | {expected.get(key, 'n/a')} | {legacy_rows} | {library_rows} | {outcome['duckdb_rows']} | {outcome['datafusion_rows']} | {'yes' if outcome['agree'] else 'NO'} |"
            )
        for query_id, outcome in verify["queries"].items():
            if not outcome.get("values_agree", True):
                lines.append(f"Warning: {query_id} values differ between DuckDB and DataFusion (row counts gate only); first difference: {outcome['first_difference']}")
        lines.append("")
    budgets = load(base / "budgets.json")
    if budgets:
        lines += ["### Budgets (N=3 medians, default threads)", ""]
        lines += [f"Host CPUs {budgets['host']['cpus']}, load average at start {[round(x, 2) for x in budgets['host']['loadavg_start']]}.", ""]
        lines += ["| SF | format | wall s (runs) | median wall s | budget s | peak RSS MiB (runs) | median RSS | budget MiB | load before / after | met |", "|---|---|---|---|---|---|---|---|---|---|"]
        for r in budgets["results"]:
            lines.append(
                f"| {sf_name(r['scale'])} | {r['format']} | {r['runs_wall_s']} | {r['median_wall_s']} | {r['budget_wall_s']} | {r['runs_peak_rss_mib']} | {r['median_peak_rss_mib']} | {r['budget_peak_rss_mib']} | {[round(x, 2) for x in r['loadavg_before']]} / {[round(x, 2) for x in r['loadavg_after']]} | {'yes' if r['met'] else 'NO'} |"
            )
    Path(args.out).write_text("\n".join(lines) + "\n")
    print(f"report -> {args.out}")


def main() -> None:
    global CONFIG
    parser = argparse.ArgumentParser()
    parser.add_argument("benchmark", choices=synthetic_configs.names())
    sub = parser.add_subparsers(dest="command", required=True)
    for name, handler in [("legacy", cmd_legacy), ("library", cmd_library)]:
        command = sub.add_parser(name)
        command.add_argument("--scale", type=float, required=True)
        command.add_argument("--seeds", type=int, nargs="+", default=None)
        command.add_argument("--workdir", required=True)
        command.add_argument("--out", required=True)
        command.add_argument("--queries-only", action="store_true")
        command.set_defaults(handler=handler)
    command = sub.add_parser("compare")
    command.add_argument("--legacy", required=True)
    command.add_argument("--library", required=True)
    command.add_argument("--legacy-queries")
    command.add_argument("--library-queries")
    command.add_argument("--legacy-extended")
    command.add_argument("--library-extended")
    command.add_argument("--out", required=True)
    command.set_defaults(handler=cmd_compare)
    command = sub.add_parser("verify")
    command.add_argument("--scale", type=float, required=True)
    command.add_argument("--seed", type=int, default=None)
    command.add_argument("--workdir", required=True)
    command.add_argument("--out", required=True)
    command.set_defaults(handler=cmd_verify)
    command = sub.add_parser("report")
    command.add_argument("--dir", required=True)
    command.add_argument("--out", required=True)
    command.set_defaults(handler=cmd_report)
    args = parser.parse_args()
    CONFIG = synthetic_configs.load(args.benchmark)
    for option in ("seeds", "seed"):
        if hasattr(args, option) and getattr(args, option) is None:
            setattr(args, option, SEEDS if option == "seeds" else SEEDS[0])
    args.handler(args)


if __name__ == "__main__":
    main()

import argparse
import json
import math
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = json.loads((ROOT / "tests" / "data" / "ssb_arrow_schemas.json").read_text())
TABLES = ["date", "customer", "supplier", "part", "lineorder"]
QUANTILES = [0.01, 0.25, 0.5, 0.75, 0.99]
METRICS = ["null_rate", "distinct_ratio", "mean", "p1", "p25", "p50", "p75", "p99"]
T_99_9_FOUR_DF = 8.610
QUANTILE_METRICS = {"p1": 0.01, "p25": 0.25, "p50": 0.5, "p75": 0.75, "p99": 0.99}
CDF_MAX_DISTINCT = 5000
RANK_SIGMAS = 5.0
EXCLUDED_ALWAYS = {
    ("lineorder", "lo_orderkey"): "order structure is 1-7 lines per order (legacy: geometric)",
    ("lineorder", "lo_linenumber"): "order structure is 1-7 lines per order (legacy: geometric)",
}
SHARED_DATE_COLUMNS = {"lineorder": ["lo_orderdate", "lo_commitdate"]}
SHARED_DATE_FILTER = "lo_orderdate <> 19981231"
EMPTY_RESULT_ALPHA = 0.001
BASE_SEEDS = list(range(42, 47))
EXTENDED_SEEDS = list(range(42, 72))
EXCLUDED_WHEN_PART_ROWS_DIFFER = {("part", "p_partkey"), ("part", "p_name"), ("lineorder", "lo_partkey")}
DATE_DIGEST_SQL = (
    "SELECT md5(string_agg(CAST(t AS VARCHAR), ',' ORDER BY d_datekey)) "
    "FROM (SELECT * FROM {source} WHERE d_datekey <= 19981230) t"
)
FOREIGN_KEYS = [
    ("lineorder", "lo_custkey", "customer", "c_custkey"),
    ("lineorder", "lo_partkey", "part", "p_partkey"),
    ("lineorder", "lo_suppkey", "supplier", "s_suppkey"),
    ("lineorder", "lo_orderdate", "date", "d_datekey"),
]
PRIMARY_KEYS = {
    "date": ["d_datekey"],
    "customer": ["c_custkey"],
    "supplier": ["s_suppkey"],
    "part": ["p_partkey"],
    "lineorder": ["lo_orderkey", "lo_linenumber"],
}


def expected_rows(scale_factor: float) -> dict[str, int]:
    def scaled(base: int) -> int:
        return max(1, int(base * scale_factor))

    part = scaled(200_000) if scale_factor < 1 else 200_000 * (1 + math.floor(math.log2(scale_factor)))
    return {
        "date": 2556,
        "customer": scaled(30_000),
        "supplier": scaled(2_000),
        "part": part,
        "lineorder": scaled(6_000_000),
    }


def is_text(table: str, column: str) -> bool:
    return dict((name, kind) for name, kind, _ in SCHEMAS[table])[column] == "string"


def column_stats(
    con: duckdb.DuckDBPyConnection, source: str, table: str, legacy: bool = False
) -> dict[str, dict]:
    result = _column_stats(con, source, table, [name for name, _, _ in SCHEMAS[table]])
    if legacy:
        shared_columns = SHARED_DATE_COLUMNS.get(table)
        if shared_columns:
            shared = f"(SELECT * FROM {source} WHERE {SHARED_DATE_FILTER})"
            result["columns"].update(_column_stats(con, shared, table, shared_columns)["columns"])
    return result


def _column_stats(con: duckdb.DuckDBPyConnection, source: str, table: str, columns: list[str]) -> dict[str, dict]:
    schema = [column for column in SCHEMAS[table] if column[0] in columns]
    selects = []
    for name, _, _ in schema:
        measure = f"length({name})" if is_text(table, name) else f"{name}::DOUBLE"
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
        measure = f"length({name})" if entry["text"] else f"{name}::DOUBLE"
        if entry["text"] or distinct <= CDF_MAX_DISTINCT:
            running, cdf = 0, []
            for value, count in con.execute(
                f"SELECT {measure} AS m, count(*) FROM {source} WHERE {name} IS NOT NULL GROUP BY m ORDER BY m"
            ).fetchall():
                running += count
                cdf.append([value, running / total])
            entry["cdf"] = cdf
        result[name] = entry
    return {"rows": total, "columns": result}


def table_stats(con: duckdb.DuckDBPyConnection, sources: dict[str, str], legacy: bool = False) -> dict[str, dict]:
    return {table: column_stats(con, sources[table], table, legacy) for table in TABLES}


def parquet_sources(directory: Path) -> dict[str, str]:
    return {table: f"read_parquet('{directory / (table + '.parquet')}')" for table in TABLES}


def tbl_sources(directory: Path) -> dict[str, str]:
    sources = {}
    for table in TABLES:
        columns = ", ".join(
            f"'{name}': '{'VARCHAR' if kind == 'string' else 'BIGINT'}'" for name, kind, _ in SCHEMAS[table]
        )
        sources[table] = (
            f"read_csv('{directory / (table + '.tbl')}', delim='|', header=false, columns={{{columns}}})"
        )
    return sources


def run_legacy(scale_factor: float, seed: int, workdir: Path, queries_only: bool = False) -> dict:
    from benchbox.core.ssb.generator import SSBDataGenerator

    out = Path(tempfile.mkdtemp(prefix=f"legacy-{seed}-", dir=workdir))
    try:
        generator = SSBDataGenerator(scale_factor=scale_factor, output_dir=out, quiet=True)
        generator._rng = random.Random(seed)
        started = time.monotonic()
        generator.generate_data()
        seconds = time.monotonic() - started
        if queries_only:
            return {"seed": seed, "generate_seconds": seconds, "query_rows": query_row_counts(tbl_sources(out))}
        stats = table_stats(duckdb.connect(config={"threads": 4, "memory_limit": "2GB"}), tbl_sources(out), legacy=True)
        sources = tbl_sources(out)
        digest = duckdb.connect().execute(DATE_DIGEST_SQL.format(source=sources["date"])).fetchone()[0]
        return {
            "seed": seed,
            "generate_seconds": seconds,
            "tables": stats,
            "date_digest": digest,
            "query_rows": query_row_counts(sources),
        }
    finally:
        shutil.rmtree(out, ignore_errors=True)


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
    writer = os.environ.get("SSB_ACCEPT_WRITER")
    if writer:
        subprocess.run([writer, str(scale_factor), str(seed), "parquet", str(directory)], check=True, capture_output=True)
        return
    import benchbox_datagen

    benchbox_datagen.write_tables("ssb", scale_factor, directory, format="parquet", seed=seed)


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
                    "date_digest": duckdb.connect()
                    .execute(DATE_DIGEST_SQL.format(source=parquet_sources(out)["date"]))
                    .fetchone()[0],
                    "query_rows": query_row_counts(parquet_sources(out)),
                }
            )
        finally:
            shutil.rmtree(out, ignore_errors=True)
    Path(args.out).write_text(json.dumps({"scale": args.scale, "runs": runs}))
    print(f"library sf={args.scale} seeds={args.seeds} -> {args.out}")


def close(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)


def judge(new: float | None, legacy: list[float | None]) -> str:
    if new is None or any(v is None for v in legacy):
        return "ok" if new is None and all(v is None for v in legacy) else "FAIL"
    low, high = min(legacy), max(legacy)
    if low <= new <= high or close(new, low) or close(new, high):
        return "ok"
    if len(set(legacy)) == 1:
        return "FAIL"
    spread = statistics.stdev(legacy) * T_99_9_FOUR_DF * math.sqrt(1 + 1 / len(legacy))
    return "pi" if abs(new - statistics.mean(legacy)) <= spread else "FAIL"


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


def excluded_columns(legacy: dict, library_run: dict) -> dict[tuple[str, str], str]:
    excluded = dict(EXCLUDED_ALWAYS)
    for name in library_run["tables"]["date"]["columns"]:
        excluded[("date", name)] = "date has 2,556 rows (legacy 2,557); compared by digest of the shared rows"
    if any(run["tables"]["part"]["rows"] != library_run["tables"]["part"]["rows"] for run in legacy["runs"]):
        for key in EXCLUDED_WHEN_PART_ROWS_DIFFER:
            excluded[key] = "part row count follows 200,000 x floor(1 + log2 SF)"
    return excluded


def compare_stats(legacy: dict, library_run: dict) -> list[dict]:
    findings = []
    excluded = excluded_columns(legacy, library_run)
    digests = {run["date_digest"] for run in legacy["runs"]}
    findings.append(
        {
            "table": "date",
            "column": "*",
            "metric": "shared_rows_digest",
            "new": library_run["date_digest"],
            "legacy": sorted(digests),
            "verdict": "ok" if digests == {library_run["date_digest"]} else "FAIL",
        }
    )
    for table in TABLES:
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
            metrics = ["distinct" if m == "distinct_ratio" and name in SHARED_DATE_COLUMNS.get(table, []) else m for m in METRICS]
            for metric in metrics:
                legacy_values = [e[metric] for e in legacy_entries]
                verdict = judge(entry[metric], legacy_values)
                if verdict == "FAIL" and metric in QUANTILE_METRICS and "cdf" in entry:
                    if rank_ok(QUANTILE_METRICS[metric], legacy_values, entry["cdf"], library_run["tables"][table]["rows"]):
                        verdict = "rank"
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
        legacy_rows = seed_rows(legacy, None, query_id, BASE_SEEDS)
        library_rows = seed_rows(library, None, query_id, BASE_SEEDS)
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
    findings_by_seed = {run["seed"]: compare_stats(legacy, run) for run in library["runs"]}
    summary = {"scale": legacy["scale"], "legacy_seeds": [r["seed"] for r in legacy["runs"]], "seeds": {}}
    for seed, findings in findings_by_seed.items():
        counts = {"ok": 0, "pi": 0, "rank": 0, "excluded": 0, "FAIL": 0}
        for finding in findings:
            counts[finding["verdict"]] += 1
        summary["seeds"][seed] = {"counts": counts, "details": [f for f in findings if f["verdict"] != "ok"]}
    def optional(path: str | None) -> dict | None:
        return json.loads(Path(path).read_text()) if path else None

    summary["queries"] = query_rule(legacy, library, optional(args.legacy_queries), optional(args.library_queries))
    for run in legacy["runs"]:
        summary.setdefault("legacy_rows", {}).update({t: run["tables"][t]["rows"] for t in TABLES})
    summary["library_rows"] = {t: library["runs"][0]["tables"][t]["rows"] for t in TABLES}
    Path(args.out).write_text(json.dumps(summary, indent=1))
    for seed, data in summary["seeds"].items():
        print(f"sf={legacy['scale']} seed={seed} {data['counts']}")
    bad_queries = [q for q, v in summary["queries"].items() if not v["ok"]]
    print(f"sf={legacy['scale']} queries violating the empty-result rule: {bad_queries}")
    failed = bad_queries or any(d["counts"]["FAIL"] for d in summary["seeds"].values())
    sys.exit(1 if failed else 0)


def query_row_counts(sources: dict[str, str]) -> dict[str, int]:
    con = duckdb.connect()
    for table in TABLES:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM {sources[table]}")
    return {query_id: len(con.execute(sql).fetchall()) for query_id, sql in query_texts().items()}


def query_texts() -> dict[str, str]:
    from benchbox.core.ssb.queries import SSBQueryManager

    manager = SSBQueryManager()
    return {query_id: manager.get_query(query_id) for query_id in manager.get_all_queries()}


def duckdb_rows(directory: Path, sql: str, con: duckdb.DuckDBPyConnection) -> list[tuple]:
    return [tuple(row) for row in con.execute(sql).fetchall()]


def normalise(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=repr)


def verify_tables(scale_factor: float, directory: Path) -> dict:
    con = duckdb.connect()
    sources = parquet_sources(directory)
    result: dict = {"rows": {}, "orphans": {}, "duplicates": {}}
    expected = expected_rows(scale_factor)
    for table in TABLES:
        result["rows"][table] = {"expected": expected[table], "actual": con.execute(f"SELECT count(*) FROM {sources[table]}").fetchone()[0]}
        keys = ", ".join(PRIMARY_KEYS[table])
        result["duplicates"][table] = con.execute(
            f"SELECT count(*) - count(DISTINCT ({keys})) FROM {sources[table]}"
        ).fetchone()[0]
    for child, child_col, parent, parent_col in FOREIGN_KEYS:
        result["orphans"][f"{child}.{child_col}->{parent}.{parent_col}"] = con.execute(
            f"SELECT count(*) FROM {sources[child]} c ANTI JOIN {sources[parent]} p ON c.{child_col} = p.{parent_col}"
        ).fetchone()[0]
    return result


def run_queries(directory: Path) -> dict:
    import datafusion

    queries = query_texts()
    duck = duckdb.connect()
    for table in TABLES:
        duck.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{directory / (table + '.parquet')}')")
    context = datafusion.SessionContext()
    for table in TABLES:
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
        outcomes[query_id] = {
            "duckdb_rows": len(duck_rows),
            "datafusion_rows": len(df_rows),
            "agree": normalise(duck_rows) == normalise(df_rows),
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
        print(f"verify sf={args.scale} seed={args.seed} failed={failed} -> {args.out}")
        sys.exit(1 if failed else 0)
    finally:
        shutil.rmtree(out, ignore_errors=True)


def load(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def sf_name(scale: float) -> str:
    return f"{scale:g}"


def cmd_report(args: argparse.Namespace) -> None:
    from benchbox.core.expected_results.ssb_results import SSB_EXPECTED_ROW_COUNTS

    base = Path(args.dir)
    scales = [0.01, 0.1, 1.0, 10.0]
    lines = ["### Row counts and referential integrity (seed 42)", ""]
    lines += ["| SF | " + " | ".join(TABLES) + " | orphan FKs | duplicate keys | DuckDB = DataFusion on all 13 |", "|---|" + "---|" * (len(TABLES) + 3)]
    for scale in scales:
        verify = load(base / f"verify_{sf_name(scale)}.json")
        if verify is None:
            continue
        counts = " | ".join(
            f"{verify['rows'][t]['actual']:,}{'' if verify['rows'][t]['actual'] == verify['rows'][t]['expected'] else ' (EXPECTED ' + format(verify['rows'][t]['expected'], ',') + ')'}"
            for t in TABLES
        )
        agree = all(q["agree"] for q in verify["queries"].values())
        lines.append(
            f"| {sf_name(scale)} | {counts} | {sum(verify['orphans'].values())} | {sum(verify['duplicates'].values())} | {'yes' if agree else 'NO'} |"
        )
    lines += ["", "### Distribution comparison with 5 legacy seeds (42-46)", ""]
    lines += ["| SF | library seed | in legacy range | in 99.9% prediction interval | rank-equivalent quantile | excluded | outside |", "|---|---|---|---|---|---|---|"]
    details = []
    excluded = {}
    for scale in (0.1, 1.0):
        compare = load(base / f"compare_{sf_name(scale)}.json")
        if compare is None:
            continue
        for seed, data in compare["seeds"].items():
            c = data["counts"]
            lines.append(f"| {sf_name(scale)} | {seed} | {c['ok']} | {c['pi']} | {c['rank']} | {c['excluded']} | {c['FAIL']} |")
        for finding in compare["seeds"]["42"]["details"]:
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
    lines += ["", "### Empty-result rule (seeds 42-46 per side; seeds 42-71 per side for queries legacy leaves empty on any of 42-46)", ""]
    lines += ["| SF | queries empty on some legacy seed | rule violations |", "|---|---|---|"]
    for scale in (0.01, 0.1, 1.0):
        compare = load(base / f"compare_{sf_name(scale)}.json")
        if compare is None:
            continue
        sometimes = [f"{q} ({v['legacy_empty']} legacy / {v['library_empty']} library empty of {v['seeds']} seeds, p={v['p_value']:.3g})" for q, v in compare["queries"].items() if v["legacy_empty"]]
        bad = [q for q, v in compare["queries"].items() if not v["ok"]]
        lines.append(f"| {sf_name(scale)} | {', '.join(sometimes) or 'none'} | {', '.join(bad) or 'none'} |")
    lines += ["", "### SSB queries (BenchBox query texts, default parameters)", ""]
    for scale in scales:
        verify = load(base / f"verify_{sf_name(scale)}.json")
        if verify is None:
            continue
        legacy = load(base / f"legacy_{sf_name(scale)}.json")
        library = load(base / f"library_{sf_name(scale)}.json")
        lines += [f"SF {sf_name(scale)}", "", "| query | SSB_EXPECTED_ROW_COUNTS | legacy seeds 42-46 | library seeds 42-46 (DuckDB) | library seed 42 DuckDB | DataFusion | agree |", "|---|---|---|---|---|---|---|"]
        expected = SSB_EXPECTED_ROW_COUNTS.get(scale, {})
        for query_id, outcome in verify["queries"].items():
            key = query_id.removeprefix("Q")
            legacy_rows = [r["query_rows"][query_id] for r in legacy["runs"]] if legacy else "n/a"
            library_rows = [r["query_rows"][query_id] for r in library["runs"]] if library else "n/a"
            lines.append(
                f"| {query_id} | {expected.get(key, 'n/a')} | {legacy_rows} | {library_rows} | {outcome['duckdb_rows']} | {outcome['datafusion_rows']} | {'yes' if outcome['agree'] else 'NO'} |"
            )
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
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name, handler in [("legacy", cmd_legacy), ("library", cmd_library)]:
        command = sub.add_parser(name)
        command.add_argument("--scale", type=float, required=True)
        command.add_argument("--seeds", type=int, nargs="+", default=BASE_SEEDS)
        command.add_argument("--workdir", required=True)
        command.add_argument("--out", required=True)
        command.add_argument("--queries-only", action="store_true")
        command.set_defaults(handler=handler)
    command = sub.add_parser("compare")
    command.add_argument("--legacy", required=True)
    command.add_argument("--library", required=True)
    command.add_argument("--legacy-queries")
    command.add_argument("--library-queries")
    command.add_argument("--out", required=True)
    command.set_defaults(handler=cmd_compare)
    command = sub.add_parser("verify")
    command.add_argument("--scale", type=float, required=True)
    command.add_argument("--seed", type=int, default=42)
    command.add_argument("--workdir", required=True)
    command.add_argument("--out", required=True)
    command.set_defaults(handler=cmd_verify)
    command = sub.add_parser("report")
    command.add_argument("--dir", required=True)
    command.add_argument("--out", required=True)
    command.set_defaults(handler=cmd_report)
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()

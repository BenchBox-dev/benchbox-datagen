import random
from pathlib import Path

from synthetic_configs.base import SharedDomain, SyntheticConfig, log2_floor, scaled

PART_ROWS_REASON = "part row count follows 200,000 x floor(1 + log2 SF)"
ORDER_STRUCTURE_REASON = "order structure is 1-7 lines per order (legacy: geometric)"


def expected_rows(scale_factor: float) -> dict[str, int]:
    part = scaled(200_000, scale_factor) if scale_factor < 1 else 200_000 * (1 + log2_floor(scale_factor))
    return {
        "date": 2556,
        "customer": scaled(30_000, scale_factor),
        "supplier": scaled(2_000, scale_factor),
        "part": part,
        "lineorder": scaled(6_000_000, scale_factor),
    }


def conditional_exclusions(legacy: dict, library_run: dict) -> dict[tuple[str, str], str]:
    if all(run["tables"]["part"]["rows"] == library_run["tables"]["part"]["rows"] for run in legacy["runs"]):
        return {}
    return {key: PART_ROWS_REASON for key in [("part", "p_partkey"), ("part", "p_name"), ("lineorder", "lo_partkey")]}


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.ssb.generator import SSBDataGenerator

    generator = SSBDataGenerator(scale_factor=scale_factor, output_dir=out, quiet=True)
    generator._rng = random.Random(seed)
    generator.generate_data()


def queries() -> dict[str, str]:
    from benchbox.core.ssb.queries import SSBQueryManager

    manager = SSBQueryManager()
    return {query_id: manager.get_query(query_id) for query_id in manager.get_all_queries()}


def expected_query_rows(scale_factor: float) -> dict[str, object]:
    from benchbox.core.expected_results.ssb_results import SSB_EXPECTED_ROW_COUNTS

    return SSB_EXPECTED_ROW_COUNTS.get(scale_factor, {})


def benchbox_schema() -> dict:
    from benchbox.core.ssb.benchmark import SSBBenchmark

    return SSBBenchmark(scale_factor=0.01).get_schema()


CONFIG = SyntheticConfig(
    name="ssb",
    tables=["date", "customer", "supplier", "part", "lineorder"],
    primary_keys={
        "date": ["d_datekey"],
        "customer": ["c_custkey"],
        "supplier": ["s_suppkey"],
        "part": ["p_partkey"],
        "lineorder": ["lo_orderkey", "lo_linenumber"],
    },
    foreign_keys=[
        ("lineorder", "lo_custkey", "customer", "c_custkey"),
        ("lineorder", "lo_partkey", "part", "p_partkey"),
        ("lineorder", "lo_suppkey", "supplier", "s_suppkey"),
        ("lineorder", "lo_orderdate", "date", "d_datekey"),
    ],
    expected_rows=expected_rows,
    queries=queries,
    legacy_generate=legacy_generate,
    benchbox_schema=benchbox_schema,
    budgets={
        (1.0, "tbl"): (5.0, 512),
        (1.0, "parquet"): (6.0, 512),
        (10.0, "tbl"): (50.0, 1024),
        (10.0, "parquet"): (60.0, 1024),
    },
    exclusions={
        ("lineorder", "lo_orderkey"): ORDER_STRUCTURE_REASON,
        ("lineorder", "lo_linenumber"): ORDER_STRUCTURE_REASON,
    },
    table_exclusions={"date": "date has 2,556 rows (legacy 2,557); compared by digest of the shared rows"},
    conditional_exclusions=conditional_exclusions,
    shared_domains={
        "lineorder": SharedDomain(columns=["lo_orderdate", "lo_commitdate"], filter="lo_orderdate <> 19981231")
    },
    row_digests={
        "date": (
            "SELECT md5(string_agg(CAST(t AS VARCHAR), ',' ORDER BY d_datekey)) "
            "FROM (SELECT * FROM {source} WHERE d_datekey <= 19981230) t"
        )
    },
    expected_query_rows=expected_query_rows,
)

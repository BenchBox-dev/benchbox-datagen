from pathlib import Path

from synthetic_configs.base import SyntheticConfig

LINEITEM_ROWS = {0.01: 60175, 0.1: 600572, 1.0: 6001215, 10.0: 59986052}


def expected_rows(scale_factor: float) -> dict[str, int]:
    parts = int(200_000 * scale_factor)
    return {
        "nation": 25,
        "region": 5,
        "part": parts,
        "supplier": int(10_000 * scale_factor),
        "partsupp": parts * 4,
        "customer": int(150_000 * scale_factor),
        "orders": int(1_500_000 * scale_factor),
        "lineitem": LINEITEM_ROWS[scale_factor],
    }


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.tpch_skew.generator import TPCHSkewDataGenerator
    from benchbox.core.tpch_skew.skew_config import SkewPreset, get_preset_config

    generator = TPCHSkewDataGenerator(
        scale_factor=scale_factor,
        output_dir=out,
        skew_config=get_preset_config(SkewPreset.MODERATE, seed=seed),
        quiet=True,
        force_regenerate=True,
    )
    generator.generate()


def queries() -> dict[str, str]:
    from benchbox.core.tpch_skew.benchmark import TPCHSkewBenchmark

    texts = TPCHSkewBenchmark(scale_factor=1.0).get_queries()
    return {f"Q{query_id}": sql for query_id, sql in texts.items()}


def benchbox_schema() -> dict:
    from benchbox.core.tpch_skew.benchmark import TPCHSkewBenchmark

    return TPCHSkewBenchmark(scale_factor=0.01).get_schema()


CONFIG = SyntheticConfig(
    name="tpch_skew",
    tables=["nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"],
    primary_keys={
        "nation": ["n_nationkey"],
        "region": ["r_regionkey"],
        "part": ["p_partkey"],
        "supplier": ["s_suppkey"],
        "partsupp": ["ps_partkey", "ps_suppkey"],
        "customer": ["c_custkey"],
        "orders": ["o_orderkey"],
        "lineitem": ["l_orderkey", "l_linenumber"],
    },
    foreign_keys=[
        ("nation", "n_regionkey", "region", "r_regionkey"),
        ("supplier", "s_nationkey", "nation", "n_nationkey"),
        ("customer", "c_nationkey", "nation", "n_nationkey"),
        ("partsupp", "ps_partkey", "part", "p_partkey"),
        ("partsupp", "ps_suppkey", "supplier", "s_suppkey"),
        ("orders", "o_custkey", "customer", "c_custkey"),
        ("lineitem", "l_orderkey", "orders", "o_orderkey"),
        ("lineitem", "l_partkey", "part", "p_partkey"),
        ("lineitem", "l_suppkey", "supplier", "s_suppkey"),
    ],
    expected_rows=expected_rows,
    queries=queries,
    legacy_generate=legacy_generate,
    benchbox_schema=benchbox_schema,
    budget_class="tpch",
)

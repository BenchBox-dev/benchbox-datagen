from pathlib import Path

from synthetic_configs.base import SyntheticConfig, scaled

DOT = "sum(x * y)"
COSINE = f"{DOT} / (sqrt(sum(x * x)) * sqrt(sum(y * y)))"
EUCLIDEAN = "sqrt(sum((x - y) * (x - y)))"


def expected_rows(scale_factor: float) -> dict[str, int]:
    return {"vectors": scaled(1_000_000, scale_factor), "vector_queries": 100}


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.vector_search import generator
    from benchbox.core.vector_search.generator import VectorSearchDataGenerator

    generator._SEED = seed
    VectorSearchDataGenerator(scale_factor=scale_factor, output_dir=out, quiet=True).generate_data()


def top_k(measure: str, order: str, query_id: int, limit: int, condition: str = "") -> str:
    where = f" WHERE {condition}" if condition else ""
    return (
        f"SELECT id FROM (SELECT id, {measure} AS score FROM "
        "(SELECT v.id AS id, unnest(v.embedding) AS x, unnest(q.query_vector) AS y "
        f"FROM vectors v CROSS JOIN (SELECT query_vector FROM vector_queries WHERE query_id = {query_id}) q{where}) t "
        f"GROUP BY id) s ORDER BY score {order} LIMIT {limit}"
    )


def queries() -> dict[str, str]:
    return {
        "Q1": top_k(COSINE, "DESC", 1, 10),
        "Q2": top_k(EUCLIDEAN, "ASC", 1, 10),
        "Q3": top_k(COSINE, "DESC", 1, 10, "v.category = 'category_01'"),
        "Q4": top_k(COSINE, "DESC", 1, 100),
        "Q5": top_k(COSINE, "DESC", 1, 10),
        "Q6": top_k(COSINE, "DESC", 2, 20, "v.category IN ('category_01', 'category_02', 'category_03')"),
    }


def benchbox_schema() -> dict:
    from benchbox.core.vector_search.schema import TABLES

    return TABLES


CONFIG = SyntheticConfig(
    name="vector_search",
    tables=["vectors", "vector_queries"],
    primary_keys={"vectors": ["id"], "vector_queries": ["query_id"]},
    foreign_keys=[],
    expected_rows=expected_rows,
    queries=queries,
    legacy_generate=legacy_generate,
    benchbox_schema=benchbox_schema,
    budgets={
        (1.0, "tbl"): (15.0, 512),
        (1.0, "parquet"): (15.0, 512),
        (10.0, "tbl"): (150.0, 512),
        (10.0, "parquet"): (150.0, 512),
    },
    legacy_suffix="csv",
    legacy_delimiter="|",
    legacy_header=True,
    verify_scales=(0.01, 0.1, 1.0),
)

import random
from pathlib import Path

from synthetic_configs.base import SyntheticConfig, scaled

TABLES = [
    "kind_type",
    "company_type",
    "info_type",
    "role_type",
    "comp_cast_type",
    "link_type",
    "title",
    "name",
    "company_name",
    "keyword",
    "char_name",
    "cast_info",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "person_info",
    "complete_cast",
    "aka_name",
    "aka_title",
]
LOOKUP_ROWS = {"kind_type": 7, "company_type": 4, "info_type": 24, "role_type": 12, "comp_cast_type": 4, "link_type": 18}
BASE_ROWS = {
    "title": 500_000,
    "name": 800_000,
    "company_name": 60_000,
    "keyword": 24_000,
    "char_name": 600_000,
    "cast_info": 7_000_000,
    "movie_companies": 520_000,
    "movie_info": 3_000_000,
    "movie_info_idx": 280_000,
    "movie_keyword": 1_000_000,
    "movie_link": 6_000,
    "person_info": 600_000,
    "complete_cast": 30_000,
    "aka_name": 180_000,
    "aka_title": 80_000,
}
PLANTED = {"title", "name", "company_name", "keyword"}
PLANTED_ROWS = 60


def expected_rows(scale_factor: float) -> dict[str, int]:
    rows = dict(LOOKUP_ROWS)
    for table, base in BASE_ROWS.items():
        rows[table] = scaled(base, scale_factor) + (PLANTED_ROWS if table in PLANTED else 0)
    return {table: rows[table] for table in TABLES}


class SeededRandom(random.Random):
    def seed(self, *_args, **_kwargs) -> None:
        return None


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.joinorder_synthetic import generator as legacy

    source = SeededRandom()
    random.Random.seed(source, seed)
    legacy.random = source
    legacy.JoinOrderGenerator(scale_factor=scale_factor, output_dir=out, quiet=True).generate_data()


def queries() -> dict[str, str]:
    from benchbox.core.joinorder_synthetic.queries import JoinOrderQueryManager

    return JoinOrderQueryManager().get_all_queries()


def benchbox_schema() -> dict:
    from benchbox.core.joinorder_synthetic.benchmark import JoinOrderSyntheticBenchmark

    return JoinOrderSyntheticBenchmark(scale_factor=0.01).get_schema()


CONFIG = SyntheticConfig(
    name="joinorder_synthetic",
    tables=TABLES,
    primary_keys={table: ["id"] for table in TABLES},
    foreign_keys=[
        ("title", "kind_id", "kind_type", "id"),
        ("cast_info", "person_id", "name", "id"),
        ("cast_info", "movie_id", "title", "id"),
        ("cast_info", "role_id", "role_type", "id"),
        ("movie_companies", "movie_id", "title", "id"),
        ("movie_companies", "company_id", "company_name", "id"),
        ("movie_companies", "company_type_id", "company_type", "id"),
        ("movie_info", "movie_id", "title", "id"),
        ("movie_info", "info_type_id", "info_type", "id"),
        ("movie_info_idx", "movie_id", "title", "id"),
        ("movie_info_idx", "info_type_id", "info_type", "id"),
        ("movie_keyword", "movie_id", "title", "id"),
        ("movie_keyword", "keyword_id", "keyword", "id"),
        ("movie_link", "movie_id", "title", "id"),
        ("movie_link", "linked_movie_id", "title", "id"),
        ("movie_link", "link_type_id", "link_type", "id"),
        ("person_info", "person_id", "name", "id"),
        ("complete_cast", "movie_id", "title", "id"),
        ("complete_cast", "subject_id", "comp_cast_type", "id"),
        ("complete_cast", "status_id", "comp_cast_type", "id"),
        ("aka_name", "person_id", "name", "id"),
        ("aka_title", "movie_id", "title", "id"),
        ("aka_title", "kind_id", "kind_type", "id"),
    ],
    expected_rows=expected_rows,
    queries=queries,
    legacy_generate=legacy_generate,
    benchbox_schema=benchbox_schema,
    budgets={
        (1.0, "tbl"): (15.0, 512),
        (1.0, "parquet"): (15.0, 512),
        (10.0, "tbl"): (150.0, 1024),
        (10.0, "parquet"): (150.0, 1024),
    },
    legacy_suffix="csv",
    legacy_delimiter=",",
    distribution_scales=(0.01, 0.1, 1.0),
    verify_scales=(0.01, 0.1, 0.5, 1.0, 2.0, 10.0),
)

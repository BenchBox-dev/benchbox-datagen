import random
import re
from pathlib import Path

from synthetic_configs.base import SyntheticConfig

USERVISITS_KEY = [
    "sourceIP",
    "destURL",
    "visitDate",
    "adRevenue",
    "userAgent",
    "countryCode",
    "languageCode",
    "searchWord",
    "duration",
]
BASE_ROWS = {"rankings": 250_000, "uservisits": 2_500_000, "documents": 125_000}


def expected_rows(scale_factor: float) -> dict[str, int]:
    return {table: int(base * scale_factor) for table, base in BASE_ROWS.items()}


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.amplab.generator import AMPLabDataGenerator

    generator = AMPLabDataGenerator(scale_factor=scale_factor, output_dir=out, quiet=True)
    random.seed(seed)
    generator.generate_data()


MIXED_CASE_COLUMNS = re.compile(
    r"\b(pageURL|pageRank|avgDuration|sourceIP|destURL|visitDate|adRevenue|userAgent|countryCode|languageCode|searchWord)\b"
)


def queries() -> dict[str, str]:
    from benchbox.core.amplab.queries import AMPLabQueryManager

    texts = AMPLabQueryManager().get_all_queries()
    return {query_id: MIXED_CASE_COLUMNS.sub(r'"\1"', sql) for query_id, sql in texts.items()}


def benchbox_schema() -> dict:
    from benchbox.core.amplab.benchmark import AMPLabBenchmark

    return AMPLabBenchmark(scale_factor=0.01).get_schema()


CONFIG = SyntheticConfig(
    name="amplab",
    tables=["rankings", "documents", "uservisits"],
    primary_keys={"rankings": ["pageURL"], "documents": ["url"], "uservisits": USERVISITS_KEY},
    foreign_keys=[],
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
)

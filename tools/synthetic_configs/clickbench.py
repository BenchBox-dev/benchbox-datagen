import random
import re
from pathlib import Path
from unittest import mock

from synthetic_configs.base import SyntheticConfig, scaled


def expected_rows(scale_factor: float) -> dict[str, int]:
    return {"hits": scaled(1_000_000, scale_factor)}


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.clickbench.generator import ClickBenchDataGenerator

    generator = ClickBenchDataGenerator(scale_factor=scale_factor, output_dir=out, quiet=True)
    reseed = random.seed
    with mock.patch.object(random, "seed", lambda *_args, **_kwargs: reseed(seed)):
        generator.generate_data()


def queries() -> dict[str, str]:
    from benchbox.core.clickbench.queries import ClickBenchQueryManager
    from benchbox.core.clickbench.schema import TABLES

    names = sorted((column["name"] for column in TABLES["hits"]["columns"]), key=len, reverse=True)
    token = re.compile(r"'(?:[^']|'')*'|\b(" + "|".join(names) + r")\b")
    manager = ClickBenchQueryManager()
    return {
        query_id: token.sub(lambda m: f'"{m.group(1)}"' if m.group(1) else m.group(0), manager.get_query(query_id))
        for query_id in manager.get_all_queries()
    }


def benchbox_schema() -> dict:
    from benchbox.core.clickbench.benchmark import ClickBenchBenchmark

    return ClickBenchBenchmark(scale_factor=0.01).get_schema()


CONFIG = SyntheticConfig(
    name="clickbench",
    tables=["hits"],
    primary_keys={"hits": ["CounterID", "EventDate", "UserID", "EventTime", "WatchID"]},
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
)

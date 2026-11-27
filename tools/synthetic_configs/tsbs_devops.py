import json
from pathlib import Path

from synthetic_configs.base import SyntheticConfig

TIMESTAMPS = 2 * 24 * 60 * 60 // 10
MIN_HOSTS = 10
TAGS_VALUE_SET_REASON = (
    "tags holds one random row per host and the five legacy seeds disagree with each other on this column's "
    "value set, so the value-set rule (and the metrics that follow from it) cannot hold for any generator"
)
QUERY_SEED = 42
QUERY_HOSTS = 10
QUERY_DAYS = 2


def hosts(scale_factor: float) -> int:
    return max(MIN_HOSTS, int(100 * scale_factor))


def expected_rows(scale_factor: float) -> dict[str, int]:
    count = hosts(scale_factor)
    return {
        "tags": count,
        "cpu": count * TIMESTAMPS,
        "mem": count * TIMESTAMPS,
        "disk": count * 2 * TIMESTAMPS,
        "net": count * 2 * TIMESTAMPS,
    }


def conditional_exclusions(legacy: dict, library_run: dict) -> dict[tuple[str, str], str]:
    excluded = {}
    for name in library_run["tables"]["tags"]["columns"]:
        value_sets = {json.dumps(run["tables"]["tags"]["columns"][name].get("values")) for run in legacy["runs"]}
        if len(value_sets) > 1:
            excluded[("tags", name)] = TAGS_VALUE_SET_REASON
    return excluded


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.tsbs_devops.generator import TSBSDevOpsDataGenerator

    generator = TSBSDevOpsDataGenerator(scale_factor=scale_factor, output_dir=out, seed=seed, quiet=True)
    generator.generate()


def queries() -> dict[str, str]:
    from benchbox.core.tsbs_devops.queries import TSBSDevOpsQueryManager

    manager = TSBSDevOpsQueryManager(num_hosts=QUERY_HOSTS, duration_days=QUERY_DAYS, seed=QUERY_SEED)
    return manager.get_queries()


def benchbox_schema() -> dict:
    from benchbox.core.tsbs_devops.benchmark import TSBSDevOpsBenchmark

    return TSBSDevOpsBenchmark(scale_factor=0.01).get_schema()


CONFIG = SyntheticConfig(
    name="tsbs_devops",
    tables=["tags", "cpu", "mem", "disk", "net"],
    primary_keys={
        "tags": ["hostname"],
        "cpu": ["time", "hostname"],
        "mem": ["time", "hostname"],
        "disk": ["time", "hostname", "device"],
        "net": ["time", "hostname", "interface"],
    },
    foreign_keys=[
        ("cpu", "hostname", "tags", "hostname"),
        ("mem", "hostname", "tags", "hostname"),
        ("disk", "hostname", "tags", "hostname"),
        ("net", "hostname", "tags", "hostname"),
    ],
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
    legacy_delimiter=",",
    legacy_header=True,
    conditional_exclusions=conditional_exclusions,
)

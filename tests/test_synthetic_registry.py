import sys
from pathlib import Path

import pyarrow as pa
import pytest

import benchbox_datagen

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

import synthetic_configs
from synthetic_configs.base import SyntheticConfig

NAMES = synthetic_configs.names()


def test_every_config_has_a_registered_generator():
    assert NAMES
    for name in NAMES:
        config = synthetic_configs.load(name)
        assert config.name == name
        assert benchbox_datagen.table_names(name) == config.tables


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("scale", [0.01, 1.0, 10.0])
def test_config_expected_rows_match_the_library(name, scale):
    assert synthetic_configs.load(name).expected_rows(scale) == benchbox_datagen.row_counts(name, scale)


@pytest.mark.parametrize("name", NAMES)
def test_config_schema_file_matches_the_library(name):
    config = synthetic_configs.load(name)
    assert set(config.schemas()) == set(config.tables)
    for table in config.tables:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table(name, table, 0.01)).schema
        assert [[field.name, str(field.type), field.nullable] for field in schema] == config.schemas()[table]


@pytest.mark.parametrize("name", NAMES)
def test_config_keys_and_exclusions_reference_real_columns(name):
    config = synthetic_configs.load(name)
    columns = {table: {column[0] for column in config.schemas()[table]} for table in config.tables}
    for table, keys in config.primary_keys.items():
        assert set(keys) <= columns[table]
    for child, child_column, parent, parent_column in config.foreign_keys:
        assert child_column in columns[child] and parent_column in columns[parent]
    for table, column in config.exclusions:
        assert column in columns[table]
    for table, domain in config.shared_domains.items():
        assert set(domain.columns) <= columns[table]
    assert set(config.table_exclusions) <= set(config.tables)
    assert set(config.row_digests) <= set(config.tables)


def rebuilt(**changes) -> SyntheticConfig:
    config = synthetic_configs.load(NAMES[0])
    fields = {name: getattr(config, name) for name in config.__dataclass_fields__}
    fields.update(changes)
    return SyntheticConfig(**fields)


def test_an_exclusion_without_a_reason_is_rejected():
    config = synthetic_configs.load(NAMES[0])
    with pytest.raises(ValueError, match="written reason"):
        rebuilt(exclusions={(config.tables[0], "c"): "  "})


def test_a_table_exclusion_without_a_reason_is_rejected():
    config = synthetic_configs.load(NAMES[0])
    with pytest.raises(ValueError, match="written reason"):
        rebuilt(table_exclusions={config.tables[0]: ""})


def test_a_conditional_exclusion_without_a_reason_is_rejected():
    config = synthetic_configs.load(NAMES[0])
    table = config.tables[0]
    column = config.schemas()[table][0][0]
    run = {"tables": {table: {"columns": {column: {}}}}}
    blank = rebuilt(conditional_exclusions=lambda legacy, library: {(table, column): " "})
    with pytest.raises(ValueError, match="written reason"):
        blank.excluded_columns({}, run)
    reasoned = rebuilt(conditional_exclusions=lambda legacy, library: {(table, column): "because"})
    assert reasoned.excluded_columns({}, run)[(table, column)] == "because"


def test_table_exclusions_cover_every_column_of_the_table_with_the_reason():
    config = synthetic_configs.load(NAMES[0])
    table = config.tables[0]
    columns = [column[0] for column in config.schemas()[table]]
    run = {"tables": {table: {"columns": dict.fromkeys(columns, {})}}}
    excluded = rebuilt(table_exclusions={table: "the table is compared another way"}, conditional_exclusions=None).excluded_columns({}, run)
    assert {column: excluded[(table, column)] for column in columns} == dict.fromkeys(
        columns, "the table is compared another way"
    )


def test_a_budget_class_must_name_an_adr_class():
    with pytest.raises(ValueError, match="budget_class"):
        rebuilt(budget_class="nonsense")
    assert rebuilt(budget_class="tpch").budget_class == "tpch"


def test_a_budget_class_config_is_judged_against_the_class_budget(monkeypatch):
    import bench_budgets

    config = rebuilt(name="classed", budget_class="tpch", budgets={})
    monkeypatch.setattr(bench_budgets.synthetic_configs, "names", lambda: ["classed"])
    monkeypatch.setattr(bench_budgets.synthetic_configs, "load", lambda name: config)
    bench_budgets.register_synthetic_budgets()
    try:
        assert bench_budgets.BUDGETS[("classed", 1.0, "tbl")] == bench_budgets.BUDGETS[("tpch", 1.0, "tbl")]
        assert bench_budgets.BUDGETS[("classed", 10.0, "parquet")] == (40.0, 2048)
    finally:
        for key in [k for k in bench_budgets.BUDGETS if k[0] == "classed"]:
            del bench_budgets.BUDGETS[key]


def test_budgets_and_a_budget_class_together_are_rejected():
    with pytest.raises(ValueError, match="not both"):
        rebuilt(budget_class="tpch", budgets={(1.0, "tbl"): (1.0, 1)})

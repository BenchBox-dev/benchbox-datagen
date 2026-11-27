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
        assert [field.name for field in schema] == [column[0] for column in config.schemas()[table]]


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


def test_an_exclusion_without_a_reason_is_rejected():
    config = synthetic_configs.load(NAMES[0])
    fields = {name: getattr(config, name) for name in config.__dataclass_fields__}
    fields["exclusions"] = {(config.tables[0], "c"): "  "}
    with pytest.raises(ValueError, match="written reason"):
        SyntheticConfig(**fields)

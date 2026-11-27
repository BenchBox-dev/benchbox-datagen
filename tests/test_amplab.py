import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = ["rankings", "documents", "uservisits"]


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_table_names_and_row_counts():
    assert benchbox_datagen.table_names("amplab") == TABLES
    assert benchbox_datagen.row_counts("amplab", 0.01) == {
        "rankings": 2_500,
        "documents": 1_250,
        "uservisits": 25_000,
    }
    assert benchbox_datagen.row_counts("amplab", 10.0)["uservisits"] == 25_000_000


def test_stream_and_parquet_schemas_equal_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "amplab_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("amplab", table, 0.01)).schema
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[table], table
    for stat in benchbox_datagen.write_tables("amplab", 0.01, tmp_path, format="parquet"):
        schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[stat.table]


def test_seed_defaults_to_42_and_changes_the_data(tmp_path):
    def uservisits(seed):
        directory = tmp_path / str(seed)
        benchbox_datagen.write_tables("amplab", 0.01, directory, format="tbl", seed=seed)
        return sha256(str(directory / "uservisits.tbl"))

    assert uservisits(None) == uservisits(42)
    assert uservisits(43) != uservisits(42)


def test_delimited_text_uses_legacy_layout(tmp_path):
    benchbox_datagen.write_tables("amplab", 0.01, tmp_path, format="tbl")
    first = (tmp_path / "uservisits.tbl").read_text().splitlines()[0].split("|")
    assert len(first) == 9
    assert first[1].startswith("http://")
    assert len(first[2]) == 10

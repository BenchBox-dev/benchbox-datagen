import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = ["date", "customer", "supplier", "part", "lineorder"]


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_table_names_and_row_counts():
    assert benchbox_datagen.table_names("ssb") == TABLES
    assert benchbox_datagen.row_counts("ssb", 0.01) == {
        "date": 2_556,
        "customer": 300,
        "supplier": 20,
        "part": 2_000,
        "lineorder": 60_000,
    }
    assert benchbox_datagen.row_counts("ssb", 10.0)["lineorder"] == 60_000_000
    assert benchbox_datagen.row_counts("ssb", 10.0)["part"] == 800_000
    assert benchbox_datagen.row_counts("ssb", 1.0)["part"] == 200_000


def test_stream_and_parquet_schemas_equal_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "ssb_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("ssb", table, 0.01)).schema
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[table], table
    for stat in benchbox_datagen.write_tables("ssb", 0.01, tmp_path, format="parquet"):
        schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[stat.table]


def test_key_ranges_bound_every_foreign_key():
    ranges = benchbox_datagen.key_ranges("ssb", 0.01)
    assert ranges["customer"]["c_custkey"] == (1, 300)
    assert ranges["lineorder"]["lo_suppkey"] == ranges["supplier"]["s_suppkey"] == (1, 20)
    assert ranges["lineorder"]["lo_orderdate"] == ranges["date"]["d_datekey"] == (19920101, 19981230)
    for table, rows in benchbox_datagen.row_counts("ssb", 0.01).items():
        batches = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("ssb", table, 0.01))
        assert batches.read_all().num_rows == rows


def test_seed_defaults_to_42_and_changes_the_data(tmp_path):
    def lineorder(seed):
        directory = tmp_path / str(seed)
        benchbox_datagen.write_tables("ssb", 0.01, directory, format="tbl", seed=seed)
        return sha256(str(directory / "lineorder.tbl"))

    assert lineorder(None) == lineorder(42)
    assert lineorder(43) != lineorder(42)


def test_files_do_not_depend_on_threads(tmp_path):
    for fmt in ("tbl", "parquet"):
        hashes = []
        for threads in (1, 4, None):
            directory = tmp_path / f"{fmt}-{threads}"
            stats = benchbox_datagen.write_tables("ssb", 0.1, directory, format=fmt, threads=threads)
            hashes.append([sha256(stat.path) for stat in stats])
        assert hashes[0] == hashes[1] == hashes[2]


def test_unseeded_benchmarks_reject_a_seed(tmp_path):
    with pytest.raises(ValueError, match="not seeded"):
        benchbox_datagen.write_tables("tpch", 0.01, tmp_path, format="tbl", seed=1)
    with pytest.raises(ValueError, match="not seeded"):
        benchbox_datagen.generate_table("tpch", "nation", 1.0, seed=1)

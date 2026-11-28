import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = ["nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
UNCHANGED = ["nation", "region", "supplier", "partsupp"]


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_table_names_and_row_counts_equal_tpch():
    assert benchbox_datagen.table_names("tpch_skew") == TABLES
    for scale in (0.01, 1.0, 10.0):
        assert benchbox_datagen.row_counts("tpch_skew", scale) == benchbox_datagen.row_counts("tpch", scale)
    assert benchbox_datagen.row_counts("tpch_skew", 1.0)["lineitem"] == 6_001_215


def test_stream_and_parquet_schemas_equal_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "tpch_skew_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpch_skew", table, 0.01)).schema
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[table], table
    for stat in benchbox_datagen.write_tables("tpch_skew", 0.01, tmp_path, format="parquet"):
        schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[stat.table]


def test_key_ranges_and_table_scales_equal_tpch():
    assert benchbox_datagen.key_ranges("tpch_skew", 0.1) == benchbox_datagen.key_ranges("tpch", 0.1)
    assert benchbox_datagen.table_scales("tpch_skew", 0.1) == benchbox_datagen.table_scales("tpch", 0.1)


def test_unchanged_tables_are_the_tpch_files_and_skewed_tables_differ(tmp_path):
    skewed = benchbox_datagen.write_tables("tpch_skew", 0.01, tmp_path / "skew", format="tbl")
    plain = benchbox_datagen.write_tables("tpch", 0.01, tmp_path / "plain", format="tbl")
    skewed, plain = {s.table: sha256(s.path) for s in skewed}, {s.table: sha256(s.path) for s in plain}
    for table in TABLES:
        assert (skewed[table] == plain[table]) == (table in UNCHANGED), table


def test_seed_defaults_to_42_and_changes_the_skewed_tables(tmp_path):
    def lineitem(seed):
        directory = tmp_path / str(seed)
        benchbox_datagen.write_tables("tpch_skew", 0.01, directory, format="tbl", seed=seed)
        return sha256(str(directory / "lineitem.tbl"))

    assert lineitem(None) == lineitem(42)
    assert lineitem(43) != lineitem(42)


def test_files_do_not_depend_on_threads(tmp_path):
    for fmt in ("tbl", "parquet"):
        hashes = []
        for threads in (1, 4, None):
            directory = tmp_path / f"{fmt}-{threads}"
            stats = benchbox_datagen.write_tables("tpch_skew", 0.1, directory, format=fmt, threads=threads)
            hashes.append([sha256(stat.path) for stat in stats])
        assert hashes[0] == hashes[1] == hashes[2]


def test_the_skew_concentrates_on_the_first_keys():
    table = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpch_skew", "lineitem", 0.1)).read_all()
    keys = table.column("l_partkey").to_pylist()
    assert min(keys) >= 1 and max(keys) <= 20_000
    assert keys.count(1) / len(keys) > 0.05


def test_tpch_skew_refresh_sets_are_not_available(tmp_path):
    with pytest.raises(ValueError, match="no refresh sets"):
        benchbox_datagen.write_refresh("tpch_skew", 0.01, tmp_path)

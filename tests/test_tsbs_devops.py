import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = ["tags", "cpu", "mem", "disk", "net"]


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_table_names_and_row_counts():
    assert benchbox_datagen.table_names("tsbs_devops") == TABLES
    assert benchbox_datagen.row_counts("tsbs_devops", 0.01) == {
        "tags": 10,
        "cpu": 172_800,
        "mem": 172_800,
        "disk": 345_600,
        "net": 345_600,
    }
    assert benchbox_datagen.row_counts("tsbs_devops", 0.1) == benchbox_datagen.row_counts("tsbs_devops", 0.01)
    assert benchbox_datagen.row_counts("tsbs_devops", 1.0)["tags"] == 100
    assert benchbox_datagen.row_counts("tsbs_devops", 10.0)["disk"] == 34_560_000


def test_stream_and_parquet_schemas_equal_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "tsbs_devops_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tsbs_devops", table, 0.01)).schema
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[table], table
    for stat in benchbox_datagen.write_tables("tsbs_devops", 0.01, tmp_path, format="parquet"):
        schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[stat.table]


def test_there_are_no_integer_key_ranges():
    assert benchbox_datagen.key_ranges("tsbs_devops", 0.01) == {}


def test_delimited_files_are_csv_with_a_header_and_two_decimals(tmp_path):
    benchbox_datagen.write_tables("tsbs_devops", 0.01, tmp_path, format="tbl")
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted(f"{table}.csv" for table in TABLES)
    lines = (tmp_path / "cpu.csv").read_text().splitlines()
    assert lines[0].split(",")[:3] == ["time", "hostname", "usage_user"]
    assert len(lines) == 172_801
    fields = lines[1].split(",")
    assert fields[:2] == ["2024-01-01 00:00:00", "host_0"]
    assert all(len(field.split(".")[1]) == 2 for field in fields[2:])


def test_seed_defaults_to_42_and_changes_the_data(tmp_path):
    def disk(seed):
        directory = tmp_path / str(seed)
        benchbox_datagen.write_tables("tsbs_devops", 0.01, directory, format="tbl", seed=seed)
        return sha256(str(directory / "disk.csv"))

    assert disk(None) == disk(42)
    assert disk(43) != disk(42)


def test_files_do_not_depend_on_threads(tmp_path):
    for fmt in ("tbl", "parquet"):
        hashes = []
        for threads in (1, 4, None):
            directory = tmp_path / f"{fmt}-{threads}"
            stats = benchbox_datagen.write_tables("tsbs_devops", 0.01, directory, format=fmt, threads=threads)
            hashes.append([sha256(stat.path) for stat in stats])
        assert hashes[0] == hashes[1] == hashes[2]


def test_table_scales_equal_row_counts():
    scales = benchbox_datagen.table_scales("tsbs_devops", 0.01)
    for table, rows in benchbox_datagen.row_counts("tsbs_devops", 0.01).items():
        assert scales[table] == {"id_count": rows, "source_rows": rows}


def test_refresh_sets_are_not_available(tmp_path):
    with pytest.raises(ValueError, match="no refresh sets"):
        benchbox_datagen.write_refresh("tsbs_devops", 0.01, tmp_path)

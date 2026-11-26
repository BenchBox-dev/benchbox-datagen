import _thread
import hashlib
import json
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = benchbox_datagen.table_names("tpch")
ROW_GROUP_ROWS = 122_880


def schema_of(table: str) -> list[list]:
    schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpch", table, 0.01)).schema
    return [[field.name, str(field.type), field.nullable] for field in schema]


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_stream_schemas_equal_the_benchbox_converter_schemas():
    expected = json.loads((DATA / "tpch_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        assert schema_of(table) == expected[table], table


def test_parquet_files_carry_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "tpch_arrow_schemas.json").read_text())
    for stat in benchbox_datagen.write_tables("tpch", 0.01, tmp_path, format="parquet"):
        schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[stat.table]


def test_row_counts_cover_every_table():
    counts = benchbox_datagen.row_counts("tpch", 0.01)
    assert list(counts) == TABLES
    assert counts == {
        "nation": 25,
        "region": 5,
        "part": 2_000,
        "supplier": 100,
        "partsupp": 8_000,
        "customer": 1_500,
        "orders": 15_000,
        "lineitem": 60_175,
    }
    assert benchbox_datagen.row_counts("tpch", 1.0)["lineitem"] == 6_001_215
    for table, rows in counts.items():
        stream = benchbox_datagen.generate_table("tpch", table, 0.01)
        assert pa.RecordBatchReader.from_stream(stream).read_all().num_rows == rows


def test_row_counts_reject_invalid_requests():
    with pytest.raises(ValueError, match="unknown benchmark"):
        benchbox_datagen.row_counts("tpcx", 1.0)
    with pytest.raises(ValueError, match="scale factor"):
        benchbox_datagen.row_counts("tpch", 0.0)


def test_delimited_output_matches_the_committed_dbgen_hashes(tmp_path):
    golden = json.loads((DATA / "tpch_dbgen_sha256.json").read_text())
    for scale, expected in golden.items():
        out = tmp_path / scale
        stats = benchbox_datagen.write_tables("tpch", float(scale), out, format="tbl")
        assert {stat.table: sha256(stat.path) for stat in stats} == expected, scale


def test_delimited_files_have_no_trailing_separator_and_report_statistics(tmp_path):
    stats = benchbox_datagen.write_tables("tpch", 0.01, tmp_path, format="tbl", threads=2)
    assert [stat.table for stat in stats] == TABLES
    counts = benchbox_datagen.row_counts("tpch", 0.01)
    for stat in stats:
        path = Path(stat.path)
        assert path == tmp_path / f"{stat.table}.tbl"
        data = path.read_bytes()
        assert stat.rows == counts[stat.table] == data.count(b"\n")
        assert stat.bytes == len(data)
        assert b"|\n" not in data
    assert (tmp_path / "region.tbl").read_text().splitlines()[0].startswith("0|AFRICA|")


def test_delimited_output_is_independent_of_threads(tmp_path):
    one = benchbox_datagen.write_tables("tpch", 0.01, tmp_path / "one", format="tbl", threads=1)
    many = benchbox_datagen.write_tables("tpch", 0.01, tmp_path / "many", format="tbl", threads=5)
    assert [sha256(s.path) for s in one] == [sha256(s.path) for s in many]


def test_parquet_output_is_independent_of_threads(tmp_path):
    runs = [
        benchbox_datagen.write_tables("tpch", 0.1, tmp_path / str(threads), threads=threads)
        for threads in (1, 4, None)
    ]
    hashes = [[sha256(s.path) for s in run] for run in runs]
    assert hashes[0] == hashes[1] == hashes[2]


def test_parquet_layout_and_writer_identity(tmp_path):
    stats = {s.table: s for s in benchbox_datagen.write_tables("tpch", 0.1, tmp_path)}
    lineitem = pq.ParquetFile(stats["lineitem"].path)
    groups = [lineitem.metadata.row_group(i).num_rows for i in range(lineitem.num_row_groups)]
    assert groups[:-1] == [ROW_GROUP_ROWS] * (len(groups) - 1)
    assert sum(groups) == stats["lineitem"].rows == benchbox_datagen.row_counts("tpch", 0.1)["lineitem"]
    assert lineitem.metadata.metadata[b"benchbox.parquet_writer"] == b"benchbox-datagen-0"
    assert lineitem.metadata.row_group(0).column(0).compression == "SNAPPY"
    assert stats["lineitem"].bytes == Path(stats["lineitem"].path).stat().st_size


@pytest.mark.parametrize(("name", "codec"), [("none", "UNCOMPRESSED"), ("snappy", "SNAPPY"), ("gzip", "GZIP"), ("zstd", "ZSTD")])
def test_parquet_compression_options(tmp_path, name, codec):
    stats = benchbox_datagen.write_tables("tpch", 0.01, tmp_path, compression=name)
    metadata = pq.ParquetFile(stats[0].path).metadata
    assert metadata.row_group(0).column(0).compression == codec
    assert pq.read_table(stats[-1].path).num_rows == 60_175


def test_parquet_values_equal_the_arrow_stream(tmp_path):
    stats = benchbox_datagen.write_tables("tpch", 0.01, tmp_path, compression="zstd")
    for stat in stats:
        stream = benchbox_datagen.generate_table("tpch", stat.table, 0.01)
        expected = pa.RecordBatchReader.from_stream(stream).read_all()
        assert pq.read_table(stat.path).equals(expected), stat.table


def test_parts_write_one_file_per_part_except_nation_and_region(tmp_path):
    stats = benchbox_datagen.write_tables("tpch", 0.01, tmp_path, format="tbl", parts=3, threads=2)
    counts = benchbox_datagen.row_counts("tpch", 0.01)
    for table in TABLES:
        files = [s for s in stats if s.table == table]
        assert len(files) == (1 if table in ("nation", "region") else 3)
        assert sum(s.rows for s in files) == counts[table]
    assert (tmp_path / "lineitem.tbl.2").exists()
    whole = benchbox_datagen.write_tables("tpch", 0.01, tmp_path / "whole", format="tbl")
    joined = b"".join((tmp_path / f"lineitem.tbl.{p}").read_bytes() for p in (1, 2, 3))
    assert joined == next(Path(s.path).read_bytes() for s in whole if s.table == "lineitem")


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"format": "csv"}, ValueError, "unknown output format"),
        ({"format": "tbl", "compression": "zstd"}, ValueError, "does not support"),
        ({"compression": "lz4"}, ValueError, "unknown compression"),
        ({"threads": 0}, ValueError, "threads must be"),
        ({"parts": 0}, ValueError, "parts must be"),
    ],
)
def test_write_tables_rejects_invalid_options(tmp_path, kwargs, error, message):
    with pytest.raises(error, match=message):
        benchbox_datagen.write_tables("tpch", 0.01, tmp_path, **kwargs)


def test_write_tables_reports_unwritable_destinations_as_os_errors(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    with pytest.raises(OSError):
        benchbox_datagen.write_tables("tpch", 0.01, blocker / "out")


def test_a_keyboard_interrupt_stops_a_long_write_and_removes_partial_files(tmp_path):
    timer = threading.Timer(0.3, _thread.interrupt_main)
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            benchbox_datagen.write_tables("tpch", 3.0, tmp_path, format="tbl", threads=1)
    finally:
        timer.cancel()
    assert list(tmp_path.iterdir()) == []

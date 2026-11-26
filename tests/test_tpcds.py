import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = benchbox_datagen.table_names("tpcds")
SF1_ROWS = {
    "call_center": 6,
    "catalog_page": 11_718,
    "catalog_returns": 144_067,
    "catalog_sales": 1_441_548,
    "customer": 100_000,
    "customer_address": 50_000,
    "customer_demographics": 1_920_800,
    "date_dim": 73_049,
    "household_demographics": 7_200,
    "income_band": 20,
    "inventory": 11_745_000,
    "item": 18_000,
    "promotion": 300,
    "reason": 75,
    "ship_mode": 20,
    "store": 12,
    "store_returns": 287_514,
    "store_sales": 2_880_404,
    "time_dim": 86_400,
    "warehouse": 5,
    "web_page": 60,
    "web_returns": 71_763,
    "web_sales": 719_384,
    "web_site": 30,
}


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_tpcds_lists_the_24_benchmark_tables_without_dbgen_version():
    assert len(TABLES) == 24
    assert "dbgen_version" not in TABLES
    with pytest.raises(ValueError, match="unknown tpcds table"):
        benchbox_datagen.generate_table("tpcds", "dbgen_version", 1.0)


def test_stream_schemas_equal_the_benchbox_converter_schemas():
    expected = json.loads((DATA / "tpcds_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpcds", table, 1.0)).schema
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[table], table


@pytest.mark.parametrize("scale", [0.01, 0.5, 0.999])
def test_fractional_scale_factors_are_refused_for_generation(scale, tmp_path):
    with pytest.raises(ValueError, match="generation requires scale factor >= 1"):
        benchbox_datagen.generate_table("tpcds", "item", scale)
    with pytest.raises(ValueError, match="generation requires scale factor >= 1"):
        benchbox_datagen.write_tables("tpcds", scale, tmp_path)


def test_row_counts_equal_the_published_scale_factor_one_cardinalities():
    counts = benchbox_datagen.row_counts("tpcds", 1.0)
    assert list(counts) == TABLES
    assert counts == SF1_ROWS


def test_row_counts_scale_with_the_scale_factor():
    counts = benchbox_datagen.row_counts("tpcds", 10.0)
    assert counts["store_sales"] == 28_800_991
    assert counts["customer"] == 500_000
    assert counts["inventory"] == 133_110_000


def test_key_ranges_equal_the_observed_key_ranges_at_scale_factor_one():
    ranges = benchbox_datagen.key_ranges("tpcds", 1.0)
    assert ranges["item"]["i_item_sk"] == (1, 18_000)
    for table, columns in ranges.items():
        data = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpcds", table, 1.0)).read_all()
        for column, (low, high) in columns.items():
            observed = pc.min_max(data[column]).as_py()
            assert (observed["min"], observed["max"]) == (low, high), f"{table}.{column}"


def test_tpch_key_ranges_equal_the_observed_key_ranges():
    ranges = benchbox_datagen.key_ranges("tpch", 0.01)
    assert set(ranges) <= set(benchbox_datagen.table_names("tpch"))
    for table, columns in ranges.items():
        data = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpch", table, 0.01)).read_all()
        for column, (low, high) in columns.items():
            observed = pc.min_max(data[column]).as_py()
            assert (observed["min"], observed["max"]) == (low, high), f"{table}.{column}"


def test_delimited_output_is_independent_of_threads_and_has_no_trailing_separator(tmp_path):
    one = benchbox_datagen.write_tables("tpcds", 1.0, tmp_path / "one", format="tbl", threads=1)
    many = benchbox_datagen.write_tables("tpcds", 1.0, tmp_path / "many", format="tbl", threads=7)
    assert [s.table for s in one] == TABLES
    assert [sha256(s.path) for s in one] == [sha256(s.path) for s in many]
    for stat in one:
        assert stat.rows == SF1_ROWS[stat.table], stat.table
        assert stat.bytes == Path(stat.path).stat().st_size
        assert Path(stat.path).name == f"{stat.table}.tbl"
    first_line = Path(one[0].path).read_text().splitlines()[0]
    assert not first_line.endswith("|") or first_line.endswith("||")


def test_parquet_output_is_independent_of_threads_and_matches_the_stream(tmp_path):
    runs = [
        benchbox_datagen.write_tables("tpcds", 1.0, tmp_path / str(threads), threads=threads)
        for threads in (1, 4, None)
    ]
    hashes = [[sha256(s.path) for s in run] for run in runs]
    assert hashes[0] == hashes[1] == hashes[2]
    for stat in runs[0]:
        assert stat.rows == SF1_ROWS[stat.table]
        assert pq.ParquetFile(stat.path).metadata.metadata[b"benchbox.parquet_writer"] == b"benchbox-datagen-0"
    store = next(s for s in runs[0] if s.table == "store")
    expected = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpcds", "store", 1.0)).read_all()
    assert pq.read_table(store.path).equals(expected)


def test_parts_split_only_tables_with_a_million_or_more_source_rows(tmp_path):
    stats = benchbox_datagen.write_tables("tpcds", 1.0, tmp_path, format="tbl", parts=3, threads=2)
    for table in TABLES:
        files = [s for s in stats if s.table == table]
        assert len(files) == (3 if table in ("customer_demographics", "inventory") else 1), table
        assert sum(s.rows for s in files) == SF1_ROWS[table], table
    whole = benchbox_datagen.write_tables("tpcds", 1.0, tmp_path / "whole", format="tbl")
    parts = [tmp_path / f"inventory.tbl.{part}" for part in (1, 2, 3)]
    joined = b"".join(path.read_bytes() for path in parts)
    assert joined == next(Path(s.path).read_bytes() for s in whole if s.table == "inventory")


def test_delimited_output_matches_the_committed_dsdgen_hashes(tmp_path):
    golden = json.loads((DATA / "tpcds_dsdgen_sha256.json").read_text())
    for scale, expected in golden.items():
        stats = benchbox_datagen.write_tables("tpcds", float(scale), tmp_path / scale, format="tbl")
        assert {stat.table: sha256(stat.path) for stat in stats} == expected, scale


GATE = json.loads((DATA / "tpcds_rowcount_gate.json").read_text())


@pytest.mark.parametrize("scale", list(GATE))
def test_dsqgen_rowcount_inputs_equal_the_patched_dsdgen_values(scale):
    scales = benchbox_datagen.table_scales("tpcds", float(scale))
    expected = GATE[scale]
    assert scales["store"]["id_count"] == expected["store"]
    assert scales["call_center"]["id_count"] == expected["call_center"]
    assert scales["reason"]["source_rows"] == expected["reason"]
    assert scales["customer"]["source_rows"] == expected["customer"]
    for table in ("store_sales", "web_sales", "catalog_sales"):
        assert scales[table]["source_rows"] == expected[table], table


def test_subscale_row_counts_use_the_proportional_model_with_a_one_row_floor():
    counts = benchbox_datagen.row_counts("tpcds", 0.01)
    assert counts["store"] == counts["call_center"] == counts["warehouse"] == 1
    assert counts["customer_demographics"] == 19_208
    assert counts["date_dim"] == 73_049
    assert counts["store_sales"] == 28_810
    assert counts["inventory"] == 23_490
    assert benchbox_datagen.key_ranges("tpcds", 0.01)["customer"]["c_customer_sk"] == (1, 1_000)



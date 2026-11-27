import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"
TABLES = ["dim_locations", "dim_products", "order_lines"]


def test_table_names_and_row_counts():
    assert benchbox_datagen.table_names("coffeeshop") == TABLES
    assert benchbox_datagen.row_counts("coffeeshop", 0.01) == {
        "dim_locations": 1_000,
        "dim_products": 26,
        "order_lines": 132_600,
    }
    assert benchbox_datagen.row_counts("coffeeshop", 1.0)["order_lines"] == 13_260_000
    assert benchbox_datagen.row_counts("coffeeshop", 10.0)["order_lines"] == 132_600_000
    assert benchbox_datagen.row_counts("coffeeshop", 1e-9)["order_lines"] == 1


def test_stream_and_parquet_schemas_equal_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "coffeeshop_arrow_schemas.json").read_text())
    assert sorted(expected) == sorted(TABLES)
    for table in TABLES:
        schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("coffeeshop", table, 0.01)).schema
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[table], table
    for stat in benchbox_datagen.write_tables("coffeeshop", 0.01, tmp_path, format="parquet"):
        schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in schema] == expected[stat.table]


def test_key_ranges_bound_every_foreign_key():
    ranges = benchbox_datagen.key_ranges("coffeeshop", 0.01)
    assert ranges["dim_locations"]["record_id"] == ranges["order_lines"]["location_record_id"] == (1, 1_000)
    assert ranges["dim_products"]["record_id"] == ranges["order_lines"]["product_record_id"] == (1, 26)
    for table, rows in benchbox_datagen.row_counts("coffeeshop", 0.01).items():
        batches = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("coffeeshop", table, 0.01))
        assert batches.read_all().num_rows == rows


def test_delimited_files_are_comma_separated_csv_without_a_header(tmp_path):
    stats = benchbox_datagen.write_tables("coffeeshop", 0.001, tmp_path, format="tbl")
    assert sorted(Path(stat.path).name for stat in stats) == [f"{table}.csv" for table in sorted(TABLES)]
    first = (tmp_path / "order_lines.csv").read_text().splitlines()[0]
    assert first == "1,1,994,HOUTX249,1,1,2023-01-01,06:45:00,1,4.37,4.37,South"
    assert (tmp_path / "dim_products.csv").read_text().splitlines()[0] == "1,1,Latte,Hot,Coffee,2.00,4.50,2022-01-01,2023-02-28"


def test_the_seed_is_accepted_and_does_not_change_the_data(tmp_path):
    hashes = []
    for seed in (None, 42, 7):
        directory = tmp_path / str(seed)
        benchbox_datagen.write_tables("coffeeshop", 0.001, directory, format="tbl", seed=seed)
        hashes.append((directory / "order_lines.csv").read_bytes())
    assert hashes[0] == hashes[1] == hashes[2]


def test_files_do_not_depend_on_threads(tmp_path):
    for fmt in ("tbl", "parquet"):
        outputs = []
        for threads in (1, 4, None):
            directory = tmp_path / f"{fmt}-{threads}"
            stats = benchbox_datagen.write_tables("coffeeshop", 0.01, directory, format=fmt, threads=threads)
            outputs.append([Path(stat.path).read_bytes() for stat in stats])
        assert outputs[0] == outputs[1] == outputs[2]


def test_coffeeshop_table_scales_equal_row_counts():
    scales = benchbox_datagen.table_scales("coffeeshop", 0.01)
    counts = dict(benchbox_datagen.row_counts("coffeeshop", 0.01))
    assert set(scales) == set(counts)
    for table, rows in counts.items():
        assert scales[table] == {"id_count": rows, "source_rows": rows}


def test_coffeeshop_refresh_sets_are_not_available(tmp_path):
    with pytest.raises(ValueError, match="no refresh sets"):
        benchbox_datagen.write_refresh("coffeeshop", 0.01, tmp_path)

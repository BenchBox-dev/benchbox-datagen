import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"


def sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_table_names_and_row_counts():
    assert benchbox_datagen.table_names("clickbench") == ["hits"]
    assert benchbox_datagen.row_counts("clickbench", 0.01) == {"hits": 10_000}
    assert benchbox_datagen.row_counts("clickbench", 10.0) == {"hits": 10_000_000}
    assert benchbox_datagen.table_scales("clickbench", 0.01) == {"hits": {"id_count": 10_000, "source_rows": 10_000}}
    assert benchbox_datagen.key_ranges("clickbench", 0.01) == {}


def test_stream_and_parquet_schemas_equal_the_benchbox_converter_schemas(tmp_path):
    expected = json.loads((DATA / "clickbench_arrow_schemas.json").read_text())
    schema = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("clickbench", "hits", 0.01)).schema
    assert [[f.name, str(f.type), f.nullable] for f in schema] == expected["hits"]
    for stat in benchbox_datagen.write_tables("clickbench", 0.01, tmp_path, format="parquet"):
        parquet_schema = pq.read_schema(stat.path)
        assert [[f.name, str(f.type), f.nullable] for f in parquet_schema] == expected["hits"]


def test_delimited_file_is_the_legacy_csv_name_and_shape(tmp_path):
    stats = benchbox_datagen.write_tables("clickbench", 0.01, tmp_path, format="tbl")
    assert [Path(stat.path).name for stat in stats] == ["hits.csv"]
    first = Path(stats[0].path).read_text().splitlines()[0].split("|")
    assert len(first) == 105 and first[3] == "1" and first[25] == '""'


def test_seed_defaults_to_42_and_changes_the_data(tmp_path):
    def hits(seed):
        directory = tmp_path / str(seed)
        benchbox_datagen.write_tables("clickbench", 0.01, directory, format="parquet", seed=seed)
        return sha256(str(directory / "hits.parquet"))

    assert hits(None) == hits(42)
    assert hits(43) != hits(42)


def test_files_do_not_depend_on_threads(tmp_path):
    for fmt in ("tbl", "parquet"):
        hashes = []
        for threads in (1, 4, None):
            stats = benchbox_datagen.write_tables(
                "clickbench", 0.1, tmp_path / f"{fmt}-{threads}", format=fmt, threads=threads
            )
            hashes.append([sha256(stat.path) for stat in stats])
        assert hashes[0] == hashes[1] == hashes[2]


def test_refresh_sets_are_not_available(tmp_path):
    with pytest.raises(ValueError, match="no refresh sets"):
        benchbox_datagen.write_refresh("clickbench", 0.01, tmp_path)

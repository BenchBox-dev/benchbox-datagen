import pyarrow as pa
import pytest

import benchbox_datagen


def test_generate_table_streams_arrow_batches_through_the_pycapsule_interface():
    reader = pa.RecordBatchReader.from_stream(benchbox_datagen.generate_table("tpch", "nation", 1.0))
    table = reader.read_all()
    assert table.num_rows == 25
    assert table.column_names == ["n_nationkey", "n_name", "n_regionkey", "n_comment"]


@pytest.mark.parametrize("table", benchbox_datagen.table_names("tpch"))
def test_parts_cover_each_table_exactly_once(table):
    def rows(part: int, parts: int) -> int:
        return pa.RecordBatchReader.from_stream(
            benchbox_datagen.generate_table("tpch", table, 0.01, part, parts)
        ).read_all().num_rows

    assert sum(rows(part, 3) for part in (1, 2, 3)) == rows(1, 1)


def test_the_returned_object_exposes_only_the_arrow_stream_protocol():
    stream = benchbox_datagen.generate_table("tpch", "region", 1.0)
    assert hasattr(stream, "__arrow_c_stream__")
    assert not hasattr(stream, "read_all")
    assert not hasattr(stream, "__next__")


def test_table_names_lists_every_tpch_table():
    assert benchbox_datagen.table_names("tpch") == [
        "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
    ]


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (("tpcx", "nation", 1.0), "unknown benchmark"),
        (("tpch", "nope", 1.0), "unknown tpch table"),
        (("tpch", "nation", 0.0), "scale factor"),
        (("tpch", "nation", 1.0, 2, 1), "part must be"),
    ],
)
def test_invalid_requests_raise_value_error(args, message):
    with pytest.raises(ValueError, match=message):
        benchbox_datagen.generate_table(*args)

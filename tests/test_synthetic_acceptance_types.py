import dataclasses
import json
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

duckdb = pytest.importorskip("duckdb")

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

import synthetic_acceptance as acceptance
from synthetic_configs.base import EXTENDED_SEEDS, SEEDS, SyntheticConfig

SCHEMA = [
    ["id", "int64", False],
    ["i32", "int32", True],
    ["f", "float", True],
    ["d", "double", True],
    ["flag", "bool", True],
    ["day", "date32[day]", True],
    ["ts", "timestamp[us]", True],
    ["price", "decimal128(15, 2)", True],
    ["s", "string", True],
    ["v", "fixed_size_list<item: float>[3]", True],
]

CSV = "\n".join(
    [
        "id,i32,f,d,flag,day,ts,price,s,v",
        '1,10,0.5,0.1,true,1992-01-01,2013-07-01 00:00:00,1.50,a,"[1,2,3]"',
        '2,20,1.5,0.2,false,1992-01-02,2013-07-01 00:00:01,2.25,bb,"[0.5,0.25,0.125]"',
        '3,,2.5,0.3,true,1992-01-03,2013-07-01 00:00:01.500000,3.00,ccc,"[1e-05,0,1]"',
        '4,40,,0.4,,1992-01-04,,0.05,,',
    ]
)


def library_table() -> pa.Table:
    item = pa.field("item", pa.float32())
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int64()),
            "i32": pa.array([10, 20, None, 40], pa.int32()),
            "f": pa.array([0.5, 1.5, 2.5, None], pa.float32()),
            "d": pa.array([0.1, 0.2, 0.3, 0.4], pa.float64()),
            "flag": pa.array([True, False, True, None]),
            "day": pa.array([8035, 8036, 8037, 8038], pa.date32()),
            "ts": pa.array([1372636800000000, 1372636801000000, 1372636801500000, None], pa.timestamp("us")),
            "price": pa.array([Decimal("1.50"), Decimal("2.25"), Decimal("3.00"), Decimal("0.05")], pa.decimal128(15, 2)),
            "s": pa.array(["a", "bb", "ccc", None]),
            "v": pa.array([[1, 2, 3], [0.5, 0.25, 0.125], [1e-5, 0, 1], None], pa.list_(item, 3)),
        }
    )


@pytest.fixture
def fixture(tmp_path):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps({"t": SCHEMA}))
    config = SyntheticConfig(
        name="fixture",
        tables=["t"],
        primary_keys={"t": ["id"]},
        foreign_keys=[],
        expected_rows=lambda scale: {"t": 4},
        queries={},
        legacy_generate=lambda scale, seed, out: None,
        benchbox_schema=lambda: {},
        budgets={},
        legacy_suffix="csv",
        legacy_delimiter=",",
        legacy_header=True,
        schema_path=schema_path,
    )
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    (legacy / "t.csv").write_text(CSV + "\n")
    library = tmp_path / "library"
    library.mkdir()
    table = library_table()
    pq.write_table(table, library / "t.parquet")
    acceptance.CONFIG = config
    yield config, legacy, library
    acceptance.CONFIG = None


def test_every_arrow_kind_maps_to_a_duckdb_type(fixture):
    types = {name: acceptance.tbl_type(kind) for name, kind, _ in SCHEMA}
    assert types == {
        "id": "BIGINT",
        "i32": "INTEGER",
        "f": "FLOAT",
        "d": "DOUBLE",
        "flag": "BOOLEAN",
        "day": "DATE",
        "ts": "TIMESTAMP",
        "price": "DECIMAL(15,2)",
        "s": "VARCHAR",
        "v": "FLOAT[3]",
    }
    assert acceptance.tbl_type("date32") == "DATE"
    with pytest.raises(ValueError, match="unsupported Arrow type"):
        acceptance.tbl_type("uint8")


def test_legacy_csv_and_library_parquet_give_identical_statistics(fixture):
    config, legacy_dir, library_dir = fixture
    con = duckdb.connect()
    legacy = acceptance.table_stats(con, acceptance.legacy_sources(legacy_dir), legacy=True)
    library = acceptance.table_stats(con, acceptance.parquet_sources(library_dir))
    assert legacy["t"]["rows"] == library["t"]["rows"] == 4
    findings = acceptance.compare_stats({"runs": [{"tables": legacy}]}, {"tables": library})
    assert findings
    assert {finding["verdict"] for finding in findings} == {"ok"}


def test_statistics_are_defined_for_every_kind(fixture):
    config, legacy_dir, _ = fixture
    columns = acceptance.table_stats(duckdb.connect(), acceptance.legacy_sources(legacy_dir), legacy=True)["t"]["columns"]
    assert set(columns) == {name for name, _, _ in SCHEMA}
    assert columns["flag"]["mean"] == pytest.approx(2 / 3)
    assert columns["flag"]["null_rate"] == pytest.approx(0.25)
    assert columns["day"]["mean"] == pytest.approx((8035 + 1.5) * 86_400 * 1_000_000)
    assert columns["day"]["p1"] == 8035 * 86_400 * 1_000_000
    assert columns["ts"]["p99"] == 1372636801500000
    assert columns["price"]["mean"] == pytest.approx((1.5 + 2.25 + 3 + 0.05) / 4)
    assert columns["f"]["mean"] == pytest.approx(1.5)
    assert columns["d"]["distinct"] == 4
    assert columns["i32"]["null_rate"] == pytest.approx(0.25)
    assert columns["v"]["null_rate"] == pytest.approx(0.25)
    assert columns["v"]["distinct"] == 3
    assert columns["v"]["p50"] == pytest.approx(0.4714021638)
    assert columns["s"]["text"] and columns["s"]["values"] == ["a", "bb", "ccc", None]


def test_legacy_sources_follow_the_config(fixture):
    config, legacy_dir, _ = fixture
    source = acceptance.legacy_sources(legacy_dir)["t"]
    assert "t.csv" in source and "delim=','" in source and "header=true" in source


def test_seed_counts_are_fixed():
    assert SEEDS == [42, 43, 44, 45, 46]
    assert EXTENDED_SEEDS == list(range(42, 72))
    assert EXTENDED_SEEDS[: len(SEEDS)] == SEEDS


def test_legacy_nullstr_distinguishes_empty_strings_from_nulls(fixture):
    config, legacy_dir, _ = fixture
    (legacy_dir / "t.csv").write_text("id,s\n1,\"\"\n2,__NULL__\n3,x\n")
    schema = dataclasses.replace(config, schema_path=legacy_dir / "s.json")
    (legacy_dir / "s.json").write_text(json.dumps({"t": [["id", "int64", False], ["s", "string", True]]}))
    acceptance.CONFIG = schema
    con = duckdb.connect()
    default = con.sql(f"select s from {acceptance.legacy_sources(legacy_dir)['t']} order by id").fetchall()
    assert default == [(None,), ("__NULL__",), ("x",)]
    acceptance.CONFIG = dataclasses.replace(schema, legacy_nullstr="__NULL__")
    source = acceptance.legacy_sources(legacy_dir)["t"]
    assert "nullstr='__NULL__'" in source
    assert con.sql(f"select s from {source} order by id").fetchall() == [("",), (None,), ("x",)]


def test_value_comparison_tolerates_engine_arithmetic_types():
    assert acceptance.values_match(47.517606900000004, Decimal("47.517606"))
    assert acceptance.values_match(Decimal("46.38"), 46.38)
    assert acceptance.values_match(0.0, 1e-10)
    assert not acceptance.values_match(1, 2)
    assert not acceptance.values_match("a", "b")
    duck = [(2, 46.38), (1, 47.517606900000004)]
    other = [(1, Decimal("47.517606")), (2, Decimal("46.38"))]
    assert acceptance.first_difference(duck, other) is None
    assert acceptance.first_difference(duck, [(1, Decimal("47.6")), (2, Decimal("46.38"))]) == {
        "row": 0,
        "column": 1,
        "duckdb": "47.517606900000004",
        "datafusion": "Decimal('47.6')",
    }
    assert acceptance.first_difference(duck, other[:1])["row"] is None


def test_row_count_is_the_gate_and_values_are_reported_separately(fixture):
    pytest.importorskip("datafusion")
    config, _, library_dir = fixture
    pq.write_table(
        pa.table({"id": pa.array([1, 2, 3], pa.int64()), "price": pa.array([Decimal("46.38"), Decimal("47.52"), Decimal("48.99")], pa.decimal128(15, 2))}),
        library_dir / "t.parquet",
    )
    queries = {
        "avg": "SELECT avg(price) FROM t",
        "median": "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY price) FROM t",
        "rows": "SELECT id FROM t",
    }
    acceptance.CONFIG = dataclasses.replace(config, queries=lambda: queries)
    outcomes = acceptance.run_queries(library_dir)
    assert all(outcome["agree"] for outcome in outcomes.values())
    assert all(outcome["values_agree"] for outcome in outcomes.values())
    assert outcomes["rows"]["first_difference"] is None

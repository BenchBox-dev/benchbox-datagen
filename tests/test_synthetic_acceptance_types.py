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


def test_cdf_of_a_nullable_column_is_normalised_by_the_non_null_count(fixture):
    _, legacy_dir, library_dir = fixture
    con = duckdb.connect()
    for sources in (acceptance.legacy_sources(legacy_dir), acceptance.parquet_sources(library_dir)):
        columns = acceptance.table_stats(con, sources)["t"]["columns"]
        assert columns["i32"]["null_rate"] == pytest.approx(0.25)
        assert columns["i32"]["cdf"][-1][1] == pytest.approx(1.0)
        assert [fraction for _, fraction in columns["i32"]["cdf"]] == pytest.approx([1 / 3, 2 / 3, 1.0])


def test_a_rank_check_on_a_nullable_column_uses_the_non_null_distribution():
    cdf = [[10.0, 1 / 3], [20.0, 2 / 3], [40.0, 1.0]]
    assert acceptance.rank_ok(0.5, [20.0], cdf, 1_000_000)
    assert not acceptance.rank_ok(0.9, [20.0], cdf, 1_000_000)


def test_null_foreign_keys_are_not_orphans(tmp_path):
    con = duckdb.connect()
    con.execute("CREATE TABLE child AS SELECT * FROM (VALUES (1), (NULL), (9), (NULL)) t(parent_id)")
    con.execute("CREATE TABLE parent AS SELECT * FROM (VALUES (1), (2)) t(id)")
    assert acceptance.orphan_count(con, "child", "parent_id", "parent", "id") == 1
    con.execute("DELETE FROM child WHERE parent_id = 9")
    assert acceptance.orphan_count(con, "child", "parent_id", "parent", "id") == 0


def test_statistics_in_column_batches_equal_statistics_in_one_query(fixture, monkeypatch):
    _, legacy_dir, _ = fixture
    sources = acceptance.legacy_sources(legacy_dir)
    results = []
    for batch in (1, 3, 100):
        monkeypatch.setattr(acceptance, "COLUMN_BATCH", batch)
        results.append(acceptance.table_stats(duckdb.connect(), sources, legacy=True))
    assert results[0] == results[1] == results[2]


def test_the_statistics_memory_limit_is_a_config_field_defaulting_to_two_gigabytes(fixture):
    config, _, _ = fixture
    assert config.stats_memory_limit == "2GB"
    assert dataclasses.replace(config, stats_memory_limit="6GB").stats_memory_limit == "6GB"


def test_a_constant_statistic_tolerates_summation_noise_only_when_it_is_not_integer_valued():
    constant = [0.3] * 5
    assert acceptance.judge(0.3 * (1 + 1.7e-12), constant, "mean") == "ok"
    assert acceptance.judge(0.3 * (1 + 1.7e-12), constant, "p50") == "ok"
    assert acceptance.judge(0.3 * (1 + 1e-6), constant, "mean") == "FAIL"
    assert acceptance.judge(5.0, [5.0] * 5, "p50") == "ok"
    assert acceptance.judge(5.0 * (1 + 1e-11), [5.0] * 5, "p50") == "FAIL"
    assert acceptance.judge(5.0 * (1 + 1e-11), [5.0] * 5, "mean") == "ok"
    assert acceptance.judge(0.0, [0.0] * 5, "null_rate") == "ok"
    assert acceptance.judge(1e-15, [0.0] * 5, "null_rate") == "FAIL"


def test_values_match_treats_nan_on_both_engines_as_agreement():
    nan = float("nan")
    assert acceptance.values_match(nan, nan)
    assert acceptance.values_match(Decimal("1.5"), 1.5)
    assert not acceptance.values_match(nan, 1.0)
    assert acceptance.first_difference([(nan, 1)], [(nan, 1)]) is None


def test_mann_whitney_matches_known_values():
    assert acceptance.mann_whitney_p([1, 2, 3, 4, 5], [6, 7, 8, 9, 10]) == pytest.approx(0.01218578, abs=1e-7)
    assert acceptance.mann_whitney_p([1, 2, 3, 4, 5], [6, 7, 8, 9, 10]) == acceptance.mann_whitney_p(
        [6, 7, 8, 9, 10], [1, 2, 3, 4, 5]
    )
    assert acceptance.mann_whitney_p([1, 2, 3, 4], [1, 2, 3, 4]) == 1.0
    assert acceptance.mann_whitney_p([2.0] * 10, [2.0] * 10) == 1.0
    first = [1.2, 3.4, 3.4, 5.6, 7.8, 7.8, 7.8, 9.0, 2.2, 4.4]
    second = [3.4, 5.6, 5.6, 8.8, 9.0, 9.9, 10.1, 10.1, 6.6, 7.0, 12.0]
    assert acceptance.mann_whitney_p(first, second) == pytest.approx(0.04022172, abs=1e-7)
    assert acceptance.mann_whitney_p([1, 1, 2, 2, 3], [1, 2, 2, 3, 3]) == pytest.approx(0.50672871, abs=1e-7)


def rejudge_run(seed: int, value: float) -> dict:
    entry = {"text": False, "null_rate": 0.0, "distinct_ratio": 1.0, "distinct": 1, "mean": value}
    entry.update({metric: value for metric in ("p1", "p25", "p50", "p75", "p99")})
    return {"seed": seed, "tables": {"t": {"rows": 4, "columns": {"id": entry}}}}


def rejudged_mean(library_values: list[float]) -> tuple[str, acceptance.Rejudge]:
    tight = [100.0, 100.01, 100.02, 100.01, 100.0]
    spread = [100.0 + 2 * i / 24 for i in range(25)]
    legacy = [rejudge_run(seed, value) for seed, value in zip(EXTENDED_SEEDS, tight + spread)]
    library = [rejudge_run(seed, value) for seed, value in zip(EXTENDED_SEEDS, library_values)]
    rejudge = acceptance.Rejudge(legacy, library)
    findings = acceptance.compare_stats({"runs": legacy[:5]}, library[0], rejudge)
    return next(f["verdict"] for f in findings if f["metric"] == "mean"), rejudge


def test_a_metric_failing_five_seeds_passes_when_thirty_seeds_do_not_differ(fixture):
    chance = [101.0] + [100.0 + 2 * i / 29 for i in range(1, 30)]
    verdict, rejudge = rejudged_mean(chance)
    assert verdict == "mw"
    result = rejudge.judge("t", "id", "mean")
    assert result["ok"] and result["p_value"] >= 0.001
    assert result["legacy"]["max"] == pytest.approx(102.0) and result["library"]["min"] == pytest.approx(100.0 + 2 / 29)


def test_a_metric_failing_five_seeds_fails_when_thirty_seeds_differ(fixture):
    verdict, rejudge = rejudged_mean([105.0 + i / 100 for i in range(30)])
    assert verdict == "FAIL"
    assert rejudge.judge("t", "id", "mean")["p_value"] < 0.001


def test_missing_extended_seeds_stop_the_comparison(fixture):
    legacy = [rejudge_run(seed, 100.0) for seed in EXTENDED_SEEDS]
    library = [rejudge_run(seed, 105.0) for seed in EXTENDED_SEEDS[:29]]
    rejudge = acceptance.Rejudge(legacy, library)
    with pytest.raises(SystemExit, match="extended seeds"):
        acceptance.compare_stats({"runs": legacy[:5]}, rejudge_run(42, 105.0), rejudge)

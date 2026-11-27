import hashlib
import json
import os
from pathlib import Path

import pytest

import benchbox_datagen

DATA = Path(__file__).parent / "data"


def test_tpch_refresh_sets_match_the_committed_dbgen_hashes(tmp_path):
    golden = json.loads((DATA / "tpch_refresh_sf0.01_sha256.json").read_text())
    stats = benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, sets=20)
    assert len(stats) == len(golden) == 60
    for stat in stats:
        path = Path(stat.path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == golden[path.name], path.name
        assert stat.bytes == path.stat().st_size


def test_tpch_refresh_files_follow_the_dbgen_layout(tmp_path):
    stats = benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, sets=2)
    assert sorted(Path(s.path).name for s in stats) == [
        "delete.1", "delete.2", "lineitem.tbl.u1", "lineitem.tbl.u2", "orders.tbl.u1", "orders.tbl.u2",
    ]
    assert (tmp_path / "delete.1").read_text().split() == ["1", "2", "3", "4", "5", "6", "7", "32", "33", "34"]
    first = (tmp_path / "orders.tbl.u1").read_text().splitlines()[0]
    assert first.startswith("9|1385|F|179366.58|")
    assert not first.endswith("|")


def _tpcds_golden(scale):
    return json.loads((DATA / "tpcds_refresh_sha256.json").read_text())[scale]


def _check_against(golden, stats):
    assert {Path(s.path).name for s in stats} == set(golden)
    for stat in stats:
        path = Path(stat.path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == golden[path.name], path.name


def test_tpcds_refresh_sets_match_the_committed_dsdgen_hashes_at_scale_factor_one(tmp_path):
    golden = _tpcds_golden("1")
    stats = benchbox_datagen.write_refresh("tpcds", 1.0, tmp_path, sets=7)
    assert len(stats) == len(golden) == 161
    for stat in stats:
        path = Path(stat.path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == golden[path.name], path.name
        assert stat.bytes == path.stat().st_size
        assert stat.rows == len(path.read_bytes().splitlines())


def test_tpcds_refresh_sets_match_the_committed_dsdgen_hashes_below_scale_factor_one(tmp_path):
    golden = _tpcds_golden("0.01")
    stats = benchbox_datagen.write_refresh("tpcds", 0.01, tmp_path)
    assert {Path(s.path).name for s in stats} == set(golden)
    for stat in stats:
        path = Path(stat.path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == golden[path.name], path.name


@pytest.mark.skipif(
    not os.environ.get("BENCHBOX_DATAGEN_HEAVY"),
    reason="set BENCHBOX_DATAGEN_HEAVY=1 and run under make heavy-run",
)
def test_tpcds_refresh_set_matches_the_committed_dsdgen_hashes_at_scale_factor_100(tmp_path):
    golden = _tpcds_golden("100")
    stats = benchbox_datagen.write_refresh("tpcds", 100.0, tmp_path)
    assert {Path(s.path).name for s in stats} == set(golden) and len(golden) == 23
    for stat in stats:
        path = Path(stat.path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == golden[path.name], path.name


def test_tpcds_refresh_set_127_matches_the_committed_dsdgen_hashes(tmp_path):
    golden = _tpcds_golden("0.01_set127")
    stats = benchbox_datagen.write_refresh("tpcds", 0.01, tmp_path, sets=127)
    _check_against(golden, [s for s in stats if Path(s.path).name.endswith("_127.dat")])


def test_tpcds_refresh_with_a_non_default_rng_seed_matches_the_committed_dsdgen_hashes(tmp_path):
    stats = benchbox_datagen.write_refresh("tpcds", 1.0, tmp_path, sets=2, rng_seed=20260709)
    _check_against(_tpcds_golden("1_rng_seed_20260709"), stats)


def test_tpcds_refresh_writes_no_file_for_tables_without_rows_like_dsdgen(tmp_path):
    stats = benchbox_datagen.write_refresh("tpcds", 0.0001, tmp_path)
    _check_against(_tpcds_golden("0.0001"), stats)
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(_tpcds_golden("0.0001"))


@pytest.mark.parametrize("scale", [2.0, 3.0, 13.0, 301.0, 65536.0, 100001.0])
def test_tpcds_refresh_rejects_scale_factors_off_the_dsdgen_grid_and_writes_nothing(tmp_path, scale):
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000"):
        benchbox_datagen.write_refresh("tpcds", scale, out)
    assert not out.exists()


@pytest.mark.skipif(
    not os.environ.get("BENCHBOX_DATAGEN_HEAVY"),
    reason="set BENCHBOX_DATAGEN_HEAVY=1 and run under make heavy-run",
)
def test_tpcds_refresh_set_matches_the_committed_dsdgen_hashes_at_scale_factor_10(tmp_path):
    golden = _tpcds_golden("10")
    stats = benchbox_datagen.write_refresh("tpcds", 10.0, tmp_path)
    assert len(golden) == 23
    _check_against(golden, stats)


def test_tpcds_refresh_files_follow_the_dsdgen_layout(tmp_path):
    stats = benchbox_datagen.write_refresh("tpcds", 0.01, tmp_path, sets=2)
    names = sorted(Path(s.path).name for s in stats)
    assert len(names) == 46
    for n in (1, 2):
        assert f"delete_{n}.dat" in names
        assert f"inventory_delete_{n}.dat" in names
        assert f"s_purchase_lineitem_{n}.dat" in names
        assert f"s_zip_to_gmt_{n}.dat" in names
    assert (tmp_path / "delete_1.dat").read_text().splitlines()[0] == "2000-05-20|2000-05-21"


def test_tpcds_refresh_rng_seed_is_deterministic_and_changes_the_data(tmp_path):
    first, second, default = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    benchbox_datagen.write_refresh("tpcds", 0.01, first, rng_seed=20260709)
    benchbox_datagen.write_refresh("tpcds", 0.01, second, rng_seed=20260709)
    benchbox_datagen.write_refresh("tpcds", 0.01, default)
    assert (first / "s_purchase_1.dat").read_bytes() == (second / "s_purchase_1.dat").read_bytes()
    assert (first / "s_purchase_1.dat").read_bytes() != (default / "s_purchase_1.dat").read_bytes()


def test_tpch_refresh_rejects_an_rng_seed(tmp_path):
    with pytest.raises(ValueError, match="rng seed"):
        benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, rng_seed=1)


def test_refresh_rejects_invalid_requests(tmp_path):
    with pytest.raises(ValueError, match="sets must be at least 1"):
        benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, sets=0)
    with pytest.raises(ValueError, match="scale factor"):
        benchbox_datagen.write_refresh("tpch", 0.0, tmp_path)


def test_failed_refresh_leaves_no_partial_files(tmp_path):
    (tmp_path / "lineitem.tbl.u2").mkdir()
    with pytest.raises(OSError):
        benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, sets=3)
    assert [p.name for p in tmp_path.iterdir()] == ["lineitem.tbl.u2"]

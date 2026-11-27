import hashlib
import json
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


def test_tpcds_refresh_sets_are_not_available(tmp_path):
    with pytest.raises(ValueError, match="not available"):
        benchbox_datagen.write_refresh("tpcds", 0.01, tmp_path)


def test_tpch_refresh_rejects_an_rng_seed(tmp_path):
    with pytest.raises(ValueError, match="rng seed"):
        benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, rng_seed=1)


def test_refresh_rejects_invalid_requests(tmp_path):
    with pytest.raises(ValueError, match="sets must be at least 1"):
        benchbox_datagen.write_refresh("tpch", 0.01, tmp_path, sets=0)
    with pytest.raises(ValueError, match="scale factor"):
        benchbox_datagen.write_refresh("tpch", 0.0, tmp_path)

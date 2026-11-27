import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

import benchbox_datagen


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", choices=["tpch", "tpcds"], default="tpch")
    parser.add_argument("--dbgen-dir", required=True, help="directory holding the oracle refresh files (dbgen -U or dsdgen -update output)")
    parser.add_argument("--rng-seed", type=int, default=None, help="tpcds only: dsdgen -rngseed")
    parser.add_argument("--scale", type=float, required=True)
    parser.add_argument("--sets", type=int, required=True)
    parser.add_argument("--workdir", default=None)
    parser.add_argument("--golden-out")
    args = parser.parse_args()
    oracle = Path(args.dbgen_dir)
    out = Path(tempfile.mkdtemp(prefix="refresh-", dir=args.workdir))
    try:
        extra = {} if args.rng_seed is None else {"rng_seed": args.rng_seed}
        stats = benchbox_datagen.write_refresh(args.benchmark, args.scale, out, sets=args.sets, **extra)
        report = {}
        ok = True
        for stat in stats:
            name = Path(stat.path).name
            expected = sha256(oracle / name)
            actual = sha256(Path(stat.path))
            report[name] = {"sha256": actual, "rows": stat.rows, "match": expected == actual}
            ok &= expected == actual
        extra = sorted(p.name for p in oracle.glob("*.dat") if p.name not in report)
        ok &= not extra
        if args.golden_out:
            Path(args.golden_out).write_text(
                json.dumps({name: entry["sha256"] for name, entry in report.items()}, indent=1) + "\n"
            )
        print(json.dumps({"scale": args.scale, "sets": args.sets, "files": len(report), "ok": ok, "mismatches": [n for n, e in report.items() if not e["match"]], "oracle_only": extra}, indent=1))
        return 0 if ok else 1
    finally:
        shutil.rmtree(out, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

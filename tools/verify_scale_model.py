import argparse
import json
import sys
from pathlib import Path

import benchbox_datagen

SPEC = Path(__file__).resolve().parent.parent / "crates" / "benchbox-datagen-core" / "src" / "tpcds_schema.txt"


def columns() -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for line in SPEC.read_text().splitlines():
        table, name = line.split()[:2]
        result.setdefault(table, []).append(name)
    return result


def observed(path: Path, names: list[str], wanted: list[str]) -> dict[str, tuple[int, int]]:
    indexes = {name: names.index(name) for name in wanted}
    low = {name: None for name in wanted}
    high = {name: None for name in wanted}
    rows = 0
    with path.open() as handle:
        for line in handle:
            rows += 1
            fields = line.rstrip("\n").split("|")
            for name, index in indexes.items():
                if index < len(fields) and fields[index] != "":
                    value = int(fields[index])
                    low[name] = value if low[name] is None else min(low[name], value)
                    high[name] = value if high[name] is None else max(high[name], value)
    result = {name: (low[name], high[name]) for name in wanted}
    result["__rows__"] = (rows, rows)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=float, required=True)
    parser.add_argument("--oracle-dir", required=True)
    parser.add_argument("--gate-out")
    args = parser.parse_args()
    oracle = Path(args.oracle_dir)
    names = columns()
    scales = benchbox_datagen.table_scales("tpcds", args.scale)
    ranges = benchbox_datagen.key_ranges("tpcds", args.scale)
    report: dict[str, object] = {"scale": args.scale, "rows": {}, "key_ranges": {}}
    ok = True
    for table, scale in scales.items():
        wanted = list(ranges.get(table, {}))
        seen = observed(oracle / f"{table}.dat", names[table], wanted)
        rows_match = seen["__rows__"][0] == scale["rows"]
        ok &= rows_match
        report["rows"][table] = {"library": scale["rows"], "dsdgen": seen["__rows__"][0], "match": rows_match}
        for column in wanted:
            match = tuple(seen[column]) == tuple(ranges[table][column])
            ok &= match
            report["key_ranges"][f"{table}.{column}"] = {
                "library": ranges[table][column],
                "dsdgen": seen[column],
                "match": match,
            }
    report["ok"] = ok
    print(json.dumps(report, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

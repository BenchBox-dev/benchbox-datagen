import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

TEMPLATE = """define SS = rowcount("store_sales");
define ST = rowcount("store");
define CC = rowcount("call_center");
define RS = rowcount("reason");
define WS = rowcount("web_sales");
define CS = rowcount("catalog_sales");
define CU = rowcount("customer");
select [SS], [ST], [CC], [RS], [WS], [CS], [CU];
"""
KEYS = ["store_sales", "store", "call_center", "reason", "web_sales", "catalog_sales", "customer"]
INTEGER_SCALES = [1, 10, 100, 1000, 3000]
FRACTIONAL_SCALES = ["0.01", "0.05", "0.1", "0.25", "0.5"]


def history_ids(rows: int) -> int:
    unique = (rows // 6) * 3
    return unique + {1: 1, 2: 2, 3: 2, 4: 3, 5: 3}.get(rows % 6, 0)


def dsqgen(binary: Path, scale: int, templates: Path) -> dict[str, int]:
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        for name in ("ansi.tpl", "netezza.tpl", "db2.tpl"):
            shutil.copy(templates / name, work / name)
        shutil.copy(binary.parent / "tpcds.idx", work / "tpcds.idx")
        (work / "t1.tpl").write_text(TEMPLATE)
        (work / "list.lst").write_text("t1.tpl\n")
        subprocess.run(
            [str(binary), "-directory", ".", "-input", "list.lst", "-scale", str(scale), "-dialect", "ansi", "-output_dir", ".", "-quiet", "y"],
            cwd=work,
            check=True,
            capture_output=True,
        )
        select = next(line for line in (work / "query_0.sql").read_text().splitlines() if line.lower().startswith("select"))
    values = [int(value) for value in select.removeprefix("select ").rstrip(";").split(",")]
    return dict(zip(KEYS, values))


def from_dsdgen(directory: Path) -> dict[str, int]:
    def lines(table: str) -> int:
        with (directory / f"{table}.dat").open("rb") as handle:
            return sum(1 for _ in handle)

    def tickets(table: str, column: int) -> int:
        highest = 0
        with (directory / f"{table}.dat").open() as handle:
            for line in handle:
                highest = max(highest, int(line.split("|")[column]))
        return highest

    return {
        "store_sales": tickets("store_sales", 9),
        "store": history_ids(lines("store")),
        "call_center": history_ids(lines("call_center")),
        "reason": lines("reason"),
        "web_sales": tickets("web_sales", 17),
        "catalog_sales": tickets("catalog_sales", 17),
        "customer": lines("customer"),
    }


def main() -> int:
    binary, templates, oracle_root = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    gate = {str(scale): dsqgen(binary, scale, templates) for scale in INTEGER_SCALES}
    for scale in FRACTIONAL_SCALES:
        gate[scale] = from_dsdgen(oracle_root / f"dsdgen-sf{scale}")
    json.dump(gate, sys.stdout, indent=1)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

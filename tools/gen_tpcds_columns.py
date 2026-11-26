import re
import sys
from pathlib import Path


def main() -> int:
    source = Path(sys.argv[1])
    out = Path(sys.argv[2])
    columns = []
    tables = {}
    number = 1
    for line in source.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name, table = parts[0], parts[1]
        per_row = int(parts[2]) if len(parts) >= 3 else 1
        columns.append((name, number, per_row))
        first, _ = tables.get(table, (number, number))
        tables[table] = (first, number)
        number += 1
    lines = [
        "#![allow(dead_code)]",
        "",
        "#[derive(Clone, Copy, Debug)]",
        "pub(super) struct Col {",
        "    pub(super) number: usize,",
        "    pub(super) per_row: u32,",
        "}",
        "",
        "#[derive(Clone, Copy, Debug)]",
        "pub(super) struct Span {",
        "    pub(super) first: usize,",
        "    pub(super) last: usize,",
        "}",
        "",
        f"pub(super) const MAX_COLUMN: usize = {number - 1};",
        "",
        "pub(super) const PER_ROW: [u32; MAX_COLUMN + 1] = [",
        "    0,",
    ]
    lines += [f"    {per_row}," for _, _, per_row in columns]
    lines += ["];", ""]
    for name, num, per_row in columns:
        lines.append(f"pub(super) const {name}: Col = Col {{ number: {num}, per_row: {per_row} }};")
    lines.append("")
    for table, (first, last) in tables.items():
        lines.append(f"pub(super) const TABLE_{table}: Span = Span {{ first: {first}, last: {last} }};")
    out.write_text("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

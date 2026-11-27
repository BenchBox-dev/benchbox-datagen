import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "THIRD_PARTY_LICENSES"


TPC_DERIVED = [
    (
        "crates/benchbox-datagen-core/src/tpcds_update/columns.rs",
        "generated from tools/column_list.txt of the TPC-DS 4.0.0 kit (the column list mkheader turns into dsdgen's columns.h)",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/dims.rs",
        "the scaling.dst row-count weights (row_spec, source_table_rows), the zip-to-GMT offset table (GMT_OFFSETS) and dimension source-table generators ported from dsdgen",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/scd.rs",
        "slowly-changing dimension source-table generators ported from dsdgen",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/orders.rs",
        "purchase, catalog order and web order source-table generators ported from dsdgen",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/plan.rs",
        "update-date selection and per-day row scaling ported from dsdgen",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/calendar.rs",
        "calendar and date routines ported from dsdgen, reading calendar.dst",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/ctx.rs",
        "join-key and catalog-page routines ported from dsdgen",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/rng.rs",
        "random-stream seeding by column number ported from dsdgen",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/out.rs",
        "field formatting of dsdgen's row printers",
    ),
    (
        "crates/benchbox-datagen-core/src/tpcds_update/mod.rs",
        "refresh-set file layout and delete files ported from dsdgen",
    ),
]


def tpc_derived_notice() -> str:
    directory = ROOT / "crates" / "benchbox-datagen-core" / "src" / "tpcds_update"
    listed = {ROOT / path for path, _ in TPC_DERIVED}
    unlisted = sorted(str(path.relative_to(ROOT)) for path in directory.glob("*.rs") if path not in listed)
    if unlisted:
        raise SystemExit(f"tpcds_update files missing from TPC_DERIVED in tools/third_party_licenses.py: {unlisted}")
    readme = (ROOT / "README.md").read_text()
    for path, _ in TPC_DERIVED:
        if not (ROOT / path).is_file():
            raise SystemExit(f"TPC_DERIVED lists a missing file: {path}")
        if f"`{path}`" not in readme:
            raise SystemExit(f"README.md does not list the TPC-derived file {path}")
    entries = "\n".join(f"- {path}: {what}" for path, what in TPC_DERIVED)
    return (
        "================================================================================\n"
        "TPC-DERIVED FILES in benchbox-datagen (TPC-DS 4.0.0 kit)\n\n"
        "The following files are derived from the TPC-DS 4.0.0 kit (dsdgen, its distribution\n"
        "files and column list), a Work covered by the TPC legal notice above. The notice\n"
        "applies to them:\n\n"
        f"{entries}\n"
    )


def tpc_notice(packages: dict, seen: set[str]) -> str:
    package = next(packages[pid] for pid in seen if packages[pid]["name"] == "tpcdsgen")
    source = Path(package["manifest_path"]).parent / "data" / "calendar.dst"
    lines = source.read_text().splitlines()
    end = next(index for index, line in enumerate(lines) if line.startswith("-- Contributors:"))
    notice = "\n".join(line.removeprefix("--").strip() and line.removeprefix("-- ").rstrip() for line in lines[:end]).strip()
    return (
        "================================================================================\n"
        f"TPC LEGAL NOTICE from {package['name']} {package['version']} (data/calendar.dst)\n\n{notice}\n"
    )


def render() -> str:
    licences = subprocess.run(
        ["cargo", "about", "generate", "--locked", "about.hbs"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    roots = [package_id for package_id, package in packages.items() if package["name"] == "benchbox-datagen-py"]
    seen: set[str] = set()
    pending = list(roots)
    while pending:
        package_id = pending.pop()
        if package_id in seen:
            continue
        seen.add(package_id)
        for dep in nodes[package_id]["deps"]:
            if any(kind["kind"] is None for kind in dep["dep_kinds"]):
                pending.append(dep["pkg"])
    notices = []
    for package_id in sorted(seen, key=lambda pid: (packages[pid]["name"], packages[pid]["version"])):
        package = packages[package_id]
        if package["source"] is None:
            continue
        directory = Path(package["manifest_path"]).parent
        for notice in sorted(directory.glob("NOTICE*")):
            notices.append(
                f"================================================================================\n"
                f"NOTICE from {package['name']} {package['version']}\n\n{notice.read_text().strip()}\n"
            )
    notices.append(tpc_notice(packages, seen))
    notices.append(tpc_derived_notice())
    return licences.rstrip() + "\n\n" + "\n".join(notices)


def main() -> int:
    text = render()
    if any(entity in text for entity in ("&quot;", "&#x27;", "&lt;", "&gt;", "&amp;")):
        print("licence text contains HTML entities; the template must not escape it", file=sys.stderr)
        return 1
    if sys.argv[1:] == ["--check"]:
        if OUTPUT.read_text() != text:
            print("THIRD_PARTY_LICENSES is out of date; run python tools/third_party_licenses.py", file=sys.stderr)
            return 1
        return 0
    OUTPUT.write_text(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

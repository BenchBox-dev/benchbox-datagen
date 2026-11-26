import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "THIRD_PARTY_LICENSES"


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
    return licences.rstrip() + "\n\n" + "\n".join(notices)


def main() -> int:
    text = render()
    if sys.argv[1:] == ["--check"]:
        if OUTPUT.read_text() != text:
            print("THIRD_PARTY_LICENSES is out of date; run python tools/third_party_licenses.py", file=sys.stderr)
            return 1
        return 0
    OUTPUT.write_text(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

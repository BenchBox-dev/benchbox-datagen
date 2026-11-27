import re
from pathlib import Path

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"


def benchbox_ref(name: str) -> str:
    match = re.search(r"^  BENCHBOX_REF: ([0-9a-f]{40})$", (WORKFLOWS / name).read_text(encoding="utf-8"), re.MULTILINE)
    assert match, name
    return match.group(1)


def test_ci_and_wheels_pin_the_same_benchbox_commit():
    assert benchbox_ref("ci.yml") == benchbox_ref("wheels.yml")

#!/usr/bin/env python3
"""Fail unless every theseus pin in the review callers is one commit.

Asserts that each ``uses: joeharris76/theseus/...@<sha>`` equals the
``theseus_ref`` input of the same file and the workflow and source pins of
``.github/theseus-lock.json``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CALLERS = ("review-verdict.yml", "review-gate.yml")
SHA = re.compile(r"[0-9a-f]{40}")
USES = re.compile(r"^\s*uses:\s*joeharris76/theseus/\S+@(\S+)\s*$", re.MULTILINE)
REF = re.compile(r"^\s*theseus_ref:\s*(\S+)\s*$", re.MULTILINE)
ANY_THESEUS = re.compile(r"joeharris76/theseus/\S*@", re.MULTILINE)


def check(root: Path) -> list[str]:
    errors: list[str] = []
    lock = json.loads((root / ".github" / "theseus-lock.json").read_text(encoding="utf-8"))
    workflow_sha = lock.get("workflow_sha")
    source_commit = lock.get("source_commit")
    if not (isinstance(workflow_sha, str) and SHA.fullmatch(workflow_sha)):
        errors.append(f"lock workflow_sha is not a 40-hex commit: {workflow_sha!r}")
    if workflow_sha != source_commit:
        errors.append(f"lock workflow_sha {workflow_sha!r} != source_commit {source_commit!r}")
    for name in CALLERS:
        path = root / ".github" / "workflows" / name
        text = path.read_text(encoding="utf-8")
        uses = USES.findall(text)
        refs = REF.findall(text)
        if len(uses) != 1 or len(refs) != 1:
            errors.append(f"{name}: expected one theseus uses and one theseus_ref, found {len(uses)} and {len(refs)}")
            continue
        if len(ANY_THESEUS.findall(text)) != 1:
            errors.append(f"{name}: unexpected extra theseus reference")
        if not (uses[0] == refs[0] == workflow_sha):
            errors.append(f"{name}: uses {uses[0]} / theseus_ref {refs[0]} / lock {workflow_sha} differ")
    return errors


def main() -> int:
    errors = check(ROOT)
    for error in errors:
        print(f"::error::{error}")
    if errors:
        return 1
    print("theseus pin is one commit across callers and lock")
    return 0


if __name__ == "__main__":
    sys.exit(main())

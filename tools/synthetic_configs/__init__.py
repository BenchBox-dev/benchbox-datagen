import importlib
from pathlib import Path

from synthetic_configs.base import SyntheticConfig

PACKAGE = Path(__file__).resolve().parent


def names() -> list[str]:
    return sorted(path.stem for path in PACKAGE.glob("*.py") if path.stem not in {"__init__", "base"})


def load(name: str) -> SyntheticConfig:
    if name not in names():
        raise SystemExit(f"no synthetic config {name!r}; known: {', '.join(names())}")
    return importlib.import_module(f"synthetic_configs.{name}").CONFIG

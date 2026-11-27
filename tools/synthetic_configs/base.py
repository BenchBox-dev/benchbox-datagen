import json
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
Column = tuple[str, str]


@dataclass(frozen=True)
class SharedDomain:
    columns: list[str]
    filter: str


@dataclass
class SyntheticConfig:
    name: str
    tables: list[str]
    primary_keys: dict[str, list[str]]
    foreign_keys: list[tuple[str, str, str, str]]
    expected_rows: Callable[[float], dict[str, int]]
    queries: Path | Callable[[], dict[str, str]]
    legacy_generate: Callable[[float, int, Path], None]
    benchbox_schema: Callable[[], dict]
    budgets: dict[tuple[float, str], tuple[float, int]]
    exclusions: dict[Column, str] = field(default_factory=dict)
    table_exclusions: dict[str, str] = field(default_factory=dict)
    conditional_exclusions: Callable[[dict, dict], dict[Column, str]] | None = None
    shared_domains: dict[str, SharedDomain] = field(default_factory=dict)
    row_digests: dict[str, str] = field(default_factory=dict)
    expected_query_rows: Callable[[float], dict[str, object]] | None = None
    seeds: list[int] = field(default_factory=lambda: list(range(42, 47)))
    extended_seeds: list[int] = field(default_factory=lambda: list(range(42, 72)))
    distribution_scales: tuple[float, ...] = (0.1, 1.0)
    query_scales: tuple[float, ...] = (0.01, 0.1, 1.0)
    verify_scales: tuple[float, ...] = (0.01, 0.1, 1.0, 10.0)

    def __post_init__(self) -> None:
        reasons = [*self.exclusions.values(), *self.table_exclusions.values()]
        if any(not reason.strip() for reason in reasons):
            raise ValueError(f"{self.name}: every exclusion needs a written reason")
        if set(self.primary_keys) != set(self.tables):
            raise ValueError(f"{self.name}: primary_keys must cover exactly the tables")
        if self.seeds != self.extended_seeds[: len(self.seeds)]:
            raise ValueError(f"{self.name}: extended_seeds must extend seeds")

    @property
    def schema_file(self) -> Path:
        return ROOT / "tests" / "data" / f"{self.name}_arrow_schemas.json"

    def schemas(self) -> dict[str, list[list]]:
        return json.loads(self.schema_file.read_text())

    def query_texts(self) -> dict[str, str]:
        if callable(self.queries):
            return self.queries()
        return json.loads(self.queries.read_text())

    def excluded_columns(self, legacy: dict, library_run: dict) -> dict[Column, str]:
        excluded = dict(self.exclusions)
        for table, reason in self.table_exclusions.items():
            for name in library_run["tables"][table]["columns"]:
                excluded[(table, name)] = reason
        if self.conditional_exclusions:
            excluded.update(self.conditional_exclusions(legacy, library_run))
        if any(not reason.strip() for reason in excluded.values()):
            raise ValueError(f"{self.name}: every exclusion needs a written reason")
        return excluded


def scaled(base: int, scale_factor: float) -> int:
    return max(1, int(base * scale_factor))


def log2_floor(scale_factor: float) -> int:
    return math.floor(math.log2(scale_factor))

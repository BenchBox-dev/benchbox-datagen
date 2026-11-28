from collections.abc import Sequence
from os import PathLike
from typing import Literal, final, overload

__version__: str

@final
class TableStream:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...

@final
class FileStat:
    table: str
    path: str
    rows: int
    bytes: int

def generate_table(
    benchmark: str, table: str, scale_factor: float, part: int = 1, parts: int = 1, seed: int | None = None
) -> TableStream: ...
def table_names(benchmark: str) -> list[str]: ...
def row_counts(
    benchmark: str, scale_factor: float, threads: int | None = None
) -> dict[str, int]: ...
def table_scales(benchmark: str, scale_factor: float) -> dict[str, dict[str, int]]: ...
def active_locations(benchmark: str, scale_factor: float) -> dict[str, int]: ...
def key_ranges(benchmark: str, scale_factor: float) -> dict[str, dict[str, tuple[int, int]]]: ...
def write_tables(
    benchmark: str,
    scale_factor: float,
    output_dir: str | PathLike[str],
    format: str = "parquet",
    compression: str | None = None,
    threads: int | None = None,
    parts: int = 1,
    seed: int | None = None,
) -> list[FileStat]: ...
def write_refresh(
    benchmark: str,
    scale_factor: float,
    output_dir: str | PathLike[str],
    sets: int = 1,
    rng_seed: int | None = None,
) -> list[FileStat]: ...
@overload
def generate_query(
    benchmark: Literal["tpch"],
    query: str,
    template_text: str,
    distributions_text: str,
    scale_factor: float,
    seed: int | None = None,
    stream: int | None = None,
    default_params: bool = False,
) -> tuple[str, list[str]]: ...
@overload
def generate_query(
    benchmark: Literal["tpcds"],
    query: str,
    template_text: str,
    distributions_text: bytes | str,
    scale_factor: float,
    seed: int | None = None,
    stream: int | None = None,
    default_params: Literal[False] = False,
    *,
    dialect_text: str,
    parameters: Sequence[tuple[str, str]] | None = None,
) -> tuple[str, dict[str, str], bool]: ...
def generate_tpcds_streams(
    templates: Sequence[tuple[str, str]],
    dialect_text: str,
    distributions: bytes | str,
    scale_factor: float,
    seed: int | None = None,
    streams: int = 1,
    qualify: bool = False,
) -> tuple[str, list[tuple[int, list[tuple[str, str, list[tuple[str, str]], bool]]]]]: ...

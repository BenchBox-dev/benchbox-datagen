from os import PathLike
from typing import final

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
    benchmark: str, table: str, scale_factor: float, part: int = 1, parts: int = 1
) -> TableStream: ...
def table_names(benchmark: str) -> list[str]: ...
def row_counts(benchmark: str, scale_factor: float) -> dict[str, int]: ...
def table_scales(benchmark: str, scale_factor: float) -> dict[str, dict[str, int]]: ...
def key_ranges(benchmark: str, scale_factor: float) -> dict[str, dict[str, tuple[int, int]]]: ...
def write_tables(
    benchmark: str,
    scale_factor: float,
    output_dir: str | PathLike[str],
    format: str = "parquet",
    compression: str | None = None,
    threads: int | None = None,
    parts: int = 1,
) -> list[FileStat]: ...
def write_refresh(
    benchmark: str, scale_factor: float, output_dir: str | PathLike[str], sets: int = 1
) -> list[FileStat]: ...

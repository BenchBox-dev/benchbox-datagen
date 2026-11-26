from typing import final

__version__: str

@final
class TableStream:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...

def generate_table(
    benchmark: str, table: str, scale_factor: float, part: int = 1, parts: int = 1
) -> TableStream: ...
def table_names(benchmark: str) -> list[str]: ...

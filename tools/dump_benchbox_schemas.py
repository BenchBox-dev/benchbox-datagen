import json
import sys

from benchbox.core.tpch.benchmark import TPCHBenchmark
from benchbox.utils.format_converters.base import ArrowTypeMapper


def main() -> int:
    schemas = TPCHBenchmark(scale_factor=0.01).get_schema()
    dump = {
        table: [
            [field.name, str(field.type), field.nullable]
            for field in ArrowTypeMapper.build_arrow_schema(schema)
        ]
        for table, schema in schemas.items()
    }
    tables = ",\n".join(
        f' "{table}": [\n' + ",\n".join("  " + json.dumps(column) for column in columns) + "\n ]"
        for table, columns in sorted(dump.items())
    )
    sys.stdout.write("{\n" + tables + "\n}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

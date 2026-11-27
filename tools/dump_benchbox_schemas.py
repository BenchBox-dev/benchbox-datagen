import json
import sys

from benchbox.utils.format_converters.base import ArrowTypeMapper


def load(benchmark: str) -> dict:
    if benchmark == "tpch":
        from benchbox.core.tpch.benchmark import TPCHBenchmark

        return TPCHBenchmark(scale_factor=0.01).get_schema()
    if benchmark == "ssb":
        from benchbox.core.ssb.benchmark import SSBBenchmark

        return SSBBenchmark(scale_factor=0.01).get_schema()
    from benchbox.core.tpcds.benchmark import TPCDSBenchmark

    schemas = TPCDSBenchmark(scale_factor=1.0).get_schema()
    schemas.pop("dbgen_version")
    return schemas


def main() -> int:
    benchmark, form = (sys.argv[1:] + ["json"])[:2] if len(sys.argv) > 1 else ("tpch", "json")
    dump = {
        table: [
            [field.name, str(field.type), field.nullable]
            for field in ArrowTypeMapper.build_arrow_schema(schema)
        ]
        for table, schema in load(benchmark).items()
    }
    if form == "spec":
        for table, columns in sorted(dump.items()):
            for name, arrow_type, nullable in columns:
                short = arrow_type.replace("date32[day]", "date32").replace(", ", ",")
                sys.stdout.write(f"{table} {name} {short} {'null' if nullable else 'not_null'}\n")
        return 0
    tables = ",\n".join(
        f' "{table}": [\n' + ",\n".join("  " + json.dumps(column) for column in columns) + "\n ]"
        for table, columns in sorted(dump.items())
    )
    sys.stdout.write("{\n" + tables + "\n}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

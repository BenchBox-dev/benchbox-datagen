import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

import benchbox_datagen
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

CHUNK = 1 << 22
INTEGER_RENDERED = {"l_quantity"}


def sha256_oracle(path: Path, strip: bool) -> str:
    return sha256_stripped(path) if strip else sha256_file(path)


def sha256_stripped(path: Path) -> str:
    digest = hashlib.sha256()
    carry = b""
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            data = (carry + chunk).replace(b"|\n", b"\n")
            carry = b"|" if data.endswith(b"|") else b""
            digest.update(data[: len(data) - len(carry)])
    digest.update(carry)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


JOIN_NULLS_AS_EMPTY = pc.JoinOptions(null_handling="replace", null_replacement="")


def sha256_parquet_as_text(path: Path) -> str:
    digest = hashlib.sha256()
    for batch in pq.ParquetFile(path).iter_batches(batch_size=1 << 16):
        columns = [
            pc.cast(pc.cast(column, pa.int64()) if name in INTEGER_RENDERED or name.endswith("_gmt_offset") else column, pa.string())
            for name, column in zip(batch.schema.names, batch.columns)
        ]
        lines = pc.binary_join_element_wise(*columns, "|", options=JOIN_NULLS_AS_EMPTY)
        terminated = pc.binary_join_element_wise(lines, "\n", "")
        size = pc.sum(pc.binary_length(terminated)).as_py() or 0
        digest.update(terminated.buffers()[2].to_pybytes()[:size])
    return digest.hexdigest()


def run(args: argparse.Namespace) -> bool:
    dbgen = Path(args.dbgen_dir)
    benchmark = args.benchmark
    extension = "tbl" if benchmark == "tpch" else "dat"
    tables = benchbox_datagen.table_names(benchmark)
    expected = {table: sha256_oracle(dbgen / f"{table}.{extension}", benchmark == "tpch") for table in tables}
    expected_rows = benchbox_datagen.row_counts(benchmark, args.scale)
    root = Path(tempfile.mkdtemp(prefix="parity-", dir=args.workdir))
    ok = True
    report: dict[str, object] = {"benchmark": benchmark, "scale": args.scale, "oracle_sha256": expected}
    try:
        for threads in args.threads:
            label = "default" if threads is None else str(threads)
            out = root / f"tbl-{label}"
            stats = benchbox_datagen.write_tables(
                benchmark, args.scale, out, format="tbl", threads=threads
            )
            result = {}
            for stat in stats:
                actual = sha256_file(Path(stat.path))
                match = actual == expected[stat.table] and stat.rows == expected_rows[stat.table]
                ok &= match
                result[stat.table] = {"sha256": actual, "rows": stat.rows, "match": match}
            report[f"tbl_threads_{label}"] = result
            shutil.rmtree(out)
        parquet_hashes: dict[str, dict[str, str]] = {}
        for index, threads in enumerate(args.parquet_threads):
            label = "default" if threads is None else str(threads)
            out = root / f"parquet-{label}"
            stats = benchbox_datagen.write_tables(
                benchmark, args.scale, out, format="parquet", threads=threads
            )
            parquet_hashes[label] = {stat.table: sha256_file(Path(stat.path)) for stat in stats}
            if index == 0:
                content = {}
                for stat in stats:
                    actual = sha256_parquet_as_text(Path(stat.path))
                    match = actual == expected[stat.table] and stat.rows == expected_rows[stat.table]
                    ok &= match
                    content[stat.table] = {"text_sha256": actual, "rows": stat.rows, "match": match}
                report["parquet_values_vs_oracle"] = content
            shutil.rmtree(out)
        labels = list(parquet_hashes)
        invariant = all(parquet_hashes[label] == parquet_hashes[labels[0]] for label in labels)
        ok &= invariant
        report["parquet_file_sha256"] = parquet_hashes[labels[0]]
        report["parquet_thread_invariant"] = {"threads": labels, "identical": invariant}
    finally:
        shutil.rmtree(root, ignore_errors=True)
    report["ok"] = ok
    print(json.dumps(report, indent=2))
    return ok


def threads_arg(value: str) -> int | None:
    return None if value == "default" else int(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", choices=["tpch", "tpcds"], required=True)
    parser.add_argument("--dbgen-dir", required=True)
    parser.add_argument("--scale", type=float, required=True)
    parser.add_argument("--workdir", default=None)
    parser.add_argument("--threads", type=threads_arg, nargs="+", default=[1, None])
    parser.add_argument("--parquet-threads", type=threads_arg, nargs="+", default=[1, 4, None])
    return 0 if run(parser.parse_args()) else 1


if __name__ == "__main__":
    sys.exit(main())

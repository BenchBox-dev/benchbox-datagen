# benchbox-datagen

Deterministic, high-throughput benchmark data generation for
[BenchBox](https://github.com/BenchBox-dev/benchbox), written in Rust and
exposed to Python through PyO3.

The design and its validation gates are recorded in BenchBox's
`docs/internal/adr/adr-benchbox-datagen.md`.

## Scope

The library is built in stages:

1. TPC-H
2. TPC-DS, including fractional scales and refresh sets
3. SSB
4. The BenchBox synthetic generators (`h2odb`, `tsbs_devops`, `clickbench`,
   `tpch_skew`, `vector_search`, `coffeeshop`, `joinorder_synthetic`,
   `amplab`)

TPC generation depends on the upstream Apache-2.0 crates from
[`datafusion-contrib/tpcgen-rs`](https://github.com/datafusion-contrib/tpcgen-rs),
pinned by exact version. This project does not fork them: fixes go upstream.

Today the library generates TPC-H (all eight tables, any scale factor) and SSB (all five tables, any scale factor) as
Arrow record batches or as files, and reports exact per-table row counts. Output
carries BenchBox's Arrow types (`int64`, `decimal128(15, 2)`, `date32`, `utf8`,
`NOT NULL` columns non-nullable). Delimited `.tbl` output is byte-identical to
the bundled `dbgen` output with the trailing `|` removed, for every table.
Parquet output is written in Rust with 122,880-row row groups and the
`benchbox.parquet_writer=benchbox-datagen-<major>` key-value entry; neither
format's bytes depend on the thread count. SSB output is described under
[Deterministic synthetic generators](#deterministic-synthetic-generators).

## Python

```python
import pyarrow as pa
import benchbox_datagen

reader = pa.RecordBatchReader.from_stream(
    benchbox_datagen.generate_table("tpch", "lineitem", scale_factor=1.0, part=1, parts=4)
)
for batch in reader:
    ...
```

`generate_table` returns an object that implements the Arrow PyCapsule stream
interface (`__arrow_c_stream__`), so any Arrow-compatible library can consume
it without copying.

```python
import benchbox_datagen

counts = benchbox_datagen.row_counts("tpch", 10.0)
assert counts["lineitem"] == 59_986_052

ranges = benchbox_datagen.key_ranges("ssb", 10.0)
assert ranges["lineorder"]["lo_custkey"] == (1, 300_000)

files = benchbox_datagen.write_tables(
    "tpch",
    10.0,
    "out/tpch_sf10",
    format="parquet",
    compression="zstd",
    threads=8,
)
for file in files:
    print(file.table, file.path, file.rows, file.bytes)
```

`write_tables(benchmark, scale_factor, output_dir, format="parquet",
compression=None, threads=None, parts=1, seed=None)` writes one `<table>.<format>` file per
table (`format` is `"parquet"` or `"tbl"`; Parquet compression is `"none"`,
`"snappy"` (default), `"gzip"` or `"zstd"`; `.tbl` is uncompressed). With
`parts > 1` each table except `nation` and `region` is split into
`<table>.<format>.<part>` files (for example `lineitem.tbl.3`, the same shard naming BenchBox uses). `threads=None` uses every available CPU.
`generate_table` takes the same `seed`. Seeded benchmarks (`ssb`) default to 42;
passing a seed to an unseeded benchmark (`tpch`) raises `ValueError`.

## Deterministic synthetic generators

Every generator that is not a TPC tool follows one contract, implemented in
`benchbox-datagen-core`'s `synthetic` module. Changing any part of it changes
the output and is a generator version bump that refreezes the golden hashes.

- **Fixed blocks.** A table is produced in blocks of 65,536 rows; block `b`
  holds rows `[b * 65,536, (b + 1) * 65,536)`. A block is a pure function of
  `(benchmark, table, scale factor, seed, b)`, so any number of threads
  produces the same bytes. The existing ordered pipeline consumes blocks in
  index order, `.tbl` writes them as they arrive, and the Parquet writer
  regroups them into 122,880-row groups.
- **Independent streams.** Each block draws from its own ChaCha8 stream
  (`rand_chacha`, pinned to an exact version) keyed by `(benchmark, table,
  stream name, seed, block)`. The 256-bit key is derived with FNV-1a 64 over
  the domain string `benchbox-datagen/synthetic/v1` and the names, then a
  SplitMix64 finalizer over the seed and block index. A table may use several
  named streams (SSB `lineorder` uses `fields` and `orders`).
- **Owned sampling.** Bounded integers use a widening multiply (32-bit draw
  for bounds up to 65,536, 64-bit otherwise), floats use the top 53 bits of a
  draw, choices and word lists index a slice, weighted choices search a
  cumulative integer table, and dates use integer civil-calendar arithmetic.
  Nothing depends on the platform's floating-point library; any
  transcendental function must come from the pure-Rust `libm` crate.
- **Cross-block state** (for example SSB order numbering) comes from a
  separate cheap stream that is summed once per table (a prefix pass), never
  from sequential generation, so blocks stay independent.
- **Rows and parts.** A table implements `schema()`, `rows()` and
  `block(index) -> RecordBatch`. `parts` splits a table into contiguous row
  ranges (blocks are sliced at the range edges), so concatenating the part
  files reproduces the single file byte for byte. Small dimension tables
  (`ssb.date`) are never split.
- **Text output.** `.tbl` is `|`-separated with a `\n` line end, no trailing
  delimiter, nulls empty and dates as `YYYY-MM-DD`. Parquet carries the same
  Arrow types as the BenchBox schema dump (`tests/data/<benchmark>_arrow_schemas.json`,
  refreshed with `tools/dump_benchbox_schemas.py <benchmark>`).
- **Seeds.** A `u64`, default 42. Row counts never depend on the seed.

### SSB

Tables, columns, types, key relationships and value domains follow BenchBox's
legacy `random.Random(42)` generator; only the random stream changed. Counts
at scale factor `SF` are `date` 2,557 (1992-01-01 to 1998-12-31), `customer`
`30,000 * SF`, `supplier` `2,000 * SF`, `part` `200,000 * SF` and `lineorder`
`6,000,000 * SF`, each truncated and at least 1. Orders are runs of
consecutive `lineorder` rows: each row after the first starts a new order with
probability 0.7, order keys are contiguous from 2, and line numbers restart at
1. `date` is byte-identical to the legacy `date.tbl` apart from line endings
(the legacy writer emits CRLF).

`tools/ssb_acceptance.py` produces the legacy-versus-library comparison,
foreign-key and uniqueness checks and the 13-query DuckDB/DataFusion agreement
that gate the frozen goldens in `crates/benchbox-datagen-core/tests/ssb.rs`.

## Layout

| Crate | Role |
| :--- | :--- |
| `benchbox-datagen-core` | Generator registry, BenchBox Arrow schemas, row counts, Arrow batch streams and file output |
| `benchbox-datagen-writers` | Parquet and delimited writers |
| `benchbox-datagen-py` | The `benchbox_datagen._native` PyO3 module |

Wheels are built for CPython 3.11+ (`abi3`) on macOS arm64 and x86_64, Linux
glibc and musl (x86_64 and aarch64), and Windows x86_64.

## Development

```bash
cargo fmt --check && cargo clippy --all-targets && cargo test
uv venv && uv pip install maturin pyarrow pytest
maturin develop --release && pytest tests
```

`tools/parity_tpch.py` compares library output with a directory of `dbgen`
output by SHA-256, and `tools/bench_budgets.py --benchmark tpch|ssb` measures the
wall-time and peak-RSS budgets from the design ADR.

## Licence

MIT. See [LICENSE](LICENSE).

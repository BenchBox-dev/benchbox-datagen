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

Today the library generates TPC-H (all eight tables, any scale factor), TPC-DS (the 24 benchmark tables at scale factor 1 or above, in `CompatMode::C`, byte-identical to the bundled `dsdgen`) and SSB (all five tables, any scale factor) as
Arrow record batches or as files, and reports exact per-table row counts. Output
carries BenchBox's Arrow types (`int64`, `decimal128(15, 2)`, `date32`, `utf8`,
`NOT NULL` columns non-nullable). Delimited output is byte-identical to the
bundled `dbgen` / `dsdgen -TERMINATE n` output (no trailing `|`), for every table.
TPC-DS below scale factor 1 is refused until fractional scales are implemented.
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

ranges = benchbox_datagen.key_ranges("tpcds", 10.0)
assert ranges["item"]["i_item_sk"] == (1, 102_000)

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
  the domain string `benchbox-datagen/synthetic/v1` and the names, then over
  the seed as its own 8-byte field (so no seed can alias another stream's
  names), then a SplitMix64 finalizer over the block index. A table may use
  several named streams (SSB `lineorder` uses `fields` and `orders`).
- **Owned sampling.** Bounded integers use an unbiased widening multiply with
  Lemire rejection (32-bit draw for bounds up to 65,536, 64-bit otherwise), floats use the top 53 bits of a
  draw, choices and word lists index a slice, weighted choices search a
  cumulative integer table, and dates use integer civil-calendar arithmetic.
  Nothing depends on the platform's floating-point library; any
  transcendental function must come from the pure-Rust `libm` crate.
- **Cross-block state** (for example SSB order numbering) comes from a
  separate cheap stream that is summed over whole orders (a prefix pass built
  only up to the block being generated and checked for cancellation between
  blocks), never from sequential generation, so blocks stay independent.
- **Rows and parts.** A table implements `schema()`, `rows()` and
  `block(index) -> RecordBatch`. `parts` splits a table into contiguous row
  ranges (blocks are sliced at the range edges), so concatenating the part
  files reproduces the single file byte for byte. Small dimension tables
  (`ssb.date`) are never split.
- **Text output.** `.tbl` is `|`-separated with a `\n` line end, no trailing
  delimiter, nulls empty and dates as `YYYY-MM-DD`. Parquet carries the same
  Arrow types as the BenchBox schema dump (`tests/data/<benchmark>_arrow_schemas.json`,
  refreshed with `tools/dump_benchbox_schemas.py <benchmark>`).
- **Seeds.** A `u64` (`0` to `2**64 - 1`; Python raises `ValueError` outside that range), default 42. Row counts never depend on the seed.

### SSB

Tables, columns, types and value domains follow BenchBox's legacy
`random.Random(42)` generator; only the random stream changed. Row counts follow
the Star Schema Benchmark specification (O'Neil, O'Neil, Chen, "The Star Schema
Benchmark", Revision 3, 2009, Figure 1.1 and sections 2.2-2.6,
<https://www.cs.umb.edu/~poneil/StarSchemaB.PDF>) as implemented by the reference
`ssb-dbgen` (`driver.c`, `dss.h`, `build.c`; checked against
`eyalroz/ssb-dbgen` at `ae1e254` and `vadimtk/ssb-dbgen`):

| Table | Rows at scale factor `SF` | Source |
| :--- | :--- | :--- |
| `date` | 2,556 (1992-01-01 to 1998-12-30), all scale factors | paper section 2.6 ("7 years of days", no count); `vadimtk/ssb-dbgen` `driver.c` and `shared.h` (`NUM_DAYS 2556`); `eyalroz/ssb-dbgen` has 2,557 |
| `customer` | `30,000 * SF` | paper section 2.5; `driver.c` base 30000 |
| `supplier` | `2,000 * SF` | paper section 2.4; `driver.c` base 2000 |
| `part` | `200,000 * floor(1 + log2(SF))` for `SF >= 1` | paper section 2.3; `driver.c` `rowcnt = base * floor(1 + log(scale)/log(2))` |
| `lineorder` | `6,000,000 * SF` | paper section 2.2 and Figure 1.1 (`ORDERS` `SF*1,500,000`, `LO_LINENUMBER` 1-7); `driver.c` order base 150,000 x 10, `O_LCNT_MIN/MAX` 1-7 |

For `SF < 1` the reference scales every base count by `SF` (truncated, at least
1), so `part` is `200,000 * SF`; the library keeps `date` at 2,556 because the
reference's own scaling of the date base would leave foreign keys without a
parent. For non-integer `SF >= 1` the reference truncates `SF` to an integer
first; the library scales `customer`, `supplier` and `lineorder` linearly and
takes `floor(log2(SF))` of the exact `SF` (computed from the float exponent, no
libm). `lineorder` is exactly `6,000,000 * SF` rows (row counts never depend on
the seed): orders are runs of consecutive rows, each order draws 1-7 lines
uniformly, and the final order is cut at the row count, so the order count is
`1,500,000 * SF` within sampling noise. Order `n` (from 1) has
key `((n >> 3) << 5) + (n & 7)`, the reference `mk_sparse` mapping for the paper's
"first 8 of each 32 keys populated", and line numbers restart at 1. `date` is
byte-identical to legacy `date.tbl` for its 2,556 rows, apart from line endings
(the legacy writer emits CRLF) and the legacy 2,557th row.

`tools/ssb_acceptance.py` produces the legacy-versus-library comparison,
foreign-key and uniqueness checks and the 13-query DuckDB/DataFusion agreement
that gate the frozen goldens in `crates/benchbox-datagen-core/tests/ssb.rs`.

Acceptance rules: a distribution metric passes inside the min-max of 5 legacy
seeds, inside their 99.9% prediction interval, or (boundary quantiles of
low-cardinality columns) by rank. For the date-valued `lo_orderdate` and
`lo_commitdate` the legacy statistics drop rows dated 1998-12-31, the one day
the two calendars do not share, and the same rule applies. `date` columns are
compared by a digest of the shared rows. A query that legacy never leaves empty
must never be empty in the library; where legacy is sometimes empty, the library
fails only if its empty-seed count is higher with one-sided Fisher exact
p < 0.001.

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

`tools/parity.py` compares library output with a directory of `dbgen`
output by SHA-256, and `tools/bench_budgets.py` measures the wall-time and
peak-RSS budgets from the design ADR (`--driver` measures a compiled driver
binary instead of the Python module).

## Licence

MIT. See [LICENSE](LICENSE).

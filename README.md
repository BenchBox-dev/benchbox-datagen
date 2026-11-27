# benchbox-datagen

Deterministic, high-throughput benchmark data generation for
[BenchBox](https://github.com/BenchBox-dev/benchbox), written in Rust and
exposed to Python through PyO3.

The design and its validation gates are recorded in BenchBox's
`docs/internal/adr/adr-benchbox-datagen.md`.

## Scope

The library is built in stages:

1. TPC-H
2. TPC-DS, including fractional scales (refresh sets follow separately)
3. SSB
4. The BenchBox synthetic generators (`h2odb`, `tsbs_devops`, `clickbench`,
   `tpch_skew`, `vector_search`, `coffeeshop`, `joinorder_synthetic`,
   `amplab`)

TPC generation depends on the upstream Apache-2.0 crates from
[`datafusion-contrib/tpcgen-rs`](https://github.com/datafusion-contrib/tpcgen-rs),
pinned by exact version. This project does not fork them: fixes go upstream, and any
patch carried until upstream releases it is listed under
[Carried upstream patches](#carried-upstream-patches).

Today the library generates TPC-H (all eight tables, any scale factor), TPC-DS (the 24 benchmark tables at scale factors below 1 and at integer scale factors of 1 or more, in `CompatMode::C`, byte-identical to the bundled patched `dsdgen`) and SSB (all five tables, any scale factor) as
Arrow record batches or as files, and reports exact per-table row counts. Output
carries BenchBox's Arrow types (`int64`, `decimal128(15, 2)`, `date32`, `utf8`,
`NOT NULL` columns non-nullable). Delimited output is byte-identical to the
bundled `dbgen` / `dsdgen -TERMINATE n` output (no trailing `|`), for every table.
Below TPC-DS scale factor 1 the patched `dsdgen` scale model (`max(1, floor(SF 1 rows x SF))`,
six static tables) supplies every table's row counts to `tpcdsgen` through a caller-supplied
`Scaling`, so all 24 tables come from `tpcdsgen` at every scale factor.
TPC-H refresh sets (`dbgen -U`) are available through `write_refresh`, byte-identical to the
bundled generator; TPC-DS and SSB have no refresh sets and `write_refresh("tpcds", ...)` and
`write_refresh("ssb", ...)` raise an error.
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
seeds (42-46), inside their 99.9% prediction interval, or (boundary quantiles of
low-cardinality columns) by rank. For the date-valued `lo_orderdate` and
`lo_commitdate` the legacy statistics drop the rows whose order date is
1998-12-31, the one day the two calendars do not share, and the same rule
applies, except that the distinct count is compared instead of the distinct
ratio (the filter changes only the legacy denominator). `date` columns are compared by a digest of the shared rows. A query
that legacy never leaves empty on seeds 42-46 must never be empty in the library
on seeds 42-46. A query that legacy leaves empty on any of seeds 42-46 is judged
on 30 seeds per side (42-71) at that scale: the library fails only if its
empty-seed count is higher than legacy's with one-sided Fisher exact p < 0.001
(5 against 5 seeds cannot reach that p-value).

`table_scales(benchmark, scale_factor)` reports each table's `id_count` and `source_rows`.
`active_locations("tpcds", scale_factor)` returns the dsqgen `rowcount()` inputs
`active_cities`, `active_counties` and `active_states` (TPC-DS only; TPC-H and SSB raise
`ValueError`). They equal what the patched `dsqgen` returns for `rowcount("active_cities")`
and friends at integer scale factors of 1 or more, and below 1 they are 1, the value the
patched `dsdgen` store, call center, warehouse and web site addresses draw from. Templates
apply `rowcount("active_x", "store")`, which is the smaller of the value and the store
`id_count`.

`key_ranges` has no entries for the six returns tables. Their order and ticket numbers
are a data-dependent subset of the matching sales range, so no exact bounds exist
without generating the returns rows; the bounds of the sales range would be only a
superset, and are left out so that every reported range is exact.

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

## TPC-DS query generation

`generate_query("tpcds", ...)` and `generate_tpcds_streams(...)` are a Rust port of `dsqgen`. They interpret
the template language over BenchBox-supplied inputs only: the `.tpl` templates, the dialect template
(`netezza.tpl` is the only one BenchBox reaches) and the `tpcds.idx` distribution index, all passed at call
time. Nothing TPC-authored is committed here beyond the language keywords, the scale-slot volumes and the
tail-word constants below, which are numbers and names, not distributions or query text. `rowcount()` comes
from the library scale model (sub-scale rules and `active_locations` included). `parameters=` renders explicit
values the way `DSQGenBinary.generate_with_parameters` does (it rewrites `[NAME.n]` tokens, then runs a seeded
dsqgen). The sub-SF1 `query93.tpl` override stays with the caller, as in BenchBox.

`tests/dsqgen_corpus.rs` replays the TPC-DS shards of the BenchBox query corpus (`tpcds_single`,
`tpcds_stream_template`, `tpcds_stream_all`, `tpcds_explicit`, plus the manifest's Q34 `-LOG`/plain cases) against
the pinned `BENCHBOX_REF` checkout. `range()` and `list`/`ulist` on `text()` and `date()` are rejected with an error:
no BenchBox template uses them.

### Heap reads in the C tool

`ulist(random(...), n)` in `dsqgen` reads `n` entries of a key permutation that holds only `max - min + 1` keys.
When `rowcount()` makes that smaller than `n` (for example three counties at SF 10 for Q16, Q27, Q34, Q36 and Q73, two cities
at SF 1 for Q46) the C tool reads past the allocation, so its output depends on glibc heap contents (this is why the
`-LOG` and plain Q34 runs differ). With one key the value is always the minimum and the read is harmless. The library
models the first word past the allocation (the next chunk's size field) and the neighbouring `StringBuffer_t` words from the
second evaluation on, with constants fitted to the corpus (`heap_word` in `dsqgen/eval.rs`; `log_run` selects the
`-LOG` run's layout). The model reproduces every single-template, stream-template, explicit and Q34 corpus record. It cannot
reproduce a few records whose neighbouring heap objects are other templates' data; the replay test names them:

- `tpcds_stream_all`, SF 10, seed `None`: Q34 text in all 21 streams, Q73 in some streams, Q16 and Q27 in stream 0
  (68 of 168 excluded checks differ; the stream corpus has 66,696 other matching checks).
- `tpcds_explicit` round-trip hash `r` for Q16, Q27, Q36 and Q73 at SF 10, seed 1001 (the other checks of those records match).

Scale factors outside the corpus (SF 2-9, 20, 30, 50, 200, 500, 2000 and 5000 for Q16/27/34/36/46/73) have the same read
and are unverified against the oracle.

## Carried upstream patches

| Patch | Fork commit | Upstream PR |
| :--- | :--- | :--- |
| `tpcdsgen`: let callers supply the `Scaling` used by a `Session` (`Scaling::with_row_counts`, `SessionBuilder::with_scaling`; 73 changed lines) | [`b4b072b`](https://github.com/BenchBox-dev/tpcgen-rs/commit/b4b072b3f3ceb9f2979382beb96b3a27051b57b1) on `BenchBox-dev/tpcgen-rs` branch `v4.0.0-scaling-override` (upstream `v4.0.0` plus the PR commit and nothing else) | [datafusion-contrib/tpcgen-rs#677](https://github.com/datafusion-contrib/tpcgen-rs/pull/677) |

The root `Cargo.toml` pins `tpcdsgen` and `tpcdsgen-arrow` to that commit through
`[patch.crates-io]`; the library uses the patch for TPC-DS scale factors below 1.
Rule for removal: when a released upstream `tpcdsgen` contains the change, bump the exact
version requirement, delete the `[patch.crates-io]` entries, regenerate `Cargo.lock` and
`THIRD_PARTY_LICENSES`, and re-run the TPC-DS parity matrix. A carried patch stays at or below
about 200 changed lines, is pinned by commit SHA and is never merged into a divergent fork.

## Licence

MIT. See [LICENSE](LICENSE).

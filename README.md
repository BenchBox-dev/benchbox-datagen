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
- **Text output.** One text form per Arrow type, shared by every synthetic
  generator (the SSB bytes are unchanged). Lines end in `\n`; a null is an empty
  field. A table declares its delimited-text properties once (`Generator::text_format`):
  the delimiter (default `|`), a header row (default none) and the file
  extension (default `tbl`; the CSV preset is `csv`, with RFC 4180 quoting of
  fields that contain the delimiter, a quote or a newline, and `""` for an empty
  string so it differs from a null). A header is written on the first part only, so
  concatenating the parts still reproduces the single file. Nothing else is
  per-generator code.

  | Arrow type | Text | Legacy evidence |
  | :--- | :--- | :--- |
  | `int32`, `int64` | plain decimal | all eight generators |
  | `date32` | `YYYY-MM-DD` | `strftime("%Y-%m-%d")`, `date.isoformat()` in `amplab`, `clickbench`, `coffeeshop`, `tpch_skew` |
  | `timestamp[us]` | `YYYY-MM-DD HH:MM:SS`, plus `.ffffff` only when the microseconds are not zero (Python `str(datetime)`) | `strftime("%Y-%m-%d %H:%M:%S")` in `h2odb`, `tsbs_devops`, `clickbench` |
  | `float`, `double` | shortest round-trip digits in Python `repr` layout (`0.1`, `1.0`, `1e-05`, `1e+16`, `nan`, `inf`) | `h2odb` and `amplab` write Python floats through `csv`/`str` |
  | fixed-scale `double` (`Kind::Fixed(n)`) | `n` decimals (`1.50`) | `tsbs_devops` metrics and `coffeeshop` prices use `f"{x:.2f}"` |
  | `decimal128(p, s)` | exactly `s` decimals, `-0.05`, no exponent | TPC-H style `dbgen` decimals; `h2odb`, `coffeeshop`, `amplab` declare `DECIMAL` columns |
  | `bool` | `true` / `false` | no legacy generator among the eight writes one; the form every target engine accepts |
  | `fixed_size_list<float>[n]` | `[v1,v2,...]`, each element `%.6g` | `vector_search._array_to_csv` (`f"{v:.6g}"` joined by `,`) |
  | `utf8` | raw bytes | all |

  Any other Arrow type (unsigned or 8/16-bit integers, other timestamp units or
  time zones, negative decimal scales, lists of other types) is reported as an
  error before the first byte is written. Parquet needs no text form and
  carries every type natively; it is round-trip tested for all of them.
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

`tools/synthetic_acceptance.py ssb ...` (config in `tools/synthetic_configs/ssb.py`)
produces the legacy-versus-library comparison,
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

The seeds are fixed in `tools/synthetic_configs/base.py` (`SEEDS` = 42-46 and
`EXTENDED_SEEDS` = 42-71, the 99.9% interval constant is the Student t value for
those 4 degrees of freedom); a config cannot change them.

Every Arrow kind has a statistic. Each column is reduced to one number per row,
then summarised by null rate, distinct ratio, mean and the 1/25/50/75/99%
quantiles: strings by their length; integers, floats, decimals as the value;
booleans as 0/1; dates and timestamps as microseconds since the epoch
(`epoch_us`); vectors (`fixed_size_list<float>[n]`) as the population standard
deviation of their components, which pins the dimension and normalisation
without being sensitive to the sign pattern. Arrow type names are normalised
(`date32[day]` is `date32`) and an unmapped type raises `ValueError` rather than
being skipped. Legacy files are read by the config's `legacy_suffix` (default
`tbl`), `legacy_delimiter` (default `|`) and `legacy_header` (default false);
`tests/test_synthetic_acceptance_types.py` runs the whole statistic and compare
path on a CSV/Parquet fixture that has every kind.

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

## Adding a synthetic generator

A synthetic generator is one Rust module plus one registry entry (one line),
one Python config file and one schema file; nothing else is edited.

1. **Module.** Add `crates/benchbox-datagen-core/src/synthetic/<name>.rs` with a
   unit struct that implements `synthetic::Generator`: `name`, `tables`,
   `schema` (build it with `schema::build` from a column list kept in the
   module), `row_count`, `open` (returns a `Table` whose `block(index)` is a
   pure function of the block index and the seed), `key_ranges`, and, only when
   the defaults are wrong, `splittable` (default: every table splits) and
   `table_scales` (default: `id_count == source_rows == row_count`). Refresh
   sets are unsupported for every synthetic benchmark. Column types come from
   `schema::Kind` (`Int`, `Int32`, `Decimal`, `Decimal128`, `Float32`,
   `Float64`, `Fixed`, `Bool`, `Timestamp`, `Date`, `Text`, `Vector`); a table whose
   legacy files are not `|`-separated `.tbl` overrides `text_format` (see Text
   output above).

   **Row counts and key ranges are closed-form functions of the scale
   factor, not of the seed**: `row_count`, `key_ranges` and `table_scales` take
   no seed, and a part is a row range of a fixed-size table. A table whose
   child count varies per parent (SSB's 1-7 `lineorder` lines per order,
   coffeeshop's `order_lines`) follows SSB: the table has a fixed row count
   (the legacy mean times the scale), the cursor that assigns rows to parents
   is seeded, and the order-structure columns are listed in the config's
   `exclusions` with a reason because legacy and library differ there by
   construction.
2. **Registry.** In `synthetic/mod.rs` add `mod <name>;` and append
   `&<name>::<Struct>` to `REGISTRY`. The registry is one entry per line, so
   parallel additions merge mechanically (the `mod` lines and the entries are the
   only shared lines, and a conflict there is resolved by keeping both); Python
   names, `table_names`, `row_counts`, `table_scales`, `key_ranges`, the seed
   default, `write_tables` and `generate_table` all follow from it.
3. **Schema file.** Add `tests/data/<name>_arrow_schemas.json`, produced with
   `tools/dump_benchbox_schemas.py <name>` from the BenchBox schema, and a
   Rust test with frozen SHA-256 goldens for `tbl` and `parquet` at the gate
   scales (see `crates/benchbox-datagen-core/tests/ssb.rs`).
4. **Config.** Add `tools/synthetic_configs/<name>.py` that defines `CONFIG =
   SyntheticConfig(...)`: tables, primary and foreign keys, `expected_rows`,
   the queries (a JSON `{id: sql}` file path or a callable), the legacy
   generator, `benchbox_schema` (for the schema dump tool), the budgets used by
   `tools/bench_budgets.py`, and the exclusions. Config files are discovered by
   file name, so no list needs editing. Every exclusion (column, whole table or
   data-dependent) needs a written reason; a blank reason raises.
5. **Evidence.** Run, for each scale in the config, `tools/synthetic_acceptance.py
   <name> legacy`, `library` (the same seeds), `compare` (add `*_queries`
   files for seeds 47-71 when legacy leaves a query empty) and `verify`, then
   `report`, and put the report in the PR. Required results, unchanged from
   SSB: every metric inside the legacy min-max or the 99.9% prediction
   interval of 5 legacy seeds (boundary quantiles of low-cardinality columns
   may pass by rank); queries never empty in legacy never empty in the
   library, otherwise 30 seeds per side with one-sided Fisher p >= 0.001; no
   orphan or duplicate keys; every query returns the same rows in DuckDB and
   DataFusion; wall-time and peak-RSS budgets met. Also run
   `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`,
   `pytest tests` and `python3 tools/third_party_licenses.py --check`.
6. **Docs.** Add a section for the benchmark to this README next to SSB
   (tables, row-count rules, sources) and add it to the list at the top.

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

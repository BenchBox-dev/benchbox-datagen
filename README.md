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
pinned by exact version. This project does not fork them: fixes go upstream, and any
patch carried until upstream releases it is listed under
[Carried upstream patches](#carried-upstream-patches).

Today the library generates TPC-H (all eight tables, any scale factor) and TPC-DS (the 24 benchmark tables at scale factors below 1 and at integer scale factors of 1 or more, in `CompatMode::C`, byte-identical to the bundled patched `dsdgen`) as
Arrow record batches or as files, and reports exact per-table row counts. Output
carries BenchBox's Arrow types (`int64`, `decimal128(15, 2)`, `date32`, `utf8`,
`NOT NULL` columns non-nullable). Delimited output is byte-identical to the
bundled `dbgen` / `dsdgen -TERMINATE n` output (no trailing `|`), for every table.
Below TPC-DS scale factor 1 the patched `dsdgen` scale model (`max(1, floor(SF 1 rows x SF))`,
six static tables) supplies every table's row counts to `tpcdsgen` through a caller-supplied
`Scaling`, so all 24 tables come from `tpcdsgen` at every scale factor.
TPC-H refresh sets (`dbgen -U`) and TPC-DS refresh sets (`dsdgen -update <n>`: the 21 `s_*`
source files, `delete_<n>.dat` and `inventory_delete_<n>.dat`) are available through
`write_refresh`, byte-identical to the bundled generators. `tpcdsgen` 4.0.0 has no refresh
generators, so the TPC-DS source tables are this library's own code (module `tpcds_update`),
built on `tpcdsgen`'s public random streams, distributions and row types, seeded by the optional
`rng_seed` (`dsdgen -rngseed`). TPC-H rejects `rng_seed`. Refresh-set generators are proposed
upstream in [datafusion-contrib/tpcgen-rs#678](https://github.com/datafusion-contrib/tpcgen-rs/issues/678).
Parquet output is written in Rust with 122,880-row row groups and the
`benchbox.parquet_writer=benchbox-datagen-<major>` key-value entry; neither
format's bytes depend on the thread count.

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
compression=None, threads=None, parts=1)` writes one `<table>.<format>` file per
table (`format` is `"parquet"` or `"tbl"`; Parquet compression is `"none"`,
`"snappy"` (default), `"gzip"` or `"zstd"`; `.tbl` is uncompressed). With
`parts > 1` each table is split into `<table>.<format>.<part>` files (for example
`lineitem.tbl.3`, the same shard naming BenchBox uses), except TPC-H `nation` and
`region` and, for TPC-DS, every table with fewer than 1,000,000 source rows (at scale
factor 1 only `customer_demographics` and `inventory` are split). `threads=None` uses
every available CPU.

`row_counts(benchmark, scale_factor, threads=None)` is closed-form for TPC-H. For TPC-DS
the sales and returns counts come from running the generator over every line item, so
the cost grows with the scale factor (about 0.2 s at scale factor 1 on all cores); it
uses `threads` workers and responds to Ctrl-C.

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
peak-RSS budgets from the design ADR.

## TPC-DS refresh sets

Output matches the bundled `dsdgen -update <n> -terminate n` for all 23 files per set. Observed
SHA-256 parity: scale factor 0.01 (set 1), 1 (sets 1 to 7), 10 (set 1) and 100 (set 1), plus
0.05 and 0.5 (set 1). The scale factor 0.01, 1 and 100 hashes are committed in
`tests/data/tpcds_refresh_sha256.json`; the scale factor 100 check is opt-in
(`BENCHBOX_DATAGEN_HEAVY=1 pytest tests/test_refresh.py`).

Reproduce an oracle comparison with
`tools/oracle_tpcds_refresh.sh <dir with dsdgen and tpcds.idx> <scale> <sets> <out>` followed by
`python tools/parity_refresh.py --benchmark tpcds --dbgen-dir <out> --scale <scale> --sets <sets>`.

Known oracle behaviours:

- `dsdgen` reads the calendar distribution out of range while advancing update dates and
  treats the missing entry as weight 0; the library reproduces that read as 0 rather than
  failing (`Calendar::weight`).
- The bundled `dsdgen` aborts (exit 133) for `-update` at scale factors of 1 or more that are
  not on the TPC-DS scale grid (observed at 3 and 30), so there is no oracle for those scale
  factors; the library still generates them but they are unverified.

Generated and TPC-derived files:

| File | Source |
| :--- | :--- |
| `crates/benchbox-datagen-core/src/tpcds_update/columns.rs` | Generated by `tools/gen_tpcds_columns.py` from the column list of the TPC-DS 4.0.0 `dsdgen` (column number and random streams per row for each column); do not edit by hand |
| `tests/data/tpcds_refresh_sha256.json` | SHA-256 of the files written by the bundled `dsdgen -update` at scale factors 0.01, 1 and 100 |
| `tests/data/tpch_refresh_sf0.01_sha256.json` | SHA-256 of the files written by the bundled `dbgen -U` |

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

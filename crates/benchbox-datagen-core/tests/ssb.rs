use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, Int64Array};
use benchbox_datagen_core::{
    Benchmark, Error, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

const TABLES: [&str; 5] = ["date", "customer", "supplier", "part", "lineorder"];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "date",
        "1c61cb7e2ca8405acf18bbc6adba75ad89dee592f1614ad741d9e17246289d37",
    ),
    (
        "tbl",
        "0.01",
        "customer",
        "97cde325ca30135b82f59d2e363f03bd7fca1837715a75abf8b45c58bbfd168c",
    ),
    (
        "tbl",
        "0.01",
        "supplier",
        "3e9774c5a753ff12f5cf59e778d6d7ea639cc459fe9e610e870b8f3d048e7bf8",
    ),
    (
        "tbl",
        "0.01",
        "part",
        "d4626d548c0f6b17a9f6fc762f69f3c96fb78d8caa04ba1e925a3af39f818455",
    ),
    (
        "tbl",
        "0.01",
        "lineorder",
        "8d28704635cc7339c10c789acd4308aacefc3214e83b09a5b0c686c055d3c5f5",
    ),
    (
        "tbl",
        "0.1",
        "date",
        "1c61cb7e2ca8405acf18bbc6adba75ad89dee592f1614ad741d9e17246289d37",
    ),
    (
        "tbl",
        "0.1",
        "customer",
        "d9430a711d2465d358b134279935299b1c6a37a5888be5d6e7eeb019ebf322e3",
    ),
    (
        "tbl",
        "0.1",
        "supplier",
        "ccd7702880147609b1724f44a289fc1dbdc833cd45ed763b92b4620b308f4062",
    ),
    (
        "tbl",
        "0.1",
        "part",
        "cf3c42436b27328bf56f480e22bdf4b520c95e04aed0f4f363d2c8a2c211611c",
    ),
    (
        "tbl",
        "0.1",
        "lineorder",
        "7739b01ebb3283b18bb099ef91d1788e25db7539c174b74bd4e5ed55ef708257",
    ),
    (
        "parquet",
        "0.01",
        "date",
        "88a730e8743697f9db657ce4727286ee2a9be60a0bb3ea9e63c254c099f0d9c9",
    ),
    (
        "parquet",
        "0.01",
        "customer",
        "aa2ab312d0d0e9d6153cb2713b670db6c42ca9a5b46eb580f4fed12b55c50d29",
    ),
    (
        "parquet",
        "0.01",
        "supplier",
        "9dcfeb3d8162f91ca3298c717d50290e1bf94a20260c779422f00fc99e4e667e",
    ),
    (
        "parquet",
        "0.01",
        "part",
        "feeee106b763960e0345cb9acc34207f3340300dcdbb6e7ce6b07548d70de3c3",
    ),
    (
        "parquet",
        "0.01",
        "lineorder",
        "b678c19b0cf2a8aaa789e22794c41df3b948399ce19a50f6e76b4e9be409df03",
    ),
    (
        "parquet",
        "0.1",
        "date",
        "88a730e8743697f9db657ce4727286ee2a9be60a0bb3ea9e63c254c099f0d9c9",
    ),
    (
        "parquet",
        "0.1",
        "customer",
        "d62965f89c7f5f1382b66bd9d6d266e1272499197bc0a35f91921690071148af",
    ),
    (
        "parquet",
        "0.1",
        "supplier",
        "4035a78ad1667276d8bacce05530038f699edf360489a539ab84cfb9e758d379",
    ),
    (
        "parquet",
        "0.1",
        "part",
        "8df9b63b9c23ab0b098f0b823c00930bf03e5fb26d1b32f143f0a97c88264adc",
    ),
    (
        "parquet",
        "0.1",
        "lineorder",
        "aae9bbfc33dffae55730061f047c387e412027f9b5f73ab4b7b2af7b5e1720cd",
    ),
];

fn request(
    dir: &Path,
    format: Format,
    threads: Option<usize>,
    parts: i32,
    scale_factor: f64,
    seed: Option<u64>,
) -> OutputRequest {
    OutputRequest {
        benchmark: Benchmark::Ssb,
        scale_factor,
        format,
        compression: None,
        threads: threads.and_then(NonZeroUsize::new),
        parts,
        seed,
        output_dir: dir.to_path_buf(),
    }
}

fn digests(request: &OutputRequest) -> Vec<(String, String)> {
    write_benchmark(request)
        .unwrap()
        .into_iter()
        .map(|stats| {
            let bytes = fs::read(&stats.path).unwrap();
            let hash = Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            (
                stats
                    .path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                hash,
            )
        })
        .collect()
}

fn table_request(table: &str, scale_factor: f64, seed: Option<u64>) -> TableRequest {
    TableRequest {
        benchmark: Benchmark::Ssb,
        table: table.to_owned(),
        scale_factor,
        part: 1,
        parts: 1,
        seed,
    }
}

fn int_column(table: &str, scale_factor: f64, column: &str) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in record_batches(&table_request(table, scale_factor, None)).unwrap() {
        let batch = batch.unwrap();
        let array = batch
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        assert_eq!(array.null_count(), 0);
        values.extend(array.values().iter().copied());
    }
    values
}

#[test]
fn row_counts_follow_the_ssb_scale_rules_at_every_gate_scale() {
    let expected = [
        (0.01, [2_556, 300, 20, 2_000, 60_000]),
        (0.1, [2_556, 3_000, 200, 20_000, 600_000]),
        (1.0, [2_556, 30_000, 2_000, 200_000, 6_000_000]),
        (10.0, [2_556, 300_000, 20_000, 800_000, 60_000_000]),
        (2.0, [2_556, 60_000, 4_000, 400_000, 12_000_000]),
        (7.9, [2_556, 237_000, 15_800, 600_000, 47_400_000]),
        (8.0, [2_556, 240_000, 16_000, 800_000, 48_000_000]),
        (16.0, [2_556, 480_000, 32_000, 1_000_000, 96_000_000]),
        (100.0, [2_556, 3_000_000, 200_000, 1_400_000, 600_000_000]),
    ];
    for (scale_factor, rows) in expected {
        let counts = row_counts(Benchmark::Ssb, scale_factor).unwrap();
        assert_eq!(
            counts.iter().map(|(table, _)| *table).collect::<Vec<_>>(),
            TABLES
        );
        assert_eq!(
            counts.iter().map(|(_, rows)| *rows).collect::<Vec<_>>(),
            rows
        );
    }
}

#[test]
fn generated_rows_equal_the_reported_counts_and_schemas() {
    for (table, rows) in row_counts(Benchmark::Ssb, 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(Benchmark::Ssb, table).unwrap();
        assert_eq!(
            arrow::record_batch::RecordBatchReader::schema(&reader),
            schema
        );
        let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(generated as u64, rows, "{table}");
    }
}

#[test]
fn foreign_keys_resolve_and_orders_are_well_formed() {
    let dates: HashSet<i64> = int_column("date", 0.1, "d_datekey").into_iter().collect();
    assert_eq!(dates.len(), 2_556);
    let ranges = key_ranges(Benchmark::Ssb, 0.1).unwrap();
    let range = |column: &str| ranges.iter().find(|r| r.column == column).unwrap();
    for (column, parent) in [
        ("lo_custkey", "c_custkey"),
        ("lo_partkey", "p_partkey"),
        ("lo_suppkey", "s_suppkey"),
    ] {
        let (low, high) = (range(column).min, range(column).max);
        assert_eq!((low, high), (range(parent).min, range(parent).max));
        assert!(
            int_column("lineorder", 0.1, column)
                .iter()
                .all(|value| (low..=high).contains(value))
        );
    }
    assert!(
        int_column("lineorder", 0.1, "lo_orderdate")
            .iter()
            .all(|value| dates.contains(value))
    );
    let orders = int_column("lineorder", 0.1, "lo_orderkey");
    let lines = int_column("lineorder", 0.1, "lo_linenumber");
    let sparse = |order: i64| ((order >> 3) << 5) + (order & 7);
    assert_eq!((orders[0], lines[0]), (1, 1));
    let (mut order_index, mut order_lines, mut longest) = (1, 1, 1);
    for index in 1..orders.len() {
        if orders[index] == orders[index - 1] {
            assert_eq!(lines[index], lines[index - 1] + 1);
            order_lines += 1;
        } else {
            order_index += 1;
            assert_eq!((orders[index], lines[index]), (sparse(order_index), 1));
            order_lines = 1;
        }
        longest = longest.max(order_lines);
    }
    assert_eq!(longest, 7);
    let order_count = order_index;
    assert!(
        (148_500..=151_500).contains(&order_count),
        "orders {order_count}"
    );
}

#[test]
fn files_are_identical_for_one_four_and_default_threads() {
    for format in [Format::Tbl, Format::Parquet] {
        let runs: Vec<_> = [Some(1), Some(4), None]
            .into_iter()
            .map(|threads| {
                let dir = tempfile::tempdir().unwrap();
                digests(&request(dir.path(), format, threads, 1, 0.1, None))
            })
            .collect();
        assert_eq!(runs[0], runs[1], "{format:?}");
        assert_eq!(runs[0], runs[2], "{format:?}");
    }
}

#[test]
fn delimited_parts_concatenate_to_the_single_file() {
    let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(whole.path(), Format::Tbl, Some(2), 1, 0.1, None)).unwrap();
    let stats =
        write_benchmark(&request(split.path(), Format::Tbl, Some(3), 3, 0.1, None)).unwrap();
    for table in TABLES {
        let files: Vec<_> = stats.iter().filter(|stats| stats.table == table).collect();
        assert_eq!(files.len(), if table == "date" { 1 } else { 3 }, "{table}");
        let joined: Vec<u8> = files
            .iter()
            .flat_map(|file| fs::read(&file.path).unwrap())
            .collect();
        assert_eq!(
            joined,
            fs::read(whole.path().join(format!("{table}.tbl"))).unwrap(),
            "{table}"
        );
    }
}

#[test]
fn seeds_select_the_stream_and_default_to_42() {
    let hash = |seed| {
        let dir = tempfile::tempdir().unwrap();
        digests(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, seed))
    };
    assert_eq!(hash(None), hash(Some(42)));
    let other = hash(Some(43));
    let default = hash(None);
    assert_eq!(other[0], default[0]);
    for table in 1..TABLES.len() {
        assert_ne!(other[table], default[table], "{}", TABLES[table]);
    }
}

#[test]
fn unseeded_benchmarks_reject_a_seed() {
    let dir = tempfile::tempdir().unwrap();
    let mut request = request(dir.path(), Format::Tbl, Some(1), 1, 0.01, Some(7));
    request.benchmark = Benchmark::Tpch;
    assert_eq!(
        write_benchmark(&request).err(),
        Some(Error::SeedNotSupported(Benchmark::Tpch))
    );
}

#[test]
fn golden_hashes_are_frozen() {
    for (format, scale, name) in [
        (Format::Tbl, 0.01, "tbl"),
        (Format::Tbl, 0.1, "tbl"),
        (Format::Parquet, 0.01, "parquet"),
        (Format::Parquet, 0.1, "parquet"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let actual = digests(&request(dir.path(), format, Some(3), 1, scale, None));
        for (file, hash) in actual {
            let table = file.split('.').next().unwrap();
            let expected = GOLDENS
                .iter()
                .find(|(f, s, t, _)| *f == name && *s == scale.to_string() && *t == table)
                .unwrap_or_else(|| panic!("missing golden {name} {scale} {table}: {hash}"));
            assert_eq!(expected.3, hash, "{name} sf={scale} {table}");
        }
    }
}

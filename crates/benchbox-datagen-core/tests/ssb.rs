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
        "e1b716d78ec7b630bb7b3cddd26a034fcf42428de80323ca7706062f6f0a9448",
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
        "6fe0193c3ff6538e8235274e014b83860205faf9dc35dc04101b5abf3e241b3b",
    ),
    (
        "tbl",
        "0.1",
        "date",
        "e1b716d78ec7b630bb7b3cddd26a034fcf42428de80323ca7706062f6f0a9448",
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
        "67757d3aa2359f0142323e0970fb122ca34e57832c3fb60f23f3ccc1b220ad57",
    ),
    (
        "parquet",
        "0.01",
        "date",
        "daed21a9eb1b55192b3f3938589cf2545c8911463ac92934c77a159dc2e69e2d",
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
        "c5dfd5e7d0b5ef12e1b13f674e7dbe1bcf23c12f845c9a204fca16a1876b0a16",
    ),
    (
        "parquet",
        "0.1",
        "date",
        "daed21a9eb1b55192b3f3938589cf2545c8911463ac92934c77a159dc2e69e2d",
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
        "5bc961bdb87b776dc3b485b7e24c3b9260a25441ddb2f03b21728e554ce2165a",
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
        (0.01, [2_557, 300, 20, 2_000, 60_000]),
        (0.1, [2_557, 3_000, 200, 20_000, 600_000]),
        (1.0, [2_557, 30_000, 2_000, 200_000, 6_000_000]),
        (10.0, [2_557, 300_000, 20_000, 2_000_000, 60_000_000]),
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
    assert_eq!(dates.len(), 2_557);
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
    assert_eq!((orders[0], lines[0]), (2, 1));
    for index in 1..orders.len() {
        if orders[index] == orders[index - 1] {
            assert_eq!(lines[index], lines[index - 1] + 1);
        } else {
            assert_eq!((orders[index], lines[index]), (orders[index - 1] + 1, 1));
        }
    }
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

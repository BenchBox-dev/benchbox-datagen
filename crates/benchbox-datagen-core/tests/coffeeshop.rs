use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, Decimal128Array, Int64Array};
use benchbox_datagen_core::{
    Benchmark, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn coffeeshop() -> Benchmark {
    "coffeeshop".parse().unwrap()
}

const TABLES: [&str; 3] = ["dim_locations", "dim_products", "order_lines"];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "csv",
        "0.01",
        "dim_locations",
        "cdb5455eca01548c652d59d79d94976d994a0b96e815f8f010d7400193a716db",
    ),
    (
        "csv",
        "0.01",
        "dim_products",
        "f64029caeaefc4ca17a06328181db588b73dee2ea355f1df62ac8c33c90f13b1",
    ),
    (
        "csv",
        "0.01",
        "order_lines",
        "70a25adc9beb0ab31b64c6e185d7ae9f807287f2a4e9d72a9f9d7ff72bde015e",
    ),
    (
        "csv",
        "0.1",
        "dim_locations",
        "cdb5455eca01548c652d59d79d94976d994a0b96e815f8f010d7400193a716db",
    ),
    (
        "csv",
        "0.1",
        "dim_products",
        "f64029caeaefc4ca17a06328181db588b73dee2ea355f1df62ac8c33c90f13b1",
    ),
    (
        "csv",
        "0.1",
        "order_lines",
        "9641df3db633d4880f7eedfcd2d8057aee65bd54c33bdfe811db0c1ddcf8a61d",
    ),
    (
        "parquet",
        "0.01",
        "dim_locations",
        "ba4cf2fe27f566a69f79d45724d0b7fde463f75c1682cd0a34e4f64aa1449b72",
    ),
    (
        "parquet",
        "0.01",
        "dim_products",
        "c84f018b13816e39eb5a776e21666c715653ea358b803eec04c0f7f24c414641",
    ),
    (
        "parquet",
        "0.01",
        "order_lines",
        "25289b0bcdeed542c126bc5b25ad102942b8fced5b57621e1ef9a7c02a8e17c6",
    ),
    (
        "parquet",
        "0.1",
        "dim_locations",
        "ba4cf2fe27f566a69f79d45724d0b7fde463f75c1682cd0a34e4f64aa1449b72",
    ),
    (
        "parquet",
        "0.1",
        "dim_products",
        "c84f018b13816e39eb5a776e21666c715653ea358b803eec04c0f7f24c414641",
    ),
    (
        "parquet",
        "0.1",
        "order_lines",
        "18c7acab97914055408e2e10925b45285a74ab63f91b7642eeb3e662915da854",
    ),
];

const LEGACY_LF_SHA256: &[(&str, &str, &str)] = &[
    (
        "0.01",
        "dim_locations",
        "cdb5455eca01548c652d59d79d94976d994a0b96e815f8f010d7400193a716db",
    ),
    (
        "0.01",
        "dim_products",
        "f64029caeaefc4ca17a06328181db588b73dee2ea355f1df62ac8c33c90f13b1",
    ),
    (
        "0.01",
        "order_lines",
        "70a25adc9beb0ab31b64c6e185d7ae9f807287f2a4e9d72a9f9d7ff72bde015e",
    ),
    (
        "0.0003",
        "dim_locations",
        "cdb5455eca01548c652d59d79d94976d994a0b96e815f8f010d7400193a716db",
    ),
    (
        "0.0003",
        "dim_products",
        "f64029caeaefc4ca17a06328181db588b73dee2ea355f1df62ac8c33c90f13b1",
    ),
    (
        "0.0003",
        "order_lines",
        "ed976fc9d87958148191268e84c44db7b87da277b64538fd2753456a5d21bf6a",
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
        benchmark: coffeeshop(),
        scale_factor,
        format,
        compression: None,
        threads: threads.and_then(NonZeroUsize::new),
        parts,
        seed,
        output_dir: dir.to_path_buf(),
    }
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn digests(request: &OutputRequest) -> Vec<(String, String)> {
    write_benchmark(request)
        .unwrap()
        .into_iter()
        .map(|stats| {
            let bytes = fs::read(&stats.path).unwrap();
            (
                stats
                    .path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                sha256(&bytes),
            )
        })
        .collect()
}

fn table_request(table: &str, scale_factor: f64, seed: Option<u64>) -> TableRequest {
    TableRequest {
        benchmark: coffeeshop(),
        table: table.to_owned(),
        scale_factor,
        part: 1,
        parts: 1,
        seed,
    }
}

fn whole_table(table: &str, scale_factor: f64) -> arrow::record_batch::RecordBatch {
    part_table(table, scale_factor, 1, 1)
}

fn part_table(
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
) -> arrow::record_batch::RecordBatch {
    let request = TableRequest {
        part,
        parts,
        ..table_request(table, scale_factor, None)
    };
    let reader = record_batches(&request).unwrap();
    let schema = arrow::record_batch::RecordBatchReader::schema(&reader);
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

fn int_column(table: &str, scale_factor: f64, column: &str) -> Vec<i64> {
    let batch = whole_table(table, scale_factor);
    let array = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(array.null_count(), 0);
    array.values().to_vec()
}

#[test]
fn row_counts_are_closed_form_in_the_scale_factor() {
    let lines = |orders: u64| {
        let prefix = [1_u64; 60]
            .iter()
            .chain(&[2; 30])
            .chain(&[3; 5])
            .chain(&[4; 4])
            .chain(&[5; 1]);
        orders / 100 * 156 + prefix.take((orders % 100) as usize).sum::<u64>()
    };
    let expected = [
        (1e-9, 1),
        (0.0003, 2_550),
        (0.00012345, 1_049),
        (0.001, 8_500),
        (0.01, 85_000),
        (0.1, 850_000),
        (1.0, 8_500_000),
        (2.5, 21_250_000),
        (10.0, 85_000_000),
    ];
    for (scale_factor, orders) in expected {
        let counts = row_counts(coffeeshop(), scale_factor).unwrap();
        assert_eq!(
            counts.iter().map(|(table, _)| *table).collect::<Vec<_>>(),
            TABLES
        );
        assert_eq!(
            counts.iter().map(|(_, rows)| *rows).collect::<Vec<_>>(),
            [1_000, 26, lines(orders)],
            "sf={scale_factor}"
        );
    }
    assert_eq!(row_counts(coffeeshop(), 1.0).unwrap()[2].1, 13_260_000);
}

#[test]
fn generated_rows_equal_the_reported_counts_and_schemas() {
    for scale_factor in [0.0003, 0.01] {
        for (table, rows) in row_counts(coffeeshop(), scale_factor).unwrap() {
            let reader = record_batches(&table_request(table, scale_factor, None)).unwrap();
            let schema = table_schema(coffeeshop(), table).unwrap();
            assert_eq!(
                arrow::record_batch::RecordBatchReader::schema(&reader),
                schema
            );
            let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
            assert_eq!(generated as u64, rows, "{table}");
        }
    }
}

#[test]
fn keys_resolve_and_orders_follow_the_legacy_line_pattern() {
    let ranges = key_ranges(coffeeshop(), 0.1).unwrap();
    let range = |table: &str, column: &str| {
        let found = ranges
            .iter()
            .find(|r| r.table == table && r.column == column)
            .unwrap();
        (found.min, found.max)
    };
    assert_eq!(range("dim_locations", "record_id"), (1, 1_000));
    assert_eq!(range("dim_products", "record_id"), (1, 26));
    assert_eq!(
        range("order_lines", "location_record_id"),
        range("dim_locations", "record_id")
    );
    let locations: HashSet<i64> = int_column("dim_locations", 0.1, "record_id")
        .into_iter()
        .collect();
    let products: HashSet<i64> = int_column("dim_products", 0.1, "record_id")
        .into_iter()
        .collect();
    assert_eq!((locations.len(), products.len()), (1_000, 26));
    let orders = int_column("order_lines", 0.1, "order_id");
    let lines = int_column("order_lines", 0.1, "line_number");
    assert!(
        int_column("order_lines", 0.1, "location_record_id")
            .iter()
            .all(|id| locations.contains(id))
    );
    assert!(
        int_column("order_lines", 0.1, "product_record_id")
            .iter()
            .all(|id| products.contains(id))
    );
    let pattern: Vec<i64> = [(1, 60), (2, 30), (3, 5), (4, 4), (5, 1)]
        .iter()
        .flat_map(|&(lines, count)| std::iter::repeat_n(lines, count))
        .collect();
    let mut keys = HashSet::new();
    let (mut order, mut expected_lines, mut seen) = (0_i64, 0_i64, 0_i64);
    for (index, (&order_id, &line)) in orders.iter().zip(&lines).enumerate() {
        assert!(keys.insert((order_id, line)));
        if index == 0 || order_id != orders[index - 1] {
            assert_eq!(seen, expected_lines);
            order += 1;
            assert_eq!((order_id, line), (order, 1));
            expected_lines = pattern[((order - 1) % 100) as usize];
            seen = 1;
        } else {
            assert_eq!(line, seen + 1);
            seen += 1;
        }
    }
    assert_eq!(order, 85_000 * 10);
}

#[test]
fn prices_are_the_rounded_legacy_products() {
    let batch = whole_table("order_lines", 0.01);
    let decimal = |name: &str| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .values()
            .to_vec()
    };
    let (unit, total) = (decimal("unit_price"), decimal("total_price"));
    let quantity = int_column("order_lines", 0.01, "quantity");
    for row in 0..unit.len() {
        assert_eq!(total[row], unit[row] * i128::from(quantity[row]));
    }
    assert_eq!(unit[0], 437);
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
        assert_eq!(
            files.len(),
            if table == "order_lines" { 3 } else { 1 },
            "{table}"
        );
        let joined: Vec<u8> = files
            .iter()
            .flat_map(|file| fs::read(&file.path).unwrap())
            .collect();
        assert_eq!(
            joined,
            fs::read(whole.path().join(format!("{table}.csv"))).unwrap(),
            "{table}"
        );
    }
}

#[test]
fn the_seed_does_not_change_the_data_because_the_legacy_generator_is_unseeded() {
    let hash = |seed| {
        let dir = tempfile::tempdir().unwrap();
        digests(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, seed))
    };
    assert_eq!(hash(None), hash(Some(42)));
    assert_eq!(hash(None), hash(Some(43)));
}

#[test]
fn golden_hashes_are_frozen() {
    for (format, scale, name) in [
        (Format::Tbl, 0.01, "csv"),
        (Format::Tbl, 0.1, "csv"),
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

#[test]
fn delimited_output_equals_the_legacy_files_apart_from_line_endings() {
    for (scale, table, golden) in LEGACY_LF_SHA256 {
        let scale_factor: f64 = scale.parse().unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_benchmark(&request(
            dir.path(),
            Format::Tbl,
            Some(2),
            1,
            scale_factor,
            None,
        ))
        .unwrap();
        let bytes = fs::read(dir.path().join(format!("{table}.csv"))).unwrap();
        assert_eq!(sha256(&bytes), *golden, "sf={scale} {table}");
    }
}

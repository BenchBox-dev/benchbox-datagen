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

fn ssb() -> Benchmark {
    "ssb".parse().unwrap()
}

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
        "0536755eb97daae0745a94e8fc5a56efd30b3bdecbb066c2fc7c578660021a2a",
    ),
    (
        "tbl",
        "0.01",
        "supplier",
        "64ec3188297a2b48d2eea1fd263ce12d1fb8ade397027ac2c3d121e5950cbe17",
    ),
    (
        "tbl",
        "0.01",
        "part",
        "039990d2bc54034c23db10e6b946c6b3c1f0ee035828f7c08a65125a711aad42",
    ),
    (
        "tbl",
        "0.01",
        "lineorder",
        "a913882b75645780ba3aea1f12b41a8c2ecc08bb7fe7257630bb78935e29c2b8",
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
        "661ae2b22d9349e2f912de23d72864f9415b2a2f0fd6ff4aa7a9fb2cf95d5a9f",
    ),
    (
        "tbl",
        "0.1",
        "supplier",
        "685cd5fbcc025c7fe2c69b07e11d5a0c13a87c332be051ea7e8317aa5cde4b84",
    ),
    (
        "tbl",
        "0.1",
        "part",
        "5aac5dfa6357ba574bb456ce464972a0434fac8990e377a8726301f9e5985e54",
    ),
    (
        "tbl",
        "0.1",
        "lineorder",
        "244b5d6a549521d2436bc74ecd49593af9b17910e6c330f5c8ad49ca26037c7a",
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
        "2f59a4621d95858be10464a7b45dda460b6ec3a81e10c094efcafad72d671e28",
    ),
    (
        "parquet",
        "0.01",
        "supplier",
        "099764086e402c7131a1583a0141d3629d760057b20f2c84c85d234bd7ff92a5",
    ),
    (
        "parquet",
        "0.01",
        "part",
        "29c964cf2f9f797372adf9bdf3880f9154a59410d9c87ee9682d259f67fe5236",
    ),
    (
        "parquet",
        "0.01",
        "lineorder",
        "3da35a0ececfbcef97c9fde65f216676d785f2244d589582f3d36d3b44a2f834",
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
        "aa8449cd5266418b4d46fa514361272c0889ed53d3122d7e938bf603aab6f59e",
    ),
    (
        "parquet",
        "0.1",
        "supplier",
        "c9cfe7c0b590a46b30614d674be46f59a00d1bfdd075841d2f4fc65652206c9f",
    ),
    (
        "parquet",
        "0.1",
        "part",
        "b889aadfc08e84ea5eed6419d76bb9f9cfe89ee166049cb574fb38730b3c7b3d",
    ),
    (
        "parquet",
        "0.1",
        "lineorder",
        "63e60b36ddbe2b317353cf4360f7219c27901b8c6e9be4f97170368a8492d615",
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
        benchmark: ssb(),
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
        benchmark: ssb(),
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
        let counts = row_counts(ssb(), scale_factor).unwrap();
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
    for (table, rows) in row_counts(ssb(), 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(ssb(), table).unwrap();
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
    let ranges = key_ranges(ssb(), 0.1).unwrap();
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

fn whole_table(table: &str, scale_factor: f64) -> arrow::record_batch::RecordBatch {
    let batches: Vec<_> = record_batches(&table_request(table, scale_factor, None))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
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
    let schema = reader.schema();
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

#[test]
fn generate_table_parts_equal_the_matching_slice_of_the_whole_table() {
    let parts = 3;
    for table in ["customer", "supplier", "part", "lineorder"] {
        let whole = whole_table(table, 0.1);
        let rows = whole.num_rows() as u128;
        for part in 1..=parts {
            let start = (rows * (part as u128 - 1) / parts as u128) as usize;
            let end = (rows * part as u128 / parts as u128) as usize;
            assert_eq!(
                part_table(table, 0.1, part, parts),
                whole.slice(start, end - start),
                "{table} part {part}"
            );
        }
    }
    let whole = whole_table("date", 0.1);
    assert_eq!(part_table("date", 0.1, 1, 3), whole);
    assert_eq!(part_table("date", 0.1, 2, 3).num_rows(), 0);
}

#[test]
fn parquet_parts_hold_the_rows_of_the_single_file() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(
        whole.path(),
        Format::Parquet,
        Some(2),
        1,
        0.1,
        None,
    ))
    .unwrap();
    let stats = write_benchmark(&request(
        split.path(),
        Format::Parquet,
        Some(3),
        3,
        0.1,
        None,
    ))
    .unwrap();
    let read = |path: &Path| {
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        reader.map(Result::unwrap).collect::<Vec<_>>()
    };
    for table in TABLES {
        let files: Vec<_> = stats.iter().filter(|stats| stats.table == table).collect();
        assert_eq!(files.len(), if table == "date" { 1 } else { 3 }, "{table}");
        let expected = read(&whole.path().join(format!("{table}.parquet")));
        let schema = expected[0].schema();
        let expected = arrow::compute::concat_batches(&schema, &expected).unwrap();
        let joined: Vec<_> = files.iter().flat_map(|file| read(&file.path)).collect();
        assert_eq!(
            arrow::compute::concat_batches(&schema, &joined).unwrap(),
            expected,
            "{table}"
        );
        assert_eq!(
            files.iter().map(|file| file.rows).sum::<u64>(),
            expected.num_rows() as u64
        );
    }
}

use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, FixedSizeListArray, Float32Array, Int64Array, StringArray};
use benchbox_datagen_core::{
    Benchmark, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn vector_search() -> Benchmark {
    "vector_search".parse().unwrap()
}

const TABLES: [&str; 2] = ["vectors", "vector_queries"];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "vectors",
        "4c6aa0f0fe110dc7823ac89ca2afc44e3b92181c394615f67ef3fccf9e6cd092",
    ),
    (
        "tbl",
        "0.01",
        "vector_queries",
        "19e7f8e71bad82c199225f5e29ba80b72a11190c0c1b82fa446aba9636a8a5ec",
    ),
    (
        "tbl",
        "0.1",
        "vectors",
        "ede7193d426736a3224571d1c01e9f3c1a018e34482fe2866a151cbf71e5b2d1",
    ),
    (
        "tbl",
        "0.1",
        "vector_queries",
        "19e7f8e71bad82c199225f5e29ba80b72a11190c0c1b82fa446aba9636a8a5ec",
    ),
    (
        "parquet",
        "0.01",
        "vectors",
        "be005191aa46fe01e154601334f4c931ef874e34bee01442ff749f84e49328d1",
    ),
    (
        "parquet",
        "0.01",
        "vector_queries",
        "c858e0c3099f168eebf869a4e2af6baa773bcd0d2e4fce88abfddb662592a37e",
    ),
    (
        "parquet",
        "0.1",
        "vectors",
        "3977045266c223664a95b4759c05f24c4cdd2ea7a644c0216fb517e60da4cdca",
    ),
    (
        "parquet",
        "0.1",
        "vector_queries",
        "c858e0c3099f168eebf869a4e2af6baa773bcd0d2e4fce88abfddb662592a37e",
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
        benchmark: vector_search(),
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
        benchmark: vector_search(),
        table: table.to_owned(),
        scale_factor,
        part: 1,
        parts: 1,
        seed,
    }
}

fn whole_table(table: &str, scale_factor: f64) -> arrow::record_batch::RecordBatch {
    let batches: Vec<_> = record_batches(&table_request(table, scale_factor, None))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

#[test]
fn row_counts_follow_the_legacy_scale_rules() {
    let expected = [
        (0.0001, [100, 100]),
        (0.01, [10_000, 100]),
        (0.1, [100_000, 100]),
        (1.0, [1_000_000, 100]),
        (2.5, [2_500_000, 100]),
        (10.0, [10_000_000, 100]),
        (0.0000001, [1, 100]),
    ];
    for (scale_factor, rows) in expected {
        let counts = row_counts(vector_search(), scale_factor).unwrap();
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
    for (table, rows) in row_counts(vector_search(), 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(vector_search(), table).unwrap();
        assert_eq!(
            arrow::record_batch::RecordBatchReader::schema(&reader),
            schema
        );
        let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(generated as u64, rows, "{table}");
    }
}

#[test]
fn key_ranges_are_exact() {
    let ranges = key_ranges(vector_search(), 0.01).unwrap();
    let range = |column: &str| ranges.iter().find(|r| r.column == column).unwrap();
    assert_eq!((range("id").min, range("id").max), (1, 10_000));
    assert_eq!((range("query_id").min, range("query_id").max), (1, 100));
}

#[test]
fn keys_categories_and_unit_vectors_follow_the_legacy_rules() {
    let vectors = whole_table("vectors", 0.01);
    let ids = vectors
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let categories = vectors
        .column_by_name("category")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let docs = vectors
        .column_by_name("doc_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..vectors.num_rows() {
        let id = row as i64 + 1;
        assert_eq!(ids.value(row), id);
        assert_eq!(
            categories.value(row),
            format!("category_{:02}", id % 10 + 1)
        );
        assert_eq!(docs.value(row), format!("doc_{id}"));
    }
    for (table, column) in [("vectors", "embedding"), ("vector_queries", "query_vector")] {
        let batch = whole_table(table, 0.01);
        let list = batch
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(list.value_length(), 128);
        assert_eq!(list.null_count(), 0);
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for row in 0..list.len() {
            let norm: f64 = (0..128)
                .map(|index| f64::from(values.value(row * 128 + index)).powi(2))
                .sum();
            assert!((norm - 1.0).abs() < 1e-5, "{table} row {row}: {norm}");
        }
    }
    let queries = whole_table("vector_queries", 0.01);
    let query_ids = queries
        .column_by_name("query_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        query_ids.values().to_vec(),
        (1..=100_i64).collect::<Vec<_>>()
    );
}

#[test]
fn files_are_identical_for_one_four_and_default_threads() {
    for format in [Format::Tbl, Format::Parquet] {
        let runs: Vec<_> = [Some(1), Some(4), None]
            .into_iter()
            .map(|threads| {
                let dir = tempfile::tempdir().unwrap();
                digests(&request(dir.path(), format, threads, 1, 0.01, None))
            })
            .collect();
        assert_eq!(runs[0], runs[1], "{format:?}");
        assert_eq!(runs[0], runs[2], "{format:?}");
    }
}

#[test]
fn delimited_files_are_pipe_csv_with_one_header_and_bracketed_vectors() {
    let dir = tempfile::tempdir().unwrap();
    write_benchmark(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, None)).unwrap();
    let vectors = fs::read_to_string(dir.path().join("vectors.csv")).unwrap();
    let mut lines = vectors.lines();
    assert_eq!(lines.next().unwrap(), "id|embedding|category|doc_id");
    let first: Vec<&str> = lines.next().unwrap().split('|').collect();
    assert_eq!(first[0], "1");
    assert_eq!(first[2], "category_02");
    assert_eq!(first[3], "doc_1");
    assert!(first[1].starts_with('[') && first[1].ends_with(']'));
    assert_eq!(first[1].split(',').count(), 128);
    assert_eq!(vectors.lines().count(), 10_001);
    let queries = fs::read_to_string(dir.path().join("vector_queries.csv")).unwrap();
    assert_eq!(queries.lines().next().unwrap(), "query_id|query_vector");
    assert_eq!(queries.lines().count(), 101);
}

#[test]
fn delimited_parts_concatenate_to_the_single_file() {
    let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(whole.path(), Format::Tbl, Some(2), 1, 0.01, None)).unwrap();
    let stats =
        write_benchmark(&request(split.path(), Format::Tbl, Some(3), 3, 0.01, None)).unwrap();
    for table in TABLES {
        let files: Vec<_> = stats.iter().filter(|stats| stats.table == table).collect();
        assert_eq!(
            files.len(),
            if table == "vector_queries" { 1 } else { 3 },
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
fn seeds_select_the_stream_and_default_to_42() {
    let hash = |seed| {
        let dir = tempfile::tempdir().unwrap();
        digests(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, seed))
    };
    assert_eq!(hash(None), hash(Some(42)));
    let (other, default) = (hash(Some(43)), hash(None));
    for table in 0..TABLES.len() {
        assert_ne!(other[table], default[table], "{}", TABLES[table]);
    }
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

#[test]
fn generate_table_parts_equal_the_matching_slice_of_the_whole_table() {
    let parts = 3;
    let whole = whole_table("vectors", 0.01);
    let rows = whole.num_rows() as u128;
    for part in 1..=parts {
        let request = TableRequest {
            part,
            parts,
            ..table_request("vectors", 0.01, None)
        };
        let reader = record_batches(&request).unwrap();
        let schema = reader.schema();
        let batches: Vec<_> = reader.map(Result::unwrap).collect();
        let joined = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let start = (rows * (part as u128 - 1) / parts as u128) as usize;
        let end = (rows * part as u128 / parts as u128) as usize;
        assert_eq!(joined, whole.slice(start, end - start), "part {part}");
    }
}

use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, Int64Array};
use benchbox_datagen_core::{
    Benchmark, Error, Format, OutputRequest, TableRequest, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn amplab() -> Benchmark {
    "amplab".parse().unwrap()
}

const TABLES: [&str; 3] = ["rankings", "documents", "uservisits"];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "rankings",
        "d6e1837d3089e68d82a8b070ac3c8d6b3a84d4191bd3eb23c27db11875847f05",
    ),
    (
        "tbl",
        "0.01",
        "documents",
        "0e9c6a1270752829fd9e0a999c1cb03599094a532e8351e656112b5d638a2a3b",
    ),
    (
        "tbl",
        "0.01",
        "uservisits",
        "81cff07b2a926cd65feff773f2ed241b2971a53ea30a49cec7f6c5d595684607",
    ),
    (
        "tbl",
        "0.1",
        "rankings",
        "8ea5d56756bb8ca055df55a0e6a7aa50dd093e3dd0fa310958d29bfbb5bb888a",
    ),
    (
        "tbl",
        "0.1",
        "documents",
        "334fe5de1d6beb9d746e6ce4852530f3a891476eb474ef85c8bac1f7f8d92a1a",
    ),
    (
        "tbl",
        "0.1",
        "uservisits",
        "1decf70b642dc25dde01a0ca6710aafe1036cc6e6b1858bcb4ae53b3fceea3f9",
    ),
    (
        "parquet",
        "0.01",
        "rankings",
        "c12941526910e01dfdcc405b7d71ec08860d3858e5fb5e7a147d5e48917cf229",
    ),
    (
        "parquet",
        "0.01",
        "documents",
        "df7a89cf4c1b129f697d9565ed494a5cb4a413807c809ee06c20282fcd57ec9f",
    ),
    (
        "parquet",
        "0.01",
        "uservisits",
        "ceacccd6c77b9f0336c30add79ccc73e0c7e11c84460a9a0b6b7a897aeb62f47",
    ),
    (
        "parquet",
        "0.1",
        "rankings",
        "95ba789bd643207af5cd9557075381ad1e4c27a3ae882836ebfd3bd2590522c3",
    ),
    (
        "parquet",
        "0.1",
        "documents",
        "63bebea1c992217153a1094beb2c8f890a925580115a981da040b50d15e88f45",
    ),
    (
        "parquet",
        "0.1",
        "uservisits",
        "8a0423d032f6ae2f9bc77e56b64256ee617d1e20472f09882c2ef9f4726324fc",
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
        benchmark: amplab(),
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
        benchmark: amplab(),
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
fn row_counts_follow_the_legacy_scale_rules() {
    let expected = [
        (0.01, [2_500, 1_250, 25_000]),
        (0.1, [25_000, 12_500, 250_000]),
        (1.0, [250_000, 125_000, 2_500_000]),
        (10.0, [2_500_000, 1_250_000, 25_000_000]),
        (0.000_001, [0, 0, 2]),
    ];
    for (scale_factor, rows) in expected {
        let counts = row_counts(amplab(), scale_factor).unwrap();
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
    for (table, rows) in row_counts(amplab(), 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(amplab(), table).unwrap();
        assert_eq!(
            arrow::record_batch::RecordBatchReader::schema(&reader),
            schema
        );
        let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(generated as u64, rows, "{table}");
    }
}

#[test]
fn ranks_and_durations_stay_in_range_and_page_urls_are_unique() {
    let ranks = int_column("rankings", 0.1, "pageRank");
    assert!(ranks.iter().all(|rank| (10..=10_000).contains(rank)));
    assert!(ranks.iter().filter(|rank| **rank < 20).count() > ranks.len() / 2);
    assert!(
        int_column("rankings", 0.1, "avgDuration")
            .iter()
            .all(|value| (30..=600).contains(value))
    );
    let mut urls = HashSet::new();
    for batch in record_batches(&table_request("rankings", 0.1, None)).unwrap() {
        let batch = batch.unwrap();
        let column = batch
            .column_by_name("pageURL")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .clone();
        for value in column.iter() {
            assert!(urls.insert(value.unwrap().to_owned()));
        }
    }
    assert_eq!(urls.len(), 25_000);
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
        assert_eq!(files.len(), 3, "{table}");
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
    for table in 0..TABLES.len() {
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
    for table in TABLES {
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
        assert_eq!(files.len(), 3, "{table}");
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

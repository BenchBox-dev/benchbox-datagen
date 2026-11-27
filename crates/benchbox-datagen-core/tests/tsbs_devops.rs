use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, StringArray};
use benchbox_datagen_core::{
    Benchmark, Error, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn tsbs() -> Benchmark {
    "tsbs_devops".parse().unwrap()
}

const TABLES: [&str; 5] = ["tags", "cpu", "mem", "disk", "net"];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "tags",
        "6b675d4af7be8375adb3afd3fc6f57ca5924f2aa47f42b29feb2a8aa3387ece7",
    ),
    (
        "tbl",
        "0.01",
        "cpu",
        "8fc69126c7cdcabb17c007e4584b65e1b9dd5eb231b86dd8775ee1dd789ccaab",
    ),
    (
        "tbl",
        "0.01",
        "mem",
        "413ec5122270ed8da2e4e7af6ec1afabf7cd735c8de47176ab8c6bfda8a8c5ab",
    ),
    (
        "tbl",
        "0.01",
        "disk",
        "af6f3b161e04e48be56dac198a564491f55574e4c8e20f51da6fc58364e6b98e",
    ),
    (
        "tbl",
        "0.01",
        "net",
        "fdb25f7ed4656a5740c9213fc779aca76489f6ca37bf4ae964b4936aa516ffcb",
    ),
    (
        "tbl",
        "0.2",
        "tags",
        "f244c1697c5f62baefd2ea9d83eeec9b65035ba0f8ec3a2d08450e5f30bf24d9",
    ),
    (
        "tbl",
        "0.2",
        "cpu",
        "333f290b60a0bb45467731755f7bb5e2bd59f1f185049688892e33781d73a6c7",
    ),
    (
        "tbl",
        "0.2",
        "mem",
        "9a3a620c4303cc433314d4f8e1aa9e3755dff6b2e341b79a64eb7ff0bfdc5e39",
    ),
    (
        "tbl",
        "0.2",
        "disk",
        "65deba0e37432a31cb4c8139f389f7f6d3710736dde80bccc1424b1432976d70",
    ),
    (
        "tbl",
        "0.2",
        "net",
        "c4f962f9708ebb2d0300dcbe0caedbce329ebc954f00561a2da2efa34345f400",
    ),
    (
        "parquet",
        "0.01",
        "tags",
        "4280989eb2737996a9fc267102f01c4e358506b8f902936936502d4969fd7529",
    ),
    (
        "parquet",
        "0.01",
        "cpu",
        "07ed3307c6ed999fc563fc794d41c11f3b8f72ccde797b8ff311ace356bd27c1",
    ),
    (
        "parquet",
        "0.01",
        "mem",
        "41881d259f8ed631755426089f8909bc88a34a2a7c8465dd8bb5a4d357bc1ec5",
    ),
    (
        "parquet",
        "0.01",
        "disk",
        "d0fe69f4086ceeed20c520474eb3aa0eba0385c25133bfc9af635764b7b52917",
    ),
    (
        "parquet",
        "0.01",
        "net",
        "a2f9a70b6dc721c757c6c8613881f1ed87331ca3150f5117c81091f775ddb922",
    ),
    (
        "parquet",
        "0.2",
        "tags",
        "f8673544f5b71833df4905fdc916cd956e1e970c672db180afdd644dd7d39550",
    ),
    (
        "parquet",
        "0.2",
        "cpu",
        "8f9cd064e5e0f2d59aef09312e809fbcbb3ff08ced1bc1a93578946075806079",
    ),
    (
        "parquet",
        "0.2",
        "mem",
        "b5e0b7c91f89ec2cc51f65a781b13b5bee0dd83541f91f451e586b7384869df3",
    ),
    (
        "parquet",
        "0.2",
        "disk",
        "5296fedf4dbe29955a0e891c59736d6d97ce0237526f6f9b4b8181cf26257aae",
    ),
    (
        "parquet",
        "0.2",
        "net",
        "ab6455587f915d814c1d03f3a312ea283c49093a88aff47b87ab899b8763bd97",
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
        benchmark: tsbs(),
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
        benchmark: tsbs(),
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

fn strings(batch: &arrow::record_batch::RecordBatch, column: &str) -> Vec<String> {
    let array = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(array.null_count(), 0);
    array
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

#[test]
fn row_counts_follow_the_host_and_timestamp_rules_at_every_gate_scale() {
    let expected = [
        (0.01, [10, 172_800, 172_800, 345_600, 345_600]),
        (0.1, [10, 172_800, 172_800, 345_600, 345_600]),
        (1.0, [100, 1_728_000, 1_728_000, 3_456_000, 3_456_000]),
        (
            10.0,
            [1_000, 17_280_000, 17_280_000, 34_560_000, 34_560_000],
        ),
        (0.25, [25, 432_000, 432_000, 864_000, 864_000]),
        (2.5, [250, 4_320_000, 4_320_000, 8_640_000, 8_640_000]),
        (
            100.0,
            [10_000, 172_800_000, 172_800_000, 345_600_000, 345_600_000],
        ),
    ];
    for (scale_factor, rows) in expected {
        let counts = row_counts(tsbs(), scale_factor).unwrap();
        assert_eq!(
            counts.iter().map(|(table, _)| *table).collect::<Vec<_>>(),
            TABLES
        );
        assert_eq!(
            counts.iter().map(|(_, rows)| *rows).collect::<Vec<_>>(),
            rows,
            "sf={scale_factor}"
        );
    }
}

#[test]
fn generated_rows_equal_the_reported_counts_and_schemas() {
    for (table, rows) in row_counts(tsbs(), 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(tsbs(), table).unwrap();
        assert_eq!(
            arrow::record_batch::RecordBatchReader::schema(&reader),
            schema
        );
        let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(generated as u64, rows, "{table}");
    }
}

#[test]
fn hostnames_resolve_and_each_series_is_complete_and_ordered() {
    assert!(key_ranges(tsbs(), 0.01).unwrap().is_empty());
    let tags = whole_table("tags", 0.01);
    let hosts = strings(&tags, "hostname");
    assert_eq!(
        hosts,
        (0..10)
            .map(|host| format!("host_{host}"))
            .collect::<Vec<_>>()
    );
    let known: HashSet<&String> = hosts.iter().collect();
    for (table, per_host, discriminator) in [
        ("cpu", 1, None),
        ("mem", 1, None),
        ("disk", 2, Some("device")),
        ("net", 2, Some("interface")),
    ] {
        let data = whole_table(table, 0.01);
        let names = strings(&data, "hostname");
        assert!(names.iter().all(|name| known.contains(name)), "{table}");
        let series: Vec<String> = discriminator
            .map(|column| strings(&data, column))
            .unwrap_or_else(|| vec![String::new(); names.len()]);
        let stamps = data
            .column_by_name("time")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        let keys: HashSet<(i64, &String, &String)> = (0..names.len())
            .map(|row| (stamps.value(row), &names[row], &series[row]))
            .collect();
        assert_eq!(keys.len(), names.len(), "{table} keys are unique");
        assert_eq!(names.len(), 10 * per_host * 17_280, "{table}");
        assert_eq!(stamps.value(0), 1_704_067_200_000_000);
        assert_eq!(
            stamps.value(17_279),
            1_704_067_200_000_000 + 17_279 * 10_000_000
        );
        assert_eq!(
            names.iter().collect::<HashSet<_>>().len(),
            10,
            "{table} covers every host"
        );
    }
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
fn delimited_parts_concatenate_to_the_single_file() {
    let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(whole.path(), Format::Tbl, Some(2), 1, 0.01, None)).unwrap();
    let stats =
        write_benchmark(&request(split.path(), Format::Tbl, Some(3), 3, 0.01, None)).unwrap();
    for table in TABLES {
        let files: Vec<_> = stats.iter().filter(|stats| stats.table == table).collect();
        assert_eq!(files.len(), 3, "{table}");
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
fn delimited_text_matches_the_legacy_csv_layout() {
    let dir = tempfile::tempdir().unwrap();
    write_benchmark(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, None)).unwrap();
    let header = |table: &str| {
        let text = fs::read_to_string(dir.path().join(format!("{table}.csv"))).unwrap();
        let mut lines = text.lines();
        (
            lines.next().unwrap().to_owned(),
            lines.next().unwrap().to_owned(),
        )
    };
    assert_eq!(
        header("cpu").0,
        "time,hostname,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice"
    );
    let row = header("cpu").1;
    let fields: Vec<&str> = row.split(',').collect();
    assert_eq!(fields.len(), 12);
    assert_eq!(fields[0], "2024-01-01 00:00:00");
    assert_eq!(fields[1], "host_0");
    for field in &fields[2..] {
        let (whole, fraction) = field.split_once('.').unwrap();
        assert!(whole.parse::<u32>().is_ok());
        assert_eq!(fraction.len(), 2, "{field}");
    }
    assert!(header("tags").1.starts_with("host_0,us-east-1,dc1,rack-a,"));
    assert!(header("net").1.contains(",eth0,"));
}

#[test]
fn golden_hashes_are_frozen() {
    for (format, scale, name) in [
        (Format::Tbl, 0.01, "tbl"),
        (Format::Tbl, 0.2, "tbl"),
        (Format::Parquet, 0.01, "parquet"),
        (Format::Parquet, 0.2, "parquet"),
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
        let whole = whole_table(table, 0.01);
        let rows = whole.num_rows() as u128;
        for part in 1..=parts {
            let start = (rows * (part as u128 - 1) / parts as u128) as usize;
            let end = (rows * part as u128 / parts as u128) as usize;
            assert_eq!(
                part_table(table, 0.01, part, parts),
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
        0.01,
        None,
    ))
    .unwrap();
    let stats = write_benchmark(&request(
        split.path(),
        Format::Parquet,
        Some(3),
        3,
        0.01,
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

use std::collections::HashSet;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use benchbox_datagen_core::{
    Benchmark, Error, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn clickbench() -> Benchmark {
    "clickbench".parse().unwrap()
}

const GOLDENS: &[(&str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "7de1504ae8ca4b6090c6949063daf53ccc9952d9e425e81621c14465fcaf60f9",
    ),
    (
        "tbl",
        "0.1",
        "4d8cb9abb74589dec427624069370fea0eb083c97a85dae1658e5e1db3bf6b4c",
    ),
    (
        "parquet",
        "0.01",
        "7e6cde7f26c6517d333c08bb0f56e1f584ba610b6780ccdf7bebcea401440002",
    ),
    (
        "parquet",
        "0.1",
        "2f913860ff641d1245794a34586b2459dbd787538e1220688000eca4f8dc1052",
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
        benchmark: clickbench(),
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

fn table_request(scale_factor: f64, part: i32, parts: i32, seed: Option<u64>) -> TableRequest {
    TableRequest {
        benchmark: clickbench(),
        table: "hits".to_owned(),
        scale_factor,
        part,
        parts,
        seed,
    }
}

fn table(scale_factor: f64, part: i32, parts: i32, seed: Option<u64>) -> RecordBatch {
    let reader = record_batches(&table_request(scale_factor, part, parts, seed)).unwrap();
    let schema = arrow::record_batch::RecordBatchReader::schema(&reader);
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

fn ints(batch: &RecordBatch, column: &str) -> Vec<i64> {
    let array = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(array.null_count(), 0);
    array.values().to_vec()
}

fn texts(batch: &RecordBatch, column: &str) -> Vec<String> {
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
fn row_counts_are_a_closed_form_of_the_scale_factor() {
    for (scale_factor, rows) in [
        (0.0001, 100),
        (0.01, 10_000),
        (0.1, 100_000),
        (1.0, 1_000_000),
        (10.0, 10_000_000),
        (2.5, 2_500_000),
        (0.0000001, 1),
    ] {
        assert_eq!(
            row_counts(clickbench(), scale_factor).unwrap(),
            vec![("hits", rows)]
        );
    }
}

#[test]
fn generated_rows_equal_the_reported_counts_and_schema() {
    let reader = record_batches(&table_request(0.01, 1, 1, None)).unwrap();
    let schema = table_schema(clickbench(), "hits").unwrap();
    assert_eq!(schema.fields().len(), 105);
    assert_eq!(
        arrow::record_batch::RecordBatchReader::schema(&reader),
        schema
    );
    let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
    assert_eq!(generated, 10_000);
    assert!(key_ranges(clickbench(), 0.01).unwrap().is_empty());
}

#[test]
fn values_stay_inside_the_legacy_domains() {
    let batch = table(0.1, 1, 1, None);
    let within = |column: &str, low: i64, high: i64| {
        assert!(
            ints(&batch, column)
                .iter()
                .all(|value| (low..=high).contains(value)),
            "{column}"
        );
    };
    within("WatchID", 1, 10_000_000);
    within("CounterID", 1, 1_000);
    within("UserID", 1, 1_000_000);
    within("RegionID", 1, 1_000);
    within("ClientIP", 1, 4_294_967_295);
    within("TraficSourceID", -1, 10);
    within("ClientTimeZone", -12, 12);
    within("Age", 18, 65);
    within("RefererHash", 1, i64::MAX);
    within("CLID", 0, 2_147_483_647);
    let widths = ints(&batch, "ResolutionWidth");
    assert_eq!(widths, ints(&batch, "WindowClientWidth"));
    assert_eq!(ints(&batch, "RegionID"), ints(&batch, "URLRegionID"));
    assert_eq!(ints(&batch, "IsMobile"), ints(&batch, "MobilePhone"));
    assert_eq!(texts(&batch, "URL"), texts(&batch, "OriginalURL"));
    let allowed: HashSet<_> = [1920, 1366, 1280, 1440, 1024, 1600].into_iter().collect();
    assert!(widths.iter().all(|width| allowed.contains(width)));
    let mobile = ints(&batch, "IsMobile");
    let models = texts(&batch, "MobilePhoneModel");
    assert!(
        mobile
            .iter()
            .zip(&models)
            .all(|(mobile, model)| *mobile == 1 || model.is_empty())
    );
    let phrases = texts(&batch, "SearchPhrase");
    let non_empty = phrases.iter().filter(|phrase| !phrase.is_empty()).count();
    assert!((24_000..=26_000).contains(&non_empty), "{non_empty}");
    assert!(
        texts(&batch, "PageCharset")
            .iter()
            .all(|value| value == "UTF-8")
    );
    assert!(
        texts(&batch, "FromTag")
            .iter()
            .all(|value| value.is_empty())
    );
}

#[test]
fn event_times_cover_july_2013_and_agree_with_the_event_date() {
    let batch = table(0.1, 1, 1, None);
    let times = batch
        .column_by_name("EventTime")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let dates = batch
        .column_by_name("EventDate")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    let first = 15_887;
    for (time, date) in times.values().iter().zip(dates.values()) {
        assert_eq!(*time / 86_400_000_000, i64::from(*date));
        assert!((first..=first + 30).contains(date));
        assert_eq!(*time % 1_000_000, 0);
    }
    for column in ["ClientEventTime", "LocalEventTime"] {
        assert_eq!(
            batch.column_by_name(column).unwrap().as_ref(),
            batch.column_by_name("EventTime").unwrap().as_ref()
        );
    }
    let days: HashSet<_> = dates.values().iter().copied().collect();
    assert_eq!(days.len(), 31);
}

#[test]
fn delimited_output_follows_the_legacy_csv_form() {
    let dir = tempfile::tempdir().unwrap();
    write_benchmark(&request(dir.path(), Format::Tbl, Some(1), 1, 0.0001, None)).unwrap();
    let text = fs::read_to_string(dir.path().join("hits.csv")).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 100);
    assert!(text.ends_with('\n'));
    for line in lines {
        let fields: Vec<_> = line.split('|').collect();
        assert_eq!(fields.len(), 105);
        assert_eq!(fields[3], "1");
        assert_eq!(fields[10], "0");
        assert_eq!(fields[25], "\"\"");
        assert_eq!(fields[50], "UTF-8");
        assert_eq!(fields[4].len(), 19);
        assert_eq!(&fields[4][..8], "2013-07-");
        assert_eq!(fields[5].len(), 10);
        assert_eq!(&fields[4][..10], fields[5]);
        assert_eq!(fields[4], fields[45]);
        assert_eq!(fields[4], fields[64]);
        assert!(fields[104].parse::<i64>().is_ok());
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
    assert_eq!(stats.len(), 3);
    let joined: Vec<u8> = stats
        .iter()
        .flat_map(|file| fs::read(&file.path).unwrap())
        .collect();
    assert_eq!(joined, fs::read(whole.path().join("hits.csv")).unwrap());
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
    let expected = read(&whole.path().join("hits.parquet"));
    let schema = expected[0].schema();
    let expected = arrow::compute::concat_batches(&schema, &expected).unwrap();
    let joined: Vec<_> = stats.iter().flat_map(|file| read(&file.path)).collect();
    assert_eq!(
        arrow::compute::concat_batches(&schema, &joined).unwrap(),
        expected
    );
    assert_eq!(
        stats.iter().map(|file| file.rows).sum::<u64>(),
        expected.num_rows() as u64
    );
}

#[test]
fn generate_table_parts_equal_the_matching_slice_of_the_whole_table() {
    let parts = 3;
    let whole = table(0.1, 1, 1, None);
    let rows = whole.num_rows() as u128;
    for part in 1..=parts {
        let start = (rows * (part as u128 - 1) / parts as u128) as usize;
        let end = (rows * part as u128 / parts as u128) as usize;
        assert_eq!(
            table(0.1, part, parts, None),
            whole.slice(start, end - start),
            "part {part}"
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
    assert_ne!(hash(Some(43)), hash(None));
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
        assert_eq!(actual.len(), 1);
        let hash = &actual[0].1;
        let expected = GOLDENS
            .iter()
            .find(|(f, s, _)| *f == name && *s == scale.to_string())
            .unwrap_or_else(|| panic!("missing golden {name} {scale}: {hash}"));
        assert_eq!(expected.2, hash, "{name} sf={scale}");
    }
}

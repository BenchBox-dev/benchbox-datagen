use std::collections::HashMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use benchbox_datagen_core::{
    Benchmark, Error, Format, OutputRequest, TableRequest, key_ranges, record_batches, row_counts,
    table_schema, write_benchmark,
};
use sha2::{Digest, Sha256};

fn tpch_skew() -> Benchmark {
    "tpch_skew".parse().unwrap()
}

const TABLES: [&str; 8] = [
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

const GOLDENS: &[(&str, &str, &str, &str)] = &[
    (
        "tbl",
        "0.01",
        "nation",
        "7d47bc9397da331054fa92b8fb92e4c074004bad72dcbb893012093218dccf6c",
    ),
    (
        "tbl",
        "0.01",
        "region",
        "5a7c2fe9718db00ff5e5bc82a9ebfa8abc492cc75260d3c0ffb411974f235ab0",
    ),
    (
        "tbl",
        "0.01",
        "part",
        "43ec93812b684b7e96206db938aa54c04a677a09fb51939def52e10c111583d5",
    ),
    (
        "tbl",
        "0.01",
        "supplier",
        "b199bef3350840676cfe4be096091851bc47b6a57cd8a71b3f559d42b7d9dacd",
    ),
    (
        "tbl",
        "0.01",
        "partsupp",
        "906f58419af6ad5d62489a7e2105257654bb6d8458a7fd8fe19b76655aac50c7",
    ),
    (
        "tbl",
        "0.01",
        "customer",
        "3c7fb06e5255778fe4511cfc24dc416478a1afc37b40a487892b58aa03ec6f7a",
    ),
    (
        "tbl",
        "0.01",
        "orders",
        "654a1c747399ec13798e3eb561a11e390a2bd777a8ac8d6d611e66075cdd465b",
    ),
    (
        "tbl",
        "0.01",
        "lineitem",
        "12a22c69181f213e712d79bf0cdc29a29531ebcd2f0a6f75c6cfd5475b59c608",
    ),
    (
        "tbl",
        "0.1",
        "nation",
        "7d47bc9397da331054fa92b8fb92e4c074004bad72dcbb893012093218dccf6c",
    ),
    (
        "tbl",
        "0.1",
        "region",
        "5a7c2fe9718db00ff5e5bc82a9ebfa8abc492cc75260d3c0ffb411974f235ab0",
    ),
    (
        "tbl",
        "0.1",
        "part",
        "32a53a633aeb8215fcaa64e990c376e8b07b1e575f7fcc81c810b354f6a258cb",
    ),
    (
        "tbl",
        "0.1",
        "supplier",
        "3a83adcdf515ca1b15bfa60b5808b03d7432b19e8b778572a97f3d0d86e1ae39",
    ),
    (
        "tbl",
        "0.1",
        "partsupp",
        "0a3026c320ddbdf62ac5d0cf9ca6541bd82f0225aa3be6f9cd0f1dffe3857fc4",
    ),
    (
        "tbl",
        "0.1",
        "customer",
        "d94b4ab625123701005f33e8836ed8f58a6dbb9539f804a101c13da0c09d44d2",
    ),
    (
        "tbl",
        "0.1",
        "orders",
        "bfe1976f0a7833df7bf87fcf3a312ff6d41bc76463f8f6ab0bf451e7d2252293",
    ),
    (
        "tbl",
        "0.1",
        "lineitem",
        "adb28e381fc6bc9e62d2b5f6dd651bdba192c748f966735a851679573cb4b0b9",
    ),
    (
        "parquet",
        "0.01",
        "nation",
        "739a385c28b1319bbc5755ac916262287b34c358da283038016bc72adba067c1",
    ),
    (
        "parquet",
        "0.01",
        "region",
        "d036288ac3aa4072261ddccba328b16a5e9d9f3f948a6185b342d04f9f1a72a4",
    ),
    (
        "parquet",
        "0.01",
        "part",
        "a6967a3c9d2b1337f1b311f6575a9ba338d7775932312ec1b8e31d881bffcbfa",
    ),
    (
        "parquet",
        "0.01",
        "supplier",
        "0672a9cb5e2b4025d8253dfad66dee8234944448142cd87738987f7d7f895b86",
    ),
    (
        "parquet",
        "0.01",
        "partsupp",
        "769899621960201b05dc56806144a107c08decd24fff2dc9b8ed806ab5357caf",
    ),
    (
        "parquet",
        "0.01",
        "customer",
        "d322370a8ada8c0da5c86fd2fb43a816405f14ce4e02a813fd262e6be1b29ec1",
    ),
    (
        "parquet",
        "0.01",
        "orders",
        "8dc7ffbe6f1a7921e2ee09ce3b87c231b3e04ea6dac75610169d34389c0f48ae",
    ),
    (
        "parquet",
        "0.01",
        "lineitem",
        "5e96f2e92b14bb1e9097e1bcb81a7bbfcc85d43056ce750dad644af982807dc4",
    ),
    (
        "parquet",
        "0.1",
        "nation",
        "739a385c28b1319bbc5755ac916262287b34c358da283038016bc72adba067c1",
    ),
    (
        "parquet",
        "0.1",
        "region",
        "d036288ac3aa4072261ddccba328b16a5e9d9f3f948a6185b342d04f9f1a72a4",
    ),
    (
        "parquet",
        "0.1",
        "part",
        "cfd6451d87766e9285c626653cbfef43d36dc142fb6d00c97a2a0f4de07d363d",
    ),
    (
        "parquet",
        "0.1",
        "supplier",
        "f50ba73baba3dd45e12dcdc9421164d406389e75d3e0950de04c3715acd72499",
    ),
    (
        "parquet",
        "0.1",
        "partsupp",
        "0df7f7c2f005f3170065089770666d6e39c9db2fc369bb0fe3bc1f14988d6de0",
    ),
    (
        "parquet",
        "0.1",
        "customer",
        "afe20037645f3bc35d182f9c5c65fdc23b6992b553e466dd78815b04a9f9acce",
    ),
    (
        "parquet",
        "0.1",
        "orders",
        "f810a6ef52ab7cad60910b9b1b05756a561fb987f70b0f213977623e3f4c4b06",
    ),
    (
        "parquet",
        "0.1",
        "lineitem",
        "6e0436c938cd672bc11854cf87187f3528349ab7946e7d3934a5953277951d63",
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
        benchmark: tpch_skew(),
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
        benchmark: tpch_skew(),
        table: table.to_owned(),
        scale_factor,
        part: 1,
        parts: 1,
        seed,
    }
}

fn tpch_request(table: &str, scale_factor: f64) -> TableRequest {
    TableRequest {
        benchmark: Benchmark::Tpch,
        ..table_request(table, scale_factor, None)
    }
}

fn read(request: &TableRequest) -> RecordBatch {
    let reader = record_batches(request).unwrap();
    let schema = arrow::record_batch::RecordBatchReader::schema(&reader);
    let batches: Vec<_> = reader.map(Result::unwrap).collect();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

fn counts<'a>(values: impl Iterator<Item = &'a str>) -> HashMap<&'a str, usize> {
    let mut counts = HashMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
}

const SKEWED: [(&str, &[&str]); 4] = [
    ("part", &["p_brand", "p_type"]),
    ("customer", &["c_nationkey", "c_mktsegment"]),
    ("orders", &["o_custkey", "o_orderpriority"]),
    ("lineitem", &["l_partkey", "l_suppkey", "l_shipmode"]),
];

#[test]
fn row_counts_equal_the_tpch_counts_and_schemas_at_every_gate_scale() {
    for scale_factor in [0.01, 0.1, 1.0, 10.0, 2.0, 0.5] {
        let counts = row_counts(tpch_skew(), scale_factor).unwrap();
        assert_eq!(
            counts.iter().map(|(table, _)| *table).collect::<Vec<_>>(),
            TABLES
        );
        assert_eq!(counts, row_counts(Benchmark::Tpch, scale_factor).unwrap());
    }
    let lineitem: Vec<_> = [0.01, 0.1, 1.0, 10.0]
        .into_iter()
        .map(|scale_factor| row_counts(tpch_skew(), scale_factor).unwrap()[7].1)
        .collect();
    assert_eq!(lineitem, [60_175, 600_572, 6_001_215, 59_986_052]);
    for table in TABLES {
        assert_eq!(
            table_schema(tpch_skew(), table).unwrap(),
            table_schema(Benchmark::Tpch, table).unwrap()
        );
    }
}

#[test]
fn generated_rows_equal_the_reported_counts_and_schemas() {
    for (table, rows) in row_counts(tpch_skew(), 0.01).unwrap() {
        let reader = record_batches(&table_request(table, 0.01, None)).unwrap();
        let schema = table_schema(tpch_skew(), table).unwrap();
        assert_eq!(
            arrow::record_batch::RecordBatchReader::schema(&reader),
            schema
        );
        let generated: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(generated as u64, rows, "{table}");
    }
}

#[test]
fn unskewed_tables_and_columns_are_the_tpch_stream() {
    for scale_factor in [0.01, 0.1] {
        for table in TABLES {
            let skewed = read(&table_request(table, scale_factor, None));
            let base = read(&tpch_request(table, scale_factor));
            let replaced = SKEWED
                .iter()
                .find(|(name, _)| *name == table)
                .map_or(&[][..], |(_, columns)| *columns);
            assert_eq!(skewed.num_rows(), base.num_rows(), "{table}");
            for field in base.schema().fields() {
                let name = field.name().as_str();
                if replaced.contains(&name) {
                    assert_ne!(
                        skewed.column_by_name(name),
                        base.column_by_name(name),
                        "{name}"
                    );
                } else {
                    assert_eq!(
                        skewed.column_by_name(name),
                        base.column_by_name(name),
                        "{table}.{name} sf={scale_factor}"
                    );
                }
            }
        }
    }
}

#[test]
fn unskewed_tables_are_byte_identical_to_the_tpch_files() {
    let (skewed, base) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_benchmark(&request(skewed.path(), Format::Tbl, Some(2), 1, 0.1, None)).unwrap();
    let mut tpch = request(base.path(), Format::Tbl, Some(2), 1, 0.1, None);
    tpch.benchmark = Benchmark::Tpch;
    tpch.seed = None;
    write_benchmark(&tpch).unwrap();
    for table in ["nation", "region", "supplier", "partsupp"] {
        let name = format!("{table}.tbl");
        assert_eq!(
            fs::read(skewed.path().join(&name)).unwrap(),
            fs::read(base.path().join(&name)).unwrap(),
            "{table}"
        );
    }
}

#[test]
fn skewed_keys_stay_inside_their_parent_ranges_and_concentrate_on_the_first_keys() {
    let ranges = key_ranges(tpch_skew(), 0.1).unwrap();
    let range = |table: &str, column: &str| {
        let found = ranges
            .iter()
            .find(|r| r.table == table && r.column == column)
            .unwrap();
        (found.min, found.max)
    };
    let lineitem = read(&table_request("lineitem", 0.1, None));
    let orders = read(&table_request("orders", 0.1, None));
    let customer = read(&table_request("customer", 0.1, None));
    for (batch, name, parent) in [
        (&lineitem, "l_partkey", range("part", "p_partkey")),
        (&lineitem, "l_suppkey", range("supplier", "s_suppkey")),
        (&orders, "o_custkey", range("customer", "c_custkey")),
        (&customer, "c_nationkey", range("nation", "n_nationkey")),
    ] {
        let values = column::<Int64Array>(batch, name);
        assert_eq!(values.null_count(), 0);
        assert!(
            values
                .values()
                .iter()
                .all(|v| (parent.0..=parent.1).contains(v))
        );
        let first = values.values().iter().filter(|v| **v == parent.0).count();
        assert!(
            first as f64 / values.len() as f64 > 0.05,
            "{name}: {first} of {}",
            values.len()
        );
    }
    let part = read(&table_request("part", 0.1, None));
    let brands = counts(column::<StringArray>(&part, "p_brand").iter().flatten());
    assert_eq!(brands.len(), 25);
    assert!(brands["Brand#11"] > brands["Brand#55"] * 5);
    let modes = column::<StringArray>(&lineitem, "l_shipmode");
    let modes = counts(modes.iter().flatten());
    assert_eq!(modes.len(), 7);
    assert!(modes["REG AIR"] > modes["FOB"]);
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
            if matches!(table, "nation" | "region") {
                1
            } else {
                3
            },
            "{table}"
        );
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
fn seeds_select_the_skewed_streams_and_default_to_42() {
    let hash = |seed| {
        let dir = tempfile::tempdir().unwrap();
        digests(&request(dir.path(), Format::Tbl, Some(2), 1, 0.01, seed))
            .into_iter()
            .collect::<HashMap<_, _>>()
    };
    assert_eq!(hash(None), hash(Some(42)));
    let (other, default) = (hash(Some(43)), hash(None));
    for table in ["nation", "region", "supplier", "partsupp"] {
        let file = format!("{table}.tbl");
        assert_eq!(other[&file], default[&file], "{table}");
    }
    for table in ["part", "customer", "orders", "lineitem"] {
        let file = format!("{table}.tbl");
        assert_ne!(other[&file], default[&file], "{table}");
    }
}

#[test]
fn seeds_beyond_the_u32_range_are_accepted() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        write_benchmark(&request(
            dir.path(),
            Format::Tbl,
            Some(1),
            1,
            0.01,
            Some(u64::MAX)
        ))
        .is_ok()
    );
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
    for table in [
        "part", "supplier", "partsupp", "customer", "orders", "lineitem",
    ] {
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
    for table in ["nation", "region"] {
        let whole = whole_table(table, 0.1);
        assert_eq!(part_table(table, 0.1, 1, 3), whole);
        assert_eq!(part_table(table, 0.1, 2, 3).num_rows(), 0);
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
        assert_eq!(
            files.len(),
            if matches!(table, "nation" | "region") {
                1
            } else {
                3
            },
            "{table}"
        );
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

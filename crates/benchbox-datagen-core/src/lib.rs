mod dsgen;
mod generate;
mod output;
mod pipeline;
mod refresh;
mod schema;
mod tpcds;
mod tpcds_update;
mod tpch;

use std::fmt;
use std::str::FromStr;

use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

pub use benchbox_datagen_writers::Compression;
pub use generate::{KeyRange, TableScale};
pub use output::{FileStats, Format, OutputRequest, write_benchmark, write_benchmark_cancellable};

pub type BatchReader = Box<dyn RecordBatchReader + Send>;

#[derive(Debug, thiserror::Error, PartialEq)]
#[non_exhaustive]
pub enum Error {
    #[error("unknown benchmark {0:?}; supported: {supported}", supported = Benchmark::ALL.map(|b| b.name()).join(", "))]
    UnknownBenchmark(String),
    #[error("unknown {benchmark} table {table:?}; tables: {tables}", tables = benchmark.tables().join(", "))]
    UnknownTable { benchmark: Benchmark, table: String },
    #[error("scale factor must be a positive finite number, got {0}")]
    InvalidScaleFactor(f64),
    #[error("part must be between 1 and parts ({parts}), got {part}")]
    InvalidPart { part: i32, parts: i32 },
    #[error("parts must be at least 1, got {0}")]
    InvalidParts(i32),
    #[error("unknown output format {0:?}; supported: tbl, parquet")]
    UnknownFormat(String),
    #[error("{format:?} output does not support {compression} compression", format = format.name())]
    UnsupportedCompression {
        format: Format,
        compression: &'static str,
    },
    #[error("{0}")]
    Write(String),
    #[error("{0}")]
    Encode(String),
    #[error("{0}")]
    RefreshUnsupported(String),
    #[error("generation was cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Benchmark {
    Tpch,
    Tpcds,
}

impl Benchmark {
    pub const ALL: [Benchmark; 2] = [Benchmark::Tpch, Benchmark::Tpcds];

    pub fn name(self) -> &'static str {
        match self {
            Benchmark::Tpch => "tpch",
            Benchmark::Tpcds => "tpcds",
        }
    }

    pub fn table(self, name: &str) -> Result<&'static str, Error> {
        self.tables()
            .iter()
            .copied()
            .find(|table| table.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::UnknownTable {
                benchmark: self,
                table: name.to_owned(),
            })
    }

    pub fn tables(self) -> &'static [&'static str] {
        match self {
            Benchmark::Tpch => &[
                "nation", "region", "part", "supplier", "partsupp", "customer", "orders",
                "lineitem",
            ],
            Benchmark::Tpcds => &tpcds::TABLES,
        }
    }
}

impl fmt::Display for Benchmark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Benchmark {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        Benchmark::ALL
            .into_iter()
            .find(|benchmark| benchmark.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::UnknownBenchmark(name.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRequest {
    pub benchmark: Benchmark,
    pub table: String,
    pub scale_factor: f64,
    pub part: i32,
    pub parts: i32,
}

pub fn table_schema(benchmark: Benchmark, table: &str) -> Result<SchemaRef, Error> {
    Ok(generate::schema(benchmark, benchmark.table(table)?))
}

pub fn row_counts(
    benchmark: Benchmark,
    scale_factor: f64,
) -> Result<Vec<(&'static str, u64)>, Error> {
    validate_scale_factor(scale_factor)?;
    Ok(generate::row_counts(benchmark, scale_factor))
}

pub fn write_refresh_sets(
    benchmark: Benchmark,
    scale_factor: f64,
    sets: i32,
    rng_seed: Option<i32>,
    output_dir: &std::path::Path,
) -> Result<Vec<FileStats>, Error> {
    validate_scale_factor(scale_factor)?;
    if sets < 1 {
        return Err(Error::RefreshUnsupported(format!(
            "sets must be at least 1, got {sets}"
        )));
    }
    match benchmark {
        Benchmark::Tpch => {
            if rng_seed.is_some() {
                return Err(Error::RefreshUnsupported(
                    "tpch refresh sets do not take an rng seed".into(),
                ));
            }
            refresh::write_tpch_refresh(scale_factor, sets, output_dir)
        }
        Benchmark::Tpcds => tpcds_update::write_tpcds_refresh(
            scale_factor,
            sets,
            rng_seed.unwrap_or(tpcds_update::DEFAULT_SEED),
            output_dir,
        ),
    }
}

pub fn table_scales(benchmark: Benchmark, scale_factor: f64) -> Result<Vec<TableScale>, Error> {
    validate_scale_factor(scale_factor)?;
    Ok(generate::table_scales(benchmark, scale_factor))
}

pub fn key_ranges(benchmark: Benchmark, scale_factor: f64) -> Result<Vec<KeyRange>, Error> {
    validate_scale_factor(scale_factor)?;
    Ok(generate::key_ranges(benchmark, scale_factor))
}

pub(crate) fn validate_scale_factor(scale_factor: f64) -> Result<(), Error> {
    if !scale_factor.is_finite() || scale_factor <= 0.0 {
        return Err(Error::InvalidScaleFactor(scale_factor));
    }
    Ok(())
}

pub(crate) struct Conformed {
    inner: BatchReader,
    schema: SchemaRef,
}

impl Conformed {
    pub(crate) fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for Conformed {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.inner.next()? {
            Ok(batch) => batch,
            Err(error) => return Some(Err(error)),
        };
        let columns = batch
            .columns()
            .iter()
            .zip(self.schema.fields())
            .map(|(column, field)| {
                if column.data_type() == field.data_type() {
                    Ok(column.clone())
                } else {
                    cast(column, field.data_type())
                }
            })
            .collect::<Result<Vec<_>, _>>();
        Some(columns.and_then(|columns| RecordBatch::try_new(self.schema.clone(), columns)))
    }
}

impl RecordBatchReader for Conformed {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub(crate) fn conformed(
    benchmark: Benchmark,
    table: &str,
    scale_factor: f64,
    slice: &generate::Slice,
) -> Conformed {
    Conformed {
        inner: generate::batches(benchmark, table, scale_factor, slice),
        schema: generate::schema(benchmark, table),
    }
}

pub fn record_batches(request: &TableRequest) -> Result<BatchReader, Error> {
    let TableRequest {
        benchmark,
        table,
        scale_factor,
        part,
        parts,
    } = request;
    validate_scale_factor(*scale_factor)?;
    if *parts < 1 || *part < 1 || part > parts {
        return Err(Error::InvalidPart {
            part: *part,
            parts: *parts,
        });
    }
    let table = benchmark.table(table)?;
    let slice = generate::part_slice(*benchmark, table, *scale_factor, *part, *parts);
    Ok(Box::new(conformed(
        *benchmark,
        table,
        *scale_factor,
        &slice,
    )))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    fn request(table: &str, scale_factor: f64, part: i32, parts: i32) -> TableRequest {
        TableRequest {
            benchmark: Benchmark::Tpch,
            table: table.to_owned(),
            scale_factor,
            part,
            parts,
        }
    }

    fn row_count(request: &TableRequest) -> usize {
        record_batches(request)
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum()
    }

    #[test]
    fn tpch_row_counts_match_the_specification_at_scale_factor_one_hundredth() {
        let expected = [
            ("nation", 25),
            ("region", 5),
            ("part", 2_000),
            ("supplier", 100),
            ("partsupp", 8_000),
            ("customer", 1_500),
            ("orders", 15_000),
        ];
        for (table, rows) in expected {
            assert_eq!(row_count(&request(table, 0.01, 1, 1)), rows, "{table}");
        }
    }

    #[test]
    fn parts_partition_rows_without_overlap_or_loss() {
        for table in Benchmark::Tpch.tables() {
            let whole = row_count(&request(table, 0.01, 1, 1));
            let split: usize = (1..=4)
                .map(|part| row_count(&request(table, 0.01, part, 4)))
                .sum();
            assert_eq!(split, whole, "{table}");
        }
    }

    #[test]
    fn benchmark_and_table_names_are_case_insensitive() {
        assert_eq!("TPCH".parse::<Benchmark>(), Ok(Benchmark::Tpch));
        assert_eq!(row_count(&request("NATION", 1.0, 1, 1)), 25);
    }

    #[test]
    fn invalid_requests_are_rejected() {
        assert_eq!(
            "tpcx".parse::<Benchmark>(),
            Err(Error::UnknownBenchmark("tpcx".into()))
        );
        assert!(matches!(
            record_batches(&request("nope", 1.0, 1, 1)),
            Err(Error::UnknownTable { .. })
        ));
        for scale_factor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                record_batches(&request("nation", scale_factor, 1, 1)),
                Err(Error::InvalidScaleFactor(_))
            ));
        }
        for (part, parts) in [(0, 1), (2, 1), (1, 0)] {
            assert_eq!(
                record_batches(&request("nation", 1.0, part, parts)).err(),
                Some(Error::InvalidPart { part, parts })
            );
        }
    }

    #[test]
    fn schemas_use_benchbox_types_and_match_the_streams() {
        for table in Benchmark::Tpch.tables() {
            let schema = table_schema(Benchmark::Tpch, table).unwrap();
            let batches = record_batches(&request(table, 0.01, 1, 1)).unwrap();
            assert_eq!(batches.schema(), schema, "{table}");
            for batch in batches {
                assert_eq!(batch.unwrap().schema(), schema, "{table}");
            }
            for field in schema.fields() {
                assert!(
                    !matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Utf8View | DataType::LargeUtf8
                    ),
                    "{table}.{}",
                    field.name()
                );
                let nullable = matches!(field.name().as_str(), "n_comment" | "r_comment");
                assert_eq!(field.is_nullable(), nullable, "{}", field.name());
            }
        }
        let lineitem = table_schema(Benchmark::Tpch, "LINEITEM").unwrap();
        assert_eq!(
            lineitem.field_with_name("l_quantity").unwrap().data_type(),
            &DataType::Decimal128(15, 2)
        );
        assert_eq!(
            lineitem.field_with_name("l_shipdate").unwrap().data_type(),
            &DataType::Date32
        );
        assert_eq!(
            lineitem.field_with_name("l_orderkey").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn row_counts_equal_the_generated_rows() {
        let counts = row_counts(Benchmark::Tpch, 0.01).unwrap();
        assert_eq!(
            counts.iter().map(|(table, _)| *table).collect::<Vec<_>>(),
            Benchmark::Tpch.tables()
        );
        for (table, rows) in counts {
            assert_eq!(
                rows as usize,
                row_count(&request(table, 0.01, 1, 1)),
                "{table}"
            );
        }
    }

    #[test]
    fn row_counts_match_the_published_scale_factor_one_cardinalities() {
        let counts = row_counts(Benchmark::Tpch, 1.0).unwrap();
        assert_eq!(
            counts,
            [
                ("nation", 25),
                ("region", 5),
                ("part", 200_000),
                ("supplier", 10_000),
                ("partsupp", 800_000),
                ("customer", 150_000),
                ("orders", 1_500_000),
                ("lineitem", 6_001_215),
            ]
        );
        assert_eq!(
            row_counts(Benchmark::Tpch, -1.0),
            Err(Error::InvalidScaleFactor(-1.0))
        );
    }

    #[test]
    fn tpcds_schemas_use_benchbox_types_and_cover_every_stream() {
        assert_eq!(Benchmark::Tpcds.tables().len(), 24);
        for table in Benchmark::Tpcds.tables() {
            let schema = table_schema(Benchmark::Tpcds, table).unwrap();
            let mut batches = record_batches(&TableRequest {
                benchmark: Benchmark::Tpcds,
                table: (*table).to_owned(),
                scale_factor: 1.0,
                part: 1,
                parts: 1,
            })
            .unwrap();
            assert_eq!(batches.schema(), schema, "{table}");
            let batch = batches.next().unwrap().unwrap();
            assert_eq!(batch.schema(), schema, "{table}");
            for field in schema.fields() {
                assert!(
                    !matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Utf8View | DataType::LargeUtf8
                    ),
                    "{table}.{}",
                    field.name()
                );
            }
        }
        assert!(matches!(
            table_schema(Benchmark::Tpcds, "dbgen_version"),
            Err(Error::UnknownTable { .. })
        ));
    }

    #[test]
    fn tpcds_generation_accepts_fractional_scale_factors() {
        for scale_factor in [0.5, 0.01, 0.999] {
            for table in ["item", "customer", "store_returns"] {
                let mut reader = record_batches(&TableRequest {
                    benchmark: Benchmark::Tpcds,
                    table: table.into(),
                    scale_factor,
                    part: 1,
                    parts: 1,
                })
                .unwrap();
                assert!(reader.next().unwrap().unwrap().num_rows() > 0);
            }
            assert!(row_counts(Benchmark::Tpcds, scale_factor).is_ok());
            assert!(key_ranges(Benchmark::Tpcds, scale_factor).is_ok());
        }
    }

    #[test]
    fn tpcds_subscale_model_matches_the_patched_dsdgen_row_counts() {
        let scales = table_scales(Benchmark::Tpcds, 0.01).unwrap();
        let find = |table: &str| *scales.iter().find(|scale| scale.table == table).unwrap();
        assert_eq!(find("store").source_rows, 1);
        assert_eq!(find("customer").source_rows, 1_000);
        assert_eq!(find("customer_demographics").source_rows, 19_208);
        assert_eq!(find("date_dim").source_rows, 73_049);
        assert_eq!(find("store_sales").source_rows, 2_400);
        assert_eq!(find("inventory").source_rows, 23_490);
        let counts: std::collections::HashMap<_, _> = row_counts(Benchmark::Tpcds, 0.01)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(counts["store_sales"], 28_810);
        assert_eq!(counts["store_returns"], 2_829);
        let reasons: Vec<u64> = [0.01, 0.1, 0.5, 1.0]
            .into_iter()
            .map(|scale_factor| {
                table_scales(Benchmark::Tpcds, scale_factor)
                    .unwrap()
                    .iter()
                    .find(|scale| scale.table == "reason")
                    .unwrap()
                    .source_rows
            })
            .collect();
        assert_eq!(reasons, [1, 7, 37, 75]);
    }

    #[test]
    fn tpcds_row_counts_match_the_published_scale_factor_one_cardinalities() {
        let counts: std::collections::HashMap<_, _> = row_counts(Benchmark::Tpcds, 1.0)
            .unwrap()
            .into_iter()
            .collect();
        for (table, rows) in [
            ("call_center", 6),
            ("catalog_page", 11_718),
            ("catalog_returns", 144_067),
            ("catalog_sales", 1_441_548),
            ("customer", 100_000),
            ("customer_address", 50_000),
            ("customer_demographics", 1_920_800),
            ("date_dim", 73_049),
            ("household_demographics", 7_200),
            ("income_band", 20),
            ("inventory", 11_745_000),
            ("item", 18_000),
            ("promotion", 300),
            ("reason", 75),
            ("ship_mode", 20),
            ("store", 12),
            ("store_returns", 287_514),
            ("store_sales", 2_880_404),
            ("time_dim", 86_400),
            ("warehouse", 5),
            ("web_page", 60),
            ("web_returns", 71_763),
            ("web_sales", 719_384),
            ("web_site", 30),
        ] {
            assert_eq!(counts[table], rows, "{table}");
        }
    }

    #[test]
    fn key_ranges_cover_dimension_keys_and_order_numbers() {
        let ranges = key_ranges(Benchmark::Tpcds, 1.0).unwrap();
        let find = |table: &str, column: &str| {
            *ranges
                .iter()
                .find(|range| range.table == table && range.column == column)
                .unwrap()
        };
        assert_eq!(find("item", "i_item_sk").max, 18_000);
        assert_eq!(find("time_dim", "t_time_sk").min, 0);
        assert_eq!(find("date_dim", "d_date_sk").min, 2_415_022);
        let tpch = key_ranges(Benchmark::Tpch, 1.0).unwrap();
        let orders = tpch
            .iter()
            .find(|range| range.column == "o_orderkey")
            .unwrap();
        assert_eq!((orders.min, orders.max), (1, 6_000_000));
    }
}

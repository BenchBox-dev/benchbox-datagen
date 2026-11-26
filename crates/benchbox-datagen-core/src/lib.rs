mod output;
mod pipeline;
mod schema;
mod synthetic;
mod tpch;

use std::fmt;
use std::str::FromStr;

use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

pub use benchbox_datagen_writers::Compression;
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
    #[error("{0} output is not seeded; omit the seed argument")]
    SeedNotSupported(Benchmark),
    #[error("generation was cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Benchmark {
    Tpch,
    Ssb,
}

impl Benchmark {
    pub const ALL: [Benchmark; 2] = [Benchmark::Tpch, Benchmark::Ssb];

    pub fn name(self) -> &'static str {
        match self {
            Benchmark::Tpch => "tpch",
            Benchmark::Ssb => "ssb",
        }
    }

    pub fn is_seeded(self) -> bool {
        matches!(self, Benchmark::Ssb)
    }

    pub(crate) fn is_unsplit(self, table: &str) -> bool {
        matches!(
            (self, table),
            (Benchmark::Tpch, "nation" | "region") | (Benchmark::Ssb, "date")
        )
    }

    pub(crate) fn resolve_seed(self, seed: Option<u64>) -> Result<u64, Error> {
        match (self.is_seeded(), seed) {
            (true, seed) => Ok(seed.unwrap_or(synthetic::DEFAULT_SEED)),
            (false, None) => Ok(0),
            (false, Some(_)) => Err(Error::SeedNotSupported(self)),
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
            Benchmark::Ssb => &["date", "customer", "supplier", "part", "lineorder"],
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
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub table: &'static str,
    pub column: &'static str,
    pub min: i64,
    pub max: i64,
}

pub fn key_ranges(benchmark: Benchmark, scale_factor: f64) -> Result<Vec<KeyRange>, Error> {
    validate_scale_factor(scale_factor)?;
    Ok(match benchmark {
        Benchmark::Tpch => tpch::key_ranges(scale_factor),
        Benchmark::Ssb => synthetic::ssb_key_ranges(scale_factor),
    })
}

pub fn table_schema(benchmark: Benchmark, table: &str) -> Result<SchemaRef, Error> {
    let table = benchmark.table(table)?;
    match benchmark {
        Benchmark::Tpch => Ok(schema::tpch(table).expect("every TPC-H table has a schema")),
        Benchmark::Ssb => Ok(synthetic::ssb_schema(table)),
    }
}

pub fn row_counts(
    benchmark: Benchmark,
    scale_factor: f64,
) -> Result<Vec<(&'static str, u64)>, Error> {
    validate_scale_factor(scale_factor)?;
    match benchmark {
        Benchmark::Tpch => Ok(benchmark
            .tables()
            .iter()
            .map(|table| (*table, tpch::row_count(table, scale_factor)))
            .collect()),
        Benchmark::Ssb => Ok(benchmark
            .tables()
            .iter()
            .map(|table| (*table, synthetic::row_count(benchmark, table, scale_factor)))
            .collect()),
    }
}

pub(crate) fn validate_scale_factor(scale_factor: f64) -> Result<(), Error> {
    if scale_factor.is_finite() && scale_factor > 0.0 {
        Ok(())
    } else {
        Err(Error::InvalidScaleFactor(scale_factor))
    }
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

pub(crate) fn conformed(table: &str, scale_factor: f64, part: i32, parts: i32) -> Conformed {
    Conformed {
        inner: tpch::batches(table, scale_factor, part, parts),
        schema: schema::tpch(table).expect("every TPC-H table has a schema"),
    }
}

pub fn record_batches(request: &TableRequest) -> Result<BatchReader, Error> {
    let TableRequest {
        benchmark,
        table,
        scale_factor,
        part,
        parts,
        seed,
    } = request;
    validate_scale_factor(*scale_factor)?;
    if *parts < 1 || *part < 1 || part > parts {
        return Err(Error::InvalidPart {
            part: *part,
            parts: *parts,
        });
    }
    let table = benchmark.table(table)?;
    let seed = benchmark.resolve_seed(*seed)?;
    match benchmark {
        Benchmark::Tpch => Ok(Box::new(conformed(table, *scale_factor, *part, *parts))),
        Benchmark::Ssb if benchmark.is_unsplit(table) => {
            let source = synthetic::open(*benchmark, table, *scale_factor, seed);
            Ok(Box::new(if *part > 1 {
                synthetic::Reader::empty(source)
            } else {
                synthetic::Reader::new(source, 1, 1)
            }))
        }
        Benchmark::Ssb => Ok(Box::new(synthetic::Reader::new(
            synthetic::open(*benchmark, table, *scale_factor, seed),
            *part,
            *parts,
        ))),
    }
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
            seed: None,
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
}

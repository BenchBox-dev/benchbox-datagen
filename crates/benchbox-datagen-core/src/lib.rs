use std::fmt;
use std::str::FromStr;

use arrow::record_batch::{RecordBatchIterator, RecordBatchReader};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Benchmark {
    Tpch,
}

impl Benchmark {
    pub const ALL: [Benchmark; 1] = [Benchmark::Tpch];

    pub fn name(self) -> &'static str {
        match self {
            Benchmark::Tpch => "tpch",
        }
    }

    pub fn tables(self) -> &'static [&'static str] {
        match self {
            Benchmark::Tpch => &[
                "nation", "region", "part", "supplier", "partsupp", "customer", "orders",
                "lineitem",
            ],
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

pub fn record_batches(request: &TableRequest) -> Result<BatchReader, Error> {
    let TableRequest {
        benchmark,
        table,
        scale_factor,
        part,
        parts,
    } = request;
    let (scale_factor, part, parts) = (*scale_factor, *part, *parts);
    if !scale_factor.is_finite() || scale_factor <= 0.0 {
        return Err(Error::InvalidScaleFactor(scale_factor));
    }
    if parts < 1 || part < 1 || part > parts {
        return Err(Error::InvalidPart { part, parts });
    }
    let unknown_table = || Error::UnknownTable {
        benchmark: *benchmark,
        table: table.clone(),
    };
    match benchmark {
        Benchmark::Tpch => Ok(match table.to_ascii_lowercase().as_str() {
            "nation" if part > 1 => {
                Box::new(RecordBatchIterator::new([], NationArrow::schema_ref()))
            }
            "region" if part > 1 => {
                Box::new(RecordBatchIterator::new([], RegionArrow::schema_ref()))
            }
            "nation" => Box::new(NationArrow::new(NationGenerator::new(scale_factor, 1, 1))),
            "region" => Box::new(RegionArrow::new(RegionGenerator::new(scale_factor, 1, 1))),
            "part" => Box::new(PartArrow::new(PartGenerator::new(
                scale_factor,
                part,
                parts,
            ))),
            "supplier" => Box::new(SupplierArrow::new(SupplierGenerator::new(
                scale_factor,
                part,
                parts,
            ))),
            "partsupp" => Box::new(PartSuppArrow::new(PartSuppGenerator::new(
                scale_factor,
                part,
                parts,
            ))),
            "customer" => Box::new(CustomerArrow::new(CustomerGenerator::new(
                scale_factor,
                part,
                parts,
            ))),
            "orders" => Box::new(OrderArrow::new(OrderGenerator::new(
                scale_factor,
                part,
                parts,
            ))),
            "lineitem" => Box::new(LineItemArrow::new(LineItemGenerator::new(
                scale_factor,
                part,
                parts,
            ))),
            _ => return Err(unknown_table()),
        }),
    }
}

#[cfg(test)]
mod tests {
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
}

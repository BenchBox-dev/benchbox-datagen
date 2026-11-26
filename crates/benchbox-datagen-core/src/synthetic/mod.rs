pub(crate) mod calendar;
pub(crate) mod delimited;
pub(crate) mod rng;
mod ssb;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::{Benchmark, schema};

pub(crate) use ssb::key_ranges as ssb_key_ranges;

pub(crate) const BLOCK_ROWS: u64 = 65_536;
pub(crate) const DEFAULT_SEED: u64 = 42;

pub(crate) trait Table: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn rows(&self) -> u64;
    fn block(&self, index: u64) -> RecordBatch;

    fn blocks(&self) -> u64 {
        self.rows().div_ceil(BLOCK_ROWS)
    }
}

pub(crate) fn block_len(rows: u64, block: u64) -> usize {
    (rows - block * BLOCK_ROWS).min(BLOCK_ROWS) as usize
}

pub(crate) fn row_count(benchmark: Benchmark, table: &str, scale_factor: f64) -> u64 {
    match benchmark {
        Benchmark::Ssb => ssb::row_count(table, scale_factor),
        Benchmark::Tpch => unreachable!("TPC-H is not a synthetic benchmark"),
    }
}

pub(crate) fn open(
    benchmark: Benchmark,
    table: &str,
    scale_factor: f64,
    seed: u64,
) -> Arc<dyn Table> {
    match benchmark {
        Benchmark::Ssb => ssb::open(table, scale_factor, seed),
        Benchmark::Tpch => unreachable!("TPC-H is not a synthetic benchmark"),
    }
}

pub(crate) fn part_rows(rows: u64, part: i32, parts: i32) -> (u64, u64) {
    let bound = |index: i32| (u128::from(rows) * index as u128 / parts as u128) as u64;
    (bound(part - 1), bound(part))
}

pub(crate) struct Part {
    table: Arc<dyn Table>,
    next: u64,
    end: u64,
}

impl Part {
    pub(crate) fn new(table: Arc<dyn Table>, part: i32, parts: i32) -> Self {
        let (next, end) = part_rows(table.rows(), part, parts);
        Self { table, next, end }
    }
}

impl Iterator for Part {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<RecordBatch> {
        if self.next >= self.end {
            return None;
        }
        let block = self.next / BLOCK_ROWS;
        let start = block * BLOCK_ROWS;
        let stop = (start + BLOCK_ROWS).min(self.end).min(self.table.rows());
        let batch = self.table.block(block);
        let slice = batch.slice((self.next - start) as usize, (stop - self.next) as usize);
        self.next = stop;
        Some(slice)
    }
}

pub(crate) struct Reader {
    part: Part,
    schema: SchemaRef,
}

impl Reader {
    pub(crate) fn empty(table: Arc<dyn Table>) -> Self {
        Self {
            schema: table.schema(),
            part: Part {
                table,
                next: 0,
                end: 0,
            },
        }
    }

    pub(crate) fn new(table: Arc<dyn Table>, part: i32, parts: i32) -> Self {
        Self {
            schema: table.schema(),
            part: Part::new(table, part, parts),
        }
    }
}

impl Iterator for Reader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.part.next().map(Ok)
    }
}

impl RecordBatchReader for Reader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub(crate) fn ssb_schema(table: &str) -> SchemaRef {
    schema::ssb(table).expect("every SSB table has a schema")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parts_cover_every_row_exactly_once() {
        for rows in [1_u64, 7, 65_536, 65_537, 300_001] {
            for parts in [1, 2, 3, 8] {
                let mut next = 0;
                for part in 1..=parts {
                    let (start, end) = part_rows(rows, part, parts);
                    assert_eq!(start, next);
                    next = end;
                }
                assert_eq!(next, rows);
            }
        }
    }
}

pub(crate) mod calendar;
pub(crate) mod delimited;
pub(crate) mod rng;
mod ssb;

use std::fmt;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::generate::{KeyRange, TableScale};

pub(crate) const BLOCK_ROWS: u64 = 65_536;
pub(crate) const DEFAULT_SEED: u64 = 42;

pub(crate) trait Table: Send + Sync + fmt::Debug {
    fn schema(&self) -> SchemaRef;
    fn rows(&self) -> u64;
    fn block(&self, index: u64) -> RecordBatch;

    fn warm(&self, _block: u64, _cancel: &AtomicBool) -> bool {
        true
    }
}

pub(crate) fn block_len(rows: u64, block: u64) -> usize {
    (rows - block * BLOCK_ROWS).min(BLOCK_ROWS) as usize
}

pub(crate) trait Generator: Sync {
    fn name(&self) -> &'static str;
    fn tables(&self) -> &'static [&'static str];
    fn schema(&self, table: &str) -> Option<SchemaRef>;
    fn row_count(&self, table: &str, scale_factor: f64) -> u64;
    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table>;
    fn key_ranges(&self, scale_factor: f64) -> Vec<KeyRange>;

    fn splittable(&self, _table: &str) -> bool {
        true
    }

    fn table_scales(&self, scale_factor: f64) -> Vec<TableScale> {
        self.tables()
            .iter()
            .map(|table| {
                let rows = self.row_count(table, scale_factor);
                TableScale {
                    table,
                    id_count: rows,
                    source_rows: rows,
                }
            })
            .collect()
    }
}

const REGISTRY: &[&dyn Generator] = &[&ssb::Ssb];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticId(usize);

impl SyntheticId {
    pub(crate) fn all() -> impl Iterator<Item = SyntheticId> {
        (0..REGISTRY.len()).map(SyntheticId)
    }

    pub(crate) fn generator(self) -> &'static dyn Generator {
        REGISTRY[self.0]
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
    pub(crate) fn new(table: Arc<dyn Table>, start: u64, end: u64) -> Self {
        Self {
            end: end.min(table.rows()),
            table,
            next: start,
        }
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
        let stop = (start + BLOCK_ROWS).min(self.end);
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
    pub(crate) fn new(table: Arc<dyn Table>, start: u64, end: u64) -> Self {
        Self {
            schema: table.schema(),
            part: Part::new(table, start, end),
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

pub(crate) fn write_delimited<W: Write>(
    table: &Arc<dyn Table>,
    start: u64,
    end: u64,
    sink: &mut W,
) -> io::Result<u64> {
    let (mut rows, mut bytes) = (0, Vec::new());
    for batch in Part::new(table.clone(), start, end) {
        bytes.clear();
        delimited::encode(&batch, &mut bytes);
        rows += batch.num_rows() as u64;
        sink.write_all(&bytes)?;
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_generator_is_consistent() {
        let mut names = std::collections::HashSet::new();
        for id in SyntheticId::all() {
            let generator = id.generator();
            assert!(names.insert(generator.name()), "duplicate name");
            assert!(!generator.tables().is_empty());
            for scale_factor in [0.01, 1.0] {
                for table in generator.tables() {
                    let schema = generator.schema(table).expect("every table has a schema");
                    let source = generator.open(table, scale_factor, DEFAULT_SEED);
                    assert_eq!(source.schema(), schema);
                    assert_eq!(source.rows(), generator.row_count(table, scale_factor));
                }
                let scales = generator.table_scales(scale_factor);
                assert_eq!(scales.len(), generator.tables().len());
                for range in generator.key_ranges(scale_factor) {
                    let schema = generator.schema(range.table).expect("key table exists");
                    assert!(schema.field_with_name(range.column).is_ok());
                    assert!(range.min <= range.max);
                }
            }
        }
    }

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

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Int32Array, Int64Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::delimited::TextFormat;
use super::{BLOCK_ROWS, Generator, Table, block_len_in};
use crate::generate::{KeyRange, key};
use crate::schema::{self, Column, Int, Int32, Vector};

const DIMENSIONS: i32 = 256;
const SMALL_BLOCK: u64 = 256;
const LARGE_BLOCK: u64 = 4_096;
const NARROW_BLOCK: u64 = 1_000;
const WIDE_ROWS_PER_SCALE: f64 = 30_000.0;
const NARROW_ROWS_PER_SCALE: f64 = 250_000.0;

const WIDE: [Column; 2] = [("id", Int, false), ("embedding", Vector(DIMENSIONS), true)];
const NARROW: [Column; 2] = [("id", Int, false), ("flag", Int32, true)];

static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> = LazyLock::new(|| {
    schema::build(&[
        ("wide_default", &WIDE),
        ("wide_small", &WIDE),
        ("wide_large", &WIDE),
        ("narrow_small", &NARROW),
    ])
});

static PEAK: Mutex<Vec<(&'static str, usize)>> = Mutex::new(Vec::new());

pub(super) fn reset_peak() {
    PEAK.lock().unwrap_or_else(|e| e.into_inner()).clear();
}

pub(super) fn peak(table: &str) -> usize {
    PEAK.lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|(name, _)| *name == table)
        .map(|(_, bytes)| *bytes)
        .max()
        .unwrap_or(0)
}

pub(super) fn block_bytes(table: &str) -> usize {
    let rows = block_rows_of(table) as usize;
    rows * (8 + DIMENSIONS as usize * 4)
}

fn block_rows_of(table: &str) -> u64 {
    match table {
        "wide_small" => SMALL_BLOCK,
        "wide_large" => LARGE_BLOCK,
        "narrow_small" => NARROW_BLOCK,
        _ => BLOCK_ROWS,
    }
}

pub(super) struct WideFixture;

impl Generator for WideFixture {
    fn name(&self) -> &'static str {
        "wide_fixture"
    }

    fn tables(&self) -> &'static [&'static str] {
        &["wide_default", "wide_small", "wide_large", "narrow_small"]
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        SCHEMAS.get(table).cloned()
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        let per_scale = if table == "narrow_small" {
            NARROW_ROWS_PER_SCALE
        } else {
            WIDE_ROWS_PER_SCALE
        };
        (per_scale * scale_factor) as u64
    }

    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
        let name = *self.tables().iter().find(|name| **name == table).unwrap();
        Arc::new(Fixture {
            name,
            rows: self.row_count(table, scale_factor),
            seed,
            live: Mutex::new(Vec::new()),
        })
    }

    fn key_ranges(&self, _scale_factor: f64) -> Vec<KeyRange> {
        vec![key("wide_default", "id", 1, 1)]
    }

    fn text_format(&self, _table: &str) -> TextFormat {
        TextFormat::csv(b',', true)
    }
}

#[derive(Debug)]
struct Fixture {
    name: &'static str,
    rows: u64,
    seed: u64,
    live: Mutex<Vec<(Buffer, usize)>>,
}

impl Fixture {
    fn element(&self, row: u64, dimension: u64) -> f32 {
        let mixed = (row + 1)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(dimension.wrapping_mul(0xBF58_476D_1CE4_E5B9))
            .wrapping_add(self.seed);
        (mixed >> 40) as f32 / 16_777_216.0
    }

    fn record(&self, buffer: Buffer, bytes: usize) {
        let mut live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        live.retain(|(held, _)| held.strong_count() > 1);
        live.push((buffer, bytes));
        let total: usize = live.iter().map(|(_, bytes)| bytes).sum();
        PEAK.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((self.name, total));
    }
}

impl Table for Fixture {
    fn schema(&self) -> SchemaRef {
        SCHEMAS[self.name].clone()
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block_rows(&self) -> u64 {
        block_rows_of(self.name)
    }

    fn block(&self, index: u64) -> RecordBatch {
        let block_rows = self.block_rows();
        let rows = block_len_in(self.rows, index, block_rows);
        let first = index * block_rows;
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..rows as u64).map(|offset| (first + offset) as i64 + 1),
        ));
        let second: ArrayRef = if self.name == "narrow_small" {
            Arc::new(Int32Array::from_iter_values(
                (0..rows as u64).map(|offset| ((first + offset) % 7) as i32),
            ))
        } else {
            let values = Float32Array::from_iter_values((0..rows as u64).flat_map(|offset| {
                { (0..DIMENSIONS as u64).map(move |dimension| (first + offset, dimension)) }
                    .map(|(row, dimension)| self.element(row, dimension))
            }));
            let buffer = values.values().inner().clone();
            self.record(buffer, rows * (8 + DIMENSIONS as usize * 4));
            let item = Arc::new(Field::new_list_field(DataType::Float32, true));
            Arc::new(FixedSizeListArray::new(
                item,
                DIMENSIONS,
                Arc::new(values),
                None,
            ))
        };
        RecordBatch::try_new(self.schema(), vec![ids, second]).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Mutex;

    use arrow::array::AsArray;
    use arrow::compute::concat_batches;
    use benchbox_datagen_writers::ROW_GROUP_ROWS;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::Benchmark;
    use crate::output::{Format, OutputRequest, write_benchmark};
    use crate::synthetic::SyntheticId;

    static SERIAL: Mutex<()> = Mutex::new(());

    fn benchmark() -> Benchmark {
        Benchmark::Synthetic(
            SyntheticId::all()
                .find(|id| id.generator().name() == "wide_fixture")
                .unwrap(),
        )
    }

    fn write(dir: &Path, format: Format, threads: usize, parts: i32, scale: f64) -> Vec<String> {
        let stats = write_benchmark(&OutputRequest {
            benchmark: benchmark(),
            scale_factor: scale,
            format,
            compression: None,
            threads: NonZeroUsize::new(threads),
            parts,
            seed: Some(7),
            output_dir: dir.to_path_buf(),
        })
        .unwrap();
        stats
            .iter()
            .map(|file| {
                file.path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    }

    fn tbl(dir: &Path, table: &str, parts: i32) -> Vec<u8> {
        let names: Vec<String> = if parts == 1 {
            vec![format!("{table}.csv")]
        } else {
            (1..=parts).map(|p| format!("{table}.csv.{p}")).collect()
        };
        names
            .iter()
            .flat_map(|name| fs::read(dir.join(name)).unwrap())
            .collect()
    }

    #[test]
    fn peak_in_flight_bytes_scale_with_the_declared_block_size() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_peak();
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), Format::Tbl, 2, 1, 1.0);
        let (small, large, default) =
            (peak("wide_small"), peak("wide_large"), peak("wide_default"));
        let window = 2 + 2 * 2 + 1;
        assert!(small >= block_bytes("wide_small"));
        assert!(small <= window * block_bytes("wide_small"), "{small}");
        assert!(large >= block_bytes("wide_large"));
        assert!(large <= window * block_bytes("wide_large"), "{large}");
        assert!(large >= 8 * small, "{large} vs {small}");
        assert!(default >= 30_000 * (8 + DIMENSIONS as usize * 4));
        assert!(default >= 3 * large, "{default} vs {large}");
    }

    #[test]
    fn output_is_identical_for_any_block_size_thread_count_and_part_count() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let reference = tempfile::tempdir().unwrap();
        write(reference.path(), Format::Tbl, 1, 1, 0.1);
        let expected = tbl(reference.path(), "wide_default", 1);
        assert!(expected.starts_with(b"id,embedding\n1,"));
        assert_eq!(expected.iter().filter(|b| **b == b'\n').count(), 3_001);
        for table in ["wide_small", "wide_large"] {
            assert_eq!(tbl(reference.path(), table, 1), expected, "{table}");
        }
        for (threads, parts) in [(1, 3), (4, 1), (4, 7)] {
            let dir = tempfile::tempdir().unwrap();
            write(dir.path(), Format::Tbl, threads, parts, 0.1);
            for table in ["wide_default", "wide_small", "wide_large"] {
                assert_eq!(
                    tbl(dir.path(), table, parts),
                    expected,
                    "{table} {threads} threads {parts} parts"
                );
            }
        }
    }

    #[test]
    fn parquet_rows_are_identical_across_block_sizes_and_parts_with_unchanged_row_groups() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let whole = tempfile::tempdir().unwrap();
        let split = tempfile::tempdir().unwrap();
        write(whole.path(), Format::Parquet, 3, 1, 1.0);
        write(split.path(), Format::Parquet, 3, 4, 1.0);
        let read = |path: &Path| {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap()).unwrap();
            let groups: Vec<usize> = builder
                .metadata()
                .row_groups()
                .iter()
                .map(|group| group.num_rows() as usize)
                .collect();
            let schema = builder.schema().clone();
            let batches: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
            (groups, concat_batches(&schema, &batches).unwrap())
        };
        let (groups, narrow) = read(&whole.path().join("narrow_small.parquet"));
        assert_eq!(
            groups,
            [ROW_GROUP_ROWS, ROW_GROUP_ROWS, 250_000 - 2 * ROW_GROUP_ROWS]
        );
        assert_eq!(narrow.num_rows(), 250_000);
        let ids = narrow
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>();
        assert!(
            ids.values()
                .iter()
                .enumerate()
                .all(|(i, id)| *id == i as i64 + 1)
        );
        let (_, default) = read(&whole.path().join("wide_default.parquet"));
        for table in ["wide_small", "wide_large"] {
            let (_, other) = read(&whole.path().join(format!("{table}.parquet")));
            assert_eq!(other, default, "{table}");
        }
        let parts: Vec<RecordBatch> = (1..=4)
            .map(|part| read(&split.path().join(format!("wide_small.parquet.{part}"))).1)
            .collect();
        assert_eq!(concat_batches(&default.schema(), &parts).unwrap(), default);
    }
}

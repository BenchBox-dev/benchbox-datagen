pub(crate) mod calendar;
mod coffeeshop;
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

use self::delimited::TextFormat;
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

    fn text_format(&self, _table: &str) -> TextFormat {
        TextFormat::tbl()
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

#[rustfmt::skip]
const REGISTRY: &[&dyn Generator] = &[
    &ssb::Ssb,
    &coffeeshop::Coffeeshop,
];

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
    format: &TextFormat,
    start: u64,
    end: u64,
    sink: &mut W,
) -> io::Result<u64> {
    let schema = table.schema();
    delimited::check(&schema)?;
    let (mut rows, mut bytes) = (0, Vec::new());
    if format.header {
        delimited::encode_header(&schema, format, &mut bytes);
        sink.write_all(&bytes)?;
    }
    for batch in Part::new(table.clone(), start, end) {
        bytes.clear();
        delimited::encode(&batch, format, &mut bytes)?;
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
                    delimited::check(&schema).expect("every column has a text encoding");
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

    mod all_types {
        use std::collections::HashMap;
        use std::sync::LazyLock;

        use arrow::array::{
            ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray, Float32Array,
            Float64Array, Int32Array, Int64Array, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, TimeUnit};
        use benchbox_datagen_writers::{Compression, ParquetTableWriter};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        use super::super::*;
        use crate::schema::{self, Column, Kind};

        const COLUMNS: [Column; 11] = [
            ("i64", Kind::Int, false),
            ("i32", Kind::Int32, true),
            ("dec", Kind::Decimal, true),
            ("wide", Kind::Decimal128(18, 14), true),
            ("f32", Kind::Float32, true),
            ("f64", Kind::Float64, true),
            ("fixed", Kind::Fixed(2), true),
            ("flag", Kind::Bool, true),
            ("ts", Kind::Timestamp, true),
            ("day", Kind::Date, true),
            ("embedding", Kind::Vector(3), true),
        ];

        static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> =
            LazyLock::new(|| schema::build(&[("all", &COLUMNS)]));

        const ROWS: u64 = 5;

        #[derive(Debug)]
        struct AllTypes;

        impl Table for AllTypes {
            fn schema(&self) -> SchemaRef {
                SCHEMAS["all"].clone()
            }

            fn rows(&self) -> u64 {
                ROWS
            }

            fn block(&self, _index: u64) -> RecordBatch {
                let decimal = |values: Vec<Option<i128>>, precision, scale| -> ArrayRef {
                    Arc::new(
                        Decimal128Array::from(values)
                            .with_precision_and_scale(precision, scale)
                            .unwrap(),
                    )
                };
                let item = Arc::new(Field::new_list_field(DataType::Float32, true));
                let vectors = FixedSizeListArray::new(
                    item,
                    3,
                    Arc::new(Float32Array::from(vec![
                        0.5, 0.25, 0.125, 1.0, 2.0, 3.0, -1.5, 0.0, 1e-5, 7.0, 8.0, 9.0, 0.1, 0.2,
                        0.3,
                    ])),
                    Some(vec![true, true, true, false, true].into()),
                );
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(Int32Array::from(vec![
                        Some(-1),
                        None,
                        Some(3),
                        Some(4),
                        Some(5),
                    ])),
                    decimal(vec![Some(150), Some(-5), None, Some(0), Some(99)], 15, 2),
                    decimal(
                        vec![Some(-7_398_000_000_000), None, Some(1), Some(2), Some(3)],
                        18,
                        14,
                    ),
                    Arc::new(Float32Array::from(vec![
                        Some(0.1),
                        Some(2.5),
                        None,
                        Some(1e-5),
                        Some(3.0),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(0.1),
                        None,
                        Some(1e16),
                        Some(-2.5),
                        Some(100.0),
                    ])),
                    Arc::new(Float64Array::from(vec![
                        Some(1.0),
                        Some(2.345),
                        Some(-0.5),
                        None,
                        Some(7.0),
                    ])),
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        Some(false),
                        None,
                        Some(true),
                        Some(false),
                    ])),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        Some(0),
                        Some(1_000_000),
                        Some(1_500_000),
                        None,
                        Some(86_400_000_000),
                    ])),
                    Arc::new(Date32Array::from(vec![
                        Some(0),
                        Some(8_035),
                        None,
                        Some(1),
                        Some(2),
                    ])),
                    Arc::new(vectors),
                ];
                RecordBatch::try_new(self.schema(), columns).unwrap()
            }
        }

        fn whole(format: &TextFormat) -> String {
            let table: Arc<dyn Table> = Arc::new(AllTypes);
            let mut out = Vec::new();
            assert_eq!(
                write_delimited(&table, format, 0, ROWS, &mut out).unwrap(),
                ROWS
            );
            String::from_utf8(out).unwrap()
        }

        fn format() -> TextFormat {
            TextFormat::tbl()
        }

        #[test]
        fn kinds_map_to_their_arrow_types() {
            let schema = AllTypes.schema();
            let types: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect();
            assert_eq!(
                types[..10],
                [
                    DataType::Int64,
                    DataType::Int32,
                    DataType::Decimal128(15, 2),
                    DataType::Decimal128(18, 14),
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Boolean,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Date32,
                ]
            );
            assert_eq!(
                types[10],
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3
                )
            );
            assert!(schema.fields()[1].is_nullable());
            assert!(!schema.fields()[0].is_nullable());
        }

        #[test]
        fn every_type_has_one_text_form_in_pipe_tables() {
            assert_eq!(
                whole(&format()),
                "1|-1|1.50|-0.07398000000000|0.1|0.1|1.00|true|1970-01-01 00:00:00|1970-01-01|[0.5,0.25,0.125]\n\
                 2||-0.05||2.5||2.35|false|1970-01-01 00:00:01|1992-01-01|[1,2,3]\n\
                 3|3||0.00000000000001||1e+16|-0.50||1970-01-01 00:00:01.500000||[-1.5,0,1e-05]\n\
                 4|4|0.00|0.00000000000002|1e-05|-2.5||true||1970-01-02|\n\
                 5|5|0.99|0.00000000000003|3.0|100.0|7.00|false|1970-01-02 00:00:00|1970-01-03|[0.1,0.2,0.3]\n"
            );
        }

        #[test]
        fn csv_tables_carry_a_header_on_the_first_part_only() {
            let csv = TextFormat::csv(b',', true);
            let all = whole(&csv);
            assert!(
                all.starts_with("i64,i32,dec,wide,f32,f64,fixed,flag,ts,day,embedding\n1,-1,1.50,")
            );
            assert!(all.contains(",\"[0.5,0.25,0.125]\"\n"));
            assert_eq!(all.matches("i64,").count(), 1);
            assert!(all.contains(",1.00,"));
            assert!(all.contains(",2.35,"));
        }

        #[derive(Debug)]
        struct Empty;

        impl Table for Empty {
            fn schema(&self) -> SchemaRef {
                SCHEMAS["all"].clone()
            }

            fn rows(&self) -> u64 {
                0
            }

            fn block(&self, _index: u64) -> RecordBatch {
                RecordBatch::new_empty(self.schema())
            }
        }

        struct CsvGenerator;

        impl Generator for CsvGenerator {
            fn name(&self) -> &'static str {
                "csv_fixture"
            }

            fn tables(&self) -> &'static [&'static str] {
                &["full", "empty"]
            }

            fn schema(&self, _table: &str) -> Option<SchemaRef> {
                Some(SCHEMAS["all"].clone())
            }

            fn row_count(&self, table: &str, _scale_factor: f64) -> u64 {
                if table == "full" { ROWS } else { 0 }
            }

            fn open(&self, table: &str, _scale_factor: f64, _seed: u64) -> Arc<dyn Table> {
                if table == "full" {
                    Arc::new(AllTypes)
                } else {
                    Arc::new(Empty)
                }
            }

            fn key_ranges(&self, _scale_factor: f64) -> Vec<KeyRange> {
                Vec::new()
            }

            fn text_format(&self, _table: &str) -> TextFormat {
                TextFormat::csv(b',', true)
            }
        }

        fn written(
            generator: &CsvGenerator,
            table: &str,
            slices: &[crate::generate::Slice],
        ) -> String {
            let mut out = Vec::new();
            for slice in slices {
                crate::generate::write_synthetic(generator, table, slice, &mut out).unwrap();
            }
            String::from_utf8(out).unwrap()
        }

        #[test]
        fn a_fixed_column_with_only_a_delimiter_override_writes_its_scale() {
            let generator = CsvGenerator;
            let slices = crate::generate::synthetic_plan(&generator, "full", 1.0, None, 0);
            let text = written(&generator, "full", &slices);
            assert!(text.contains(",1.50,"), "{text}");
            assert!(text.contains(",2.35,"), "{text}");
        }

        #[test]
        fn the_header_is_written_once_for_single_files_and_for_any_part_count() {
            let generator = CsvGenerator;
            for table in ["full", "empty"] {
                let single = written(
                    &generator,
                    table,
                    &crate::generate::synthetic_plan(&generator, table, 1.0, None, 0),
                );
                assert!(single.starts_with("i64,i32,dec,"));
                assert_eq!(single.matches("i64,").count(), 1, "{table}");
                for parts in [1, 2, 3, 8] {
                    let joined: String = (1..=parts)
                        .map(|part| {
                            let slice = crate::generate::synthetic_part(
                                &generator, table, 1.0, part, parts, 0,
                            );
                            written(&generator, table, &[slice])
                        })
                        .collect();
                    assert_eq!(joined, single, "{table} in {parts} parts");
                }
            }
        }

        #[test]
        fn unsupported_columns_are_reported_before_any_output() {
            #[derive(Debug)]
            struct Unsigned;
            impl Table for Unsigned {
                fn schema(&self) -> SchemaRef {
                    Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                        "u",
                        DataType::UInt16,
                        false,
                    )]))
                }
                fn rows(&self) -> u64 {
                    1
                }
                fn block(&self, _index: u64) -> RecordBatch {
                    RecordBatch::new_empty(self.schema())
                }
            }
            let table: Arc<dyn Table> = Arc::new(Unsigned);
            let mut out = Vec::new();
            let error = write_delimited(&table, &TextFormat::tbl(), 0, 1, &mut out).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(out.is_empty());
        }

        #[test]
        fn parquet_keeps_every_type_exactly() {
            let table: Arc<dyn Table> = Arc::new(AllTypes);
            let batches: Vec<RecordBatch> = Reader::new(table.clone(), 0, ROWS)
                .map(Result::unwrap)
                .collect();
            let file = tempfile::tempfile().unwrap();
            let mut writer =
                ParquetTableWriter::new(file, table.schema(), Compression::None, 1).unwrap();
            for batch in &batches {
                writer.write(batch.clone()).unwrap();
            }
            let (file, rows) = writer.finish().unwrap();
            assert_eq!(rows, ROWS);
            let read: Vec<RecordBatch> = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let joined = arrow::compute::concat_batches(&table.schema(), &read).unwrap();
            assert_eq!(joined, batches[0]);
        }
    }
}

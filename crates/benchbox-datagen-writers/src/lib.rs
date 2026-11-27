use std::collections::VecDeque;
use std::fmt::{self, Display};
use std::io::{self, Write};
use std::str::FromStr;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves,
};
use parquet::basic::{Compression as ParquetCompression, GzipLevel, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;

pub const ROW_GROUP_ROWS: usize = 122_880;
pub const PARQUET_WRITER_KEY: &str = "benchbox.parquet_writer";

pub fn parquet_writer_identity() -> String {
    format!("benchbox-datagen-{}", env!("CARGO_PKG_VERSION_MAJOR"))
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("unknown compression {0:?}; supported: none, snappy, gzip, zstd")]
    UnknownCompression(String),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    None,
    #[default]
    Snappy,
    Gzip,
    Zstd,
}

impl Compression {
    pub fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Snappy => "snappy",
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
        }
    }

    fn parquet(self) -> ParquetCompression {
        match self {
            Compression::None => ParquetCompression::UNCOMPRESSED,
            Compression::Snappy => ParquetCompression::SNAPPY,
            Compression::Gzip => ParquetCompression::GZIP(GzipLevel::default()),
            Compression::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        [
            Compression::None,
            Compression::Snappy,
            Compression::Gzip,
            Compression::Zstd,
        ]
        .into_iter()
        .find(|compression| compression.name().eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::UnknownCompression(name.to_owned()))
    }
}

#[derive(Debug, Default)]
pub struct DelimitedBuffer {
    bytes: Vec<u8>,
    rows: u64,
}

impl DelimitedBuffer {
    pub fn push<R: Display>(&mut self, row: &R) {
        write!(self.bytes, "{row}").expect("writing to a Vec cannot fail");
        match self.bytes.last_mut() {
            Some(last @ b'|') => *last = b'\n',
            _ => self.bytes.push(b'\n'),
        }
        self.rows += 1;
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn buffered(&self) -> usize {
        self.bytes.len()
    }

    pub fn flush_to<W: Write>(&mut self, sink: &mut W) -> io::Result<()> {
        sink.write_all(&self.bytes)?;
        self.bytes.clear();
        Ok(())
    }
}

type Encoded = Result<Vec<ArrowColumnChunk>, Error>;

struct EncodeJob {
    group: usize,
    batch: RecordBatch,
    reply: mpsc::Sender<Encoded>,
}

fn encode_group(
    factory: &ArrowRowGroupWriterFactory,
    schema: &SchemaRef,
    group: usize,
    batch: &RecordBatch,
) -> Encoded {
    let mut writers = factory.create_column_writers(group)?;
    let mut targets = writers.iter_mut();
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        for leaf in compute_leaves(field, array)? {
            targets
                .next()
                .expect("one column writer per Parquet leaf")
                .write(&leaf)?;
        }
    }
    Ok(writers
        .into_iter()
        .map(ArrowColumnWriter::close)
        .collect::<Result<_, _>>()?)
}

struct EncodePool {
    jobs: Option<mpsc::Sender<EncodeJob>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl EncodePool {
    fn new(threads: usize, factory: &Arc<ArrowRowGroupWriterFactory>, schema: &SchemaRef) -> Self {
        let (jobs, queue) = mpsc::channel::<EncodeJob>();
        let queue = Arc::new(Mutex::new(queue));
        let workers = (0..threads)
            .map(|_| {
                let (queue, factory, schema) = (queue.clone(), factory.clone(), schema.clone());
                thread::spawn(move || {
                    loop {
                        let job = match queue.lock() {
                            Ok(queue) => queue.recv(),
                            Err(_) => return,
                        };
                        let Ok(job) = job else { return };
                        let encoded = encode_group(&factory, &schema, job.group, &job.batch);
                        if job.reply.send(encoded).is_err() {
                            return;
                        }
                    }
                })
            })
            .collect();
        Self {
            jobs: Some(jobs),
            workers,
        }
    }
}

impl Drop for EncodePool {
    fn drop(&mut self) {
        self.jobs.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub struct ParquetTableWriter<W: Write + Send> {
    file: SerializedFileWriter<W>,
    factory: Arc<ArrowRowGroupWriterFactory>,
    schema: SchemaRef,
    pool: Option<EncodePool>,
    in_flight: VecDeque<mpsc::Receiver<Encoded>>,
    max_in_flight: usize,
    groups: usize,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    rows: u64,
}

impl<W: Write + Send> ParquetTableWriter<W> {
    pub fn new(
        sink: W,
        schema: SchemaRef,
        compression: Compression,
        threads: usize,
    ) -> Result<Self, Error> {
        let properties = WriterProperties::builder()
            .set_compression(compression.parquet())
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .set_created_by(format!(
                "benchbox-datagen version {}",
                env!("CARGO_PKG_VERSION")
            ))
            .set_key_value_metadata(Some(vec![KeyValue::new(
                PARQUET_WRITER_KEY.to_owned(),
                parquet_writer_identity(),
            )]))
            .build();
        let (file, factory) = ArrowWriter::try_new(sink, schema.clone(), Some(properties))?
            .into_serialized_writer()?;
        let factory = Arc::new(factory);
        let threads = threads.max(1);
        let pool = (threads > 1).then(|| EncodePool::new(threads, &factory, &schema));
        Ok(Self {
            file,
            factory,
            schema,
            pool,
            in_flight: VecDeque::new(),
            max_in_flight: threads + 1,
            groups: 0,
            pending: Vec::new(),
            pending_rows: 0,
            rows: 0,
        })
    }

    pub fn write(&mut self, batch: RecordBatch) -> Result<(), Error> {
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
        if self.pending_rows < ROW_GROUP_ROWS {
            return Ok(());
        }
        let merged = concat_batches(&self.schema, &self.pending)?;
        self.pending.clear();
        let mut offset = 0;
        while merged.num_rows() - offset >= ROW_GROUP_ROWS {
            self.submit(merged.slice(offset, ROW_GROUP_ROWS))?;
            offset += ROW_GROUP_ROWS;
        }
        self.pending_rows = merged.num_rows() - offset;
        if self.pending_rows > 0 {
            self.pending.push(merged.slice(offset, self.pending_rows));
        }
        Ok(())
    }

    fn submit(&mut self, group: RecordBatch) -> Result<(), Error> {
        self.rows += group.num_rows() as u64;
        let index = self.groups;
        self.groups += 1;
        let Some(pool) = &self.pool else {
            let chunks = encode_group(&self.factory, &self.schema, index, &group)?;
            return self.append(chunks);
        };
        let (reply, encoded) = mpsc::channel();
        let job = EncodeJob {
            group: index,
            batch: group,
            reply,
        };
        pool.jobs
            .as_ref()
            .expect("the job queue is open until the writer is finished")
            .send(job)
            .map_err(|_| Error::Io(io::Error::other("Parquet encoder thread stopped")))?;
        self.in_flight.push_back(encoded);
        while self.in_flight.len() > self.max_in_flight {
            self.drain_one()?;
        }
        Ok(())
    }

    fn drain_one(&mut self) -> Result<(), Error> {
        let Some(encoded) = self.in_flight.pop_front() else {
            return Ok(());
        };
        let chunks = encoded
            .recv()
            .map_err(|_| Error::Io(io::Error::other("Parquet encoder thread stopped")))??;
        self.append(chunks)
    }

    fn append(&mut self, chunks: Vec<ArrowColumnChunk>) -> Result<(), Error> {
        let mut group = self.file.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut group)?;
        }
        group.close()?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(W, u64), Error> {
        if self.pending_rows > 0 {
            let merged = concat_batches(&self.schema, &self.pending)?;
            self.pending.clear();
            self.submit(merged)?;
        }
        while !self.in_flight.is_empty() {
            self.drain_one()?;
        }
        let rows = self.rows;
        Ok((self.file.into_inner()?, rows))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]))
    }

    fn batch(range: std::ops::Range<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(range.clone())),
                Arc::new(StringArray::from_iter_values(
                    range.map(|value| format!("n{value}")),
                )),
            ],
        )
        .unwrap()
    }

    fn write_all(
        splits: impl IntoIterator<Item = std::ops::Range<i64>>,
        compression: Compression,
        threads: usize,
    ) -> Vec<u8> {
        let mut writer =
            ParquetTableWriter::new(Vec::new(), schema(), compression, threads).unwrap();
        for split in splits {
            writer.write(batch(split)).unwrap();
        }
        writer.finish().unwrap().0
    }

    #[test]
    fn delimited_rows_drop_the_trailing_separator() {
        struct Row;
        impl Display for Row {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("1|two|3|")
            }
        }
        let mut buffer = DelimitedBuffer::default();
        buffer.push(&Row);
        buffer.push(&Row);
        assert_eq!(buffer.as_bytes(), b"1|two|3\n1|two|3\n");
        assert_eq!(buffer.rows(), 2);
    }

    #[test]
    fn compression_names_parse_case_insensitively() {
        assert_eq!("ZSTD".parse::<Compression>().unwrap(), Compression::Zstd);
        assert!(matches!(
            "lz4".parse::<Compression>(),
            Err(Error::UnknownCompression(_))
        ));
    }

    #[test]
    fn row_groups_hold_the_fixed_row_count_whatever_the_batch_sizes() {
        let total = (ROW_GROUP_ROWS * 2 + 77) as i64;
        let coarse = write_all(std::iter::once(0..total), Compression::Zstd, 1);
        let fine_splits: Vec<_> = (0..total)
            .step_by(10_007)
            .map(|start| start..(start + 10_007).min(total))
            .collect();
        let fine = write_all(fine_splits.clone(), Compression::Zstd, 1);
        assert_eq!(coarse, fine);
        assert_eq!(coarse, write_all(fine_splits, Compression::Zstd, 4));

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(coarse)).unwrap();
        let metadata = builder.metadata().file_metadata();
        assert_eq!(metadata.num_rows(), total);
        let groups: Vec<_> = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|group| group.num_rows() as usize)
            .collect();
        assert_eq!(groups, [ROW_GROUP_ROWS, ROW_GROUP_ROWS, 77]);
        let writer = metadata
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|entry| entry.key == PARQUET_WRITER_KEY)
            .and_then(|entry| entry.value.clone());
        assert_eq!(writer, Some(parquet_writer_identity()));
    }

    #[test]
    fn every_compression_round_trips() {
        for compression in [
            Compression::None,
            Compression::Snappy,
            Compression::Gzip,
            Compression::Zstd,
        ] {
            let bytes = write_all(std::iter::once(0..1000), compression, 2);
            let rows: usize = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap()
                .map(|batch| batch.unwrap().num_rows())
                .sum();
            assert_eq!(rows, 1000, "{compression}");
        }
    }
}

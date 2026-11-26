use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use arrow::record_batch::RecordBatch;
use benchbox_datagen_writers::{Compression, Error as WriterError, ParquetTableWriter};

use crate::{Benchmark, Error, conformed, pipeline, synthetic, tpch, validate_scale_factor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Format {
    Tbl,
    Parquet,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Format::Tbl => "tbl",
            Format::Parquet => "parquet",
        }
    }
}

impl FromStr for Format {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        [Format::Tbl, Format::Parquet]
            .into_iter()
            .find(|format| format.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::UnknownFormat(name.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutputRequest {
    pub benchmark: Benchmark,
    pub scale_factor: f64,
    pub format: Format,
    pub compression: Option<Compression>,
    pub threads: Option<NonZeroUsize>,
    pub parts: i32,
    pub seed: Option<u64>,
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    pub table: &'static str,
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
}

const MAX_BUFFERED_THREADS: usize = 8;
const MAX_WORKERS: usize = 16;
const MAX_SYNTHETIC_BUFFERED: usize = 3;

fn io_error(path: &Path, error: impl Display) -> Error {
    Error::Write(format!("{}: {error}", path.display()))
}

fn encode_error(path: &Path, error: impl Display) -> Error {
    Error::Encode(format!("{}: {error}", path.display()))
}

fn writer_error(path: &Path, error: WriterError) -> Error {
    match error {
        WriterError::Io(error) => io_error(path, error),
        other => encode_error(path, other),
    }
}

fn file_path(dir: &Path, table: &str, part: Option<i32>, format: Format) -> PathBuf {
    match part {
        None => dir.join(format!("{table}.{}", format.name())),
        Some(part) => dir.join(format!("{table}.{}.{part}", format.name())),
    }
}

struct Cancellable<'a, W> {
    inner: W,
    cancel: &'a AtomicBool,
}

impl<W: Write> Write for Cancellable<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub fn write_benchmark(request: &OutputRequest) -> Result<Vec<FileStats>, Error> {
    write_benchmark_cancellable(request, &AtomicBool::new(false))
}

pub fn write_benchmark_cancellable(
    request: &OutputRequest,
    cancel: &AtomicBool,
) -> Result<Vec<FileStats>, Error> {
    write_with(request, cancel, None)
}

fn write_with(
    request: &OutputRequest,
    cancel: &AtomicBool,
    chunk_rows: Option<u64>,
) -> Result<Vec<FileStats>, Error> {
    validate_scale_factor(request.scale_factor)?;
    if request.parts < 1 {
        return Err(Error::InvalidParts(request.parts));
    }
    let compression = match (request.format, request.compression) {
        (Format::Tbl, None | Some(Compression::None)) => Compression::None,
        (Format::Tbl, Some(compression)) => {
            return Err(Error::UnsupportedCompression {
                format: Format::Tbl,
                compression: compression.name(),
            });
        }
        (Format::Parquet, compression) => compression.unwrap_or_default(),
    };
    let seed = request.benchmark.resolve_seed(request.seed)?;
    let threads = request
        .threads
        .or_else(|| thread::available_parallelism().ok())
        .map_or(1, NonZeroUsize::get);
    fs::create_dir_all(&request.output_dir).map_err(|e| io_error(&request.output_dir, e))?;
    let job = TableJob {
        benchmark: request.benchmark,
        seed,
        scale_factor: request.scale_factor,
        format: request.format,
        compression,
        workers: threads.min(MAX_WORKERS),
        buffered: threads.min(MAX_BUFFERED_THREADS),
        chunk_rows,
        cancel,
        dir: &request.output_dir,
    };
    let mut stats = Vec::new();
    let outcome = (|| {
        for table in request.benchmark.tables() {
            let split = request.parts > 1 && !request.benchmark.is_unsplit(table);
            if split {
                job.write_parts(table, request.parts, &mut stats)?;
            } else {
                stats.push(job.write_single(table)?);
            }
        }
        Ok(())
    })();
    match outcome {
        Ok(()) => Ok(stats),
        Err(error) => {
            for table in request.benchmark.tables() {
                let _ =
                    fs::remove_file(file_path(&request.output_dir, table, None, request.format));
                for part in 1..=request.parts {
                    let path = file_path(&request.output_dir, table, Some(part), request.format);
                    let _ = fs::remove_file(path);
                }
            }
            Err(if cancel.load(Ordering::Relaxed) {
                Error::Cancelled
            } else {
                error
            })
        }
    }
}

struct TableJob<'a> {
    benchmark: Benchmark,
    seed: u64,
    scale_factor: f64,
    format: Format,
    compression: Compression,
    workers: usize,
    buffered: usize,
    chunk_rows: Option<u64>,
    cancel: &'a AtomicBool,
    dir: &'a Path,
}

impl TableJob<'_> {
    fn write_parts(
        &self,
        table: &'static str,
        parts: i32,
        stats: &mut Vec<FileStats>,
    ) -> Result<(), Error> {
        pipeline::ordered(
            parts as usize,
            self.workers,
            self.workers,
            self.cancel,
            || Error::Cancelled,
            |index| {
                let part = index as i32 + 1;
                let path = file_path(self.dir, table, Some(part), self.format);
                let rows = self.stream_file(table, part, parts, &path)?;
                self.stats(table, path, rows)
            },
            |file: FileStats| {
                stats.push(file);
                Ok(())
            },
        )
    }

    fn stats(&self, table: &'static str, path: PathBuf, rows: u64) -> Result<FileStats, Error> {
        let bytes = fs::metadata(&path).map_err(|e| io_error(&path, e))?.len();
        Ok(FileStats {
            table,
            path,
            rows,
            bytes,
        })
    }

    fn stream_synthetic_file(
        &self,
        table: &str,
        part: i32,
        parts: i32,
        path: &Path,
    ) -> Result<u64, Error> {
        let file = File::create(path).map_err(|e| io_error(path, e))?;
        let source = synthetic::open(self.benchmark, table, self.scale_factor, self.seed);
        let batches = synthetic::Part::new(source.clone(), part, parts);
        match self.format {
            Format::Tbl => {
                let mut sink = Cancellable {
                    inner: BufWriter::new(file),
                    cancel: self.cancel,
                };
                let (mut rows, mut bytes) = (0, Vec::new());
                for batch in batches {
                    bytes.clear();
                    synthetic::delimited::encode(&batch, &mut bytes);
                    rows += batch.num_rows() as u64;
                    sink.write_all(&bytes).map_err(|e| io_error(path, e))?;
                }
                sink.flush().map_err(|e| io_error(path, e))?;
                Ok(rows)
            }
            Format::Parquet => {
                let mut writer = ParquetTableWriter::new(
                    BufWriter::new(file),
                    source.schema(),
                    self.compression,
                    1,
                )
                .map_err(|e| writer_error(path, e))?;
                for batch in batches {
                    if self.cancel.load(Ordering::Relaxed) {
                        return Err(Error::Cancelled);
                    }
                    writer.write(batch).map_err(|e| writer_error(path, e))?;
                }
                let (mut sink, rows) = writer.finish().map_err(|e| writer_error(path, e))?;
                sink.flush().map_err(|e| io_error(path, e))?;
                Ok(rows)
            }
        }
    }

    fn write_synthetic_single(&self, table: &'static str) -> Result<FileStats, Error> {
        let path = file_path(self.dir, table, None, self.format);
        let source = synthetic::open(self.benchmark, table, self.scale_factor, self.seed);
        let file = File::create(&path).map_err(|e| io_error(&path, e))?;
        let buffered = self.buffered.min(MAX_SYNTHETIC_BUFFERED);
        let window = buffered * 2;
        let jobs = source.blocks() as usize;
        let rows = match self.format {
            Format::Tbl => {
                let mut file = BufWriter::new(file);
                let mut rows = 0;
                pipeline::ordered(
                    jobs,
                    self.workers,
                    window,
                    self.cancel,
                    || Error::Cancelled,
                    |index| {
                        let batch = source.block(index as u64);
                        let mut bytes = Vec::new();
                        synthetic::delimited::encode(&batch, &mut bytes);
                        Ok((bytes, batch.num_rows() as u64))
                    },
                    |(bytes, count): (Vec<u8>, u64)| {
                        rows += count;
                        file.write_all(&bytes).map_err(|e| io_error(&path, e))
                    },
                )?;
                file.flush().map_err(|e| io_error(&path, e))?;
                rows
            }
            Format::Parquet => {
                let mut writer = ParquetTableWriter::new(
                    BufWriter::new(file),
                    source.schema(),
                    self.compression,
                    buffered,
                )
                .map_err(|e| writer_error(&path, e))?;
                pipeline::ordered(
                    jobs,
                    self.workers,
                    window,
                    self.cancel,
                    || Error::Cancelled,
                    |index| Ok(source.block(index as u64)),
                    |batch: RecordBatch| writer.write(batch).map_err(|e| writer_error(&path, e)),
                )?;
                let (mut sink, rows) = writer.finish().map_err(|e| writer_error(&path, e))?;
                sink.flush().map_err(|e| io_error(&path, e))?;
                rows
            }
        };
        self.stats(table, path, rows)
    }

    fn stream_file(&self, table: &str, part: i32, parts: i32, path: &Path) -> Result<u64, Error> {
        if self.benchmark == Benchmark::Ssb {
            return self.stream_synthetic_file(table, part, parts, path);
        }
        let file = File::create(path).map_err(|e| io_error(path, e))?;
        match self.format {
            Format::Tbl => {
                let mut sink = Cancellable {
                    inner: file,
                    cancel: self.cancel,
                };
                tpch::write_delimited(table, self.scale_factor, part, parts, &mut sink)
                    .map_err(|e| io_error(path, e))
            }
            Format::Parquet => {
                let reader = conformed(table, self.scale_factor, part, parts);
                let mut writer = ParquetTableWriter::new(
                    BufWriter::new(file),
                    reader.schema_ref(),
                    self.compression,
                    1,
                )
                .map_err(|e| writer_error(path, e))?;
                for batch in reader {
                    if self.cancel.load(Ordering::Relaxed) {
                        return Err(Error::Cancelled);
                    }
                    writer
                        .write(batch.map_err(|e| encode_error(path, e))?)
                        .map_err(|e| writer_error(path, e))?;
                }
                let (mut sink, rows) = writer.finish().map_err(|e| writer_error(path, e))?;
                sink.flush().map_err(|e| io_error(path, e))?;
                Ok(rows)
            }
        }
    }

    fn write_single(&self, table: &'static str) -> Result<FileStats, Error> {
        if self.benchmark == Benchmark::Ssb {
            return self.write_synthetic_single(table);
        }
        let path = file_path(self.dir, table, None, self.format);
        let (driver_rows, default_chunk_rows) = tpch::chunk_driver_rows(table, self.scale_factor);
        let chunk_rows = self.chunk_rows.unwrap_or(default_chunk_rows).max(1);
        let chunks = driver_rows.div_ceil(chunk_rows).clamp(1, i32::MAX as u64) as i32;
        let file = File::create(&path).map_err(|e| io_error(&path, e))?;
        let window = self.buffered * 2;
        let rows = match self.format {
            Format::Tbl => {
                let mut file = file;
                let mut rows = 0;
                pipeline::ordered(
                    chunks as usize,
                    self.workers,
                    window,
                    self.cancel,
                    || Error::Cancelled,
                    |index| {
                        let mut bytes = Vec::new();
                        let rows = tpch::write_delimited(
                            table,
                            self.scale_factor,
                            index as i32 + 1,
                            chunks,
                            &mut bytes,
                        )
                        .map_err(|e| io_error(&path, e))?;
                        Ok((bytes, rows))
                    },
                    |(bytes, count): (Vec<u8>, u64)| {
                        rows += count;
                        file.write_all(&bytes).map_err(|e| io_error(&path, e))
                    },
                )?;
                rows
            }
            Format::Parquet => {
                let schema = conformed(table, self.scale_factor, 1, 1).schema_ref();
                let mut writer = ParquetTableWriter::new(
                    BufWriter::new(file),
                    schema,
                    self.compression,
                    self.buffered,
                )
                .map_err(|e| writer_error(&path, e))?;
                pipeline::ordered(
                    chunks as usize,
                    self.workers,
                    window,
                    self.cancel,
                    || Error::Cancelled,
                    |index| {
                        conformed(table, self.scale_factor, index as i32 + 1, chunks)
                            .collect::<Result<Vec<RecordBatch>, _>>()
                            .map_err(|e| encode_error(&path, e))
                    },
                    |batches: Vec<RecordBatch>| {
                        for batch in batches {
                            writer.write(batch).map_err(|e| writer_error(&path, e))?;
                        }
                        Ok(())
                    },
                )?;
                let (mut sink, rows) = writer.finish().map_err(|e| writer_error(&path, e))?;
                sink.flush().map_err(|e| io_error(&path, e))?;
                rows
            }
        };
        self.stats(table, path, rows)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow::datatypes::DataType;
    use benchbox_datagen_writers::{PARQUET_WRITER_KEY, ROW_GROUP_ROWS, parquet_writer_identity};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::{row_counts, table_schema};

    fn request(
        dir: &Path,
        format: Format,
        threads: usize,
        parts: i32,
        scale_factor: f64,
    ) -> OutputRequest {
        OutputRequest {
            benchmark: Benchmark::Tpch,
            scale_factor,
            format,
            compression: None,
            threads: NonZeroUsize::new(threads),
            parts,
            seed: None,
            output_dir: dir.to_path_buf(),
        }
    }

    fn written(request: &OutputRequest) -> Vec<(FileStats, Vec<u8>)> {
        write_benchmark(request)
            .unwrap()
            .into_iter()
            .map(|stats| {
                let bytes = fs::read(&stats.path).unwrap();
                (stats, bytes)
            })
            .collect()
    }

    fn hashes(files: &[(FileStats, Vec<u8>)]) -> Vec<(String, &Vec<u8>)> {
        files
            .iter()
            .map(|(stats, bytes)| {
                (
                    stats.path.file_name().unwrap().to_string_lossy().into(),
                    bytes,
                )
            })
            .collect()
    }

    #[test]
    fn delimited_files_are_identical_for_any_thread_count() {
        let (one, four) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let single = written(&request(one.path(), Format::Tbl, 1, 1, 0.01));
        let many = written(&request(four.path(), Format::Tbl, 4, 1, 0.01));
        assert_eq!(hashes(&single), hashes(&many));
        let counts = row_counts(Benchmark::Tpch, 0.01).unwrap();
        for ((stats, bytes), (table, rows)) in single.iter().zip(counts) {
            assert_eq!(stats.table, table);
            assert_eq!(stats.rows, rows, "{table}");
            assert_eq!(stats.bytes as usize, bytes.len(), "{table}");
            assert_eq!(
                bytes.iter().filter(|byte| **byte == b'\n').count() as u64,
                rows
            );
            assert!(!bytes.windows(2).any(|pair| pair == b"|\n"), "{table}");
        }
    }

    #[test]
    fn delimited_parts_concatenate_to_the_single_file() {
        let (whole, split) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        written(&request(whole.path(), Format::Tbl, 2, 1, 0.01));
        let stats = write_benchmark(&request(split.path(), Format::Tbl, 3, 4, 0.01)).unwrap();
        for table in Benchmark::Tpch.tables() {
            let expected = fs::read(whole.path().join(format!("{table}.tbl"))).unwrap();
            let files: Vec<_> = stats.iter().filter(|stats| stats.table == *table).collect();
            let mut joined = Vec::new();
            for file in &files {
                joined.extend(fs::read(&file.path).unwrap());
            }
            assert_eq!(joined, expected, "{table}");
            let single = matches!(*table, "nation" | "region");
            assert_eq!(files.len(), if single { 1 } else { 4 }, "{table}");
        }
    }

    #[test]
    fn parquet_bytes_do_not_depend_on_threads() {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let runs: Vec<_> = [1, 4, 9]
            .into_iter()
            .zip(&dirs)
            .map(|(threads, dir)| written(&request(dir.path(), Format::Parquet, threads, 1, 0.1)))
            .collect();
        assert_eq!(hashes(&runs[0]), hashes(&runs[1]));
        assert_eq!(hashes(&runs[0]), hashes(&runs[2]));
    }

    #[test]
    fn parquet_files_carry_benchbox_types_row_groups_and_writer_identity() {
        let dir = tempfile::tempdir().unwrap();
        let files = write_benchmark(&request(dir.path(), Format::Parquet, 2, 1, 0.1)).unwrap();
        assert_eq!(files.len(), 8);
        let counts = row_counts(Benchmark::Tpch, 0.1).unwrap();
        for (file, (table, rows)) in files.iter().zip(counts) {
            assert_eq!(file.rows, rows, "{table}");
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path).unwrap()).unwrap();
            assert_eq!(
                builder.schema().fields(),
                table_schema(Benchmark::Tpch, table).unwrap().fields(),
                "{table}"
            );
            assert!(
                !builder
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| matches!(field.data_type(), DataType::Utf8View | DataType::Int32))
            );
            let metadata = builder.metadata();
            let identity = metadata
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .find(|entry| entry.key == PARQUET_WRITER_KEY)
                .and_then(|entry| entry.value.clone());
            assert_eq!(identity, Some(parquet_writer_identity()));
            let groups: Vec<usize> = metadata
                .row_groups()
                .iter()
                .map(|group| group.num_rows() as usize)
                .collect();
            assert!(
                groups
                    .iter()
                    .rev()
                    .skip(1)
                    .all(|rows| *rows == ROW_GROUP_ROWS)
            );
            assert_eq!(groups.iter().sum::<usize>() as u64, rows);
            assert_eq!(file.bytes, fs::metadata(&file.path).unwrap().len());
        }
        let lineitem = files.iter().find(|file| file.table == "lineitem").unwrap();
        let groups = ParquetRecordBatchReaderBuilder::try_new(File::open(&lineitem.path).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        assert_eq!(groups as u64, lineitem.rows.div_ceil(ROW_GROUP_ROWS as u64));
    }

    #[test]
    fn parquet_parts_write_one_file_per_part() {
        let dir = tempfile::tempdir().unwrap();
        let files = write_benchmark(&request(dir.path(), Format::Parquet, 3, 3, 0.01)).unwrap();
        let counts = row_counts(Benchmark::Tpch, 0.01).unwrap();
        for (table, rows) in counts {
            let parts: Vec<_> = files.iter().filter(|file| file.table == table).collect();
            let single = matches!(table, "nation" | "region");
            assert_eq!(parts.len(), if single { 1 } else { 3 }, "{table}");
            assert_eq!(parts.iter().map(|file| file.rows).sum::<u64>(), rows);
        }
        assert!(dir.path().join("lineitem.parquet.2").exists());
    }

    #[test]
    fn unsupported_requests_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut bad = request(dir.path(), Format::Tbl, 1, 1, 0.01);
        bad.compression = Some(Compression::Zstd);
        assert!(matches!(
            write_benchmark(&bad),
            Err(Error::UnsupportedCompression { .. })
        ));
        assert_eq!(
            write_benchmark(&request(dir.path(), Format::Tbl, 1, 0, 0.01)).err(),
            Some(Error::InvalidParts(0))
        );
        assert_eq!(
            write_benchmark(&request(dir.path(), Format::Tbl, 1, 1, 0.0)).err(),
            Some(Error::InvalidScaleFactor(0.0))
        );
        assert_eq!("PARQUET".parse::<Format>(), Ok(Format::Parquet));
        assert!(matches!(
            "csv".parse::<Format>(),
            Err(Error::UnknownFormat(_))
        ));
    }

    #[test]
    fn part_files_follow_the_table_format_part_naming() {
        let dir = tempfile::tempdir().unwrap();
        write_benchmark(&request(dir.path(), Format::Tbl, 2, 3, 0.01)).unwrap();
        for part in 1..=3 {
            assert!(dir.path().join(format!("lineitem.tbl.{part}")).exists());
        }
        assert!(dir.path().join("nation.tbl").exists());
    }

    #[test]
    fn multi_chunk_output_equals_single_chunk_output() {
        for format in [Format::Tbl, Format::Parquet] {
            for threads in [1, 4] {
                let (single, many) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
                let request = request(single.path(), format, threads, 1, 0.01);
                let whole =
                    write_with(&request, &AtomicBool::new(false), Some(u64::MAX / 2)).unwrap();
                let mut chunked_request = request.clone();
                chunked_request.output_dir = many.path().to_path_buf();
                let chunked =
                    write_with(&chunked_request, &AtomicBool::new(false), Some(97)).unwrap();
                for (a, b) in whole.iter().zip(&chunked) {
                    assert_eq!(a.table, b.table);
                    assert_eq!(
                        fs::read(&a.path).unwrap(),
                        fs::read(&b.path).unwrap(),
                        "{} {format:?} threads={threads}",
                        a.table
                    );
                }
            }
        }
    }

    #[test]
    fn a_cancelled_write_stops_and_leaves_no_files() {
        for (format, parts) in [(Format::Tbl, 1), (Format::Parquet, 1), (Format::Tbl, 3)] {
            let dir = tempfile::tempdir().unwrap();
            let result = write_benchmark_cancellable(
                &request(dir.path(), format, 2, parts, 0.01),
                &AtomicBool::new(true),
            );
            assert_eq!(result, Err(Error::Cancelled));
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
        }
    }
}

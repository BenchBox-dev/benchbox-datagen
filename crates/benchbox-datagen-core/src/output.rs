use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use arrow::record_batch::RecordBatch;
use benchbox_datagen_writers::{Compression, Error as WriterError, ParquetTableWriter};
use parquet::errors::ParquetError;

use crate::generate;
use crate::{Benchmark, Error, conformed, pipeline, validate_scale_factor};

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

const MAX_BUFFERED_THREADS: usize = 4;
const MAX_WORKERS: usize = 16;

fn io_error(path: &Path, error: impl Display) -> Error {
    Error::Write(format!("{}: {error}", path.display()))
}

fn encode_error(path: &Path, error: impl Display) -> Error {
    Error::Encode(format!("{}: {error}", path.display()))
}

fn writer_error(path: &Path, error: WriterError) -> Error {
    match error {
        WriterError::Io(error) => io_error(path, error),
        WriterError::Parquet(ParquetError::External(source)) => {
            match source.downcast::<io::Error>() {
                Ok(error) => io_error(path, error),
                Err(source) => encode_error(path, source),
            }
        }
        other => encode_error(path, other),
    }
}

fn file_path(
    dir: &Path,
    benchmark: Benchmark,
    table: &str,
    part: Option<i32>,
    format: Format,
) -> PathBuf {
    let extension = match format {
        Format::Tbl => generate::delimited_extension(benchmark, table),
        Format::Parquet => format.name(),
    };
    match part {
        None => dir.join(format!("{table}.{extension}")),
        Some(part) => dir.join(format!("{table}.{extension}.{part}")),
    }
}

struct Cancellable<'a, W> {
    inner: W,
    cancel: &'a AtomicBool,
}

impl<W: Write> Write for Cancellable<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(io::Error::other("cancelled"));
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
    validate_scale_factor(request.benchmark, request.scale_factor)?;
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
        created: Mutex::new(Vec::new()),
    };
    let mut stats = Vec::new();
    let outcome = (|| {
        for table in request.benchmark.tables() {
            let split = request.parts > 1
                && generate::splittable(request.benchmark, table, request.scale_factor);
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
            let created = job.created.into_inner().unwrap_or_else(|e| e.into_inner());
            for path in created {
                let _ = fs::remove_file(path);
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
    created: Mutex<Vec<PathBuf>>,
}

impl TableJob<'_> {
    fn create(&self, path: &Path) -> Result<File, Error> {
        let file = File::create(path).map_err(|e| io_error(path, e))?;
        self.created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(path.to_path_buf());
        Ok(file)
    }

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
                let path = file_path(self.dir, self.benchmark, table, Some(part), self.format);
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

    fn stream_file(&self, table: &str, part: i32, parts: i32, path: &Path) -> Result<u64, Error> {
        let file = self.create(path)?;
        let slice = generate::part_slice(
            self.benchmark,
            table,
            self.scale_factor,
            part,
            parts,
            self.seed,
        );
        generate::warm(&slice, self.cancel)?;
        match self.format {
            Format::Tbl => {
                let mut sink = Cancellable {
                    inner: file,
                    cancel: self.cancel,
                };
                generate::write_delimited(
                    self.benchmark,
                    table,
                    self.scale_factor,
                    &slice,
                    &mut sink,
                )
                .map_err(|e| io_error(path, e))
            }
            Format::Parquet => {
                let reader = conformed(self.benchmark, table, self.scale_factor, &slice);
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
        let path = file_path(self.dir, self.benchmark, table, None, self.format);
        let slices = generate::plan(
            self.benchmark,
            table,
            self.scale_factor,
            self.chunk_rows,
            self.seed,
        );
        let file = self.create(&path)?;
        let window = self.buffered * 2;
        let rows = match self.format {
            Format::Tbl => {
                let mut file = file;
                let mut rows = 0;
                pipeline::ordered(
                    slices.len(),
                    self.workers,
                    window,
                    self.cancel,
                    || Error::Cancelled,
                    |index| {
                        let mut bytes = Vec::new();
                        let rows = generate::write_delimited(
                            self.benchmark,
                            table,
                            self.scale_factor,
                            &slices[index],
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
                let schema = generate::schema(self.benchmark, table);
                let mut writer = ParquetTableWriter::new(
                    BufWriter::new(file),
                    schema,
                    self.compression,
                    self.buffered,
                )
                .map_err(|e| writer_error(&path, e))?;
                pipeline::ordered(
                    slices.len(),
                    self.workers,
                    window,
                    self.cancel,
                    || Error::Cancelled,
                    |index| {
                        conformed(self.benchmark, table, self.scale_factor, &slices[index])
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
    fn tpcds_multi_chunk_output_equals_single_chunk_output() {
        let tables = [
            "call_center",
            "item",
            "store",
            "web_page",
            "web_site",
            "web_sales",
            "web_returns",
        ];
        let write = |table: &'static str, format: Format, chunk_rows: u64| {
            let dir = tempfile::tempdir().unwrap();
            let cancel = AtomicBool::new(false);
            let job = TableJob {
                benchmark: Benchmark::Tpcds,
                seed: 0,
                scale_factor: 1.0,
                format,
                compression: Compression::None,
                workers: 4,
                buffered: 2,
                chunk_rows: Some(chunk_rows),
                cancel: &cancel,
                dir: dir.path(),
                created: Mutex::new(Vec::new()),
            };
            let stats = job.write_single(table).unwrap();
            (stats.rows, fs::read(&stats.path).unwrap())
        };
        for format in [Format::Tbl, Format::Parquet] {
            for table in tables {
                let total = crate::tpcds::source_rows(table, 1.0);
                let mut step = total.div_ceil(11).max(5);
                while step.is_multiple_of(2) || step.is_multiple_of(3) {
                    step += 1;
                }
                assert!(generate::plan(Benchmark::Tpcds, table, 1.0, Some(step), 0).len() > 1);
                assert_eq!(
                    write(table, format, step),
                    write(table, format, u64::MAX / 2),
                    "{table} {format:?}"
                );
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

    fn cancel_once_output_appears(
        request: OutputRequest,
        watched: PathBuf,
    ) -> Result<Vec<FileStats>, Error> {
        let cancel = std::sync::Arc::new(AtomicBool::new(false));
        let (sender, receiver) = std::sync::mpsc::channel();
        let runner = {
            let cancel = cancel.clone();
            thread::spawn(move || {
                sender
                    .send(write_benchmark_cancellable(&request, &cancel))
                    .unwrap();
            })
        };
        let mut early = None;
        while !watched.exists() {
            match receiver.try_recv() {
                Ok(result) => {
                    early = Some(result);
                    break;
                }
                Err(_) => thread::sleep(std::time::Duration::from_millis(1)),
            }
        }
        cancel.store(true, Ordering::Relaxed);
        let result = early.unwrap_or_else(|| {
            receiver
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("a cancelled write must return promptly")
        });
        runner.join().unwrap();
        result
    }

    #[test]
    fn cancelling_partway_through_a_split_tbl_write_returns_and_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let watched = dir.path().join("lineitem.tbl.1");
        let result =
            cancel_once_output_appears(request(dir.path(), Format::Tbl, 3, 3, 0.1), watched);
        assert_eq!(result, Err(Error::Cancelled));
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn cancelling_partway_through_a_single_parquet_file_returns_and_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let watched = dir.path().join("lineitem.parquet");
        let result =
            cancel_once_output_appears(request(dir.path(), Format::Parquet, 2, 1, 0.1), watched);
        assert_eq!(result, Err(Error::Cancelled));
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn a_cancelled_sink_fails_write_all_instead_of_retrying() {
        let cancel = AtomicBool::new(true);
        let mut sink = Cancellable {
            inner: Vec::new(),
            cancel: &cancel,
        };
        let error = sink.write_all(b"row").unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::Interrupted);
        assert!(sink.inner.is_empty());
    }

    #[test]
    fn parquet_errors_wrapping_io_errors_map_to_the_write_variant() {
        let path = Path::new("out.parquet");
        let io = io::Error::new(io::ErrorKind::StorageFull, "disk full");
        let wrapped = WriterError::Parquet(ParquetError::External(Box::new(io)));
        assert!(matches!(writer_error(path, wrapped), Error::Write(_)));
        let plain = WriterError::Io(io::Error::other("closed"));
        assert!(matches!(writer_error(path, plain), Error::Write(_)));
        let encode = WriterError::Parquet(ParquetError::General("bad page".into()));
        assert!(matches!(writer_error(path, encode), Error::Encode(_)));
        let external = ParquetError::External("not io".into());
        assert!(matches!(
            writer_error(path, WriterError::Parquet(external)),
            Error::Encode(_)
        ));
    }

    #[test]
    fn a_failed_run_keeps_files_it_did_not_create() {
        let dir = tempfile::tempdir().unwrap();
        let unreached = dir.path().join("lineitem.tbl");
        fs::write(&unreached, b"keep me").unwrap();
        fs::create_dir(dir.path().join("orders.tbl")).unwrap();
        let result = write_benchmark(&request(dir.path(), Format::Tbl, 2, 1, 0.01));
        assert!(matches!(result, Err(Error::Write(_))));
        assert_eq!(fs::read(&unreached).unwrap(), b"keep me");
        assert!(!dir.path().join("nation.tbl").exists());
        assert!(!dir.path().join("supplier.tbl").exists());
    }
}

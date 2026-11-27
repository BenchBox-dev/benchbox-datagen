use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use benchbox_datagen_writers::DelimitedBuffer;
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

use crate::{Error, FileStats};

const ORDERS_PER_REFRESH_UNIT: f64 = 150.0;
const REFRESH_UNIT_ORDERS: i64 = 10;
const STREAM_PARTS: i32 = 100;
const KEY_PHASE_SETS: i64 = 1000;
const FLUSH_BYTES: usize = 1 << 20;

fn io_error(path: &Path, error: impl std::fmt::Display) -> Error {
    Error::Write(format!("{}: {error}", path.display()))
}

pub(crate) fn orders_per_set(scale_factor: f64) -> i64 {
    REFRESH_UNIT_ORDERS * (ORDERS_PER_REFRESH_UNIT * scale_factor).floor() as i64
}

fn sparse_key(index: i64, phase: i64) -> i64 {
    ((((index >> 3) << 2) | phase) << 3) | (index & 7)
}

fn insert_key(index: i64, set: i64) -> i64 {
    sparse_key(index, 1 + set / KEY_PHASE_SETS)
}

struct Sink {
    table: &'static str,
    path: PathBuf,
    file: BufWriter<File>,
    buffer: DelimitedBuffer,
    bytes: u64,
}

impl Sink {
    fn create(
        table: &'static str,
        path: PathBuf,
        created: &mut Vec<PathBuf>,
    ) -> Result<Self, Error> {
        let file = File::create(&path).map_err(|e| io_error(&path, e))?;
        created.push(path.clone());
        Ok(Self {
            table,
            path,
            file: BufWriter::new(file),
            buffer: DelimitedBuffer::default(),
            bytes: 0,
        })
    }

    fn push<R: std::fmt::Display>(&mut self, row: &R) -> Result<(), Error> {
        self.buffer.push(row);
        if self.buffer.buffered() >= FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.bytes += self.buffer.buffered() as u64;
        self.buffer
            .flush_to(&mut self.file)
            .map_err(|e| io_error(&self.path, e))
    }

    fn finish(mut self) -> Result<FileStats, Error> {
        self.flush()?;
        self.file.flush().map_err(|e| io_error(&self.path, e))?;
        Ok(FileStats {
            table: self.table,
            path: self.path,
            rows: self.buffer.rows(),
            bytes: self.bytes,
        })
    }
}

struct OpenSet {
    index: i64,
    orders: Sink,
    lineitems: Sink,
    deletes: FileStats,
}

fn open_set(
    index: i64,
    per_set: i64,
    output_dir: &Path,
    created: &mut Vec<PathBuf>,
) -> Result<OpenSet, Error> {
    let number = index + 1;
    let mut deletes = Sink::create(
        "delete",
        output_dir.join(format!("delete.{number}")),
        created,
    )?;
    let first = index * per_set;
    for order in first + 1..=first + per_set {
        deletes.push(&OrderGenerator::make_order_key(order))?;
    }
    Ok(OpenSet {
        index,
        orders: Sink::create(
            "orders",
            output_dir.join(format!("orders.tbl.u{number}")),
            created,
        )?,
        lineitems: Sink::create(
            "lineitem",
            output_dir.join(format!("lineitem.tbl.u{number}")),
            created,
        )?,
        deletes: deletes.finish()?,
    })
}

fn close_set(set: OpenSet, stats: &mut Vec<FileStats>) -> Result<(), Error> {
    stats.push(set.orders.finish()?);
    stats.push(set.lineitems.finish()?);
    stats.push(set.deletes);
    Ok(())
}

pub(crate) fn write_tpch_refresh(
    scale_factor: f64,
    sets: i32,
    output_dir: &Path,
    cancel: &AtomicBool,
) -> Result<Vec<FileStats>, Error> {
    let per_set = orders_per_set(scale_factor);
    let base_orders = OrderGenerator::calculate_row_count(scale_factor, 1, 1);
    if per_set < 1 || base_orders % i64::from(STREAM_PARTS) != 0 {
        return Err(Error::RefreshUnsupported(format!(
            "tpch scale factor {scale_factor} has no whole-number refresh stream"
        )));
    }
    fs::create_dir_all(output_dir).map_err(|e| io_error(output_dir, e))?;
    let mut created = Vec::new();
    let outcome = stream(
        scale_factor,
        sets,
        per_set,
        output_dir,
        cancel,
        &mut created,
    );
    if outcome.is_err() {
        for path in created {
            let _ = fs::remove_file(path);
        }
    }
    match outcome {
        Err(_) if cancel.load(Ordering::Relaxed) => Err(Error::Cancelled),
        other => other,
    }
}

fn stream(
    scale_factor: f64,
    sets: i32,
    per_set: i64,
    output_dir: &Path,
    cancel: &AtomicBool,
    created: &mut Vec<PathBuf>,
) -> Result<Vec<FileStats>, Error> {
    let wanted = per_set * i64::from(sets);
    let mut stats = Vec::new();
    let mut current: Option<OpenSet> = None;
    let mut consumed = 0_i64;
    let mut part = STREAM_PARTS + 1;
    while consumed < wanted {
        let mut orders = OrderGenerator::new(scale_factor, part, STREAM_PARTS).iter();
        let mut lines = LineItemGenerator::new(scale_factor, part, STREAM_PARTS)
            .iter()
            .peekable();
        let mut produced = 0;
        while consumed < wanted {
            if cancel.load(Ordering::Relaxed) {
                return Err(Error::Cancelled);
            }
            let Some(mut order) = orders.next() else {
                break;
            };
            produced += 1;
            let set_index = consumed / per_set;
            if current.as_ref().is_none_or(|set| set.index != set_index) {
                if let Some(done) = current.take() {
                    close_set(done, &mut stats)?;
                }
                current = Some(open_set(set_index, per_set, output_dir, created)?);
            }
            let target = current.as_mut().expect("a set is open");
            let key = insert_key(consumed + 1, set_index + 1);
            let base_key = order.o_orderkey;
            order.o_orderkey = key;
            target.orders.push(&order)?;
            while let Some(mut line) = lines.next_if(|line| line.l_orderkey == base_key) {
                line.l_orderkey = key;
                target.lineitems.push(&line)?;
            }
            consumed += 1;
        }
        if produced == 0 {
            return Err(Error::RefreshUnsupported(
                "the refresh stream ended before the requested sets".into(),
            ));
        }
        part += 1;
    }
    if let Some(done) = current.take() {
        close_set(done, &mut stats)?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn a_cancelled_run_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            write_tpch_refresh(0.01, 3, dir.path(), &cancelled),
            Err(Error::Cancelled)
        );
        assert!(entries(dir.path()).is_empty());
    }

    #[test]
    fn a_failed_run_removes_the_files_it_created() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("lineitem.tbl.u2")).unwrap();
        let result = write_tpch_refresh(0.01, 3, dir.path(), &AtomicBool::new(false));
        assert!(matches!(result, Err(Error::Write(_))));
        assert_eq!(entries(dir.path()), ["lineitem.tbl.u2"]);
    }

    #[test]
    fn sets_are_streamed_to_their_files() {
        let dir = tempfile::tempdir().unwrap();
        let stats = write_tpch_refresh(0.01, 2, dir.path(), &AtomicBool::new(false)).unwrap();
        assert_eq!(stats.len(), 6);
        for stat in &stats {
            assert_eq!(fs::metadata(&stat.path).unwrap().len(), stat.bytes);
        }
    }
}

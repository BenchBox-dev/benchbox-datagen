use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use benchbox_datagen_writers::DelimitedBuffer;
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

use crate::{Error, FileStats};

const ORDERS_PER_REFRESH_UNIT: f64 = 150.0;
const REFRESH_UNIT_ORDERS: i64 = 10;
const STREAM_PARTS: i32 = 100;
const KEY_PHASE_SETS: i64 = 1000;

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

struct Set {
    orders: DelimitedBuffer,
    lineitems: DelimitedBuffer,
    deletes: Vec<i64>,
}

fn open(path: &Path) -> Result<BufWriter<File>, Error> {
    File::create(path)
        .map(BufWriter::new)
        .map_err(|e| io_error(path, e))
}

fn finish(path: &Path, bytes: &[u8], mut file: BufWriter<File>) -> Result<(), Error> {
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .map_err(|e| io_error(path, e))
}

pub(crate) fn write_tpch_refresh(
    scale_factor: f64,
    sets: i32,
    output_dir: &Path,
) -> Result<Vec<FileStats>, Error> {
    let per_set = orders_per_set(scale_factor);
    let base_orders = OrderGenerator::calculate_row_count(scale_factor, 1, 1);
    if per_set < 1 || base_orders % i64::from(STREAM_PARTS) != 0 {
        return Err(Error::RefreshUnsupported(format!(
            "tpch scale factor {scale_factor} has no whole-number refresh stream"
        )));
    }
    fs::create_dir_all(output_dir).map_err(|e| io_error(output_dir, e))?;
    let wanted = per_set * i64::from(sets);
    let mut built: Vec<Set> = (0..sets)
        .map(|_| Set {
            orders: DelimitedBuffer::default(),
            lineitems: DelimitedBuffer::default(),
            deletes: Vec::new(),
        })
        .collect();
    for (index, set) in built.iter_mut().enumerate() {
        let first = index as i64 * per_set;
        set.deletes = (first + 1..=first + per_set)
            .map(OrderGenerator::make_order_key)
            .collect();
    }
    let mut consumed = 0_i64;
    let mut part = STREAM_PARTS + 1;
    while consumed < wanted {
        let mut orders = OrderGenerator::new(scale_factor, part, STREAM_PARTS).iter();
        let mut lines = LineItemGenerator::new(scale_factor, part, STREAM_PARTS)
            .iter()
            .peekable();
        let mut produced = 0;
        while consumed < wanted {
            let Some(mut order) = orders.next() else {
                break;
            };
            produced += 1;
            let set_index = consumed / per_set;
            let index = consumed + 1;
            let key = insert_key(index, set_index + 1);
            let base_key = order.o_orderkey;
            let target = &mut built[set_index as usize];
            order.o_orderkey = key;
            target.orders.push(&order);
            while let Some(mut line) = lines.next_if(|line| line.l_orderkey == base_key) {
                line.l_orderkey = key;
                target.lineitems.push(&line);
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
    let mut stats = Vec::new();
    for (index, set) in built.into_iter().enumerate() {
        let number = index + 1;
        for (name, bytes, rows) in [
            (
                format!("orders.tbl.u{number}"),
                set.orders.as_bytes().to_vec(),
                set.orders.rows(),
            ),
            (
                format!("lineitem.tbl.u{number}"),
                set.lineitems.as_bytes().to_vec(),
                set.lineitems.rows(),
            ),
            (
                format!("delete.{number}"),
                set.deletes
                    .iter()
                    .flat_map(|key| format!("{key}\n").into_bytes())
                    .collect(),
                set.deletes.len() as u64,
            ),
        ] {
            let path = output_dir.join(&name);
            let file = open(&path)?;
            finish(&path, &bytes, file)?;
            stats.push(FileStats {
                table: match name.split('.').next() {
                    Some("orders") => "orders",
                    Some("lineitem") => "lineitem",
                    _ => "delete",
                },
                path,
                rows,
                bytes: bytes.len() as u64,
            });
        }
    }
    Ok(stats)
}

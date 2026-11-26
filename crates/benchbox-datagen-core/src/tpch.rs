use std::io::{self, Write};

use arrow::record_batch::RecordBatchIterator;
use benchbox_datagen_writers::DelimitedBuffer;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

use crate::{BatchReader, KeyRange};

const FLUSH_BYTES: usize = 1 << 20;
const SUPPLIERS_PER_PART: u64 = 4;

pub(crate) fn batches(table: &str, scale_factor: f64, part: i32, parts: i32) -> BatchReader {
    match table {
        "nation" if part > 1 => Box::new(RecordBatchIterator::new([], NationArrow::schema_ref())),
        "region" if part > 1 => Box::new(RecordBatchIterator::new([], RegionArrow::schema_ref())),
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
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

fn drain<W: Write, R: std::fmt::Display>(
    rows: impl Iterator<Item = R>,
    sink: &mut W,
) -> io::Result<u64> {
    let mut buffer = DelimitedBuffer::default();
    for row in rows {
        buffer.push(&row);
        if buffer.buffered() >= FLUSH_BYTES {
            buffer.flush_to(sink)?;
        }
    }
    buffer.flush_to(sink)?;
    Ok(buffer.rows())
}

pub(crate) fn write_delimited<W: Write>(
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
    sink: &mut W,
) -> io::Result<u64> {
    match table {
        "nation" if part > 1 => Ok(0),
        "region" if part > 1 => Ok(0),
        "nation" => drain(NationGenerator::new(scale_factor, 1, 1).iter(), sink),
        "region" => drain(RegionGenerator::new(scale_factor, 1, 1).iter(), sink),
        "part" => drain(PartGenerator::new(scale_factor, part, parts).iter(), sink),
        "supplier" => drain(
            SupplierGenerator::new(scale_factor, part, parts).iter(),
            sink,
        ),
        "partsupp" => drain(
            PartSuppGenerator::new(scale_factor, part, parts).iter(),
            sink,
        ),
        "customer" => drain(
            CustomerGenerator::new(scale_factor, part, parts).iter(),
            sink,
        ),
        "orders" => drain(OrderGenerator::new(scale_factor, part, parts).iter(), sink),
        "lineitem" => drain(
            LineItemGenerator::new(scale_factor, part, parts).iter(),
            sink,
        ),
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

pub(crate) fn row_count(table: &str, scale_factor: f64) -> u64 {
    let count = |rows: i64| rows as u64;
    match table {
        "nation" => 25,
        "region" => 5,
        "part" => count(PartGenerator::calculate_row_count(scale_factor, 1, 1)),
        "supplier" => count(SupplierGenerator::calculate_row_count(scale_factor, 1, 1)),
        "partsupp" => {
            count(PartSuppGenerator::calculate_row_count(scale_factor, 1, 1)) * SUPPLIERS_PER_PART
        }
        "customer" => count(CustomerGenerator::calculate_row_count(scale_factor, 1, 1)),
        "orders" => count(OrderGenerator::calculate_row_count(scale_factor, 1, 1)),
        "lineitem" => {
            let orders = OrderGenerator::calculate_row_count(scale_factor, 1, 1);
            let mut line_count = OrderGenerator::create_line_count_random();
            let mut lines = 0_u64;
            for _ in 0..orders {
                lines += line_count.next_value() as u64;
                line_count.row_finished();
            }
            lines
        }
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

pub(crate) fn chunk_driver_rows(table: &str, scale_factor: f64) -> (u64, u64) {
    match table {
        "nation" | "region" => (1, 1),
        "lineitem" => (
            OrderGenerator::calculate_row_count(scale_factor, 1, 1) as u64,
            25_000,
        ),
        "partsupp" => (
            PartSuppGenerator::calculate_row_count(scale_factor, 1, 1) as u64,
            25_000,
        ),
        other => (row_count(other, scale_factor), 100_000),
    }
}

fn order_key(index: i64) -> i64 {
    ((index - 1) / 8) * 32 + (index - 1) % 8 + 1
}

pub(crate) fn key_ranges(scale_factor: f64) -> Vec<KeyRange> {
    let rows = |table| row_count(table, scale_factor) as i64;
    let (parts, suppliers, customers, orders) = (
        rows("part"),
        rows("supplier"),
        rows("customer"),
        rows("orders"),
    );
    let last_order = order_key(orders);
    let key = |table, column, min, max| KeyRange {
        table,
        column,
        min,
        max,
    };
    vec![
        key("nation", "n_nationkey", 0, 24),
        key("nation", "n_regionkey", 0, 4),
        key("region", "r_regionkey", 0, 4),
        key("part", "p_partkey", 1, parts),
        key("supplier", "s_suppkey", 1, suppliers),
        key("supplier", "s_nationkey", 0, 24),
        key("partsupp", "ps_partkey", 1, parts),
        key("partsupp", "ps_suppkey", 1, suppliers),
        key("customer", "c_custkey", 1, customers),
        key("customer", "c_nationkey", 0, 24),
        key("orders", "o_orderkey", 1, last_order),
        key("lineitem", "l_orderkey", 1, last_order),
        key("lineitem", "l_partkey", 1, parts),
        key("lineitem", "l_suppkey", 1, suppliers),
    ]
}

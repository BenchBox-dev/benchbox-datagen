use std::io::{self, Write};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use arrow::datatypes::SchemaRef;

use crate::{BatchReader, Benchmark, schema, tpcds, tpch};

#[derive(Debug, Clone)]
pub(crate) enum Slice {
    Tpch { part: i32, parts: i32 },
    Tpcds(RangeInclusive<u64>),
}

pub(crate) fn schema(benchmark: Benchmark, table: &str) -> SchemaRef {
    match benchmark {
        Benchmark::Tpch => schema::tpch(table),
        Benchmark::Tpcds => schema::tpcds(table),
    }
    .expect("every table of every benchmark has a schema")
}

pub(crate) fn batches(
    benchmark: Benchmark,
    table: &str,
    scale_factor: f64,
    slice: &Slice,
) -> BatchReader {
    match (benchmark, slice) {
        (Benchmark::Tpch, Slice::Tpch { part, parts }) => {
            tpch::batches(table, scale_factor, *part, *parts)
        }
        (Benchmark::Tpcds, Slice::Tpcds(range)) => {
            tpcds::batches(table, scale_factor, range.clone())
        }
        _ => unreachable!("slices are built for their own benchmark"),
    }
}

pub(crate) fn write_delimited<W: Write>(
    benchmark: Benchmark,
    table: &str,
    scale_factor: f64,
    slice: &Slice,
    sink: &mut W,
) -> io::Result<u64> {
    match (benchmark, slice) {
        (Benchmark::Tpch, Slice::Tpch { part, parts }) => {
            tpch::write_delimited(table, scale_factor, *part, *parts, sink)
        }
        (Benchmark::Tpcds, Slice::Tpcds(range)) => {
            tpcds::write_delimited(table, scale_factor, range.clone(), sink)
        }
        _ => unreachable!("slices are built for their own benchmark"),
    }
}

pub(crate) fn part_slice(
    benchmark: Benchmark,
    table: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
) -> Slice {
    match benchmark {
        Benchmark::Tpch => Slice::Tpch { part, parts },
        Benchmark::Tpcds => Slice::Tpcds(tpcds::part_range(table, scale_factor, part, parts)),
    }
}

pub(crate) fn splittable(benchmark: Benchmark, table: &str, scale_factor: f64) -> bool {
    match benchmark {
        Benchmark::Tpch => !matches!(table, "nation" | "region"),
        Benchmark::Tpcds => tpcds::splittable(table, scale_factor),
    }
}

pub(crate) fn plan(
    benchmark: Benchmark,
    table: &str,
    scale_factor: f64,
    chunk_rows: Option<u64>,
) -> Vec<Slice> {
    match benchmark {
        Benchmark::Tpch => {
            let (driver_rows, default_chunk_rows) = tpch::chunk_driver_rows(table, scale_factor);
            let chunks = driver_rows
                .div_ceil(chunk_rows.unwrap_or(default_chunk_rows).max(1))
                .clamp(1, i32::MAX as u64) as i32;
            (1..=chunks)
                .map(|part| Slice::Tpch {
                    part,
                    parts: chunks,
                })
                .collect()
        }
        Benchmark::Tpcds => {
            let total = tpcds::source_rows(table, scale_factor);
            let step = chunk_rows
                .unwrap_or_else(|| tpcds::chunk_source_rows(table))
                .max(1);
            let mut slices = Vec::new();
            let mut start = 1;
            while start <= total {
                let end = (start + step - 1).min(total);
                slices.push(Slice::Tpcds(start..=end));
                start = end + 1;
            }
            if slices.is_empty() {
                slices.push(Slice::Tpcds(RangeInclusive::new(1, 0)));
            }
            slices
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableScale {
    pub table: &'static str,
    pub id_count: u64,
    pub source_rows: u64,
}

pub(crate) fn table_scales(benchmark: Benchmark, scale_factor: f64) -> Vec<TableScale> {
    benchmark
        .tables()
        .iter()
        .map(|table| match benchmark {
            Benchmark::Tpch => {
                let rows = tpch::row_count(table, scale_factor);
                TableScale {
                    table,
                    id_count: rows,
                    source_rows: if *table == "lineitem" {
                        tpch::row_count("orders", scale_factor)
                    } else {
                        rows
                    },
                }
            }
            Benchmark::Tpcds => TableScale {
                table,
                id_count: tpcds::id_count(table, scale_factor),
                source_rows: tpcds::source_rows(table, scale_factor),
            },
        })
        .collect()
}

pub(crate) fn row_counts(benchmark: Benchmark, scale_factor: f64) -> Vec<(&'static str, u64)> {
    benchmark
        .tables()
        .iter()
        .map(|table| {
            let rows = match benchmark {
                Benchmark::Tpch => tpch::row_count(table, scale_factor),
                Benchmark::Tpcds => tpcds_row_count(table, scale_factor),
            };
            (*table, rows)
        })
        .collect()
}

fn tpcds_row_count(table: &str, scale_factor: f64) -> u64 {
    let generated = matches!(
        table,
        "store_sales"
            | "store_returns"
            | "catalog_sales"
            | "catalog_returns"
            | "web_sales"
            | "web_returns"
    );
    if !generated {
        return tpcds::source_rows(table, scale_factor);
    }
    let slices = plan(Benchmark::Tpcds, table, scale_factor, Some(25_000));
    let next = AtomicUsize::new(0);
    let workers = thread::available_parallelism().map_or(1, |count| count.get());
    thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut rows = 0;
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(Slice::Tpcds(range)) = slices.get(index) else {
                            return rows;
                        };
                        rows += tpcds::count_rows(table, scale_factor, range.clone());
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("row counting does not panic"))
            .sum()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub table: &'static str,
    pub column: &'static str,
    pub min: i64,
    pub max: i64,
}

const fn key(table: &'static str, column: &'static str, min: i64, max: i64) -> KeyRange {
    KeyRange {
        table,
        column,
        min,
        max,
    }
}

fn tpch_order_key(index: i64) -> i64 {
    ((index >> 3) << 5) + (index & 7)
}

pub(crate) fn key_ranges(benchmark: Benchmark, scale_factor: f64) -> Vec<KeyRange> {
    match benchmark {
        Benchmark::Tpch => tpch_key_ranges(scale_factor),
        Benchmark::Tpcds => tpcds_key_ranges(scale_factor),
    }
}

fn tpch_key_ranges(scale_factor: f64) -> Vec<KeyRange> {
    let rows = |table| tpch::row_count(table, scale_factor) as i64;
    let (parts, suppliers, customers, orders) = (
        rows("part"),
        rows("supplier"),
        rows("customer"),
        rows("orders"),
    );
    let last_order = tpch_order_key(orders);
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

const DATE_DIM_FIRST_SK: i64 = 2_415_022;

fn tpcds_key_ranges(scale_factor: f64) -> Vec<KeyRange> {
    let rows = |table| tpcds::source_rows(table, scale_factor) as i64;
    let mut ranges: Vec<KeyRange> = [
        ("call_center", "cc_call_center_sk"),
        ("catalog_page", "cp_catalog_page_sk"),
        ("customer", "c_customer_sk"),
        ("customer_address", "ca_address_sk"),
        ("customer_demographics", "cd_demo_sk"),
        ("household_demographics", "hd_demo_sk"),
        ("income_band", "ib_income_band_sk"),
        ("item", "i_item_sk"),
        ("promotion", "p_promo_sk"),
        ("reason", "r_reason_sk"),
        ("ship_mode", "sm_ship_mode_sk"),
        ("store", "s_store_sk"),
        ("warehouse", "w_warehouse_sk"),
        ("web_page", "wp_web_page_sk"),
        ("web_site", "web_site_sk"),
    ]
    .into_iter()
    .map(|(table, column)| key(table, column, 1, rows(table)))
    .collect();
    let dates = rows("date_dim");
    ranges.push(key(
        "date_dim",
        "d_date_sk",
        DATE_DIM_FIRST_SK,
        DATE_DIM_FIRST_SK + dates - 1,
    ));
    ranges.push(key("time_dim", "t_time_sk", 0, rows("time_dim") - 1));
    for (table, column) in [
        ("store_sales", "ss_ticket_number"),
        ("catalog_sales", "cs_order_number"),
        ("web_sales", "ws_order_number"),
    ] {
        ranges.push(key(table, column, 1, rows(table)));
    }
    ranges.sort_by_key(|range| (range.table, range.column));
    ranges
}

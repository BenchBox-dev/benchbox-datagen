use std::collections::HashMap;
use std::f64::consts::{LN_2, SQRT_2};
use std::sync::{Arc, LazyLock, Mutex};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use tpchgen::generators::OrderGenerator;

use super::rng::Stream;
use super::{BLOCK_ROWS, Generator, Table, block_len};
use crate::generate::{KeyRange, Slice, TableScale, key};
use crate::{Benchmark, conformed, schema, tpch};

const BENCHMARK: &str = "tpch_skew";
const CHUNK_DRIVER_UNITS: u64 = 2_048;
const PARTS_SCALE: u64 = 200_000;
const SUPPLIERS_SCALE: u64 = 10_000;
const CUSTOMERS_SCALE: u64 = 150_000;
const NATIONS: i64 = 25;
const MAX_EXP_ARGUMENT: f64 = 700.0;
const LN_SERIES_TERMS: usize = 24;
const EXP_SERIES_TERMS: usize = 28;

const TABLES: [&str; 8] = [
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];
const SEGMENTS: [&str; 5] = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "HOUSEHOLD",
    "MACHINERY",
];
const BRANDS: [&str; 25] = [
    "Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15", "Brand#21", "Brand#22", "Brand#23",
    "Brand#24", "Brand#25", "Brand#31", "Brand#32", "Brand#33", "Brand#34", "Brand#35", "Brand#41",
    "Brand#42", "Brand#43", "Brand#44", "Brand#45", "Brand#51", "Brand#52", "Brand#53", "Brand#54",
    "Brand#55",
];
const TYPES: [&str; 20] = [
    "STANDARD ANODIZED TIN",
    "STANDARD ANODIZED NICKEL",
    "STANDARD ANODIZED BRASS",
    "STANDARD ANODIZED STEEL",
    "STANDARD ANODIZED COPPER",
    "SMALL ANODIZED TIN",
    "SMALL ANODIZED NICKEL",
    "SMALL ANODIZED BRASS",
    "MEDIUM POLISHED TIN",
    "MEDIUM POLISHED NICKEL",
    "MEDIUM POLISHED BRASS",
    "LARGE POLISHED TIN",
    "LARGE POLISHED NICKEL",
    "LARGE POLISHED STEEL",
    "ECONOMY BRUSHED BRASS",
    "ECONOMY BRUSHED COPPER",
    "ECONOMY BRUSHED STEEL",
    "PROMO PLATED TIN",
    "PROMO PLATED NICKEL",
    "PROMO PLATED BRASS",
];
const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const SHIP_MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

fn ln(x: f64) -> f64 {
    let bits = x.to_bits();
    let mut exponent = ((bits >> 52) & 0x7ff) as i64 - 1023;
    let mut mantissa = f64::from_bits((bits & 0x000f_ffff_ffff_ffff) | 0x3ff0_0000_0000_0000);
    if mantissa > SQRT_2 {
        mantissa *= 0.5;
        exponent += 1;
    }
    let t = (mantissa - 1.0) / (mantissa + 1.0);
    let squared = t * t;
    let (mut power, mut sum) = (t, 0.0);
    for term in 0..LN_SERIES_TERMS {
        sum += power / (2 * term + 1) as f64;
        power *= squared;
    }
    2.0 * sum + exponent as f64 * LN_2
}

fn exp(x: f64) -> f64 {
    debug_assert!(x.abs() < MAX_EXP_ARGUMENT);
    let scale = (x / LN_2 + 0.5).floor();
    let remainder = x - scale * LN_2;
    let (mut term, mut sum) = (1.0, 1.0);
    for n in 1..EXP_SERIES_TERMS {
        term *= remainder / n as f64;
        sum += term;
    }
    sum * f64::from_bits(((scale as i64 + 1023) as u64) << 52)
}

fn power(base: f64, exponent: f64) -> f64 {
    if exponent == 1.0 {
        base
    } else {
        exp(exponent * ln(base))
    }
}

#[derive(Debug)]
struct Zipf {
    cdf: Vec<f64>,
    first: i64,
}

impl Zipf {
    fn new(skew: f64, first: i64, last: i64) -> Self {
        let count = (last - first + 1).max(1) as usize;
        let exponent = skew * 2.0;
        let mut cdf: Vec<f64> = (1..=count)
            .map(|rank| power(rank as f64, exponent))
            .collect();
        let harmonic: f64 = cdf.iter().map(|value| 1.0 / value).sum();
        let mut running = 0.0;
        for value in &mut cdf {
            running += 1.0 / (*value * harmonic);
            *value = running;
        }
        Self { cdf, first }
    }

    fn sample(&self, unit: f64) -> i64 {
        let count = self.cdf.len();
        let rank = self
            .cdf
            .partition_point(|value| *value < unit)
            .min(count - 1);
        let scaled = rank as f64 / count as f64;
        (scaled * count as f64 + self.first as f64).floor() as i64
    }
}

#[derive(Debug)]
enum Replacement {
    Keys(Zipf),
    Words(Zipf, &'static [&'static str]),
}

#[derive(Debug)]
struct Skew {
    column: &'static str,
    replacement: Replacement,
}

impl Skew {
    fn keys(column: &'static str, skew: f64, first: i64, last: i64) -> Self {
        Self {
            column,
            replacement: Replacement::Keys(Zipf::new(skew, first, last)),
        }
    }

    fn words(column: &'static str, skew: f64, words: &'static [&'static str]) -> Self {
        Self {
            column,
            replacement: Replacement::Words(Zipf::new(skew, 0, words.len() as i64 - 1), words),
        }
    }

    fn column(&self, stream: &Stream, block: u64, rows: usize) -> ArrayRef {
        let mut draw = stream.block(block);
        match &self.replacement {
            Replacement::Keys(zipf) => Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|_| zipf.sample(draw.unit_f64())),
            )),
            Replacement::Words(zipf, words) => Arc::new(StringArray::from_iter_values(
                (0..rows).map(|_| words[zipf.sample(draw.unit_f64()) as usize]),
            )),
        }
    }
}

fn domain(base: u64, scale_factor: f64) -> i64 {
    ((base as f64 * scale_factor) as i64).max(1)
}

fn skews(table: &str, scale_factor: f64) -> Vec<Skew> {
    match table {
        "customer" => vec![
            Skew::keys("c_nationkey", 0.5, 0, NATIONS - 1),
            Skew::words("c_mktsegment", 0.4, &SEGMENTS),
        ],
        "part" => vec![
            Skew::words("p_brand", 0.5, &BRANDS),
            Skew::words("p_type", 0.3, &TYPES),
        ],
        "orders" => vec![
            Skew::keys("o_custkey", 0.5, 1, domain(CUSTOMERS_SCALE, scale_factor)),
            Skew::words("o_orderpriority", 0.3, &PRIORITIES),
        ],
        "lineitem" => vec![
            Skew::keys("l_partkey", 0.5, 1, domain(PARTS_SCALE, scale_factor)),
            Skew::keys("l_suppkey", 0.4, 1, domain(SUPPLIERS_SCALE, scale_factor)),
            Skew::words("l_shipmode", 0.4, &SHIP_MODES),
        ],
        _ => Vec::new(),
    }
}

#[derive(Debug)]
struct Layout {
    parts: i32,
    bounds: Vec<u64>,
}

impl Layout {
    fn build(table: &str, scale_factor: f64) -> Self {
        if matches!(table, "nation" | "region") {
            return Self {
                parts: 1,
                bounds: vec![0, tpch::row_count(table, scale_factor)],
            };
        }
        let (driver, _) = tpch::chunk_driver_rows(table, scale_factor);
        let parts = (driver / CHUNK_DRIVER_UNITS).clamp(1, i32::MAX as u64);
        let per_part = driver / parts;
        let mut bounds = Vec::with_capacity(parts as usize + 1);
        match table {
            "lineitem" => {
                let mut line_count = OrderGenerator::create_line_count_random();
                let mut lines = 0_u64;
                for order in 0..driver {
                    if order % per_part == 0 && (order / per_part) < parts {
                        bounds.push(lines);
                    }
                    lines += line_count.next_value() as u64;
                    line_count.row_finished();
                }
                bounds.push(lines);
            }
            _ => {
                let rows_per_unit = if table == "partsupp" { 4 } else { 1 };
                bounds.extend((0..parts).map(|part| part * per_part * rows_per_unit));
                bounds.push(tpch::row_count(table, scale_factor));
            }
        }
        Self {
            parts: parts as i32,
            bounds,
        }
    }

    fn rows(&self) -> u64 {
        *self.bounds.last().expect("bounds are never empty")
    }

    fn chunk_of(&self, row: u64) -> usize {
        self.bounds.partition_point(|bound| *bound <= row) - 1
    }
}

type LayoutKey = (&'static str, u64);

static LAYOUTS: LazyLock<Mutex<HashMap<LayoutKey, Arc<Layout>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn layout(table: &'static str, scale_factor: f64) -> Arc<Layout> {
    let key = (table, scale_factor.to_bits());
    if let Some(layout) = LAYOUTS.lock().expect("layout cache").get(&key) {
        return layout.clone();
    }
    let built = Arc::new(Layout::build(table, scale_factor));
    LAYOUTS
        .lock()
        .expect("layout cache")
        .entry(key)
        .or_insert(built)
        .clone()
}

#[derive(Debug)]
struct SkewedTable {
    table: &'static str,
    scale_factor: f64,
    schema: SchemaRef,
    layout: Arc<Layout>,
    skews: Vec<(usize, Stream, Skew)>,
}

impl SkewedTable {
    fn new(table: &'static str, scale_factor: f64, seed: u64) -> Self {
        let schema = schema::tpch(table).expect("every TPC-H table has a schema");
        let skews = skews(table, scale_factor)
            .into_iter()
            .map(|skew| {
                let index = schema
                    .index_of(skew.column)
                    .expect("skewed columns exist in the schema");
                (
                    index,
                    Stream::new(BENCHMARK, table, skew.column, seed),
                    skew,
                )
            })
            .collect();
        Self {
            table,
            scale_factor,
            schema,
            layout: layout(table, scale_factor),
            skews,
        }
    }

    fn base(&self, start: u64, end: u64) -> RecordBatch {
        let (first, last) = (self.layout.chunk_of(start), self.layout.chunk_of(end - 1));
        let batches: Vec<RecordBatch> = (first..=last)
            .flat_map(|chunk| {
                let slice = Slice::Tpch {
                    part: chunk as i32 + 1,
                    parts: self.layout.parts,
                };
                conformed(Benchmark::Tpch, self.table, self.scale_factor, &slice)
            })
            .map(|batch| batch.expect("TPC-H batches are generated in memory"))
            .collect();
        concat_batches(&self.schema, &batches)
            .expect("chunks share the TPC-H schema")
            .slice(
                (start - self.layout.bounds[first]) as usize,
                (end - start) as usize,
            )
    }
}

impl Table for SkewedTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn rows(&self) -> u64 {
        self.layout.rows()
    }

    fn block(&self, index: u64) -> RecordBatch {
        let start = index * BLOCK_ROWS;
        let rows = block_len(self.rows(), index);
        let base = self.base(start, start + rows as u64);
        let mut columns = base.columns().to_vec();
        for (position, stream, skew) in &self.skews {
            columns[*position] = skew.column(stream, index, rows);
        }
        RecordBatch::try_new(self.schema.clone(), columns)
            .expect("skewed columns keep the TPC-H types")
    }
}

fn table_name(table: &str) -> &'static str {
    TABLES
        .iter()
        .copied()
        .find(|name| *name == table)
        .unwrap_or_else(|| unreachable!("table {table:?} was validated by the caller"))
}

fn order_key(index: i64) -> i64 {
    ((index >> 3) << 5) + (index & 7)
}

pub(super) struct TpchSkew;

impl Generator for TpchSkew {
    fn name(&self) -> &'static str {
        BENCHMARK
    }

    fn tables(&self) -> &'static [&'static str] {
        &TABLES
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        schema::tpch(table)
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        match table_name(table) {
            "lineitem" => layout("lineitem", scale_factor).rows(),
            other => tpch::row_count(other, scale_factor),
        }
    }

    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
        Arc::new(SkewedTable::new(table_name(table), scale_factor, seed))
    }

    fn splittable(&self, table: &str) -> bool {
        !matches!(table, "nation" | "region")
    }

    fn key_ranges(&self, scale_factor: f64) -> Vec<KeyRange> {
        let rows = |table| tpch::row_count(table, scale_factor) as i64;
        let (parts, suppliers, customers, orders) = (
            rows("part"),
            rows("supplier"),
            rows("customer"),
            rows("orders"),
        );
        let last_order = order_key(orders);
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

    fn table_scales(&self, scale_factor: f64) -> Vec<TableScale> {
        TABLES
            .iter()
            .map(|table| {
                let rows = self.row_count(table, scale_factor);
                TableScale {
                    table,
                    id_count: rows,
                    source_rows: if *table == "lineitem" {
                        tpch::row_count("orders", scale_factor)
                    } else {
                        rows
                    },
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logarithm_and_exponential_agree_with_closed_forms() {
        for rank in [1.0_f64, 2.0, 3.0, 10.0, 25.0, 1.0e4, 1.0e6] {
            let reference = rank.ln();
            assert!((ln(rank) - reference).abs() <= 1e-14 * reference.max(1.0));
            for exponent in [0.6, 0.8, 1.0] {
                let expected = rank.powf(exponent);
                assert!((power(rank, exponent) - expected).abs() <= 1e-13 * expected);
            }
        }
        assert!((exp(0.0) - 1.0).abs() < 1e-15);
        assert!((exp(-1.0) - (-1.0_f64).exp()).abs() < 1e-15);
    }

    #[test]
    fn zipf_cdf_is_monotone_and_ends_at_one() {
        for (skew, last) in [(0.5, 24), (0.4, 4), (0.3, 19), (0.4, 10_000)] {
            let zipf = Zipf::new(skew, 0, last);
            assert!(zipf.cdf.windows(2).all(|pair| pair[0] < pair[1]));
            assert!((zipf.cdf.last().unwrap() - 1.0).abs() < 1e-9);
            assert_eq!(zipf.sample(0.0), 0);
            assert!(zipf.sample(0.999_999_999_999) <= last);
        }
    }

    #[test]
    fn layouts_cover_every_row_once() {
        for scale_factor in [0.01, 0.1] {
            for table in TABLES {
                let built = Layout::build(table, scale_factor);
                assert_eq!(built.bounds.len(), built.parts as usize + 1);
                assert!(built.bounds.windows(2).all(|pair| pair[0] < pair[1]));
                assert_eq!(
                    built.rows(),
                    tpch::row_count(table, scale_factor),
                    "{table}"
                );
            }
        }
    }
}

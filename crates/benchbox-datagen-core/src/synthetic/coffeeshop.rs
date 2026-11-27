use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int64Array, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::calendar::{civil_from_days, days_from_civil};
use super::delimited::TextFormat;
use super::{BLOCK_ROWS, Generator, Table, block_len};
use crate::generate::{KeyRange, key};
use crate::schema::{self, Column, Date, Decimal128, Int, Text};

const ORDERS_PER_SCALE: f64 = 8_500_000.0;
const FIRST_DATE: (i64, i64, i64) = (2023, 1, 1);
const LAST_DATE: (i64, i64, i64) = (2024, 12, 31);
const MONTH_WEIGHTS: [f64; 12] = [
    0.82, 0.85, 0.95, 1.00, 1.05, 0.98, 0.90, 0.92, 1.08, 1.15, 1.22, 1.38,
];
const REGION_WEIGHTS: [(&str, f64); 3] = [("South", 1.25), ("Southeast", 1.10), ("West", 0.90)];
const TREND_START_WEIGHT: f64 = 0.85;
const TREND_END_WEIGHT: f64 = 1.20;
const ORDER_LINE_PATTERN: [(u8, usize); 5] = [(1, 60), (2, 30), (3, 5), (4, 4), (5, 1)];
const QUANTITY_PATTERN: [(i64, usize); 5] = [(1, 55), (2, 30), (3, 10), (4, 4), (5, 1)];
const PRICE_MULTIPLIERS: [f64; 5] = [0.97, 0.99, 1.00, 1.02, 1.04];
const PRICE_MULTIPLIER_PATTERN: [(usize, usize); 5] = [(0, 5), (1, 10), (2, 20), (3, 10), (4, 5)];
const SUBCATEGORIES: [&str; 3] = ["Coffee", "Pastries", "Tea"];
const SUBCATEGORY_PATTERN: [(usize, usize); 3] = [(0, 60), (1, 25), (2, 15)];
const TIME_PATTERN: [((u32, u32), usize); 11] = [
    ((6, 45), 4),
    ((7, 30), 14),
    ((8, 15), 16),
    ((9, 0), 8),
    ((11, 0), 6),
    ((12, 15), 10),
    ((14, 0), 8),
    ((16, 0), 12),
    ((18, 30), 12),
    ((20, 0), 6),
    ((21, 15), 4),
];
const ORDER_PERIOD: usize = 100;
const LINE_PERIOD: usize = 156;
const LOCATIONS_CSV: &str = include_str!("coffeeshop_data/locations.csv");
const PRODUCTS_CSV: &str = include_str!("coffeeshop_data/products.csv");

const COLUMNS: [(&str, &[Column]); 3] = [
    (
        "dim_locations",
        &[
            ("record_id", Int, true),
            ("location_id", Text, true),
            ("city", Text, true),
            ("state", Text, true),
            ("country", Text, true),
            ("region", Text, true),
        ],
    ),
    (
        "dim_products",
        &[
            ("record_id", Int, true),
            ("product_id", Int, true),
            ("name", Text, true),
            ("category", Text, true),
            ("subcategory", Text, true),
            ("standard_cost", Decimal128(8, 2), true),
            ("standard_price", Decimal128(8, 2), true),
            ("from_date", Date, true),
            ("to_date", Date, true),
        ],
    ),
    (
        "order_lines",
        &[
            ("order_id", Int, true),
            ("line_number", Int, true),
            ("location_record_id", Int, true),
            ("location_id", Text, true),
            ("product_record_id", Int, true),
            ("product_id", Int, true),
            ("order_date", Date, true),
            ("order_time", Text, true),
            ("quantity", Int, true),
            ("unit_price", Decimal128(8, 2), true),
            ("total_price", Decimal128(10, 2), true),
            ("region", Text, true),
        ],
    ),
];

static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> =
    LazyLock::new(|| schema::build(&COLUMNS));

fn coffeeshop_schema(table: &str) -> SchemaRef {
    SCHEMAS
        .get(table)
        .cloned()
        .expect("every coffeeshop table has a schema")
}

#[derive(Debug)]
struct Location {
    record_id: i64,
    location_id: &'static str,
    city: &'static str,
    state: &'static str,
    country: &'static str,
    region: &'static str,
}

#[derive(Debug)]
struct Product {
    record_id: i64,
    product_id: i64,
    name: &'static str,
    category: &'static str,
    subcategory: &'static str,
    cost_cents: i128,
    price_cents: i128,
    from_day: i64,
    to_day: i64,
}

#[derive(Debug)]
struct Window {
    start_day: i64,
    end_day: i64,
    by_subcategory: [Vec<usize>; 3],
}

#[derive(Debug)]
struct Seeds {
    locations: Vec<Location>,
    products: Vec<Product>,
    windows: Vec<Window>,
    window_order: Vec<usize>,
    unit_cents: Vec<[i64; 5]>,
}

fn data_rows(csv: &'static str) -> impl Iterator<Item = Vec<&'static str>> {
    csv.lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.split(',').map(str::trim).collect())
}

fn parse_day(text: &str) -> i64 {
    let mut parts = text
        .split('-')
        .map(|part| part.parse::<i64>().expect("date"));
    let (year, month, day) = (
        parts.next().expect("year"),
        parts.next().expect("month"),
        parts.next().expect("day"),
    );
    days_from_civil(year, month, day)
}

fn parse_cents(text: &str) -> i128 {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let fraction = format!("{fraction:0<2}");
    assert!(
        fraction.len() == 2,
        "seed prices carry at most two decimals"
    );
    whole.parse::<i128>().expect("whole") * 100 + fraction.parse::<i128>().expect("fraction")
}

fn round_to_cents(value: f64) -> i64 {
    let mut text = String::new();
    write!(text, "{value:.2}").expect("writing to a String cannot fail");
    text.retain(|c| c != '.');
    text.parse().expect("rounded price")
}

static SEEDS: LazyLock<Seeds> = LazyLock::new(|| {
    let locations: Vec<Location> = data_rows(LOCATIONS_CSV)
        .map(|row| Location {
            record_id: row[0].parse().expect("record_id"),
            location_id: row[1],
            city: row[2],
            state: row[3],
            country: row[4],
            region: row[5],
        })
        .collect();
    let products: Vec<Product> = data_rows(PRODUCTS_CSV)
        .map(|row| Product {
            record_id: row[0].parse().expect("record_id"),
            product_id: row[1].parse().expect("product_id"),
            name: row[2],
            category: row[3],
            subcategory: row[4],
            cost_cents: parse_cents(row[5]),
            price_cents: parse_cents(row[6]),
            from_day: parse_day(row[7]),
            to_day: parse_day(row[8]),
        })
        .collect();
    let mut spans: Vec<(i64, i64)> = products
        .iter()
        .map(|product| (product.from_day, product.to_day))
        .collect();
    spans.sort_unstable();
    spans.dedup();
    let windows: Vec<Window> = spans
        .iter()
        .map(|&(start_day, end_day)| {
            let mut by_subcategory: [Vec<usize>; 3] = std::array::from_fn(|_| Vec::new());
            let mut members: Vec<usize> = (0..products.len())
                .filter(|&index| {
                    (products[index].from_day, products[index].to_day) == (start_day, end_day)
                })
                .collect();
            members.sort_by_key(|&index| (products[index].product_id, products[index].record_id));
            for index in members {
                let slot = SUBCATEGORIES
                    .iter()
                    .position(|name| *name == products[index].subcategory)
                    .expect("every seed subcategory is in the pattern");
                by_subcategory[slot].push(index);
            }
            Window {
                start_day,
                end_day,
                by_subcategory,
            }
        })
        .collect();
    let window_order = windows
        .iter()
        .flat_map(|window| window.by_subcategory.iter().flatten().copied())
        .collect();
    let unit_cents = products
        .iter()
        .map(|product| {
            let base: f64 = format!(
                "{}.{:02}",
                product.price_cents / 100,
                product.price_cents % 100
            )
            .parse()
            .expect("price");
            PRICE_MULTIPLIERS.map(|multiplier| round_to_cents(base * multiplier))
        })
        .collect();
    Seeds {
        locations,
        products,
        windows,
        window_order,
        unit_cents,
    }
});

fn expand<T: Copy>(pattern: &[(T, usize)]) -> Vec<T> {
    pattern
        .iter()
        .flat_map(|&(value, count)| std::iter::repeat_n(value, count))
        .collect()
}

#[derive(Debug)]
struct Patterns {
    order_prefix: Vec<u64>,
    row_order: Vec<u8>,
    row_line: Vec<u8>,
    subcategories: Vec<usize>,
    subcategory_prefix: [Vec<u64>; 3],
    subcategory_total: [u64; 3],
    quantities: Vec<i64>,
    multipliers: Vec<usize>,
    times: Vec<u8>,
    time_text: Vec<String>,
}

static PATTERNS: LazyLock<Patterns> = LazyLock::new(|| {
    let order_lines = expand(&ORDER_LINE_PATTERN);
    assert_eq!(order_lines.len(), ORDER_PERIOD);
    let mut order_prefix = vec![0_u64];
    let (mut row_order, mut row_line) = (Vec::new(), Vec::new());
    for (order, &lines) in order_lines.iter().enumerate() {
        order_prefix.push(order_prefix[order] + u64::from(lines));
        for line in 1..=lines {
            row_order.push(order as u8);
            row_line.push(line);
        }
    }
    assert_eq!(row_order.len(), LINE_PERIOD);
    let subcategories = expand(&SUBCATEGORY_PATTERN);
    assert_eq!(subcategories.len(), ORDER_PERIOD);
    let subcategory_prefix: [Vec<u64>; 3] = std::array::from_fn(|slot| {
        let mut prefix = vec![0_u64];
        for &entry in &subcategories {
            prefix.push(prefix.last().copied().unwrap_or(0) + u64::from(entry == slot));
        }
        prefix
    });
    let subcategory_total = std::array::from_fn(|slot| subcategory_prefix[slot][ORDER_PERIOD]);
    let times_pattern: Vec<(u8, usize)> = TIME_PATTERN
        .iter()
        .enumerate()
        .map(|(index, &(_, count))| (index as u8, count))
        .collect();
    Patterns {
        order_prefix,
        row_order,
        row_line,
        subcategories,
        subcategory_prefix,
        subcategory_total,
        quantities: expand(&QUANTITY_PATTERN),
        multipliers: expand(&PRICE_MULTIPLIER_PATTERN),
        times: expand(&times_pattern),
        time_text: TIME_PATTERN
            .iter()
            .map(|&((hour, minute), _)| format!("{hour:02}:{minute:02}:00"))
            .collect(),
    }
});

fn lines_before_order(order: u64) -> u64 {
    let patterns = &*PATTERNS;
    order / ORDER_PERIOD as u64 * LINE_PERIOD as u64
        + patterns.order_prefix[(order % ORDER_PERIOD as u64) as usize]
}

fn subcategory_count_before(line: u64, slot: usize) -> u64 {
    let patterns = &*PATTERNS;
    line / ORDER_PERIOD as u64 * patterns.subcategory_total[slot]
        + patterns.subcategory_prefix[slot][(line % ORDER_PERIOD as u64) as usize]
}

fn first_day() -> i64 {
    days_from_civil(FIRST_DATE.0, FIRST_DATE.1, FIRST_DATE.2)
}

fn day_count() -> usize {
    (days_from_civil(LAST_DATE.0, LAST_DATE.1, LAST_DATE.2) - first_day() + 1) as usize
}

fn order_count(scale_factor: f64) -> u64 {
    ((ORDERS_PER_SCALE * scale_factor).round_ties_even() as u64).max(1)
}

fn line_count(scale_factor: f64) -> u64 {
    lines_before_order(order_count(scale_factor))
}

fn row_count(table: &str, scale_factor: f64) -> u64 {
    match table {
        "dim_locations" => SEEDS.locations.len() as u64,
        "dim_products" => SEEDS.products.len() as u64,
        "order_lines" => line_count(scale_factor),
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

fn python_sum(values: &[f64]) -> f64 {
    let (mut total, mut compensation) = (0.0_f64, 0.0_f64);
    for &value in values {
        let next = total + value;
        if total.abs() >= value.abs() {
            compensation += (total - next) + value;
        } else {
            compensation += (value - next) + total;
        }
        total = next;
    }
    if compensation != 0.0 && compensation.is_finite() {
        total += compensation;
    }
    total
}

fn distribute(total: u64, weights: &[f64]) -> Vec<u64> {
    if total == 0 {
        return vec![0; weights.len()];
    }
    let weights: Vec<f64> = weights.iter().map(|weight| weight.max(0.0)).collect();
    let weight_sum = python_sum(&weights);
    let scaled: Vec<f64> = weights
        .iter()
        .map(|weight| total as f64 * weight / weight_sum)
        .collect();
    let mut counts: Vec<u64> = scaled.iter().map(|value| value.floor() as u64).collect();
    let remainder = total - counts.iter().sum::<u64>();
    let mut remainders: Vec<(f64, usize)> = scaled
        .iter()
        .zip(&counts)
        .enumerate()
        .map(|(index, (value, count))| (value - *count as f64, index))
        .collect();
    remainders.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(Ordering::Equal)
            .then(b.1.cmp(&a.1))
    });
    for step in 0..remainder as usize {
        counts[remainders[step % remainders.len()].1] += 1;
    }
    counts
}

fn day_weights() -> Vec<f64> {
    let days = day_count();
    (0..days)
        .map(|index| {
            let (_, month, _) = civil_from_days(first_day() + index as i64);
            let progress = index as f64 / (days - 1) as f64;
            let trend = TREND_START_WEIGHT + (TREND_END_WEIGHT - TREND_START_WEIGHT) * progress;
            MONTH_WEIGHTS[(month - 1) as usize] * trend
        })
        .collect()
}

#[derive(Debug)]
struct Placement {
    cumulative: Vec<u64>,
    cells_per_day: usize,
    order: Vec<usize>,
    window_of_day: Vec<usize>,
    window_first_line: Vec<u64>,
}

fn place(orders: u64) -> Placement {
    let seeds = &*SEEDS;
    let mut regions: Vec<(&str, Vec<usize>)> = Vec::new();
    let mut by_record: Vec<usize> = (0..seeds.locations.len()).collect();
    by_record.sort_by_key(|&index| seeds.locations[index].record_id);
    for index in by_record {
        let region = seeds.locations[index].region;
        match regions.iter_mut().find(|(name, _)| *name == region) {
            Some((_, members)) => members.push(index),
            None => regions.push((region, vec![index])),
        }
    }
    let region_weights: Vec<f64> = regions
        .iter()
        .map(|(name, _)| {
            REGION_WEIGHTS
                .iter()
                .find(|(region, _)| region == name)
                .map_or(1.0, |&(_, weight)| weight)
        })
        .collect();
    let cells_per_day = seeds.locations.len();
    let order: Vec<usize> = regions
        .iter()
        .flat_map(|(_, members)| members.iter().copied())
        .collect();
    let per_day = distribute(orders, &day_weights());
    let mut cumulative = Vec::with_capacity(per_day.len() * cells_per_day + 1);
    cumulative.push(0_u64);
    let mut running = 0_u64;
    for &day_orders in &per_day {
        for (count, (_, members)) in distribute(day_orders, &region_weights)
            .into_iter()
            .zip(&regions)
        {
            for location_orders in distribute(count, &vec![1.0; members.len()]) {
                running += location_orders;
                cumulative.push(running);
            }
        }
    }
    let first = first_day();
    let window_of_day = (0..per_day.len())
        .map(|index| {
            let day = first + index as i64;
            seeds
                .windows
                .iter()
                .position(|window| window.start_day <= day && day <= window.end_day)
                .expect("every order date has a product window")
        })
        .collect();
    let window_first_line = seeds
        .windows
        .iter()
        .map(|window| {
            let day_index = (window.start_day - first).clamp(0, per_day.len() as i64) as usize;
            lines_before_order(cumulative[day_index * cells_per_day])
        })
        .collect();
    Placement {
        cumulative,
        cells_per_day,
        order,
        window_of_day,
        window_first_line,
    }
}

pub(super) struct Coffeeshop;

impl Generator for Coffeeshop {
    fn name(&self) -> &'static str {
        "coffeeshop"
    }

    fn tables(&self) -> &'static [&'static str] {
        &["dim_locations", "dim_products", "order_lines"]
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        SCHEMAS.get(table).cloned()
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        row_count(table, scale_factor)
    }

    fn open(&self, table: &str, scale_factor: f64, _seed: u64) -> Arc<dyn Table> {
        match table {
            "dim_locations" => Arc::new(DimLocations),
            "dim_products" => Arc::new(DimProducts),
            "order_lines" => Arc::new(OrderLines::new(scale_factor)),
            other => unreachable!("table {other:?} was validated by the caller"),
        }
    }

    fn splittable(&self, table: &str) -> bool {
        table == "order_lines"
    }

    fn text_format(&self, _table: &str) -> TextFormat {
        TextFormat::csv(b',', false)
    }

    fn key_ranges(&self, _scale_factor: f64) -> Vec<KeyRange> {
        let seeds = &*SEEDS;
        let record_ids = |ids: &mut dyn Iterator<Item = i64>| {
            ids.fold((i64::MAX, i64::MIN), |(low, high), id| {
                (low.min(id), high.max(id))
            })
        };
        let (location_low, location_high) =
            record_ids(&mut seeds.locations.iter().map(|location| location.record_id));
        let (product_low, product_high) =
            record_ids(&mut seeds.products.iter().map(|product| product.record_id));
        vec![
            key("dim_locations", "record_id", location_low, location_high),
            key("dim_products", "record_id", product_low, product_high),
            key(
                "order_lines",
                "location_record_id",
                location_low,
                location_high,
            ),
            key(
                "order_lines",
                "product_record_id",
                product_low,
                product_high,
            ),
        ]
    }
}

fn int(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn text(mut builder: StringBuilder) -> ArrayRef {
    Arc::new(builder.finish())
}

fn dates(values: Vec<i32>) -> ArrayRef {
    Arc::new(Date32Array::from(values))
}

fn decimals(values: Vec<i128>, precision: u8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, 2)
            .expect("generated cents fit the column precision"),
    )
}

fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("generated columns match the schema")
}

#[derive(Debug)]
struct DimLocations;

impl Table for DimLocations {
    fn schema(&self) -> SchemaRef {
        coffeeshop_schema("dim_locations")
    }

    fn rows(&self) -> u64 {
        SEEDS.locations.len() as u64
    }

    fn block(&self, index: u64) -> RecordBatch {
        let locations = &SEEDS.locations;
        let start = (index * BLOCK_ROWS) as usize;
        let rows = &locations[start..start + block_len(locations.len() as u64, index)];
        let mut builders: [StringBuilder; 5] = std::array::from_fn(|_| StringBuilder::new());
        for location in rows {
            for (builder, value) in builders.iter_mut().zip([
                location.location_id,
                location.city,
                location.state,
                location.country,
                location.region,
            ]) {
                builder.append_value(value);
            }
        }
        let [location_id, city, state, country, region] = builders;
        batch(
            self.schema(),
            vec![
                int(rows.iter().map(|location| location.record_id).collect()),
                text(location_id),
                text(city),
                text(state),
                text(country),
                text(region),
            ],
        )
    }
}

#[derive(Debug)]
struct DimProducts;

impl Table for DimProducts {
    fn schema(&self) -> SchemaRef {
        coffeeshop_schema("dim_products")
    }

    fn rows(&self) -> u64 {
        SEEDS.products.len() as u64
    }

    fn block(&self, index: u64) -> RecordBatch {
        let seeds = &*SEEDS;
        let start = (index * BLOCK_ROWS) as usize;
        let rows: Vec<&Product> = seeds.window_order
            [start..start + block_len(seeds.products.len() as u64, index)]
            .iter()
            .map(|&position| &seeds.products[position])
            .collect();
        let mut builders: [StringBuilder; 3] = std::array::from_fn(|_| StringBuilder::new());
        for product in &rows {
            for (builder, value) in
                builders
                    .iter_mut()
                    .zip([product.name, product.category, product.subcategory])
            {
                builder.append_value(value);
            }
        }
        let [name, category, subcategory] = builders;
        batch(
            self.schema(),
            vec![
                int(rows.iter().map(|product| product.record_id).collect()),
                int(rows.iter().map(|product| product.product_id).collect()),
                text(name),
                text(category),
                text(subcategory),
                decimals(rows.iter().map(|product| product.cost_cents).collect(), 8),
                decimals(rows.iter().map(|product| product.price_cents).collect(), 8),
                dates(rows.iter().map(|product| product.from_day as i32).collect()),
                dates(rows.iter().map(|product| product.to_day as i32).collect()),
            ],
        )
    }
}

#[derive(Debug)]
struct OrderLines {
    rows: u64,
    placement: Placement,
}

impl OrderLines {
    fn new(scale_factor: f64) -> Self {
        Self {
            rows: line_count(scale_factor),
            placement: place(order_count(scale_factor)),
        }
    }
}

impl Table for OrderLines {
    fn schema(&self) -> SchemaRef {
        coffeeshop_schema("order_lines")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let (seeds, patterns, placement) = (&*SEEDS, &*PATTERNS, &self.placement);
        let first_line = index * BLOCK_ROWS;
        let first_order = first_line / LINE_PERIOD as u64 * ORDER_PERIOD as u64
            + u64::from(patterns.row_order[(first_line % LINE_PERIOD as u64) as usize]);
        let mut cell = placement
            .cumulative
            .partition_point(|&upper| upper <= first_order)
            - 1;
        let mut ints: [Vec<i64>; 6] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut order_dates = Vec::with_capacity(rows);
        let mut unit_prices = Vec::with_capacity(rows);
        let mut total_prices = Vec::with_capacity(rows);
        let mut location_ids = StringBuilder::with_capacity(rows, rows * 8);
        let mut order_times = StringBuilder::with_capacity(rows, rows * 8);
        let mut regions = StringBuilder::with_capacity(rows, rows * 8);
        let first = first_day();
        for line in first_line..first_line + rows as u64 {
            let period = line / LINE_PERIOD as u64;
            let offset = (line % LINE_PERIOD as u64) as usize;
            let order = period * ORDER_PERIOD as u64 + u64::from(patterns.row_order[offset]);
            while placement.cumulative[cell + 1] <= order {
                cell += 1;
            }
            let day = cell / placement.cells_per_day;
            let location = &seeds.locations[placement.order[cell % placement.cells_per_day]];
            let window = placement.window_of_day[day];
            let slot = patterns.subcategories[(line % ORDER_PERIOD as u64) as usize];
            let rank = subcategory_count_before(line, slot)
                - subcategory_count_before(placement.window_first_line[window], slot);
            let members = &seeds.windows[window].by_subcategory[slot];
            let product_index = members[(rank % members.len() as u64) as usize];
            let product = &seeds.products[product_index];
            let quantity = patterns.quantities[(line % ORDER_PERIOD as u64) as usize];
            let multiplier = patterns.multipliers[(line % 50) as usize];
            let unit = seeds.unit_cents[product_index][multiplier];
            for (column, value) in ints.iter_mut().zip([
                order as i64 + 1,
                i64::from(patterns.row_line[offset]),
                location.record_id,
                product.record_id,
                product.product_id,
                quantity,
            ]) {
                column.push(value);
            }
            location_ids.append_value(location.location_id);
            regions.append_value(location.region);
            order_times.append_value(
                &patterns.time_text
                    [usize::from(patterns.times[(order % ORDER_PERIOD as u64) as usize])],
            );
            order_dates.push((first + day as i64) as i32);
            unit_prices.push(i128::from(unit));
            total_prices.push(i128::from(unit * quantity));
        }
        let [
            order_id,
            line_number,
            location_record,
            product_record,
            product_id,
            quantity,
        ] = ints;
        batch(
            self.schema(),
            vec![
                int(order_id),
                int(line_number),
                int(location_record),
                text(location_ids),
                int(product_record),
                int(product_id),
                dates(order_dates),
                text(order_times),
                int(quantity),
                decimals(unit_prices, 8),
                decimals(total_prices, 10),
                text(regions),
            ],
        )
    }
}

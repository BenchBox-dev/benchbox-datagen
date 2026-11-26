use std::fmt::Write;
use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, Int64Array, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::calendar::{
    civil_from_days, date_key, days_from_civil, days_in_month, iso_week, weekday_from_monday,
};
use super::rng::{Draw, Stream};
use super::{BLOCK_ROWS, Table, block_len, ssb_schema};
use crate::KeyRange;

const BENCHMARK: &str = "ssb";
const CUSTOMERS_PER_SCALE: u64 = 30_000;
const SUPPLIERS_PER_SCALE: u64 = 2_000;
const PARTS_PER_SCALE: u64 = 200_000;
const LINEORDERS_PER_SCALE: u64 = 6_000_000;
const DATE_ROWS: u64 = 2_557;
const MAX_COMMIT_DAYS: i64 = 121;
const NEW_ORDER_THRESHOLD: u32 = 3_006_477_107;
const FIRST_DATE: (i64, i64, i64) = (1992, 1, 1);

const NATIONS: [(&str, &str); 25] = [
    ("ALGERIA", "AFRICA"),
    ("ARGENTINA", "AMERICA"),
    ("BRAZIL", "AMERICA"),
    ("CANADA", "AMERICA"),
    ("EGYPT", "AFRICA"),
    ("ETHIOPIA", "AFRICA"),
    ("FRANCE", "EUROPE"),
    ("GERMANY", "EUROPE"),
    ("INDIA", "ASIA"),
    ("INDONESIA", "ASIA"),
    ("IRAN", "MIDDLE EAST"),
    ("IRAQ", "MIDDLE EAST"),
    ("JAPAN", "ASIA"),
    ("JORDAN", "MIDDLE EAST"),
    ("KENYA", "AFRICA"),
    ("MOROCCO", "AFRICA"),
    ("MOZAMBIQUE", "AFRICA"),
    ("PERU", "AMERICA"),
    ("CHINA", "ASIA"),
    ("ROMANIA", "EUROPE"),
    ("SAUDI ARABIA", "MIDDLE EAST"),
    ("VIETNAM", "ASIA"),
    ("RUSSIA", "EUROPE"),
    ("UNITED KINGDOM", "EUROPE"),
    ("UNITED STATES", "AMERICA"),
];
const SEGMENTS: [&str; 5] = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "HOUSEHOLD",
    "MACHINERY",
];
const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const SHIP_MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
const COLORS: [&str; 13] = [
    "almond",
    "antique",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanched",
    "blue",
    "blush",
    "brown",
    "burlywood",
    "burnished",
];
const TYPES: [&str; 6] = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
const CONTAINERS: [&str; 12] = [
    "SM CASE", "SM BOX", "SM PACK", "SM PKG", "MED BAG", "MED BOX", "MED PKG", "MED PACK",
    "LG CASE", "LG BOX", "LG PACK", "LG PKG",
];
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

fn scaled(base: u64, scale_factor: f64) -> u64 {
    ((base as f64 * scale_factor) as u64).max(1)
}

pub(super) fn row_count(table: &str, scale_factor: f64) -> u64 {
    match table {
        "date" => DATE_ROWS,
        "customer" => scaled(CUSTOMERS_PER_SCALE, scale_factor),
        "supplier" => scaled(SUPPLIERS_PER_SCALE, scale_factor),
        "part" => scaled(PARTS_PER_SCALE, scale_factor),
        "lineorder" => scaled(LINEORDERS_PER_SCALE, scale_factor),
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

pub(super) fn open(table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
    let rows = row_count(table, scale_factor);
    match table {
        "date" => Arc::new(DateTable),
        "customer" => Arc::new(Geography::customers(rows, seed)),
        "supplier" => Arc::new(Geography::suppliers(rows, seed)),
        "part" => Arc::new(Part { rows, seed }),
        "lineorder" => Arc::new(Lineorder::new(scale_factor, seed)),
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

pub(crate) fn key_ranges(scale_factor: f64) -> Vec<KeyRange> {
    let first = date_key(days_from_civil(FIRST_DATE.0, FIRST_DATE.1, FIRST_DATE.2));
    let last =
        date_key(days_from_civil(FIRST_DATE.0, FIRST_DATE.1, FIRST_DATE.2) + DATE_ROWS as i64 - 1);
    let (customers, suppliers, parts) = (
        row_count("customer", scale_factor) as i64,
        row_count("supplier", scale_factor) as i64,
        row_count("part", scale_factor) as i64,
    );
    let key = |table, column, min, max| KeyRange {
        table,
        column,
        min,
        max,
    };
    vec![
        key("customer", "c_custkey", 1, customers),
        key("date", "d_datekey", first, last),
        key("lineorder", "lo_custkey", 1, customers),
        key("lineorder", "lo_orderdate", first, last),
        key("lineorder", "lo_partkey", 1, parts),
        key("lineorder", "lo_suppkey", 1, suppliers),
        key("part", "p_partkey", 1, parts),
        key("supplier", "s_suppkey", 1, suppliers),
    ]
}

fn int(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn text(mut builder: StringBuilder) -> ArrayRef {
    Arc::new(builder.finish())
}

fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("generated columns match the schema")
}

fn first_day() -> i64 {
    days_from_civil(FIRST_DATE.0, FIRST_DATE.1, FIRST_DATE.2)
}

struct DateTable;

impl Table for DateTable {
    fn schema(&self) -> SchemaRef {
        ssb_schema("date")
    }

    fn rows(&self) -> u64 {
        DATE_ROWS
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(DATE_ROWS, index);
        let first = first_day() + (index * BLOCK_ROWS) as i64;
        let mut ints: [Vec<i64>; 12] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut dates = StringBuilder::new();
        let mut weekdays = StringBuilder::new();
        let mut months = StringBuilder::new();
        let mut year_months = StringBuilder::new();
        let mut seasons = StringBuilder::new();
        let mut scratch = String::new();
        for day in first..first + rows as i64 {
            let (year, month, day_of_month) = civil_from_days(day);
            let weekday = weekday_from_monday(day);
            let month_index = (month - 1) as usize;
            scratch.clear();
            write!(scratch, "{year:04}-{month:02}-{day_of_month:02}").expect("string write");
            dates.append_value(&scratch);
            weekdays.append_value(DAYS[weekday as usize]);
            months.append_value(MONTHS[month_index]);
            scratch.clear();
            write!(scratch, "{}{year}", &MONTHS[month_index][..3]).expect("string write");
            year_months.append_value(&scratch);
            seasons.append_value(match month {
                12 | 1 | 2 => "Winter",
                3..=5 => "Spring",
                6..=8 => "Summer",
                _ => "Fall",
            });
            let values = [
                date_key(day),
                year,
                year * 100 + month,
                weekday + 1,
                day_of_month,
                day - days_from_civil(year, 1, 1) + 1,
                month,
                iso_week(day),
                i64::from(weekday == 6),
                i64::from(day_of_month == days_in_month(year, month)),
                i64::from((month == 12 && day_of_month == 25) || (month == 1 && day_of_month == 1)),
                i64::from(weekday < 5),
            ];
            for (column, value) in ints.iter_mut().zip(values) {
                column.push(value);
            }
        }
        let [
            datekey,
            year,
            yearmonthnum,
            daynuminweek,
            daynuminmonth,
            daynuminyear,
            monthnuminyear,
            weeknuminyear,
            lastdayinweek,
            lastdayinmonth,
            holiday,
            weekday,
        ] = ints;
        batch(
            self.schema(),
            vec![
                int(datekey),
                text(dates),
                text(weekdays),
                text(months),
                int(year),
                int(yearmonthnum),
                text(year_months),
                int(daynuminweek),
                int(daynuminmonth),
                int(daynuminyear),
                int(monthnuminyear),
                int(weeknuminyear),
                text(seasons),
                int(lastdayinweek),
                int(lastdayinmonth),
                int(holiday),
                int(weekday),
            ],
        )
    }
}

struct Geography {
    table: &'static str,
    prefix: &'static str,
    street: &'static str,
    segments: bool,
    rows: u64,
    stream: Stream,
}

impl Geography {
    fn customers(rows: u64, seed: u64) -> Self {
        Self {
            table: "customer",
            prefix: "Customer",
            street: "Street",
            segments: true,
            rows,
            stream: Stream::new(BENCHMARK, "customer", "fields", seed),
        }
    }

    fn suppliers(rows: u64, seed: u64) -> Self {
        Self {
            table: "supplier",
            prefix: "Supplier",
            street: "Avenue",
            segments: false,
            rows,
            stream: Stream::new(BENCHMARK, "supplier", "fields", seed),
        }
    }
}

fn phone(draw: &mut Draw, out: &mut String) {
    let (area, exchange, line) = (
        draw.range(10, 99),
        draw.range(100, 999),
        draw.range(1000, 9999),
    );
    write!(out, "{area}-{exchange}-{line}").expect("string write");
}

impl Table for Geography {
    fn schema(&self) -> SchemaRef {
        ssb_schema(self.table)
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut draw = self.stream.block(index);
        let first = (index * BLOCK_ROWS) as i64 + 1;
        let mut keys = Vec::with_capacity(rows);
        let mut names = StringBuilder::new();
        let mut addresses = StringBuilder::new();
        let mut cities = StringBuilder::new();
        let mut nations = StringBuilder::new();
        let mut regions = StringBuilder::new();
        let mut phones = StringBuilder::new();
        let mut segments = StringBuilder::new();
        let mut scratch = String::new();
        for key in first..first + rows as i64 {
            keys.push(key);
            scratch.clear();
            write!(scratch, "{}#{key:09}", self.prefix).expect("string write");
            names.append_value(&scratch);
            scratch.clear();
            write!(scratch, "Address {} {}", draw.range(1, 999), self.street)
                .expect("string write");
            addresses.append_value(&scratch);
            let (nation, region) = NATIONS[draw.below(NATIONS.len() as u64) as usize];
            scratch.clear();
            write!(
                scratch,
                "{}{}",
                &nation[..nation.len().min(9)],
                draw.range(0, 9)
            )
            .expect("string write");
            cities.append_value(&scratch);
            nations.append_value(nation);
            regions.append_value(region);
            scratch.clear();
            phone(&mut draw, &mut scratch);
            phones.append_value(&scratch);
            if self.segments {
                segments.append_value(draw.pick(&SEGMENTS));
            }
        }
        let mut columns = vec![
            int(keys),
            text(names),
            text(addresses),
            text(cities),
            text(nations),
            text(regions),
            text(phones),
        ];
        if self.segments {
            columns.push(text(segments));
        }
        batch(self.schema(), columns)
    }
}

struct Part {
    rows: u64,
    seed: u64,
}

impl Table for Part {
    fn schema(&self) -> SchemaRef {
        ssb_schema("part")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut draw = Stream::new(BENCHMARK, "part", "fields", self.seed).block(index);
        let first = (index * BLOCK_ROWS) as i64 + 1;
        let mut keys = Vec::with_capacity(rows);
        let mut sizes = Vec::with_capacity(rows);
        let mut names = StringBuilder::new();
        let mut mfgrs = StringBuilder::new();
        let mut categories = StringBuilder::new();
        let mut brands = StringBuilder::new();
        let mut colors = StringBuilder::new();
        let mut types = StringBuilder::new();
        let mut containers = StringBuilder::new();
        let mut scratch = String::new();
        for key in first..first + rows as i64 {
            keys.push(key);
            scratch.clear();
            write!(scratch, "Part {key}").expect("string write");
            names.append_value(&scratch);
            let mfgr = (key - 1) % 5 + 1;
            let category = ((key - 1) / 5) % 5 + 1;
            let brand = ((key - 1) / 25) % 40 + 1;
            scratch.clear();
            write!(scratch, "MFGR#{mfgr}").expect("string write");
            mfgrs.append_value(&scratch);
            scratch.clear();
            write!(scratch, "MFGR#{mfgr}{category}").expect("string write");
            categories.append_value(&scratch);
            scratch.clear();
            write!(scratch, "MFGR#{mfgr}{category}{brand:02}").expect("string write");
            brands.append_value(&scratch);
            colors.append_value(draw.pick(&COLORS));
            types.append_value(draw.pick(&TYPES));
            sizes.push(draw.range(1, 50));
            containers.append_value(draw.pick(&CONTAINERS));
        }
        batch(
            self.schema(),
            vec![
                int(keys),
                text(names),
                text(mfgrs),
                text(categories),
                text(brands),
                text(colors),
                text(types),
                int(sizes),
                text(containers),
            ],
        )
    }
}

struct Lineorder {
    rows: u64,
    customers: u64,
    suppliers: u64,
    parts: u64,
    fields: Stream,
    orders: Stream,
    date_keys: Vec<i64>,
    starts: OnceLock<Vec<(i64, i64)>>,
}

impl Lineorder {
    fn new(scale_factor: f64, seed: u64) -> Self {
        let first = first_day();
        Self {
            rows: row_count("lineorder", scale_factor),
            customers: row_count("customer", scale_factor),
            suppliers: row_count("supplier", scale_factor),
            parts: row_count("part", scale_factor),
            fields: Stream::new(BENCHMARK, "lineorder", "fields", seed),
            orders: Stream::new(BENCHMARK, "lineorder", "orders", seed),
            date_keys: (0..DATE_ROWS as i64 + MAX_COMMIT_DAYS)
                .map(|offset| date_key(first + offset))
                .collect(),
            starts: OnceLock::new(),
        }
    }

    fn starts(&self) -> &[(i64, i64)] {
        self.starts.get_or_init(|| {
            let mut starts = Vec::with_capacity(self.blocks() as usize);
            let (mut key, mut line) = (1_i64, 0_i64);
            for block in 0..self.blocks() {
                starts.push((key, line));
                let rows = block_len(self.rows, block);
                let mut flags = self.orders.block(block);
                let (mut count, mut last) = (0_i64, None);
                for row in 0..rows {
                    if flags.u32() < NEW_ORDER_THRESHOLD || (block == 0 && row == 0) {
                        count += 1;
                        last = Some(row);
                    }
                }
                match last {
                    Some(last) => {
                        key += count;
                        line = (rows - last) as i64;
                    }
                    None => line += rows as i64,
                }
            }
            starts
        })
    }
}

impl Table for Lineorder {
    fn schema(&self) -> SchemaRef {
        ssb_schema("lineorder")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let (mut order_key, mut line) = self.starts()[index as usize];
        let mut flags = self.orders.block(index);
        let mut draw = self.fields.block(index);
        let mut ints: [Vec<i64>; 15] = std::array::from_fn(|_| Vec::with_capacity(rows));
        let mut priorities = StringBuilder::with_capacity(rows, rows * 8);
        let mut ship_modes = StringBuilder::with_capacity(rows, rows * 8);
        let (customers, suppliers, parts) = (
            self.customers as i64,
            self.suppliers as i64,
            self.parts as i64,
        );
        for row in 0..rows {
            if flags.u32() < NEW_ORDER_THRESHOLD || (index == 0 && row == 0) {
                order_key += 1;
                line = 1;
            } else {
                line += 1;
            }
            let customer = draw.range(1, customers);
            let part = draw.range(1, parts);
            let supplier = draw.range(1, suppliers);
            let order_day = draw.below(DATE_ROWS) as usize;
            priorities.append_value(draw.pick(&PRIORITIES));
            let ship_priority = draw.range(0, 1);
            let quantity = draw.range(1, 50);
            let unit_price = draw.range(90_000, 200_000);
            let extended = unit_price * quantity;
            let discount = draw.range(0, 10);
            let supply_cost = unit_price * draw.range(50, 80) / 100;
            let tax = draw.range(0, 8);
            let commit_day = order_day + draw.range(1, MAX_COMMIT_DAYS) as usize;
            ship_modes.append_value(draw.pick(&SHIP_MODES));
            let values = [
                order_key,
                line,
                customer,
                part,
                supplier,
                self.date_keys[order_day],
                ship_priority,
                quantity,
                extended,
                extended,
                discount,
                extended * (100 - discount) / 100,
                supply_cost,
                tax,
                self.date_keys[commit_day],
            ];
            for (column, value) in ints.iter_mut().zip(values) {
                column.push(value);
            }
        }
        let [
            order_key,
            line,
            customer,
            part,
            supplier,
            order_date,
            ship_priority,
            quantity,
            extended,
            total,
            discount,
            revenue,
            supply_cost,
            tax,
            commit,
        ] = ints;
        batch(
            self.schema(),
            vec![
                int(order_key),
                int(line),
                int(customer),
                int(part),
                int(supplier),
                int(order_date),
                text(priorities),
                int(ship_priority),
                int(quantity),
                int(extended),
                int(total),
                int(discount),
                int(revenue),
                int(supply_cost),
                int(tax),
                int(commit),
                text(ship_modes),
            ],
        )
    }
}

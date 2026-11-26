use std::path::Path;

use tpcdsgen::distribution::{
    CallCenterDistributions, ColorsWeights, FirstNamesWeights, IdWeights, NamesDistributions,
    SizeWeights, WebPageUseDistribution, get_has_size_at_index, get_syllables_distribution,
    pick_random_category_index, pick_random_color, pick_random_current_price_range,
    pick_random_manager_id_range, pick_random_size, pick_random_unit,
};
use tpcdsgen::permutations::make_permutation;
use tpcdsgen::pseudo_table_scaling_infos::PseudoTableScalingInfos;
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::types::Decimal;

use super::calendar::{DATA_END_DATE, DATA_START_DATE, TODAYS_DATE};
use super::columns::*;
use super::ctx::Ctx;
use super::out::Out;
use crate::{Error, FileStats};

const SCALE_VOLUMES: [i64; 9] = [1, 10, 100, 300, 1000, 3000, 10_000, 30_000, 100_000];

struct Rowcount {
    weights: [i64; 9],
    power: u32,
}

fn rowcount(table: &str) -> Rowcount {
    let (weights, power) = match table {
        "s_call_center" | "s_warehouse" | "s_web_site" => ([1; 9], 0),
        "s_catalog_page" => ([150, 210, 240, 240, 240, 240, 240, 240, 240], 0),
        "s_customer" => ([5, 10, 20, 50, 120, 300, 650, 800, 1000], 3),
        "s_customer_address" => ([25, 40, 100, 250, 600, 1500, 3250, 4000, 5000], 0),
        "s_item" => ([5, 7, 10, 13, 15, 18, 20, 23, 25], 2),
        "s_promotion" => ([5, 7, 10, 13, 15, 18, 20, 23, 25], 0),
        "s_store" => ([1, 1, 2, 4, 5, 6, 7, 8, 9], 0),
        "s_web_page" => ([6, 1, 200, 260, 300, 360, 400, 460, 500], 0),
        "s_zip_to_gmt" => ([100; 9], 3),
        other => unreachable!("no source rowcount for {other:?}"),
    };
    Rowcount { weights, power }
}

pub(super) fn s_rows(table: &str, scale: f64) -> i64 {
    let Rowcount { weights, power } = rowcount(table);
    let multiplier = 10_i64.pow(power);
    if scale < 1.0 {
        return (((weights[0] * multiplier) as f64 * scale) as i64).max(1);
    }
    let mut target = scale as i64;
    let base = match SCALE_VOLUMES.iter().position(|volume| *volume == target) {
        Some(index) => weights[index],
        None => {
            let mut total = 0;
            for index in (0..SCALE_VOLUMES.len()).rev() {
                while target >= SCALE_VOLUMES[index] {
                    total += weights[index];
                    target -= SCALE_VOLUMES[index];
                }
            }
            total
        }
    };
    base * multiplier
}

fn concurrent_web_sites(scale: f64) -> i64 {
    if scale < 1.0 {
        return ((2.0 * scale) as i64).max(1);
    }
    PseudoTableScalingInfos::get_concurrent_web_sites()
        .get_row_count_for_scale(scale)
        .expect("concurrent web sites is defined for every scale")
}

const CALL_CENTER_TABLE: i64 = 0;
const ITEM_TABLE: i64 = 11;
const WEB_PAGE_TABLE: i64 = 20;
const WEB_SITE_TABLE: i64 = 23;
const STORE_SOURCE_TABLE: i64 = 49;
const ITEM_STREAM: Col = CC_HOURS;

const WEB_DATE_STAGGER: i64 = 17;
const WEB_PAGES_PER_SITE: i64 = 123;
const WEB_MAX_DATE: i64 = 2_452_640;
const CC_EMPLOYEE_MAX: i32 = 7;
const STORE_CLOSED_PCT: i32 = 30;
const STORE_MIN_DAYS_OPEN: i32 = 5;
const STORE_MAX_DAYS_OPEN: i32 = 500;
const WP_AUTOGEN_PCT: i32 = 30;
const DEFAULT_NOT_NULL: i64 = 0x0B;

struct Revision {
    business_key: i64,
    end_date: i64,
    first: bool,
}

fn revision(table: i64, row: i64) -> Revision {
    let start = i64::from(DATA_START_DATE);
    let end = i64::from(DATA_END_DATE);
    let half = start + (end - start) / 2;
    let third = (end - start) / 3;
    let first_third = start + third;
    let second_third = first_third + third;
    let offset = table * 6;
    let (business_key, end_date, first) = match row % 6 {
        1 => (row, -1, true),
        2 => (row, half - offset, true),
        3 => (row - 1, -1, false),
        4 => (row, first_third - offset, true),
        5 => (row - 1, second_third - offset, false),
        _ => (row - 2, -1, false),
    };
    Revision {
        business_key,
        end_date: if end_date > end { -1 } else { end_date },
        first,
    }
}

struct Flags {
    bits: i32,
    first: bool,
}

impl Flags {
    fn keep<T: Clone>(&mut self, new: &mut T, old: &mut T) {
        if self.bits & 1 == 1 && !self.first {
            *new = old.clone();
        } else {
            *old = new.clone();
        }
        self.bits >>= 1;
    }

    fn skip(&mut self, fields: u32) {
        self.bits >>= fields;
    }
}

struct Nulls {
    mask: i64,
    first_column: usize,
}

impl Nulls {
    fn draw(ctx: &mut Ctx, column: Col, span: Span, percent: i32, not_null: i64) -> Self {
        let threshold = ctx.rng.int(column, 0, 9999);
        let bits = ctx.rng.key(column, 1, i64::from(i32::MAX));
        Self {
            mask: if threshold < percent { bits & !not_null } else { 0 },
            first_column: span.first,
        }
    }

    fn put(&self, out: &mut Out, column: Col, write: impl FnOnce(&mut Out)) {
        if (self.mask >> (column.number - self.first_column)) & 1 == 1 {
            out.text("");
        } else {
            write(out);
        }
    }
}

struct Order {
    ids: Vec<i32>,
}

impl Order {
    fn new(ctx: &mut Ctx, table: &str, column: Col) -> Self {
        let count = ctx.ids(table) as usize;
        Self {
            ids: make_permutation(count, ctx.rng.stream(column)),
        }
    }

    fn surrogate_key(&self, ctx: &mut Ctx, index: i64, column: Col) -> i64 {
        let id = i64::from(self.ids[(index - 1) as usize]) + 1;
        let block = id / 3;
        match id % 3 {
            1 => block * 6 + 1,
            2 => block * 6 + i64::from(ctx.rng.int(column, 2, 3)),
            _ => (block - 1) * 6 + i64::from(ctx.rng.int(column, 4, 6)),
        }
    }
}

fn person(ctx: &mut Ctx, column: Col) -> String {
    let first = NamesDistributions::pick_random_first_name(
        FirstNamesWeights::MaleFrequency,
        ctx.rng.stream(column),
    );
    let last = NamesDistributions::pick_random_last_name(ctx.rng.stream(column));
    format!("{first} {last}")
}

fn tax(ctx: &mut Ctx, column: Col, max: i64) -> Decimal {
    RandomValueGenerator::generate_uniform_random_decimal(
        Decimal::new(0, 2),
        Decimal::new(max, 2),
        ctx.rng.stream(column),
    )
}

pub(super) fn write_scd_tables(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    Ok(vec![
        write_call_center(ctx, dir)?,
        write_item(ctx, dir)?,
        write_store(ctx, dir)?,
        write_web_page(ctx, dir)?,
        write_web_site(ctx, dir)?,
    ])
}

#[derive(Default)]
struct CallCenterOld {
    employees: i32,
    sq_ft: i32,
    manager: String,
    tax: Decimal,
}

fn write_call_center(ctx: &mut Ctx, dir: &Path) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_call_center", ctx.plan.update)?;
    let rows = s_rows("s_call_center", ctx.scale);
    let mut order: Option<Order> = None;
    let mut old = CallCenterOld::default();
    let mut open_date = 0;
    let mut name = String::new();
    let employee_limit = {
        let scale = ctx.scale.ceil() as i32;
        CC_EMPLOYEE_MAX.wrapping_mul(scale).wrapping_mul(scale)
    };
    let centers = CallCenterDistributions::get_number_of_call_centers() as i64;
    for index in 1..=rows {
        let order = order.get_or_insert_with(|| Order::new(ctx, "call_center", S_CALL_CENTER_ID));
        let sk = order.surrogate_key(ctx, index, S_CALL_CENTER_ID);
        let nulls = Nulls::draw(ctx, CC_NULLS, TABLE_CALL_CENTER, 100, DEFAULT_NOT_NULL);
        let key = revision(CALL_CENTER_TABLE, sk);
        if key.first {
            let draw = ctx.rng.int(CC_OPEN_DATE_ID, -365, 0);
            open_date = DATA_START_DATE - 23 - draw;
            let base = CallCenterDistributions::get_call_center_at_index((sk % centers) as usize);
            let suffix = sk / centers;
            name = if suffix > 0 {
                format!("{base}_{suffix}")
            } else {
                base.to_string()
            };
        }
        let mut flags = Flags {
            bits: ctx.rng.next(CC_SCD) as i32,
            first: key.first,
        };
        let class = CallCenterDistributions::pick_random_call_center_class(ctx.rng.stream(CC_CLASS));
        flags.skip(1);
        let mut employees = ctx.rng.int(CC_EMPLOYEES, 1, employee_limit);
        flags.keep(&mut employees, &mut old.employees);
        let mut sq_ft = ctx
            .rng
            .int(CC_SQ_FT, 100, 700)
            .wrapping_mul(employees);
        flags.keep(&mut sq_ft, &mut old.sq_ft);
        let hours = CallCenterDistributions::pick_random_call_center_hours(ctx.rng.stream(CC_HOURS));
        flags.skip(1);
        let mut manager = person(ctx, CC_MANAGER);
        flags.keep(&mut manager, &mut old.manager);
        flags.skip(8);
        let mut tax = tax(ctx, CC_TAX_PERCENTAGE, 12);
        flags.keep(&mut tax, &mut old.tax);

        nulls.put(&mut out, CC_CALL_CENTER_SK, |o| {
            o.id(sk);
        });
        nulls.put(&mut out, CC_OPEN_DATE_ID, |o| {
            o.date(open_date);
        });
        nulls.put(&mut out, CC_CLOSED_DATE_ID, |o| {
            o.date(-1);
        });
        nulls.put(&mut out, CC_NAME, |o| {
            o.text(&name);
        });
        nulls.put(&mut out, CC_CLASS, |o| {
            o.text(class);
        });
        nulls.put(&mut out, CC_EMPLOYEES, |o| {
            o.int(employees);
        });
        nulls.put(&mut out, CC_SQ_FT, |o| {
            o.int(sq_ft);
        });
        nulls.put(&mut out, CC_HOURS, |o| {
            o.text(hours);
        });
        nulls.put(&mut out, CC_MANAGER, |o| {
            o.text(&manager);
        });
        nulls.put(&mut out, CC_TAX_PERCENTAGE, |o| {
            o.decimal(&tax);
        });
        out.end()?;
        ctx.rng.row_stop(TABLE_S_CALL_CENTER);
    }
    out.finish()
}

#[derive(Default)]
struct ItemOld {
    description: String,
    wholesale: Decimal,
    formulation: String,
}

fn write_item(ctx: &mut Ctx, dir: &Path) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_item", ctx.plan.update)?;
    let rows = s_rows("s_item", ctx.scale);
    let mut order: Option<Order> = None;
    let mut old = ItemOld::default();
    for index in 1..=rows {
        let order = order.get_or_insert_with(|| Order::new(ctx, "item", S_ITEM_PERMUTE));
        let sk = order.surrogate_key(ctx, index, S_ITEM_ID);
        let key = revision(ITEM_TABLE, sk);

        let (low, high) = pick_random_manager_id_range(IdWeights::Unified, ctx.rng.stream(I_MANAGER_ID));
        let manager = ctx.rng.key(I_MANAGER_ID, i64::from(low), i64::from(high));
        let mut flags = Flags {
            bits: ctx.rng.next(I_SCD) as i32,
            first: key.first,
        };
        let mut description = ctx.gen_text(I_ITEM_DESC, 1, 200);
        flags.keep(&mut description, &mut old.description);
        let (low, high) = pick_random_current_price_range(ctx.rng.stream(I_CURRENT_PRICE));
        let price = RandomValueGenerator::generate_uniform_random_decimal(
            low,
            high,
            ctx.rng.stream(I_CURRENT_PRICE),
        );
        flags.skip(1);
        let markdown = RandomValueGenerator::generate_uniform_random_decimal(
            Decimal::new(30, 2),
            Decimal::new(90, 2),
            ctx.rng.stream(I_WHOLESALE_COST),
        );
        let mut wholesale = Decimal::multiply(price, markdown);
        flags.keep(&mut wholesale, &mut old.wholesale);
        let category = pick_random_category_index(ctx.rng.stream(I_CATEGORY));
        let sizes = if get_has_size_at_index(category) == 0 {
            SizeWeights::NoSize
        } else {
            SizeWeights::Sized
        };
        let size = pick_random_size(sizes, ctx.rng.stream(I_SIZE));
        flags.skip(5);
        let mut formulation = RandomValueGenerator::generate_random_charset(
            RandomValueGenerator::DIGITS,
            20,
            20,
            ctx.rng.stream(I_FORMULATION),
        );
        let embedded = pick_random_color(ColorsWeights::Skewed, ctx.rng.stream(I_FORMULATION));
        let position = RandomValueGenerator::generate_uniform_random_int(
            0,
            formulation.len() as i32 - embedded.len() as i32 - 1,
            ctx.rng.stream(I_FORMULATION),
        ) as usize;
        formulation.replace_range(position..position + embedded.len(), &embedded);
        flags.keep(&mut formulation, &mut old.formulation);
        let color = pick_random_color(ColorsWeights::Skewed, ctx.rng.stream(I_COLOR));
        let units = pick_random_unit(ctx.rng.stream(I_UNITS));
        ctx.rng.next(ITEM_STREAM);
        ctx.rng.row_stop(TABLE_ITEM);

        out.id(key.business_key)
            .text(&description)
            .decimal(&price)
            .decimal(&wholesale)
            .text(&size)
            .text(&formulation)
            .text(&color)
            .text(&units)
            .text("Unknown")
            .key(manager)
            .end()?;
        ctx.rng.row_stop(TABLE_S_ITEM);
    }
    out.finish()
}

#[derive(Default)]
struct StoreOld {
    closed_date: i64,
    name: String,
    employees: i32,
    floor_space: i32,
    manager: String,
    market_id: i32,
    tax: Decimal,
    market_manager: String,
}

fn write_store(ctx: &mut Ctx, dir: &Path) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_store", ctx.plan.update)?;
    let rows = s_rows("s_store", ctx.scale);
    let mut order: Option<Order> = None;
    let mut old = StoreOld::default();
    for index in 1..=rows {
        let order = order.get_or_insert_with(|| Order::new(ctx, "store", S_STORE_ID));
        let sk = order.surrogate_key(ctx, index, S_STORE_ID);
        let nulls = Nulls::draw(ctx, W_STORE_NULLS, TABLE_STORE, 100, DEFAULT_NOT_NULL);
        let key = revision(STORE_SOURCE_TABLE, sk);
        let mut flags = Flags {
            bits: ctx.rng.next(W_STORE_SCD) as i32,
            first: key.first,
        };
        let percentage = ctx.rng.int(W_STORE_CLOSED_DATE_ID, 1, 100);
        let days_open = ctx
            .rng
            .int(W_STORE_CLOSED_DATE_ID, STORE_MIN_DAYS_OPEN, STORE_MAX_DAYS_OPEN);
        let mut closed_date = if percentage < STORE_CLOSED_PCT {
            i64::from(DATA_START_DATE) + i64::from(days_open)
        } else {
            -1
        };
        flags.keep(&mut closed_date, &mut old.closed_date);
        if closed_date == 0 {
            closed_date = -1;
        }
        let mut name = RandomValueGenerator::generate_word(sk, 5, get_syllables_distribution());
        flags.keep(&mut name, &mut old.name);
        let mut employees = ctx.rng.int(W_STORE_EMPLOYEES, 200, 300);
        flags.keep(&mut employees, &mut old.employees);
        let mut floor_space = ctx.rng.int(W_STORE_FLOOR_SPACE, 5_000_000, 10_000_000);
        flags.keep(&mut floor_space, &mut old.floor_space);
        let hours = CallCenterDistributions::pick_random_call_center_hours(ctx.rng.stream(W_STORE_HOURS));
        flags.skip(1);
        let mut manager = person(ctx, W_STORE_MANAGER);
        flags.keep(&mut manager, &mut old.manager);
        let mut market_id = ctx.rng.int(W_STORE_MARKET_ID, 1, 10);
        flags.keep(&mut market_id, &mut old.market_id);
        let mut tax = tax(ctx, W_STORE_TAX_PERCENTAGE, 11);
        flags.keep(&mut tax, &mut old.tax);
        flags.skip(2);
        let mut market_manager = person(ctx, W_STORE_MARKET_MANAGER);
        flags.keep(&mut market_manager, &mut old.market_manager);

        nulls.put(&mut out, W_STORE_ID, |o| {
            o.id(key.business_key);
        });
        nulls.put(&mut out, W_STORE_CLOSED_DATE_ID, |o| {
            o.date(closed_date as i32);
        });
        nulls.put(&mut out, W_STORE_NAME, |o| {
            o.text(&name);
        });
        nulls.put(&mut out, W_STORE_EMPLOYEES, |o| {
            o.int(employees);
        });
        nulls.put(&mut out, W_STORE_FLOOR_SPACE, |o| {
            o.int(floor_space);
        });
        nulls.put(&mut out, W_STORE_HOURS, |o| {
            o.text(hours);
        });
        nulls.put(&mut out, W_STORE_MANAGER, |o| {
            o.text(&manager);
        });
        nulls.put(&mut out, W_STORE_MARKET_ID, |o| {
            o.int(market_id);
        });
        nulls.put(&mut out, W_STORE_GEOGRAPHY_CLASS, |o| {
            o.text("Unknown");
        });
        nulls.put(&mut out, W_STORE_MARKET_MANAGER, |o| {
            o.text(&market_manager);
        });
        nulls.put(&mut out, W_STORE_TAX_PERCENTAGE, |o| {
            o.decimal(&tax);
        });
        out.end()?;
        ctx.rng.row_stop(TABLE_S_STORE);
    }
    out.finish()
}

#[derive(Default)]
struct WebPageOld {
    creation_date: i64,
    access_date: i64,
    autogen: i32,
    link_count: i32,
    image_count: i32,
    max_ad_count: i32,
    char_count: i32,
}

fn web_site_duration(scale: f64) -> i64 {
    (WEB_MAX_DATE - i64::from(DATA_START_DATE)) * concurrent_web_sites(scale)
}

fn write_web_page(ctx: &mut Ctx, dir: &Path) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_web_page", ctx.plan.update)?;
    let rows = s_rows("s_web_page", ctx.scale);
    let mut order: Option<Order> = None;
    let mut old = WebPageOld::default();
    let duration = web_site_duration(ctx.scale);
    for index in 1..=rows {
        let order = order.get_or_insert_with(|| Order::new(ctx, "web_page", S_WPAG_PERMUTE));
        let sk = order.surrogate_key(ctx, index, S_WPAG_ID);
        let nulls = Nulls::draw(ctx, WP_NULLS, TABLE_WEB_PAGE, 250, DEFAULT_NOT_NULL);
        let key = revision(WEB_PAGE_TABLE, sk);
        let mut flags = Flags {
            bits: ctx.rng.next(WP_SCD) as i32,
            first: key.first,
        };
        ctx.rng.int(WP_CREATION_DATE_SK, 1998, 2002);
        let site = sk / WEB_PAGES_PER_SITE + 1;
        let earliest = i64::from(DATA_START_DATE) - (site * WEB_DATE_STAGGER) % duration / 2;
        let mut creation_date = i64::from(ctx.rng.int(
            WP_CREATION_DATE_SK,
            earliest as i32,
            DATA_START_DATE,
        ));
        flags.keep(&mut creation_date, &mut old.creation_date);
        let idle = ctx.rng.int(WP_ACCESS_DATE_SK, 0, 100);
        let mut access_date = i64::from(TODAYS_DATE - idle);
        flags.keep(&mut access_date, &mut old.access_date);
        if access_date == 0 {
            access_date = -1;
        }
        let mut autogen = i32::from(ctx.rng.int(WP_AUTOGEN_FLAG, 0, 99) < WP_AUTOGEN_PCT);
        flags.keep(&mut autogen, &mut old.autogen);
        flags.skip(2);
        let page_type = WebPageUseDistribution::pick_random_web_page_use_type(ctx.rng.stream(WP_TYPE));
        flags.skip(1);
        let mut link_count = ctx.rng.int(WP_LINK_COUNT, 2, 25);
        flags.keep(&mut link_count, &mut old.link_count);
        let mut image_count = ctx.rng.int(WP_IMAGE_COUNT, 1, 7);
        flags.keep(&mut image_count, &mut old.image_count);
        let mut max_ad_count = ctx.rng.int(WP_MAX_AD_COUNT, 0, 4);
        flags.keep(&mut max_ad_count, &mut old.max_ad_count);
        let mut char_count = ctx.rng.int(
            WP_CHAR_COUNT,
            link_count * 125 + image_count * 50,
            link_count * 300 + image_count * 150,
        );
        flags.keep(&mut char_count, &mut old.char_count);

        nulls.put(&mut out, WP_PAGE_ID, |o| {
            o.id(key.business_key);
        });
        nulls.put(&mut out, WP_CREATION_DATE_SK, |o| {
            o.date(creation_date as i32);
        });
        nulls.put(&mut out, WP_ACCESS_DATE_SK, |o| {
            o.date(access_date as i32);
        });
        nulls.put(&mut out, WP_AUTOGEN_FLAG, |o| {
            o.flag(autogen != 0);
        });
        nulls.put(&mut out, WP_URL, |o| {
            o.text("http://www.foo.com");
        });
        nulls.put(&mut out, WP_TYPE, |o| {
            o.text(&page_type);
        });
        nulls.put(&mut out, WP_CHAR_COUNT, |o| {
            o.int(char_count);
        });
        nulls.put(&mut out, WP_LINK_COUNT, |o| {
            o.int(link_count);
        });
        nulls.put(&mut out, WP_IMAGE_COUNT, |o| {
            o.int(image_count);
        });
        nulls.put(&mut out, WP_MAX_AD_COUNT, |o| {
            o.int(max_ad_count);
        });
        out.end()?;
        ctx.rng.row_stop(TABLE_S_WEB_PAGE);
    }
    out.finish()
}

#[derive(Default)]
struct WebSiteOld {
    manager: String,
    tax: Decimal,
}

fn write_web_site(ctx: &mut Ctx, dir: &Path) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_web_site", ctx.plan.update)?;
    let rows = s_rows("s_web_site", ctx.scale);
    let mut old = WebSiteOld::default();
    let duration = web_site_duration(ctx.scale);
    let mut open_date = 0;
    let mut close_date = 0;
    let mut name = String::new();
    for index in 1..=rows {
        let sk = index;
        let nulls = Nulls::draw(ctx, WEB_NULLS, TABLE_WEB_SITE, 100, DEFAULT_NOT_NULL);
        let key = revision(WEB_SITE_TABLE, sk);
        if key.first {
            let opened = i64::from(DATA_START_DATE) - (sk * WEB_DATE_STAGGER) % duration / 2;
            let mut closed = opened - duration;
            if sk % 2 == 0 && (sk / 2) % 2 == 0 {
                closed += duration / 2;
            }
            open_date = opened as i32;
            close_date = if closed > key.end_date { -1 } else { closed as i32 };
            name = format!("site_{}", sk / 6);
        }
        let mut flags = Flags {
            bits: ctx.rng.next(WEB_SCD) as i32,
            first: key.first,
        };
        let mut manager = person(ctx, WEB_MANAGER);
        flags.keep(&mut manager, &mut old.manager);
        flags.skip(15);
        let mut tax = tax(ctx, WEB_TAX_PERCENTAGE, 12);
        flags.keep(&mut tax, &mut old.tax);

        nulls.put(&mut out, WEB_SITE_ID, |o| {
            o.id(key.business_key);
        });
        nulls.put(&mut out, WEB_OPEN_DATE, |o| {
            o.date(open_date);
        });
        nulls.put(&mut out, WEB_CLOSE_DATE, |o| {
            o.date(close_date);
        });
        nulls.put(&mut out, WEB_NAME, |o| {
            o.text(&name);
        });
        nulls.put(&mut out, WEB_CLASS, |o| {
            o.text("Unknown");
        });
        nulls.put(&mut out, WEB_MANAGER, |o| {
            o.text(&manager);
        });
        nulls.put(&mut out, WEB_TAX_PERCENTAGE, |o| {
            o.decimal(&tax);
        });
        out.end()?;
        ctx.rng.row_stop(TABLE_S_WEB_SITE);
    }
    out.finish()
}

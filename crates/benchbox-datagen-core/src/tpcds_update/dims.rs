use std::path::Path;
use std::sync::OnceLock;

use tpcdsgen::config::{CompatMode, SessionBuilder, Table as UpstreamTable};
use tpcdsgen::distribution::english_distributions::get_syllables_distribution;
use tpcdsgen::distribution::string_values_distribution::StringValuesDistribution;
use tpcdsgen::distribution::utils::Distribution;
use tpcdsgen::distribution::{
    DemographicsDistributions, FirstNamesWeights, IntValuesDistribution, LocationTypeWeights,
    LocationTypesDistribution, NamesDistributions, SalutationsWeights,
};
use tpcdsgen::permutations::make_permutation;
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::{CatalogPageRowGenerator, PromotionRowGenerator, WarehouseRowGenerator};
use tpcdsgen::types::{Address, Decimal};

use super::calendar::{TODAYS_DATE, dttoj};
use super::columns::*;
use super::ctx::Ctx;
use super::out::Out;
use super::plan::Channel;
use crate::{Error, FileStats};

const CD_MAX_CHILDREN: i64 = 7;
const CD_MAX_EMPLOYED: i64 = 7;
const CD_MAX_COLLEGE: i64 = 7;

struct RowSpec {
    multiplier: u32,
    weights: [i64; 9],
}

fn row_spec(table: &str) -> RowSpec {
    let (multiplier, weights) = match table {
        "s_customer_address" => (0, [25, 40, 100, 250, 600, 1500, 3250, 4000, 5000]),
        "s_call_center" => (0, [1; 9]),
        "s_catalog_page" => (0, [150, 210, 240, 240, 240, 240, 240, 240, 240]),
        "s_customer" => (3, [5, 10, 20, 50, 120, 300, 650, 800, 1000]),
        "s_item" => (2, [5, 7, 10, 13, 15, 18, 20, 23, 25]),
        "s_promotion" => (0, [5, 7, 10, 13, 15, 18, 20, 23, 25]),
        "s_store" => (0, [1, 1, 2, 4, 5, 6, 7, 8, 9]),
        "s_warehouse" => (0, [1; 9]),
        "s_web_page" => (0, [6, 1, 200, 260, 300, 360, 400, 460, 500]),
        "s_web_site" => (0, [1; 9]),
        "s_zip_to_gmt" => (3, [100; 9]),
        other => unreachable!("no source table {other:?}"),
    };
    RowSpec { multiplier, weights }
}

pub(super) fn source_table_rows(table: &str, scale: f64) -> i64 {
    const VOLUMES: [i64; 9] = [1, 10, 100, 300, 1000, 3000, 10_000, 30_000, 100_000];
    let spec = row_spec(table);
    let factor = 10_i64.pow(spec.multiplier);
    if scale < 1.0 {
        let base = spec.weights[0] * factor;
        return (((base as f64) * scale) as i64).max(1);
    }
    let mut remaining = scale as i64;
    let mut rows = 0;
    for slot in (0..9).rev() {
        while remaining >= VOLUMES[slot] {
            rows += spec.weights[slot];
            remaining -= VOLUMES[slot];
        }
    }
    rows * factor
}

fn first_name_weights(gender: &str) -> (FirstNamesWeights, SalutationsWeights) {
    match gender {
        "M" => (FirstNamesWeights::MaleFrequency, SalutationsWeights::Male),
        "F" => (FirstNamesWeights::FemaleFrequency, SalutationsWeights::Female),
        _ => (
            FirstNamesWeights::GeneralFrequency,
            SalutationsWeights::GenderNeutral,
        ),
    }
}

fn buy_potential() -> &'static StringValuesDistribution {
    static DISTRIBUTION: OnceLock<StringValuesDistribution> = OnceLock::new();
    DISTRIBUTION.get_or_init(|| {
        StringValuesDistribution::build_string_values_distribution("buy_potential.dst", 1, 1)
            .expect("buy_potential.dst is embedded")
    })
}

fn vehicle_count() -> &'static IntValuesDistribution {
    static DISTRIBUTION: OnceLock<IntValuesDistribution> = OnceLock::new();
    DISTRIBUTION.get_or_init(|| {
        IntValuesDistribution::build_int_values_distribution("vehicle_count.dst", 1, 1)
            .expect("vehicle_count.dst is embedded")
    })
}

fn address_scaling(scale: f64) -> tpcdsgen::config::Scaling {
    SessionBuilder::new()
        .with_scale_factor(scale.max(1.0))
        .with_compat_mode(CompatMode::C)
        .build()
        .expect("scale factor is positive")
        .get_scaling()
        .clone()
}

fn make_address(ctx: &mut Ctx, scaling: &tpcdsgen::config::Scaling) -> Address {
    Address::make_address_for_column(
        UpstreamTable::CustomerAddress,
        ctx.rng.stream(CA_ADDRESS),
        scaling,
    )
}

fn upstream_session(scale: f64) -> tpcdsgen::config::Session {
    SessionBuilder::new()
        .with_scale_factor(scale.max(1.0))
        .with_compat_mode(CompatMode::C)
        .build()
        .expect("scale factor is positive")
}

pub(super) fn write_dimensions(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let update = ctx.plan.update;
    let mut stats = Vec::new();
    let scaling = address_scaling(ctx.scale);
    stats.push(write_customer_address(ctx, dir, update, &scaling)?);
    stats.push(write_catalog_page(ctx, dir, update)?);
    stats.push(write_customer(ctx, dir, update, &scaling)?);
    stats.push(write_promotion(ctx, dir, update)?);
    stats.push(write_warehouse(ctx, dir, update)?);
    Ok(stats)
}

fn write_customer_address(
    ctx: &mut Ctx,
    dir: &Path,
    update: i32,
    scaling: &tpcdsgen::config::Scaling,
) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_customer_address", update)?;
    for index in 1..=source_table_rows("s_customer_address", ctx.scale) {
        ctx.rng.next(CA_NULLS);
        let address = make_address(ctx, scaling);
        ctx.rng.next(CA_LOCATION_TYPE);
        out.id(index)
            .int(address.get_street_number())
            .text(address.get_street_name1())
            .text(address.get_street_name2())
            .text(address.get_street_type())
            .text(address.get_suite_number())
            .text(address.get_city())
            .text(address.get_county().unwrap_or(""))
            .text(address.get_state())
            .text(&format!("{:05}", address.get_zip()))
            .text(address.get_country())
            .end()?;
    }
    out.finish()
}

fn write_catalog_page(ctx: &mut Ctx, dir: &Path, update: i32) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_catalog_page", update)?;
    let rows = source_table_rows("s_catalog_page", ctx.scale);
    let generator = CatalogPageRowGenerator::new(upstream_session(ctx.scale), rows as u64);
    for row in generator {
        out.int(row.get_cp_catalog_number())
            .int(row.get_cp_catalog_page_number())
            .text(row.get_cp_department())
            .text(row.get_cp_catalog_page_id())
            .date(row.get_cp_start_date_id() as i32)
            .date(row.get_cp_end_date_id() as i32)
            .text(row.get_cp_description())
            .text(row.get_cp_type())
            .end()?;
    }
    out.finish()
}

fn dependents(value: &mut i64, modulus: i64) -> i32 {
    let result = (*value % modulus) as i32;
    *value /= modulus;
    result
}

fn write_customer(
    ctx: &mut Ctx,
    dir: &Path,
    update: i32,
    scaling: &tpcdsgen::config::Scaling,
) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_customer", update)?;
    let customers = ctx.ids("customer") as usize;
    let permutation = make_permutation(customers, ctx.rng.stream(S_CUST_ID));
    let birth_min = dttoj(1924, 1, 1);
    let birth_range = dttoj(1992, 12, 31) - birth_min;
    let one_year_ago = TODAYS_DATE - 365;
    let demographics = ctx.rows("customer_demographics");
    let min_income = Decimal::new(0, 2);
    let max_income = Decimal::new(20_000_000, 2);
    for index in 1..=source_table_rows("s_customer", ctx.scale) {
        let id = i64::from(permutation[(index - 1) as usize]) + 1;
        let mut demographic = ctx.rng.key(S_CUST_GENDER, 1, demographics) - 1;
        let size = DemographicsDistributions::get_gender_size() as i64;
        let gender = DemographicsDistributions::get_gender_for_index_mod_size(demographic);
        demographic /= size;
        let (first_weights, salutation_weights) = first_name_weights(gender);
        let first = NamesDistributions::pick_random_first_name(
            first_weights,
            ctx.rng.stream(S_CUST_FIRST_NAME),
        );
        let salutation = NamesDistributions::pick_random_salutation(
            salutation_weights,
            ctx.rng.stream(S_CUST_SALUTATION),
        );
        let marital = DemographicsDistributions::get_marital_status_for_index_mod_size(demographic);
        demographic /= DemographicsDistributions::get_marital_status_size() as i64;
        let education = DemographicsDistributions::get_education_for_index_mod_size(demographic);
        demographic /= DemographicsDistributions::get_education_size() as i64;
        let purchase_estimate =
            DemographicsDistributions::get_purchase_band_for_index_mod_size(demographic);
        demographic /= DemographicsDistributions::get_purchase_band_size() as i64;
        let credit = DemographicsDistributions::get_credit_rating_for_index_mod_size(demographic);
        demographic /= DemographicsDistributions::get_credit_rating_size() as i64;
        let dependent_count = dependents(&mut demographic, CD_MAX_CHILDREN);
        let employed = dependents(&mut demographic, CD_MAX_EMPLOYED);
        let college = (demographic % CD_MAX_COLLEGE) as i32;
        let last = NamesDistributions::pick_random_last_name(ctx.rng.stream(S_CUST_LAST_NAME));
        let preferred = ctx.rng.int(S_CUST_PREFERRED_FLAG, 1, 100) > 50;
        let birth = birth_min + ctx.rng.int(S_CUST_BIRTH_DATE, 0, birth_range);
        let first_purchase = one_year_ago + ctx.rng.int(S_CUST_FIRST_PURCHASE_DATE, 0, 0);
        let first_shipto = first_purchase + ctx.rng.int(S_CUST_FIRST_SHIPTO_DATE, 1, 30);
        let login = RandomValueGenerator::generate_random_charset(
            RandomValueGenerator::ALPHA_NUMERIC,
            5,
            13,
            ctx.rng.stream(S_CUST_LOGIN),
        );
        let email =
            RandomValueGenerator::generate_random_email(first, last, ctx.rng.stream(S_CUST_EMAIL));
        let last_login = one_year_ago + ctx.rng.int(S_CUST_LAST_LOGIN, 0, TODAYS_DATE - one_year_ago);
        let review = one_year_ago + ctx.rng.int(S_CUST_LAST_REVIEW, 0, TODAYS_DATE - one_year_ago);
        let primary = ip_address(ctx, S_CUST_PRIMARY_MACHINE);
        let secondary = ip_address(ctx, S_CUST_SECONDARY_MACHINE);
        let location = LocationTypesDistribution::pick_random_location_type(
            LocationTypeWeights::Uniform,
            ctx.rng.stream(S_CUST_LOCATION_TYPE),
        );
        let vehicles = vehicle_count().pick_random_value(0, 0, ctx.rng.stream(S_CUST_VEHICLE_CNT));
        let income = RandomValueGenerator::generate_uniform_random_decimal(
            min_income,
            max_income,
            ctx.rng.stream(S_CUST_INCOME),
        );
        let potential = buy_potential()
            .pick_random_value(0, 0, ctx.rng.stream(S_CUST_PURCHASE_ESTIMATE))
            .to_string();
        ctx.rng.next(CA_NULLS);
        let address = make_address(ctx, scaling);
        ctx.rng.next(CA_LOCATION_TYPE);
        out.id(id)
            .text(salutation)
            .text(last)
            .text(first)
            .flag(preferred)
            .date(birth)
            .text("<UNKNOWN>")
            .text(&login)
            .text(&email)
            .date(last_login)
            .date(first_shipto)
            .date(first_purchase)
            .date(review)
            .text(&primary)
            .text(&secondary)
            .int(address.get_street_number())
            .text(address.get_suite_number())
            .text(address.get_street_name1())
            .text(address.get_street_name2())
            .text(address.get_street_type())
            .text(address.get_city())
            .text(&format!("{:05}", address.get_zip()))
            .text(address.get_county().unwrap_or(""))
            .text(address.get_state())
            .text(address.get_country())
            .text(&location)
            .text(gender)
            .text(marital)
            .text(education)
            .text(credit)
            .int(purchase_estimate)
            .text(&potential)
            .int(dependent_count)
            .int(employed)
            .int(college)
            .int(vehicles)
            .decimal(&income)
            .end()?;
        ctx.rng.row_stop(TABLE_S_CUSTOMER);
    }
    out.finish()
}

fn ip_address(ctx: &mut Ctx, column: Col) -> String {
    let quads: Vec<i32> = (0..4).map(|_| ctx.rng.int(column, 1, 255)).collect();
    format!("{:03}.{:03}.{:03}.{:03}", quads[0], quads[1], quads[2], quads[3])
}

pub(super) fn make_word(source: i64, max_chars: usize) -> String {
    let syllables = get_syllables_distribution();
    let size = syllables.get_size() as i64;
    let mut word = String::new();
    let mut remaining = source;
    while remaining > 0 {
        let syllable = syllables.get_value_at_index(0, (remaining % size) as usize);
        remaining /= size;
        if word.len() + syllable.len() <= max_chars {
            word.push_str(syllable);
        } else {
            break;
        }
    }
    word
}

fn write_promotion(ctx: &mut Ctx, dir: &Path, update: i32) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_promotion", update)?;
    let rows = source_table_rows("s_promotion", ctx.scale);
    let permutation = make_permutation(ctx.ids("promotion") as usize, ctx.rng.stream(S_PROMOTION_ID));
    let generator = PromotionRowGenerator::new(upstream_session(ctx.scale), rows as u64);
    for (index, row) in generator.enumerate() {
        let id = i64::from(permutation[index]) + 1;
        out.id(id)
            .text(&make_word(id, 5))
            .date(row.get_p_start_date_id() as i32)
            .date(row.get_p_end_date_id() as i32)
            .decimal(&row.get_p_cost())
            .int(row.get_p_response_target())
            .flag(row.get_p_channel_dmail())
            .flag(row.get_p_channel_email())
            .flag(row.get_p_channel_catalog())
            .flag(row.get_p_channel_tv())
            .flag(row.get_p_channel_radio())
            .flag(row.get_p_channel_press())
            .flag(row.get_p_channel_event())
            .flag(row.get_p_channel_demo())
            .text(row.get_p_channel_details())
            .text(row.get_p_purpose())
            .flag(row.get_p_discount_active())
            .end()?;
    }
    out.finish()
}

fn write_warehouse(ctx: &mut Ctx, dir: &Path, update: i32) -> Result<FileStats, Error> {
    let mut out = Out::create(dir, "s_warehouse", update)?;
    let rows = source_table_rows("s_warehouse", ctx.scale);
    let permutation = make_permutation(ctx.ids("warehouse") as usize, ctx.rng.stream(S_WRHS_ID));
    let generator = WarehouseRowGenerator::new(upstream_session(ctx.scale), rows as u64);
    for (index, row) in generator.enumerate() {
        let id = i64::from(permutation[index]) + 1;
        out.id(id)
            .text(row.get_w_warehouse_name())
            .int(row.get_w_warehouse_sq_ft())
            .end()?;
    }
    out.finish()
}

fn first_sk(id: i64) -> i64 {
    match id % 3 {
        1 => (id / 3) * 6 + 1,
        2 => (id / 3) * 6 + 2,
        _ => (id / 3 - 1) * 6 + 4,
    }
}

pub(super) fn write_inventory(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let mut out = Out::create(dir, "s_inventory", ctx.plan.update)?;
    let items = ctx.ids("item");
    let warehouses = ctx.rows("warehouse");
    for index in 1..=ctx.plan.rows(Channel::Inventory) {
        let mut n = index - 1;
        let item = n % items + 1;
        n /= items;
        let warehouse = n % warehouses + 1;
        let date = ctx.plan.update_date(Channel::Inventory, index);
        let quantity = ctx.rng.int(S_INVN_QUANTITY, 0, 250);
        out.id(warehouse)
            .id(first_sk(item))
            .date(date)
            .int(quantity)
            .end()?;
        ctx.rng.row_stop(TABLE_S_INVENTORY);
    }
    Ok(vec![out.finish()?])
}

const GMT_OFFSETS: [(i64, i32); 63] = [
    (599, -99),
    (999, -5),
    (2799, -5),
    (2999, -5),
    (3899, -5),
    (4999, -5),
    (5999, -5),
    (6999, -5),
    (8999, -5),
    (9999, -5),
    (14999, -5),
    (19699, -5),
    (19999, -5),
    (20599, -5),
    (21999, -5),
    (24699, -5),
    (26999, -5),
    (28999, -5),
    (29999, -5),
    (31999, -5),
    (33999, -5),
    (34999, -6),
    (34095, -5),
    (36999, -6),
    (38599, -5),
    (39999, -6),
    (41799, -5),
    (42799, -6),
    (45999, -5),
    (47999, -5),
    (49999, -5),
    (52999, -6),
    (54999, -6),
    (56999, -6),
    (57499, -6),
    (57999, -7),
    (58499, -6),
    (58499, -7),
    (59999, -7),
    (62999, -6),
    (65999, -6),
    (67999, -6),
    (68999, -6),
    (69999, -7),
    (71599, -6),
    (72999, -6),
    (74999, -6),
    (78999, -6),
    (79999, -7),
    (81999, -7),
    (83199, -7),
    (83699, -7),
    (83999, -8),
    (84999, -7),
    (86999, -7),
    (88999, -7),
    (89999, -8),
    (95999, -8),
    (96699, -8),
    (96899, -10),
    (97999, -8),
    (99499, -8),
    (99999, -9),
];

pub(super) fn write_zip_to_gmt(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let mut out = Out::create(dir, "s_zip_to_gmt", ctx.plan.update)?;
    for index in 1..=super::dims::zip_rows(ctx.scale) {
        let zip = index - 1;
        let offset = GMT_OFFSETS
            .iter()
            .find(|(max, _)| zip <= *max)
            .map_or(-99, |(_, offset)| *offset);
        if offset == -99 {
            continue;
        }
        out.text(&format!("{zip:05}")).int(offset).end()?;
    }
    Ok(vec![out.finish()?])
}

pub(super) fn zip_rows(scale: f64) -> i64 {
    if scale < 1.0 {
        return ((100_000.0 * scale) as i64).max(1);
    }
    100_000
}

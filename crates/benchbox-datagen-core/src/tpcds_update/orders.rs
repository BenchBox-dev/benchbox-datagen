use std::path::Path;

use tpcdsgen::permutations::make_permutation;
use tpcdsgen::types::pricing::{Pricing, generate_pricing_for_sales_table, get_web_sales_pricing_limits};
use tpcdsgen::types::Decimal;

use super::calendar::TODAYS_DATE;
use super::columns::*;
use super::ctx::Ctx;
use super::out::Out;
use super::plan::Channel;
use crate::{Error, FileStats};

const GIFT_PCT: i32 = 7;
const RETURN_PCT: i32 = 10;
const CATALOG_RETURN_PCT: i32 = 1030;

fn first_sk(id: i64) -> i64 {
    match id % 3 {
        1 => (id / 3) * 6 + 1,
        2 => (id / 3) * 6 + 2,
        _ => (id / 3 - 1) * 6 + 4,
    }
}

struct Items {
    permutation: Vec<i32>,
    count: i32,
    index: i32,
}

impl Items {
    fn new(ctx: &mut Ctx, column: Col) -> Self {
        let count = ctx.ids("item") as i32;
        let permutation = make_permutation(count as usize, ctx.rng.stream(column));
        Self {
            permutation,
            count,
            index: 0,
        }
    }

    fn next(&mut self) -> i64 {
        self.index += 1;
        if self.index > self.count {
            self.index = 1;
        }
        first_sk(i64::from(self.permutation[(self.index - 1) as usize]) + 1)
    }
}

fn pricing(ctx: &mut Ctx, column: Col) -> Pricing {
    generate_pricing_for_sales_table(&get_web_sales_pricing_limits(), ctx.rng.stream(column))
}

fn zero() -> Decimal {
    Decimal::new(0, 0)
}

struct Order {
    id: i64,
    date: i32,
    time: i32,
    bill: i64,
    ship: i64,
    mode: i64,
    comment: String,
}

struct Line {
    item: i64,
    promotion: i64,
    warehouse: i64,
    ship_date: i32,
    price: Pricing,
}

pub(super) fn write_purchases(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let update = ctx.plan.update;
    let mut purchases = Out::create(dir, "s_purchase", update)?;
    let mut lines = Out::create(dir, "s_purchase_lineitem", update)?;
    let mut returns = Out::create(dir, "s_store_returns", update)?;
    let rows = ctx.plan.rows(Channel::Store);
    let item_count = ctx.ids("item") as i32;
    let mut items: Option<Items> = None;
    for index in 1..=rows {
        let store = ctx.join_scd(S_PURCHASE_STORE_ID, "store");
        let customer = ctx.join_uniform(S_PURCHASE_CUSTOMER_ID, "customer");
        let date = ctx.plan.update_date(Channel::Store, index);
        let register = ctx.rng.int(S_PURCHASE_REGISTER, 1, 17);
        let clerk = ctx.rng.int(S_PURCHASE_CLERK, 101, 300);
        let comment = ctx.gen_text(S_PURCHASE_COMMENT, 60, 100);
        let mut cursor = ctx.rng.int(S_PLINE_ITEM_ID, 1, item_count);
        for number in 1..=12 {
            let items = items.get_or_insert_with(|| Items::new(ctx, S_PLINE_PERMUTE));
            items.index = cursor;
            let item = items.next();
            cursor = items.index;
            let promotion = ctx.join_uniform(S_PLINE_PROMOTION_ID, "promotion");
            ctx.rng.int(S_PLINE_QUANTITY, 1, 100);
            let price = pricing(ctx, S_PLINE_PRICING);
            let line_comment = ctx.gen_text(S_PLINE_COMMENT, 1, 100);
            lines
                .key(index)
                .key(number)
                .id(item)
                .id(promotion)
                .int(price.get_quantity())
                .decimal(&price.get_sales_price())
                .decimal(&price.get_coupon_amount())
                .text(&line_comment)
                .end()?;
            if ctx.rng.int(S_PLINE_IS_RETURNED, 0, 99) < RETURN_PCT {
                let lag = ctx.rng.int(S_SRET_RETURN_DATE, 0, 60);
                let time = ctx.join_time(S_SRET_RETURN_TIME);
                let reason = ctx.join_uniform(S_SRET_REASON_ID, "reason");
                let quantity = ctx.rng.int(S_SRET_PRICING, 1, price.get_quantity());
                returns
                    .id(store)
                    .key(index)
                    .key(number)
                    .id(item)
                    .id(customer)
                    .date(date + lag)
                    .time(time)
                    .key(index)
                    .int(quantity)
                    .decimal(&price.get_sales_price())
                    .decimal(&price.get_ext_tax())
                    .decimal(&zero())
                    .decimal(&price.get_ext_ship_cost())
                    .decimal(&zero())
                    .decimal(&zero())
                    .decimal(&zero())
                    .id(reason)
                    .end()?;
            }
        }
        purchases
            .key(index)
            .id(store)
            .id(customer)
            .date(date)
            .int(0)
            .int(register)
            .int(clerk)
            .text(&comment)
            .end()?;
        ctx.rng.row_stop(TABLE_S_PURCHASE);
        ctx.rng.row_stop(TABLE_S_PURCHASE_LINEITEM);
        ctx.rng.row_stop(TABLE_S_STORE_RETURNS);
    }
    Ok(vec![purchases.finish()?, lines.finish()?, returns.finish()?])
}

fn master_customers(ctx: &mut Ctx, bill: Col, ship: Col) -> (i64, i64) {
    let bill_customer = ctx.join_uniform(bill, "customer");
    let gift = ctx.rng.int(ship, 0, 99);
    let ship_customer = if gift > GIFT_PCT {
        ctx.join_uniform(ship, "customer")
    } else {
        bill_customer
    };
    (bill_customer, ship_customer)
}

pub(super) fn write_catalog_orders(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let update = ctx.plan.update;
    let mut orders = Out::create(dir, "s_catalog_order", update)?;
    let mut lines = Out::create(dir, "s_catalog_order_lineitem", update)?;
    let mut returns = Out::create(dir, "s_catalog_returns", update)?;
    let rows = ctx.plan.rows(Channel::Catalog);
    let item_count = ctx.ids("item") as i32;
    let mut items: Option<Items> = None;
    for index in 1..=rows {
        let date = ctx.plan.update_date(Channel::Catalog, index);
        let time = ctx.rng.int(S_CORD_ORDER_TIME, 0, 24 * 3600 - 1);
        let (bill, ship) = master_customers(ctx, S_CORD_BILL_CUSTOMER_ID, S_CORD_SHIP_CUSTOMER_ID);
        let mode = ctx.join_uniform(S_CORD_SHIP_MODE_ID, "ship_mode");
        let comment = ctx.gen_text(S_CORD_COMMENT, 60, 100);
        let order = Order {
            id: index,
            date,
            time,
            bill,
            ship,
            mode,
            comment,
        };
        let mut cursor = ctx.rng.int(S_CLIN_ITEM_ID, 1, item_count);
        for number in 1..=9 {
            let items = items.get_or_insert_with(|| Items::new(ctx, S_CLIN_PERMUTE));
            items.index = cursor;
            let item = items.next();
            cursor = items.index;
            let promotion = ctx.join_uniform(S_CLIN_PROMOTION_ID, "promotion");
            let warehouse = ctx.join_uniform(S_CLIN_WAREHOUSE_ID, "warehouse");
            let lag = ctx.rng.int(S_CLIN_SHIP_DATE, 0, 60);
            let mut ship_date = order.date + lag;
            if ship_date > TODAYS_DATE {
                ship_date = -1;
            }
            let page = ctx.join_catalog_page(S_CLIN_CATALOG_PAGE_ID, order.date);
            let catalog = ctx.catalog_number(page);
            let price = pricing(ctx, S_CLIN_PRICING);
            let line = Line {
                item,
                promotion,
                warehouse,
                ship_date,
                price,
            };
            lines
                .key(order.id)
                .key(number)
                .id(line.item)
                .id(line.promotion)
                .int(line.price.get_quantity())
                .decimal(&line.price.get_sales_price())
                .decimal(&line.price.get_coupon_amount())
                .id(line.warehouse)
                .date(line.ship_date)
                .key(catalog)
                .key(page)
                .decimal(&line.price.get_ship_cost())
                .end()?;
            if ctx.rng.int(S_CLIN_IS_RETURNED, 0, 9999) < CATALOG_RETURN_PCT
                && line.ship_date != -1
            {
                let lag = ctx.rng.int(S_CRET_DATE, 0, 60);
                let time = ctx.join_time(S_CRET_TIME);
                let center = ctx.join_scd(S_CRET_CALL_CENTER_ID, "call_center");
                let reason = ctx.join_uniform(S_CRET_REASON_ID, "reason");
                let quantity = ctx.rng.int(S_CRET_PRICING, 1, line.price.get_quantity());
                let mode = ctx.join_uniform(S_CRET_SHIPMODE_ID, "ship_mode");
                let warehouse = ctx.join_uniform(S_CRET_WAREHOUSE_ID, "warehouse");
                returns
                    .id(center)
                    .key(order.id)
                    .key(number)
                    .id(line.item)
                    .id(order.ship)
                    .id(order.ship)
                    .date(line.ship_date + lag)
                    .time(time)
                    .int(quantity)
                    .decimal(&line.price.get_net_paid())
                    .decimal(&line.price.get_ext_tax())
                    .decimal(&zero())
                    .decimal(&line.price.get_ext_ship_cost())
                    .decimal(&zero())
                    .decimal(&zero())
                    .decimal(&zero())
                    .id(reason)
                    .id(mode)
                    .id(warehouse)
                    .id(page)
                    .end()?;
            }
        }
        orders
            .key(order.id)
            .id(order.bill)
            .id(order.ship)
            .date(order.date)
            .int(order.time)
            .id(order.mode)
            .id(0)
            .text(&order.comment)
            .end()?;
        ctx.rng.row_stop(TABLE_S_CATALOG_ORDER);
        ctx.rng.row_stop(TABLE_S_CATALOG_ORDER_LINEITEM);
        ctx.rng.row_stop(TABLE_S_CATALOG_RETURNS);
    }
    Ok(vec![orders.finish()?, lines.finish()?, returns.finish()?])
}

pub(super) fn write_web_orders(ctx: &mut Ctx, dir: &Path) -> Result<Vec<FileStats>, Error> {
    let update = ctx.plan.update;
    let mut orders = Out::create(dir, "s_web_order", update)?;
    let mut lines = Out::create(dir, "s_web_order_lineitem", update)?;
    let mut returns = Out::create(dir, "s_web_returns", update)?;
    let rows = ctx.plan.rows(Channel::Web);
    let item_count = ctx.ids("item") as i32;
    let mut items: Option<Items> = None;
    for index in 1..=rows {
        let date = ctx.plan.update_date(Channel::Web, index);
        let time = ctx.rng.int(S_WORD_ORDER_TIME, 0, 24 * 3600 - 1);
        let (bill, ship) = master_customers(ctx, S_WORD_BILL_CUSTOMER_ID, S_WORD_SHIP_CUSTOMER_ID);
        let mode = ctx.join_uniform(S_WORD_SHIP_MODE_ID, "ship_mode");
        let site = ctx.join_scd(S_WORD_WEB_SITE_ID, "web_site");
        let comment = ctx.gen_text(S_WORD_COMMENT, 60, 100);
        let order = Order {
            id: index,
            date,
            time,
            bill,
            ship,
            mode,
            comment,
        };
        let mut cursor = ctx.rng.int(S_WLIN_ITEM_ID, 1, item_count);
        for number in 1..=12 {
            let items = items.get_or_insert_with(|| Items::new(ctx, S_WLIN_PERMUTE));
            items.index = cursor;
            let item = items.next();
            cursor = items.index;
            let promotion = ctx.join_uniform(S_WLIN_PROMOTION_ID, "promotion");
            let warehouse = ctx.join_uniform(S_WLIN_WAREHOUSE_ID, "warehouse");
            let lag = ctx.rng.int(S_WLIN_SHIP_DATE, 0, 60);
            let mut ship_date = order.date + lag;
            if ship_date > TODAYS_DATE {
                ship_date = -1;
            }
            let page = ctx.join_scd(S_WLIN_WEB_PAGE_ID, "web_page");
            let price = pricing(ctx, S_WLIN_PRICING);
            lines
                .key(order.id)
                .key(number)
                .id(item)
                .id(promotion)
                .int(price.get_quantity())
                .decimal(&price.get_sales_price())
                .decimal(&price.get_coupon_amount())
                .id(warehouse)
                .date(ship_date)
                .decimal(&price.get_ship_cost())
                .id(page)
                .end()?;
            if ctx.rng.int(S_WLIN_IS_RETURNED, 0, 99) < RETURN_PCT {
                let lag = ctx.rng.int(S_WRET_RETURN_DATE, 1, 60);
                let time = ctx.join_time(S_WRET_RETURN_TIME);
                ctx.join_uniform(S_WRET_REASON_ID, "reason");
                let quantity = ctx.rng.int(S_WRET_PRICING, 1, price.get_quantity());
                let reason = ctx.join_uniform(S_WRET_REASON_ID, "reason");
                returns
                    .id(site)
                    .key(order.id)
                    .int(number as i32)
                    .id(item)
                    .id(order.ship)
                    .id(order.ship)
                    .date(order.date + lag)
                    .time(time)
                    .int(quantity)
                    .decimal(&price.get_ext_sales_price())
                    .decimal(&price.get_ext_tax())
                    .decimal(&zero())
                    .decimal(&price.get_ext_ship_cost())
                    .decimal(&zero())
                    .decimal(&zero())
                    .decimal(&zero())
                    .id(reason)
                    .end()?;
            }
        }
        orders
            .key(order.id)
            .id(order.bill)
            .id(order.ship)
            .date(order.date)
            .int(order.time)
            .id(order.mode)
            .id(site)
            .text(&order.comment)
            .end()?;
        ctx.rng.row_stop(TABLE_S_WEB_ORDER);
        ctx.rng.row_stop(TABLE_S_WEB_ORDER_LINEITEM);
        ctx.rng.row_stop(TABLE_S_WEB_RETURNS);
    }
    Ok(vec![orders.finish()?, lines.finish()?, returns.finish()?])
}

use tpcdsgen::config::Table;
use tpcdsgen::distribution::hours_distribution::HoursWeights;
use tpcdsgen::generator::{WebReturnsGeneratorColumn, WebSalesGeneratorColumn};
use tpcdsgen::join_key_utils::{
    WS_MAX_SHIP_DELAY, WS_MIN_SHIP_DELAY, generate_returns_date_join_key,
    generate_sales_date_join_key, generate_time_join_key, skip_scd_join_key, skip_uniform_join_key,
};
use tpcdsgen::nulls::{create_null_bit_map, skip_null_bit_map};
use tpcdsgen::permutations::{get_permutation_entry, make_permutation};
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::{AbstractRowGenerator, WebReturnsRow, WebSalesRow};
use tpcdsgen::types::{
    generate_pricing_for_returns_table, generate_pricing_for_sales_table,
    get_web_sales_pricing_limits, skip_pricing_for_sales_table,
};

use super::LineItem;
use super::model::Model;

const GIFT_PERCENT: i32 = 7;
const RETURN_PERCENT: i32 = 10;

#[derive(Default)]
struct Order {
    sold_date_sk: i64,
    sold_time_sk: i64,
    bill_customer_sk: i64,
    bill_cdemo_sk: i64,
    bill_hdemo_sk: i64,
    bill_addr_sk: i64,
    ship_customer_sk: i64,
    ship_cdemo_sk: i64,
    ship_hdemo_sk: i64,
    ship_addr_sk: i64,
    number: i64,
}

struct Sales {
    rows: AbstractRowGenerator<WebSalesGeneratorColumn>,
    model: Model,
    permutation: Option<Vec<i32>>,
    remaining: i32,
    order: Order,
    item_index: i32,
}

impl Sales {
    fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::WebSales));
        rows.set_source_row_range(first, last);
        Self {
            rows,
            model,
            permutation: None,
            remaining: 0,
            order: Order::default(),
            item_index: 0,
        }
    }

    fn new_order(&mut self, row_number: u64) -> Order {
        use WebSalesGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.model;
        let sold_date_sk =
            generate_sales_date_join_key(rows.get_random_number_stream(&WsSoldDateSk));
        let sold_time_sk = generate_time_join_key(
            HoursWeights::CatalogAndWeb,
            rows.get_random_number_stream(&WsSoldTimeSk),
        );
        let bill_customer_sk = model.uniform_key(
            Table::Customer,
            rows.get_random_number_stream(&WsBillCustomerSk),
        );
        let bill_cdemo_sk = model.uniform_key(
            Table::CustomerDemographics,
            rows.get_random_number_stream(&WsBillCdemoSk),
        );
        let bill_hdemo_sk = model.uniform_key(
            Table::HouseholdDemographics,
            rows.get_random_number_stream(&WsBillHdemoSk),
        );
        let bill_addr_sk = model.uniform_key(
            Table::CustomerAddress,
            rows.get_random_number_stream(&WsBillAddrSk),
        );
        let gift = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            rows.get_random_number_stream(&WsShipCustomerSk),
        );
        let (ship_customer_sk, ship_cdemo_sk, ship_hdemo_sk, ship_addr_sk) = if gift > GIFT_PERCENT
        {
            (
                model.uniform_key(
                    Table::Customer,
                    rows.get_random_number_stream(&WsShipCustomerSk),
                ),
                model.uniform_key(
                    Table::CustomerDemographics,
                    rows.get_random_number_stream(&WsShipCdemoSk),
                ),
                model.uniform_key(
                    Table::HouseholdDemographics,
                    rows.get_random_number_stream(&WsShipHdemoSk),
                ),
                model.uniform_key(
                    Table::CustomerAddress,
                    rows.get_random_number_stream(&WsShipAddrSk),
                ),
            )
        } else {
            (bill_customer_sk, bill_cdemo_sk, bill_hdemo_sk, bill_addr_sk)
        };
        Order {
            sold_date_sk,
            sold_time_sk,
            bill_customer_sk,
            bill_cdemo_sk,
            bill_hdemo_sk,
            bill_addr_sk,
            ship_customer_sk,
            ship_cdemo_sk,
            ship_hdemo_sk,
            ship_addr_sk,
            number: i64::try_from(row_number).expect("row number fits in i64"),
        }
    }

    fn next_line_item(&mut self) -> Option<LineItem> {
        use WebSalesGeneratorColumn::*;
        let row_number = self.rows.next_row_number()?;
        let item_count = self.model.ids(Table::Item) as usize;

        if self.permutation.is_none() {
            self.permutation = Some(make_permutation(
                item_count,
                self.rows.get_random_number_stream(&WsPermutation),
            ));
        }
        if self.remaining == 0 {
            self.order = self.new_order(row_number);
            self.item_index = RandomValueGenerator::generate_uniform_random_int(
                1,
                item_count as i32,
                self.rows.get_random_number_stream(&WsItemSk),
            );
            self.remaining = RandomValueGenerator::generate_uniform_random_int(
                8,
                16,
                self.rows.get_random_number_stream(&WsOrderNumber),
            );
        }
        self.item_index += 1;
        if self.item_index > item_count as i32 {
            self.item_index = 1;
        }
        let permutation = self
            .permutation
            .as_ref()
            .expect("permutation was built above");
        let item_key = get_permutation_entry(permutation, self.item_index);
        let item_sk = self.model.match_surrogate_key(
            i64::from(item_key),
            self.order.sold_date_sk,
            Table::Item,
        );
        let draw = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            self.rows.get_random_number_stream(&WrIsReturned),
        );
        Some(LineItem {
            item_sk,
            is_returned: draw < RETURN_PERCENT,
        })
    }

    fn skip_item_draws(&mut self) {
        use WebSalesGeneratorColumn::*;
        let sold_date_sk = self.order.sold_date_sk;
        skip_null_bit_map(self.rows.get_random_number_stream(&WsNulls));
        self.rows
            .get_random_number_stream(&WsShipDateSk)
            .next_random();
        skip_scd_join_key(
            sold_date_sk,
            self.rows.get_random_number_stream(&WsWebPageSk),
        );
        skip_scd_join_key(
            sold_date_sk,
            self.rows.get_random_number_stream(&WsWebSiteSk),
        );
        skip_uniform_join_key(self.rows.get_random_number_stream(&WsShipModeSk));
        skip_uniform_join_key(self.rows.get_random_number_stream(&WsWarehouseSk));
        skip_uniform_join_key(self.rows.get_random_number_stream(&WsPromoSk));
        skip_pricing_for_sales_table(self.rows.get_random_number_stream(&WsPricing));
    }

    fn sales_row(&mut self, item_sk: i64) -> WebSalesRow {
        use WebSalesGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.model;
        let order = &self.order;

        let null_bit_map =
            create_null_bit_map(Table::WebSales, rows.get_random_number_stream(&WsNulls));
        let lag = RandomValueGenerator::generate_uniform_random_int(
            1,
            120,
            rows.get_random_number_stream(&WsShipDateSk),
        );
        let web_page_sk = model.scd_key(
            Table::WebPage,
            rows.get_random_number_stream(&WsWebPageSk),
            order.sold_date_sk,
        );
        let web_site_sk = model.scd_key(
            Table::WebSite,
            rows.get_random_number_stream(&WsWebSiteSk),
            order.sold_date_sk,
        );
        let ship_mode_sk = model.uniform_key(
            Table::ShipMode,
            rows.get_random_number_stream(&WsShipModeSk),
        );
        let warehouse_sk = model.uniform_key(
            Table::Warehouse,
            rows.get_random_number_stream(&WsWarehouseSk),
        );
        let promo_sk =
            model.uniform_key(Table::Promotion, rows.get_random_number_stream(&WsPromoSk));
        let pricing = generate_pricing_for_sales_table(
            &get_web_sales_pricing_limits(),
            rows.get_random_number_stream(&WsPricing),
        );
        WebSalesRow::new(
            null_bit_map,
            order.sold_date_sk,
            order.sold_time_sk,
            order.sold_date_sk + i64::from(lag),
            item_sk,
            order.bill_customer_sk,
            order.bill_cdemo_sk,
            order.bill_hdemo_sk,
            order.bill_addr_sk,
            order.ship_customer_sk,
            order.ship_cdemo_sk,
            order.ship_hdemo_sk,
            order.ship_addr_sk,
            web_page_sk,
            web_site_sk,
            ship_mode_sk,
            warehouse_sk,
            promo_sk,
            order.number,
            pricing,
        )
    }

    fn finish_line_item(&mut self) -> bool {
        self.remaining -= 1;
        let last = self.remaining == 0;
        if last {
            self.rows.finish_row();
        }
        last
    }
}

pub(crate) struct WebSalesRows(Sales);

impl WebSalesRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        Self(Sales::new(model, first, last))
    }
}

impl Iterator for WebSalesRows {
    type Item = WebSalesRow;

    fn next(&mut self) -> Option<WebSalesRow> {
        let item = self.0.next_line_item()?;
        let row = self.0.sales_row(item.item_sk);
        self.0.finish_line_item();
        Some(row)
    }
}

pub(crate) struct WebReturnsRows {
    sales: Sales,
    rows: AbstractRowGenerator<WebReturnsGeneratorColumn>,
}

impl WebReturnsRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::WebSales));
        rows.set_source_row_range(first, last);
        Self {
            sales: Sales::new(model, first, last),
            rows,
        }
    }

    fn return_row(&mut self, sale: &WebSalesRow) -> WebReturnsRow {
        use WebReturnsGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.sales.model;

        let null_bit_map =
            create_null_bit_map(Table::WebReturns, rows.get_random_number_stream(&WrNulls));
        let returned_date_sk = generate_returns_date_join_key(
            rows.get_random_number_stream(&WrReturnedDateSk),
            sale.get_ws_ship_date_sk(),
            WS_MIN_SHIP_DELAY,
            WS_MAX_SHIP_DELAY,
        );
        let returned_time_sk = generate_time_join_key(
            HoursWeights::CatalogAndWeb,
            rows.get_random_number_stream(&WrReturnedTimeSk),
        );
        let mut customer_sk = model.uniform_key(
            Table::Customer,
            rows.get_random_number_stream(&WrRefundedCustomerSk),
        );
        let mut cdemo_sk = model.uniform_key(
            Table::CustomerDemographics,
            rows.get_random_number_stream(&WrRefundedCdemoSk),
        );
        let mut hdemo_sk = model.uniform_key(
            Table::HouseholdDemographics,
            rows.get_random_number_stream(&WrRefundedHdemoSk),
        );
        let mut addr_sk = model.uniform_key(
            Table::CustomerAddress,
            rows.get_random_number_stream(&WrRefundedAddrSk),
        );
        let gift = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            rows.get_random_number_stream(&WrReturningCustomerSk),
        );
        if gift < GIFT_PERCENT {
            customer_sk = sale.get_ws_ship_customer_sk();
            cdemo_sk = sale.get_ws_ship_cdemo_sk();
            hdemo_sk = sale.get_ws_ship_hdemo_sk();
            addr_sk = sale.get_ws_ship_addr_sk();
        }
        let reason_sk =
            model.uniform_key(Table::Reason, rows.get_random_number_stream(&WrReasonSk));
        let sale_pricing = sale.get_ws_pricing();
        let quantity = RandomValueGenerator::generate_uniform_random_int(
            1,
            sale_pricing.get_quantity(),
            rows.get_random_number_stream(&WrPricing),
        );
        let pricing = generate_pricing_for_returns_table(
            rows.get_random_number_stream(&WrPricing),
            quantity,
            sale_pricing,
        );

        WebReturnsRow::new(
            null_bit_map,
            returned_date_sk,
            returned_time_sk,
            sale.get_ws_item_sk(),
            customer_sk,
            cdemo_sk,
            hdemo_sk,
            addr_sk,
            customer_sk,
            cdemo_sk,
            hdemo_sk,
            addr_sk,
            sale.get_ws_web_page_sk(),
            reason_sk,
            sale.get_ws_order_number(),
            pricing,
        )
    }
}

impl Iterator for WebReturnsRows {
    type Item = WebReturnsRow;

    fn next(&mut self) -> Option<WebReturnsRow> {
        loop {
            let item = self.sales.next_line_item()?;
            let row = if item.is_returned {
                let sale = self.sales.sales_row(item.item_sk);
                Some(self.return_row(&sale))
            } else {
                self.sales.skip_item_draws();
                None
            };
            if self.sales.finish_line_item() {
                self.rows.consume_remaining_seeds_for_row();
            }
            if row.is_some() {
                return row;
            }
        }
    }
}

use tpcdsgen::config::Table;
use tpcdsgen::distribution::hours_distribution::HoursWeights;
use tpcdsgen::generator::{CatalogReturnsGeneratorColumn, CatalogSalesGeneratorColumn};
use tpcdsgen::join_key_utils::{
    CS_MAX_SHIP_DELAY, CS_MIN_SHIP_DELAY, generate_returns_date_join_key, generate_time_join_key,
    skip_catalog_page_join_key, skip_uniform_join_key,
};
use tpcdsgen::nulls::{create_null_bit_map, skip_null_bit_map};
use tpcdsgen::permutations::{get_permutation_entry, make_permutation};
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::{AbstractRowGenerator, CatalogReturnsRow, CatalogSalesRow};
use tpcdsgen::types::{
    Date, generate_pricing_for_returns_table, generate_pricing_for_sales_table,
    get_catalog_sales_pricing_limits, skip_pricing_for_sales_table,
};

use super::LineItem;
use super::model::Model;

const RETURN_PERCENT: i32 = 10;
const GIFT_PERCENT: i32 = 10;

#[derive(Default)]
struct Order {
    sold_date_sk: i64,
    sold_time_sk: i64,
    call_center_sk: i64,
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
    rows: AbstractRowGenerator<CatalogSalesGeneratorColumn>,
    model: Model,
    permutation: Option<Vec<i32>>,
    julian_date: i64,
    next_date_index: u64,
    remaining: i32,
    order: Order,
    item_base: i32,
}

impl Sales {
    fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::CatalogSales));
        rows.set_source_row_range(first, last);
        Self {
            rows,
            model,
            permutation: None,
            julian_date: Date::JULIAN_DATA_START_DATE,
            next_date_index: 0,
            remaining: 0,
            order: Order::default(),
            item_base: 0,
        }
    }

    fn new_order(&mut self, row_number: u64) -> Order {
        use CatalogSalesGeneratorColumn::*;
        while row_number > self.next_date_index {
            self.julian_date += 1;
            self.next_date_index += self
                .model
                .rows_for_date(Table::CatalogSales, self.julian_date);
        }
        let rows = &mut self.rows;
        let model = &self.model;
        let sold_date_sk = self.julian_date;
        let sold_time_sk = generate_time_join_key(
            HoursWeights::CatalogAndWeb,
            rows.get_random_number_stream(&CsSoldTimeSk),
        );
        let call_center_sk = if sold_date_sk == -1 {
            -1
        } else {
            model.scd_key(
                Table::CallCenter,
                rows.get_random_number_stream(&CsCallCenterSk),
                sold_date_sk,
            )
        };
        let bill_customer_sk = model.uniform_key(
            Table::Customer,
            rows.get_random_number_stream(&CsBillCustomerSk),
        );
        let bill_cdemo_sk = model.uniform_key(
            Table::CustomerDemographics,
            rows.get_random_number_stream(&CsBillCdemoSk),
        );
        let bill_hdemo_sk = model.uniform_key(
            Table::HouseholdDemographics,
            rows.get_random_number_stream(&CsBillHdemoSk),
        );
        let bill_addr_sk = model.uniform_key(
            Table::CustomerAddress,
            rows.get_random_number_stream(&CsBillAddrSk),
        );
        let gift = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            rows.get_random_number_stream(&CsShipCustomerSk),
        );
        let (ship_customer_sk, ship_cdemo_sk, ship_hdemo_sk, ship_addr_sk) = if gift <= GIFT_PERCENT
        {
            (
                model.uniform_key(
                    Table::Customer,
                    rows.get_random_number_stream(&CsShipCustomerSk),
                ),
                model.uniform_key(
                    Table::CustomerDemographics,
                    rows.get_random_number_stream(&CsShipCdemoSk),
                ),
                model.uniform_key(
                    Table::HouseholdDemographics,
                    rows.get_random_number_stream(&CsShipHdemoSk),
                ),
                model.uniform_key(
                    Table::CustomerAddress,
                    rows.get_random_number_stream(&CsShipAddrSk),
                ),
            )
        } else {
            (bill_customer_sk, bill_cdemo_sk, bill_hdemo_sk, bill_addr_sk)
        };
        Order {
            sold_date_sk,
            sold_time_sk,
            call_center_sk,
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
        use CatalogSalesGeneratorColumn::*;
        let row_number = self.rows.next_row_number()?;
        let item_count = self.model.ids(Table::Item) as usize;

        if self.permutation.is_none() {
            self.permutation = Some(make_permutation(
                item_count,
                self.rows.get_random_number_stream(&CsPermute),
            ));
            self.julian_date = Date::JULIAN_DATA_START_DATE;
            self.next_date_index = self
                .model
                .rows_for_date(Table::CatalogSales, self.julian_date)
                + 1;
        }
        if self.remaining == 0 {
            self.order = self.new_order(row_number);
            self.item_base = RandomValueGenerator::generate_uniform_random_int(
                1,
                item_count as i32,
                self.rows.get_random_number_stream(&CsSoldItemSk),
            );
            self.remaining = RandomValueGenerator::generate_uniform_random_int(
                4,
                14,
                self.rows.get_random_number_stream(&CsOrderNumber),
            );
        }
        self.item_base += 1;
        if self.item_base > item_count as i32 {
            self.item_base = 1;
        }
        let permutation = self
            .permutation
            .as_ref()
            .expect("permutation was built above");
        let item_key = get_permutation_entry(permutation, self.item_base);
        let item_sk = self.model.match_surrogate_key(
            i64::from(item_key),
            self.order.sold_date_sk,
            Table::Item,
        );
        let draw = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            self.rows.get_random_number_stream(&CrIsReturned),
        );
        Some(LineItem {
            item_sk,
            is_returned: draw < RETURN_PERCENT,
        })
    }

    fn skip_item_draws(&mut self) {
        use CatalogSalesGeneratorColumn::*;
        skip_null_bit_map(self.rows.get_random_number_stream(&CsNulls));
        self.rows
            .get_random_number_stream(&CsShipDateSk)
            .next_random();
        if self.order.sold_date_sk != -1 {
            skip_catalog_page_join_key(self.rows.get_random_number_stream(&CsCatalogPageSk));
        }
        skip_uniform_join_key(self.rows.get_random_number_stream(&CsShipModeSk));
        skip_uniform_join_key(self.rows.get_random_number_stream(&CsWarehouseSk));
        skip_uniform_join_key(self.rows.get_random_number_stream(&CsPromoSk));
        skip_pricing_for_sales_table(self.rows.get_random_number_stream(&CsPricing));
    }

    fn sales_row(&mut self, item_sk: i64) -> CatalogSalesRow {
        use CatalogSalesGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.model;
        let order = &self.order;

        let null_bit_map =
            create_null_bit_map(Table::CatalogSales, rows.get_random_number_stream(&CsNulls));
        let lag = RandomValueGenerator::generate_uniform_random_int(
            CS_MIN_SHIP_DELAY,
            CS_MAX_SHIP_DELAY,
            rows.get_random_number_stream(&CsShipDateSk),
        );
        let ship_date_sk = if order.sold_date_sk == -1 {
            -1
        } else {
            order.sold_date_sk + i64::from(lag)
        };
        let catalog_page_sk = if order.sold_date_sk == -1 {
            -1
        } else {
            model.catalog_page_key(
                rows.get_random_number_stream(&CsCatalogPageSk),
                order.sold_date_sk,
            )
        };
        let ship_mode_sk = model.uniform_key(
            Table::ShipMode,
            rows.get_random_number_stream(&CsShipModeSk),
        );
        let warehouse_sk = model.uniform_key(
            Table::Warehouse,
            rows.get_random_number_stream(&CsWarehouseSk),
        );
        let promo_sk =
            model.uniform_key(Table::Promotion, rows.get_random_number_stream(&CsPromoSk));
        let pricing = generate_pricing_for_sales_table(
            &get_catalog_sales_pricing_limits(),
            rows.get_random_number_stream(&CsPricing),
        );
        CatalogSalesRow::new(
            null_bit_map,
            order.sold_date_sk,
            order.sold_time_sk,
            ship_date_sk,
            order.bill_customer_sk,
            order.bill_cdemo_sk,
            order.bill_hdemo_sk,
            order.bill_addr_sk,
            order.ship_customer_sk,
            order.ship_cdemo_sk,
            order.ship_hdemo_sk,
            order.ship_addr_sk,
            order.call_center_sk,
            catalog_page_sk,
            ship_mode_sk,
            warehouse_sk,
            item_sk,
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

pub(crate) struct CatalogSalesRows(Sales);

impl CatalogSalesRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        Self(Sales::new(model, first, last))
    }
}

impl Iterator for CatalogSalesRows {
    type Item = CatalogSalesRow;

    fn next(&mut self) -> Option<CatalogSalesRow> {
        let item = self.0.next_line_item()?;
        let row = self.0.sales_row(item.item_sk);
        self.0.finish_line_item();
        Some(row)
    }
}

pub(crate) struct CatalogReturnsRows {
    sales: Sales,
    rows: AbstractRowGenerator<CatalogReturnsGeneratorColumn>,
}

impl CatalogReturnsRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::CatalogSales));
        rows.set_source_row_range(first, last);
        Self {
            sales: Sales::new(model, first, last),
            rows,
        }
    }

    fn return_row(&mut self, sale: &CatalogSalesRow) -> CatalogReturnsRow {
        use CatalogReturnsGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.sales.model;

        let null_bit_map = create_null_bit_map(
            Table::CatalogReturns,
            rows.get_random_number_stream(&CrNulls),
        );
        let mut returning_customer_sk = model.uniform_key(
            Table::Customer,
            rows.get_random_number_stream(&CrReturningCustomerSk),
        );
        let mut returning_cdemo_sk = model.uniform_key(
            Table::CustomerDemographics,
            rows.get_random_number_stream(&CrReturningCdemoSk),
        );
        let returning_hdemo_sk = model.uniform_key(
            Table::HouseholdDemographics,
            rows.get_random_number_stream(&CrReturningHdemoSk),
        );
        let mut returning_addr_sk = model.uniform_key(
            Table::CustomerAddress,
            rows.get_random_number_stream(&CrReturningAddrSk),
        );
        let gift = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            rows.get_random_number_stream(&CrReturningCustomerSk),
        );
        if gift < GIFT_PERCENT {
            returning_customer_sk = sale.get_cs_ship_customer_sk();
            returning_cdemo_sk = sale.get_cs_ship_cdemo_sk();
            returning_addr_sk = sale.get_cs_ship_addr_sk();
        }

        let sale_pricing = sale.get_cs_pricing();
        let quantity = if sale_pricing.get_quantity() == -1 {
            -1
        } else {
            RandomValueGenerator::generate_uniform_random_int(
                1,
                sale_pricing.get_quantity(),
                rows.get_random_number_stream(&CrPricing),
            )
        };
        let pricing = generate_pricing_for_returns_table(
            rows.get_random_number_stream(&CrPricing),
            quantity,
            sale_pricing,
        );
        let returned_date_sk = generate_returns_date_join_key(
            rows.get_random_number_stream(&CrReturnedDateSk),
            sale.get_cs_ship_date_sk(),
            CS_MIN_SHIP_DELAY,
            CS_MAX_SHIP_DELAY,
        );
        let returned_time_sk = generate_time_join_key(
            HoursWeights::CatalogAndWeb,
            rows.get_random_number_stream(&CrReturnedTimeSk),
        );
        let ship_mode_sk = model.uniform_key(
            Table::ShipMode,
            rows.get_random_number_stream(&CrShipModeSk),
        );
        let warehouse_sk = model.uniform_key(
            Table::Warehouse,
            rows.get_random_number_stream(&CrWarehouseSk),
        );
        let reason_sk =
            model.uniform_key(Table::Reason, rows.get_random_number_stream(&CrReasonSk));

        CatalogReturnsRow::new(
            null_bit_map,
            returned_date_sk,
            returned_time_sk,
            sale.get_cs_sold_item_sk(),
            sale.get_cs_bill_customer_sk(),
            sale.get_cs_bill_cdemo_sk(),
            sale.get_cs_bill_hdemo_sk(),
            sale.get_cs_bill_addr_sk(),
            returning_customer_sk,
            returning_cdemo_sk,
            returning_hdemo_sk,
            returning_addr_sk,
            sale.get_cs_call_center_sk(),
            sale.get_cs_catalog_page_sk(),
            ship_mode_sk,
            warehouse_sk,
            reason_sk,
            sale.get_cs_order_number(),
            pricing,
        )
    }
}

impl Iterator for CatalogReturnsRows {
    type Item = CatalogReturnsRow;

    fn next(&mut self) -> Option<CatalogReturnsRow> {
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

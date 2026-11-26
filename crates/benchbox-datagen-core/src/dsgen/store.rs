use tpcdsgen::config::Table;
use tpcdsgen::distribution::hours_distribution::HoursWeights;
use tpcdsgen::generator::{StoreReturnsGeneratorColumn, StoreSalesGeneratorColumn};
use tpcdsgen::join_key_utils::{
    CS_MAX_SHIP_DELAY, CS_MIN_SHIP_DELAY, generate_returns_date_join_key,
    generate_sales_date_join_key, generate_time_join_key, skip_uniform_join_key,
};
use tpcdsgen::nulls::{create_null_bit_map, skip_null_bit_map};
use tpcdsgen::permutations::{get_permutation_entry, make_permutation};
use tpcdsgen::random::RandomValueGenerator;
use tpcdsgen::row::{AbstractRowGenerator, StoreReturnsRow, StoreSalesRow};
use tpcdsgen::types::{
    generate_pricing_for_returns_table, generate_pricing_for_sales_table,
    get_store_sales_pricing_limits, skip_pricing_for_sales_table,
};

use super::LineItem;
use super::model::Model;

const RETURN_PERCENT: i32 = 10;
const SAME_CUSTOMER_PERCENT: i32 = 80;

#[derive(Default)]
struct Ticket {
    store_sk: i64,
    time_sk: i64,
    date_sk: i64,
    customer_sk: i64,
    cdemo_sk: i64,
    hdemo_sk: i64,
    addr_sk: i64,
    number: i64,
}

struct Sales {
    rows: AbstractRowGenerator<StoreSalesGeneratorColumn>,
    model: Model,
    permutation: Option<Vec<i32>>,
    remaining: i32,
    ticket: Ticket,
    item_index: i32,
}

impl Sales {
    fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::StoreSales));
        rows.set_source_row_range(first, last);
        Self {
            rows,
            model,
            permutation: None,
            remaining: 0,
            ticket: Ticket::default(),
            item_index: 0,
        }
    }

    fn new_ticket(&mut self, row_number: u64) -> Ticket {
        use StoreSalesGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.model;
        Ticket {
            store_sk: model.scd_key(
                Table::Store,
                rows.get_random_number_stream(&SsSoldStoreSk),
                1,
            ),
            time_sk: generate_time_join_key(
                HoursWeights::Store,
                rows.get_random_number_stream(&SsSoldTimeSk),
            ),
            date_sk: generate_sales_date_join_key(rows.get_random_number_stream(&SsSoldDateSk)),
            customer_sk: model.uniform_key(
                Table::Customer,
                rows.get_random_number_stream(&SsSoldCustomerSk),
            ),
            cdemo_sk: model.uniform_key(
                Table::CustomerDemographics,
                rows.get_random_number_stream(&SsSoldCdemoSk),
            ),
            hdemo_sk: model.uniform_key(
                Table::HouseholdDemographics,
                rows.get_random_number_stream(&SsSoldHdemoSk),
            ),
            addr_sk: model.uniform_key(
                Table::CustomerAddress,
                rows.get_random_number_stream(&SsSoldAddrSk),
            ),
            number: i64::try_from(row_number).expect("row number fits in i64"),
        }
    }

    fn next_line_item(&mut self) -> Option<LineItem> {
        use StoreSalesGeneratorColumn::*;
        let row_number = self.rows.next_row_number()?;
        let item_count = self.model.ids(Table::Item) as usize;

        if self.permutation.is_none() {
            self.permutation = Some(make_permutation(
                item_count,
                self.rows.get_random_number_stream(&SsPermutation),
            ));
        }
        if self.remaining == 0 {
            self.ticket = self.new_ticket(row_number);
            self.remaining = RandomValueGenerator::generate_uniform_random_int(
                8,
                16,
                self.rows.get_random_number_stream(&SsTicketNumber),
            );
            self.item_index = RandomValueGenerator::generate_uniform_random_int(
                1,
                item_count as i32,
                self.rows.get_random_number_stream(&SsSoldItemSk),
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
        let item_sk =
            self.model
                .match_surrogate_key(i64::from(item_key), self.ticket.date_sk, Table::Item);
        let draw = RandomValueGenerator::generate_uniform_random_int(
            0,
            99,
            self.rows.get_random_number_stream(&SrIsReturned),
        );
        Some(LineItem {
            item_sk,
            is_returned: draw < RETURN_PERCENT,
        })
    }

    fn skip_item_draws(&mut self) {
        use StoreSalesGeneratorColumn::*;
        skip_null_bit_map(self.rows.get_random_number_stream(&SsNulls));
        skip_uniform_join_key(self.rows.get_random_number_stream(&SsSoldPromoSk));
        skip_pricing_for_sales_table(self.rows.get_random_number_stream(&SsPricing));
    }

    fn sales_row(&mut self, item_sk: i64) -> StoreSalesRow {
        use StoreSalesGeneratorColumn::*;
        let null_bit_map = create_null_bit_map(
            Table::StoreSales,
            self.rows.get_random_number_stream(&SsNulls),
        );
        let promo_sk = self.model.uniform_key(
            Table::Promotion,
            self.rows.get_random_number_stream(&SsSoldPromoSk),
        );
        let pricing = generate_pricing_for_sales_table(
            &get_store_sales_pricing_limits(),
            self.rows.get_random_number_stream(&SsPricing),
        );
        let ticket = &self.ticket;
        StoreSalesRow::new(
            null_bit_map,
            ticket.date_sk,
            ticket.time_sk,
            item_sk,
            ticket.customer_sk,
            ticket.cdemo_sk,
            ticket.hdemo_sk,
            ticket.addr_sk,
            ticket.store_sk,
            promo_sk,
            ticket.number,
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

pub(crate) struct StoreSalesRows(Sales);

impl StoreSalesRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        Self(Sales::new(model, first, last))
    }
}

impl Iterator for StoreSalesRows {
    type Item = StoreSalesRow;

    fn next(&mut self) -> Option<StoreSalesRow> {
        let item = self.0.next_line_item()?;
        let row = self.0.sales_row(item.item_sk);
        self.0.finish_line_item();
        Some(row)
    }
}

pub(crate) struct StoreReturnsRows {
    sales: Sales,
    rows: AbstractRowGenerator<StoreReturnsGeneratorColumn>,
}

impl StoreReturnsRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::StoreSales));
        rows.set_source_row_range(first, last);
        Self {
            sales: Sales::new(model, first, last),
            rows,
        }
    }

    fn return_row(&mut self, sale: &StoreSalesRow) -> StoreReturnsRow {
        use StoreReturnsGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.sales.model;

        let null_bit_map =
            create_null_bit_map(Table::StoreReturns, rows.get_random_number_stream(&SrNulls));
        let mut customer_sk = model.uniform_key(
            Table::Customer,
            rows.get_random_number_stream(&SrCustomerSk),
        );
        let same_customer = RandomValueGenerator::generate_uniform_random_int(
            1,
            100,
            rows.get_random_number_stream(&SrTicketNumber),
        );
        if same_customer < SAME_CUSTOMER_PERCENT {
            customer_sk = sale.get_ss_sold_customer_sk();
        }
        let returned_date_sk = generate_returns_date_join_key(
            rows.get_random_number_stream(&SrReturnedDateSk),
            sale.get_ss_sold_date_sk(),
            CS_MIN_SHIP_DELAY,
            CS_MAX_SHIP_DELAY,
        );
        let returned_time_sk = i64::from(RandomValueGenerator::generate_uniform_random_int(
            8 * 3600 - 1,
            17 * 3600 - 1,
            rows.get_random_number_stream(&SrReturnedTimeSk),
        ));
        let cdemo_sk = model.uniform_key(
            Table::CustomerDemographics,
            rows.get_random_number_stream(&SrCdemoSk),
        );
        let hdemo_sk = model.uniform_key(
            Table::HouseholdDemographics,
            rows.get_random_number_stream(&SrHdemoSk),
        );
        let addr_sk = model.uniform_key(
            Table::CustomerAddress,
            rows.get_random_number_stream(&SrAddrSk),
        );
        let store_sk = model.scd_key(Table::Store, rows.get_random_number_stream(&SrStoreSk), 1);
        let reason_sk =
            model.uniform_key(Table::Reason, rows.get_random_number_stream(&SrReasonSk));

        let sale_pricing = sale.get_ss_pricing();
        let quantity = RandomValueGenerator::generate_uniform_random_int(
            1,
            sale_pricing.get_quantity(),
            rows.get_random_number_stream(&SrPricing),
        );
        let pricing = generate_pricing_for_returns_table(
            rows.get_random_number_stream(&SrPricing),
            quantity,
            sale_pricing,
        );

        StoreReturnsRow::new(
            null_bit_map,
            returned_date_sk,
            returned_time_sk,
            sale.get_ss_sold_item_sk(),
            customer_sk,
            cdemo_sk,
            hdemo_sk,
            addr_sk,
            store_sk,
            reason_sk,
            sale.get_ss_ticket_number(),
            pricing,
        )
    }
}

impl Iterator for StoreReturnsRows {
    type Item = StoreReturnsRow;

    fn next(&mut self) -> Option<StoreReturnsRow> {
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

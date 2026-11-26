use tpcdsgen::business_key_generator::make_business_key;
use tpcdsgen::config::Table;
use tpcdsgen::distribution::{
    FirstNamesWeights, NamesDistributions, SalutationsWeights, get_syllables_distribution,
    pick_random_country,
};
use tpcdsgen::generator::{
    CustomerGeneratorColumn, InventoryGeneratorColumn, PromotionGeneratorColumn,
};
use tpcdsgen::nulls::create_null_bit_map;
use tpcdsgen::random::{RandomTextGenerator, RandomValueGenerator};
use tpcdsgen::row::{AbstractRowGenerator, CustomerRow, InventoryRow, PromotionRow};
use tpcdsgen::types::{Date, Decimal};

use super::model::Model;

const PREFERRED_CUSTOMER_PERCENT: i32 = 50;
const PROMO_START_MIN: i32 = -720;
const PROMO_START_MAX: i32 = 100;
const PROMO_LENGTH_MIN: i32 = 1;
const PROMO_LENGTH_MAX: i32 = 60;
const PROMO_NAME_LENGTH: i32 = 5;
const PROMO_DETAIL_LENGTH_MIN: i32 = 20;
const PROMO_DETAIL_LENGTH_MAX: i32 = 60;

pub(crate) struct CustomerRows {
    rows: AbstractRowGenerator<CustomerGeneratorColumn>,
    model: Model,
}

impl CustomerRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::Customer));
        rows.set_source_row_range(first, last);
        Self { rows, model }
    }

    fn generate(&mut self, row_number: u64) -> CustomerRow {
        use CustomerGeneratorColumn::*;
        let rows = &mut self.rows;
        let model = &self.model;

        let c_customer_sk = i64::try_from(row_number).expect("row number fits in i64");
        let c_customer_id = make_business_key(row_number);

        let flag = RandomValueGenerator::generate_uniform_random_int(
            1,
            100,
            rows.get_random_number_stream(&CPreferredCustFlag),
        );
        let c_preferred_cust_flag = flag < PREFERRED_CUSTOMER_PERCENT;

        let c_current_hdemo_sk = model.uniform_key(
            Table::HouseholdDemographics,
            rows.get_random_number_stream(&CCurrentHdemoSk),
        );
        let c_current_cdemo_sk = model.uniform_key(
            Table::CustomerDemographics,
            rows.get_random_number_stream(&CCurrentCdemoSk),
        );
        let c_current_addr_sk = model.uniform_key(
            Table::CustomerAddress,
            rows.get_random_number_stream(&CCurrentAddrSk),
        );

        let name_index = NamesDistributions::pick_random_index(
            FirstNamesWeights::GeneralFrequency,
            rows.get_random_number_stream(&CFirstName),
        );
        let c_first_name = NamesDistributions::get_first_name_from_index(name_index).to_string();
        let c_last_name =
            NamesDistributions::pick_random_last_name(rows.get_random_number_stream(&CLastName))
                .to_string();
        let salutation_weights = if NamesDistributions::get_weight_for_index(
            name_index,
            FirstNamesWeights::FemaleFrequency,
        ) == 0
        {
            SalutationsWeights::Male
        } else {
            SalutationsWeights::Female
        };
        let c_salutation = NamesDistributions::pick_random_salutation(
            salutation_weights,
            rows.get_random_number_stream(&CSalutation),
        )
        .to_string();

        let today = Date::from_julian_days(Date::JULIAN_TODAYS_DATE);
        let birthday = RandomValueGenerator::generate_uniform_random_date(
            Date::new(1924, 1, 1),
            Date::new(1992, 12, 31),
            rows.get_random_number_stream(&CBirthDay),
        );
        let c_email_address = RandomValueGenerator::generate_random_email(
            &c_first_name,
            &c_last_name,
            rows.get_random_number_stream(&CEmailAddress),
        );
        let c_last_review_date = RandomValueGenerator::generate_uniform_random_date(
            Date::from_julian_days(Date::JULIAN_TODAYS_DATE - 365),
            today,
            rows.get_random_number_stream(&CLastReviewDate),
        )
        .to_julian_days();
        let c_first_sales_date_id = RandomValueGenerator::generate_uniform_random_date(
            Date::from_julian_days(Date::JULIAN_TODAYS_DATE - 3650),
            Date::from_julian_days(Date::JULIAN_TODAYS_DATE),
            rows.get_random_number_stream(&CFirstSalesDateId),
        )
        .to_julian_days();
        let c_birth_country =
            pick_random_country(rows.get_random_number_stream(&CBirthCountry)).to_string();
        let null_bit_map =
            create_null_bit_map(Table::Customer, rows.get_random_number_stream(&CNulls));

        CustomerRow::new(
            c_customer_sk,
            c_customer_id,
            c_current_cdemo_sk,
            c_current_hdemo_sk,
            c_current_addr_sk,
            c_first_sales_date_id + 30,
            c_first_sales_date_id,
            c_salutation,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            birthday.day(),
            birthday.month(),
            birthday.year(),
            c_birth_country,
            c_email_address,
            c_last_review_date,
            null_bit_map,
        )
    }
}

impl Iterator for CustomerRows {
    type Item = CustomerRow;

    fn next(&mut self) -> Option<CustomerRow> {
        let row_number = self.rows.next_row_number()?;
        let row = self.generate(row_number);
        self.rows.finish_row();
        Some(row)
    }
}

pub(crate) struct PromotionRows {
    rows: AbstractRowGenerator<PromotionGeneratorColumn>,
    text: RandomTextGenerator,
    model: Model,
}

impl PromotionRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::Promotion));
        rows.set_source_row_range(first, last);
        Self {
            rows,
            text: RandomTextGenerator::new(),
            model,
        }
    }

    fn generate(&mut self, row_number: u64) -> PromotionRow {
        use PromotionGeneratorColumn::*;
        let rows = &mut self.rows;

        let null_bit_map =
            create_null_bit_map(Table::Promotion, rows.get_random_number_stream(&PNulls));
        let p_promo_sk = i64::try_from(row_number).expect("row number fits in i64");
        let p_promo_id = make_business_key(row_number);
        let p_start_date_id = i64::from(Date::JULIAN_DATE_MINIMUM)
            + i64::from(RandomValueGenerator::generate_uniform_random_int(
                PROMO_START_MIN,
                PROMO_START_MAX,
                rows.get_random_number_stream(&PStartDateId),
            ));
        let p_end_date_id = p_start_date_id
            + i64::from(RandomValueGenerator::generate_uniform_random_int(
                PROMO_LENGTH_MIN,
                PROMO_LENGTH_MAX,
                rows.get_random_number_stream(&PEndDateId),
            ));
        let p_item_sk = self
            .model
            .scd_key(Table::Item, rows.get_random_number_stream(&PItemSk), 1);

        rows.get_random_number_stream(&PPromoName);
        let p_promo_name = RandomValueGenerator::generate_word(
            p_promo_sk,
            PROMO_NAME_LENGTH,
            get_syllables_distribution(),
        );

        let mut flags = RandomValueGenerator::generate_uniform_random_int(
            0,
            511,
            rows.get_random_number_stream(&PChannelDmail),
        );
        let mut next_flag = || {
            let flag = (flags & 1) != 0;
            flags <<= 1;
            flag
        };
        let p_channel_dmail = next_flag();
        let p_channel_email = next_flag();
        let p_channel_catalog = next_flag();
        let p_channel_tv = next_flag();
        let p_channel_radio = next_flag();
        let p_channel_press = next_flag();
        let p_channel_event = next_flag();
        let p_channel_demo = next_flag();
        let p_discount_active = next_flag();

        let p_channel_details = self.text.random_text(
            PROMO_DETAIL_LENGTH_MIN,
            PROMO_DETAIL_LENGTH_MAX,
            rows.get_random_number_stream(&PChannelDetails),
        );

        PromotionRow::new(
            null_bit_map,
            p_promo_sk,
            p_promo_id,
            p_start_date_id,
            p_end_date_id,
            p_item_sk,
            Decimal::new(100_000, 2),
            1,
            p_promo_name,
            p_channel_dmail,
            p_channel_email,
            p_channel_catalog,
            p_channel_tv,
            p_channel_radio,
            p_channel_press,
            p_channel_event,
            p_channel_demo,
            p_channel_details,
            "Unknown".to_owned(),
            p_discount_active,
        )
    }
}

impl Iterator for PromotionRows {
    type Item = PromotionRow;

    fn next(&mut self) -> Option<PromotionRow> {
        let row_number = self.rows.next_row_number()?;
        let row = self.generate(row_number);
        self.rows.finish_row();
        Some(row)
    }
}

pub(crate) struct InventoryRows {
    rows: AbstractRowGenerator<InventoryGeneratorColumn>,
    model: Model,
}

impl InventoryRows {
    pub(crate) fn new(model: Model, first: u64, last: u64) -> Self {
        let mut rows = AbstractRowGenerator::new(model.rows(Table::Inventory));
        rows.set_source_row_range(first, last);
        Self { rows, model }
    }

    fn generate(&mut self, row_number: u64) -> InventoryRow {
        use InventoryGeneratorColumn::*;
        let rows = &mut self.rows;

        let null_bit_map =
            create_null_bit_map(Table::Inventory, rows.get_random_number_stream(&InvNulls));

        let mut index = row_number - 1;
        let items = self.model.ids(Table::Item);
        let item_id = index % items + 1;
        index /= items;
        let warehouses = self.model.rows(Table::Warehouse);
        let inv_warehouse_sk = index % warehouses + 1;
        index /= warehouses;
        let inv_date_sk = i64::from(Date::JULIAN_DATE_MINIMUM)
            + i64::try_from(index * 7).expect("inventory date offset fits in i64");

        let inv_item_sk = self.model.match_surrogate_key(
            i64::try_from(item_id).expect("item id fits in i64"),
            inv_date_sk,
            Table::Item,
        );
        let inv_quantity_on_hand = RandomValueGenerator::generate_uniform_random_int(
            0,
            1000,
            rows.get_random_number_stream(&InvQuantityOnHand),
        );

        InventoryRow::new(
            null_bit_map,
            inv_date_sk,
            inv_item_sk,
            i64::try_from(inv_warehouse_sk).expect("warehouse key fits in i64"),
            inv_quantity_on_hand,
        )
    }
}

impl Iterator for InventoryRows {
    type Item = InventoryRow;

    fn next(&mut self) -> Option<InventoryRow> {
        let row_number = self.rows.next_row_number()?;
        let row = self.generate(row_number);
        self.rows.finish_row();
        Some(row)
    }
}

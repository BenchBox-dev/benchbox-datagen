use tpcdsgen::config::Table;
use tpcdsgen::distribution::CatalogPageTypesDistribution;
use tpcdsgen::distribution::calendar_distribution::{CalendarDistribution, CalendarWeights};
use tpcdsgen::random::{RandomNumberStream, RandomValueGenerator};
use tpcdsgen::types::Date;

use crate::tpcds;

const TABLE_SLOTS: usize = 25;
const CATALOGS_PER_YEAR: i32 = 18;
const ONE_HALF_DATE: i64 =
    Date::JULIAN_DATA_START_DATE + (Date::JULIAN_DATA_END_DATE - Date::JULIAN_DATA_START_DATE) / 2;
const ONE_THIRD_PERIOD: i64 = (Date::JULIAN_DATA_END_DATE - Date::JULIAN_DATA_START_DATE) / 3;
const ONE_THIRD_DATE: i64 = Date::JULIAN_DATA_START_DATE + ONE_THIRD_PERIOD;
const TWO_THIRDS_DATE: i64 = ONE_THIRD_DATE + ONE_THIRD_PERIOD;

#[derive(Debug, Clone)]
pub(crate) struct Model {
    rows: [u64; TABLE_SLOTS],
}

impl Model {
    pub(crate) fn new(scale_factor: f64) -> Self {
        let mut rows = [0; TABLE_SLOTS];
        for name in tpcds::TABLES {
            rows[tpcds::table(name) as usize] = tpcds::source_rows(name, scale_factor);
        }
        Self { rows }
    }

    pub(crate) fn rows(&self, table: Table) -> u64 {
        self.rows[table as usize]
    }

    pub(crate) fn ids(&self, table: Table) -> u64 {
        let rows = self.rows(table);
        if table.keeps_history() {
            tpcds::history_ids(rows)
        } else {
            rows
        }
    }

    fn rows_i64(&self, table: Table) -> i64 {
        i64::try_from(self.rows(table)).expect("row count fits in i64")
    }

    pub(crate) fn uniform_key(&self, table: Table, stream: &mut RandomNumberStream) -> i64 {
        RandomValueGenerator::generate_uniform_random_key(1, self.rows_i64(table), stream)
    }

    pub(crate) fn match_surrogate_key(&self, unique: i64, julian_date: i64, table: Table) -> i64 {
        let mut key = (unique / 3) * 6;
        match unique % 3 {
            1 => key += 1,
            2 => {
                key += 2;
                if julian_date > ONE_HALF_DATE {
                    key += 1;
                }
            }
            _ => {
                key -= 2;
                if julian_date > ONE_THIRD_DATE {
                    key += 1;
                }
                if julian_date > TWO_THIRDS_DATE {
                    key += 1;
                }
            }
        }
        key.min(self.rows_i64(table))
    }

    pub(crate) fn scd_key(
        &self,
        table: Table,
        stream: &mut RandomNumberStream,
        julian_date: i64,
    ) -> i64 {
        if julian_date > Date::JULIAN_DATA_END_DATE {
            return -1;
        }
        let ids = i64::try_from(self.ids(table)).expect("id count fits in i64");
        let unique = RandomValueGenerator::generate_uniform_random_key(1, ids, stream);
        let key = self.match_surrogate_key(unique, julian_date, table);
        if key > self.rows_i64(table) { -1 } else { key }
    }

    pub(crate) fn catalog_page_key(
        &self,
        stream: &mut RandomNumberStream,
        julian_date: i64,
    ) -> i64 {
        let years = Date::DATE_MAXIMUM.year() - Date::DATE_MINIMUM.year() + 2;
        let pages_per_catalog =
            ((self.rows(Table::CatalogPage) / CATALOGS_PER_YEAR as u64) / years as u64) as i32;
        let kind = CatalogPageTypesDistribution::pick_random_catalog_page_type(stream);
        let page = RandomValueGenerator::generate_uniform_random_int(1, pages_per_catalog, stream);
        let offset_from_start = (julian_date - Date::JULIAN_DATA_START_DATE - 1) as i32;
        let mut count = (offset_from_start / 365) * CATALOGS_PER_YEAR;
        let offset = offset_from_start % 365;
        match kind.as_str() {
            "bi-annual" => {
                if offset > 183 {
                    count += 1;
                }
            }
            "quarterly" => count += offset / 91,
            "monthly" => count += offset / 31,
            other => panic!("invalid catalog_page_type {other:?}"),
        }
        i64::from(count * pages_per_catalog + page)
    }

    pub(crate) fn rows_for_date(&self, table: Table, julian_date: i64) -> u64 {
        let rows = match table {
            Table::StoreSales | Table::CatalogSales | Table::WebSales => self.rows(table),
            _ => panic!("no date scaling for {table:?}"),
        };
        let date = Date::from_julian_days(julian_date as i32);
        let weights = if Date::is_leap_year(date.year()) {
            CalendarWeights::SalesLeapYear
        } else {
            CalendarWeights::Sales
        };
        let total = CalendarDistribution::get_max_weight(weights) as u64 * 5;
        let weight = CalendarDistribution::get_weight_for_day_number(
            CalendarDistribution::get_index_for_date(&date),
            weights,
        ) as u64;
        (rows * weight + total / 2) / total
    }
}

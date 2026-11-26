use std::io::{self, Write};
use std::ops::RangeInclusive;

use benchbox_datagen_writers::DelimitedBuffer;
use tpcdsgen::config::{CompatMode, Session, SessionBuilder, Table};
use tpcdsgen::row::{
    CallCenterRowGenerator, CatalogPageRowGenerator, CatalogReturnsRowGenerator,
    CatalogSalesRowGenerator, CustomerAddressRowGenerator, CustomerDemographicsRowGenerator,
    CustomerRowGenerator, DateDimRowGenerator, HouseholdDemographicsRowGenerator,
    IncomeBandRowGenerator, InventoryRowGenerator, ItemRowGenerator, PromotionRowGenerator,
    ReasonRowGenerator, ShipModeRowGenerator, StoreReturnsRowGenerator, StoreRowGenerator,
    StoreSalesRowGenerator, TimeDimRowGenerator, WarehouseRowGenerator, WebPageRowGenerator,
    WebReturnsRowGenerator, WebSalesRowGenerator, WebSiteRowGenerator,
};
use tpcdsgen_arrow::{
    CallCenterArrow, CatalogPageArrow, CatalogReturnsArrow, CatalogSalesArrow,
    CustomerAddressArrow, CustomerArrow, CustomerDemographicsArrow, DateDimArrow,
    HouseholdDemographicsArrow, IncomeBandArrow, InventoryArrow, ItemArrow, PromotionArrow,
    ReasonArrow, ShipModeArrow, StoreArrow, StoreReturnsArrow, StoreSalesArrow, TimeDimArrow,
    WarehouseArrow, WebPageArrow, WebReturnsArrow, WebSalesArrow, WebSiteArrow,
};

use crate::{BatchReader, dsgen, schema};

pub(crate) const TABLES: [&str; 24] = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

pub(crate) const FULL_MODEL_SCALE_FACTOR: f64 = 1.0;
const FLUSH_BYTES: usize = 1 << 20;
const SPLIT_THRESHOLD_ROWS: u64 = 1_000_000;

fn session(scale_factor: f64) -> Session {
    SessionBuilder::new()
        .with_scale_factor(scale_factor)
        .with_compat_mode(CompatMode::C)
        .build()
        .expect("scale factor was validated by the caller")
}

fn chunk_session(scale_factor: f64, part: i32, parts: i32) -> Session {
    SessionBuilder::new()
        .with_scale_factor(scale_factor)
        .with_compat_mode(CompatMode::C)
        .with_chunk_number(part)
        .with_total_chunks(parts)
        .build()
        .expect("part and parts were validated by the caller")
}

pub(crate) fn table(name: &str) -> Table {
    match name {
        "call_center" => Table::CallCenter,
        "catalog_page" => Table::CatalogPage,
        "catalog_returns" => Table::CatalogReturns,
        "catalog_sales" => Table::CatalogSales,
        "customer" => Table::Customer,
        "customer_address" => Table::CustomerAddress,
        "customer_demographics" => Table::CustomerDemographics,
        "date_dim" => Table::DateDim,
        "household_demographics" => Table::HouseholdDemographics,
        "income_band" => Table::IncomeBand,
        "inventory" => Table::Inventory,
        "item" => Table::Item,
        "promotion" => Table::Promotion,
        "reason" => Table::Reason,
        "ship_mode" => Table::ShipMode,
        "store" => Table::Store,
        "store_returns" => Table::StoreReturns,
        "store_sales" => Table::StoreSales,
        "time_dim" => Table::TimeDim,
        "warehouse" => Table::Warehouse,
        "web_page" => Table::WebPage,
        "web_returns" => Table::WebReturns,
        "web_sales" => Table::WebSales,
        "web_site" => Table::WebSite,
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

const STATIC_BELOW_ONE: [&str; 6] = [
    "catalog_page",
    "date_dim",
    "household_demographics",
    "income_band",
    "ship_mode",
    "time_dim",
];

fn upstream_rows(name: &str, scale_factor: f64) -> u64 {
    session(scale_factor)
        .get_scaling()
        .get_row_count(table(name))
}

pub(crate) fn history_ids(rows: u64) -> u64 {
    let unique = (rows / 6) * 3;
    match rows % 6 {
        1 => unique + 1,
        2 | 3 => unique + 2,
        4 | 5 => unique + 3,
        _ => unique,
    }
}

pub(crate) fn source_rows(name: &str, scale_factor: f64) -> u64 {
    if scale_factor >= FULL_MODEL_SCALE_FACTOR {
        return upstream_rows(name, scale_factor);
    }
    if name == "inventory" {
        let weeks = upstream_rows("inventory", 1.0)
            / (id_count("item", 1.0) * upstream_rows("warehouse", 1.0));
        return id_count("item", scale_factor) * source_rows("warehouse", scale_factor) * weeks;
    }
    let base = upstream_rows(name, 1.0);
    if STATIC_BELOW_ONE.contains(&name) {
        return base;
    }
    (((base as f64) * scale_factor) as u64).max(1)
}

pub(crate) fn id_count(name: &str, scale_factor: f64) -> u64 {
    let rows = source_rows(name, scale_factor);
    if table(name).keeps_history() {
        history_ids(rows)
    } else {
        rows
    }
}

pub(crate) fn part_range(
    name: &str,
    scale_factor: f64,
    part: i32,
    parts: i32,
) -> RangeInclusive<u64> {
    chunk_session(scale_factor, part, parts).get_source_row_range(table(name))
}

pub(crate) fn splittable(name: &str, scale_factor: f64) -> bool {
    source_rows(name, scale_factor) >= SPLIT_THRESHOLD_ROWS
}

pub(crate) fn chunk_source_rows(name: &str) -> u64 {
    match name {
        "store_sales" | "store_returns" | "catalog_sales" | "catalog_returns" | "web_sales"
        | "web_returns" => 2_500,
        "inventory" => 250_000,
        _ => 50_000,
    }
}

fn drain<W: Write, R: std::fmt::Display>(
    rows: impl Iterator<Item = R>,
    sink: &mut W,
) -> io::Result<u64> {
    let mut buffer = DelimitedBuffer::default();
    for row in rows {
        buffer.push(&row);
        if buffer.buffered() >= FLUSH_BYTES {
            buffer.flush_to(sink)?;
        }
    }
    buffer.flush_to(sink)?;
    Ok(buffer.rows())
}

fn row_count_differs(name: &str, scale_factor: f64) -> bool {
    scale_factor < FULL_MODEL_SCALE_FACTOR
        && upstream_rows(name, scale_factor) != source_rows(name, scale_factor)
}

macro_rules! dispatch {
    ($(($name:literal, $generator:ident, $arrow:ident)),* $(,)?) => {
        fn dat_rows(
            name: &str,
            scale_factor: f64,
            range: RangeInclusive<u64>,
        ) -> Box<dyn Iterator<Item = String> + Send> {
            if dsgen::generates(name, scale_factor) {
                return dsgen::rows(name, scale_factor, range);
            }
            let session = session(scale_factor);
            let source_rows = source_rows(name, scale_factor);
            match name {
                $($name => {
                    let mut rows = $generator::new(session, source_rows);
                    rows.set_source_row_range(*range.start(), *range.end());
                    Box::new(rows.map(|row| row.to_string()))
                })*
                other => unreachable!("table {other:?} was validated by the caller"),
            }
        }

        pub(crate) fn batches(
            name: &str,
            scale_factor: f64,
            range: RangeInclusive<u64>,
        ) -> BatchReader {
            if dsgen::generates(name, scale_factor) || row_count_differs(name, scale_factor) {
                return dsgen::batches(
                    dat_rows(name, scale_factor, range),
                    schema::tpcds(name).expect("every tpcds table has a schema"),
                );
            }
            let session = session(scale_factor);
            match name {
                $($name => Box::new(
                    $arrow::new(session).with_source_row_range(*range.start(), *range.end()),
                ),)*
                other => unreachable!("table {other:?} was validated by the caller"),
            }
        }

        pub(crate) fn write_delimited<W: Write>(
            name: &str,
            scale_factor: f64,
            range: RangeInclusive<u64>,
            sink: &mut W,
        ) -> io::Result<u64> {
            if dsgen::generates(name, scale_factor) {
                return drain(dsgen::rows(name, scale_factor, range), sink);
            }
            let session = session(scale_factor);
            let source_rows = source_rows(name, scale_factor);
            match name {
                $($name => {
                    let mut rows = $generator::new(session, source_rows);
                    rows.set_source_row_range(*range.start(), *range.end());
                    drain(rows, sink)
                })*
                other => unreachable!("table {other:?} was validated by the caller"),
            }
        }

        pub(crate) fn count_rows(
            name: &str,
            scale_factor: f64,
            range: RangeInclusive<u64>,
        ) -> u64 {
            if dsgen::generates(name, scale_factor) {
                return dsgen::rows(name, scale_factor, range).count() as u64;
            }
            let session = session(scale_factor);
            let source_rows = source_rows(name, scale_factor);
            match name {
                $($name => {
                    let mut rows = $generator::new(session, source_rows);
                    rows.set_source_row_range(*range.start(), *range.end());
                    rows.count() as u64
                })*
                other => unreachable!("table {other:?} was validated by the caller"),
            }
        }
    };
}

dispatch!(
    ("call_center", CallCenterRowGenerator, CallCenterArrow),
    ("catalog_page", CatalogPageRowGenerator, CatalogPageArrow),
    (
        "catalog_returns",
        CatalogReturnsRowGenerator,
        CatalogReturnsArrow
    ),
    ("catalog_sales", CatalogSalesRowGenerator, CatalogSalesArrow),
    ("customer", CustomerRowGenerator, CustomerArrow),
    (
        "customer_address",
        CustomerAddressRowGenerator,
        CustomerAddressArrow
    ),
    (
        "customer_demographics",
        CustomerDemographicsRowGenerator,
        CustomerDemographicsArrow
    ),
    ("date_dim", DateDimRowGenerator, DateDimArrow),
    (
        "household_demographics",
        HouseholdDemographicsRowGenerator,
        HouseholdDemographicsArrow
    ),
    ("income_band", IncomeBandRowGenerator, IncomeBandArrow),
    ("inventory", InventoryRowGenerator, InventoryArrow),
    ("item", ItemRowGenerator, ItemArrow),
    ("promotion", PromotionRowGenerator, PromotionArrow),
    ("reason", ReasonRowGenerator, ReasonArrow),
    ("ship_mode", ShipModeRowGenerator, ShipModeArrow),
    ("store", StoreRowGenerator, StoreArrow),
    ("store_returns", StoreReturnsRowGenerator, StoreReturnsArrow),
    ("store_sales", StoreSalesRowGenerator, StoreSalesArrow),
    ("time_dim", TimeDimRowGenerator, TimeDimArrow),
    ("warehouse", WarehouseRowGenerator, WarehouseArrow),
    ("web_page", WebPageRowGenerator, WebPageArrow),
    ("web_returns", WebReturnsRowGenerator, WebReturnsArrow),
    ("web_sales", WebSalesRowGenerator, WebSalesArrow),
    ("web_site", WebSiteRowGenerator, WebSiteArrow),
);

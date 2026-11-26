use std::fmt::Display;
use std::fs;

use benchbox_datagen_core::{Benchmark, table_scales};
use tpcdsgen::config::{CompatMode, SessionBuilder, Table};
use tpcdsgen::row::{
    CallCenterRowGenerator, CatalogPageRowGenerator, CatalogReturnsRowGenerator,
    CatalogSalesRowGenerator, CustomerAddressRowGenerator, CustomerDemographicsRowGenerator,
    CustomerRowGenerator, DateDimRowGenerator, HouseholdDemographicsRowGenerator,
    IncomeBandRowGenerator, InventoryRowGenerator, ItemRowGenerator, PromotionRowGenerator,
    ReasonRowGenerator, ShipModeRowGenerator, StoreReturnsRowGenerator, StoreRowGenerator,
    StoreSalesRowGenerator, TimeDimRowGenerator, WarehouseRowGenerator, WebPageRowGenerator,
    WebReturnsRowGenerator, WebSalesRowGenerator, WebSiteRowGenerator,
};

fn lines<R: Display>(rows: impl Iterator<Item = R>) -> Vec<String> {
    rows.map(|row| {
        let text = row.to_string();
        text.strip_suffix('|').unwrap_or(&text).to_owned()
    })
    .collect()
}

macro_rules! generate {
    ($table:expr, $session:expr, $rows:expr, [$(($variant:ident, $generator:ident)),* $(,)?]) => {
        match $table {
            $(Table::$variant => lines($generator::new($session, $rows)),)*
            Table::DbgenVersion => Vec::new(),
        }
    };
}

fn describe(generated: &[String], oracle: &[String]) -> String {
    if generated == oracle {
        return "identical".to_owned();
    }
    let index = (0..generated.len().min(oracle.len()))
        .find(|&row| generated[row] != oracle[row])
        .unwrap_or(generated.len().min(oracle.len()));
    let split = |lines: &[String]| -> Vec<String> {
        lines
            .get(index)
            .map(|line| line.split('|').map(str::to_owned).collect())
            .unwrap_or_default()
    };
    let (ours, theirs) = (split(generated), split(oracle));
    let columns: Vec<String> = (0..ours.len().max(theirs.len()))
        .filter(|&column| ours.get(column) != theirs.get(column))
        .take(4)
        .map(|column| {
            format!(
                "col{column}: {} != {}",
                ours.get(column).map_or("-", String::as_str),
                theirs.get(column).map_or("-", String::as_str)
            )
        })
        .collect();
    format!("differs at row {index} ({})", columns.join(", "))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let scale_factor: f64 = args.next().expect("scale factor").parse().expect("number");
    let oracle_dir = args.next().expect("dsdgen output directory");
    let scales = table_scales(Benchmark::Tpcds, scale_factor).expect("scale model");
    for scale in scales {
        let table = Table::values()
            .iter()
            .copied()
            .find(|table| table.get_name() == scale.table)
            .expect("table");
        let session = SessionBuilder::new()
            .with_scale_factor(scale_factor)
            .with_compat_mode(CompatMode::C)
            .build()
            .expect("session");
        let generated = generate!(
            table,
            session,
            scale.source_rows,
            [
                (CallCenter, CallCenterRowGenerator),
                (CatalogPage, CatalogPageRowGenerator),
                (CatalogReturns, CatalogReturnsRowGenerator),
                (CatalogSales, CatalogSalesRowGenerator),
                (Customer, CustomerRowGenerator),
                (CustomerAddress, CustomerAddressRowGenerator),
                (CustomerDemographics, CustomerDemographicsRowGenerator),
                (DateDim, DateDimRowGenerator),
                (HouseholdDemographics, HouseholdDemographicsRowGenerator),
                (IncomeBand, IncomeBandRowGenerator),
                (Inventory, InventoryRowGenerator),
                (Item, ItemRowGenerator),
                (Promotion, PromotionRowGenerator),
                (Reason, ReasonRowGenerator),
                (ShipMode, ShipModeRowGenerator),
                (Store, StoreRowGenerator),
                (StoreReturns, StoreReturnsRowGenerator),
                (StoreSales, StoreSalesRowGenerator),
                (TimeDim, TimeDimRowGenerator),
                (Warehouse, WarehouseRowGenerator),
                (WebPage, WebPageRowGenerator),
                (WebReturns, WebReturnsRowGenerator),
                (WebSales, WebSalesRowGenerator),
                (WebSite, WebSiteRowGenerator),
            ]
        );
        let oracle: Vec<String> = fs::read_to_string(format!("{oracle_dir}/{}.dat", scale.table))
            .expect("oracle file")
            .lines()
            .map(str::to_owned)
            .collect();
        println!(
            "{:<24} rows {:>9} oracle {:>9}  {}",
            scale.table,
            generated.len(),
            oracle.len(),
            describe(&generated, &oracle)
        );
    }
}

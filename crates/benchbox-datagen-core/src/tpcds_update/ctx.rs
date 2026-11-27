use tpcdsgen::distribution::CatalogPageTypesDistribution;
use tpcdsgen::distribution::hours_distribution::{HoursDistribution, HoursWeights};
use tpcdsgen::random::RandomTextGenerator;

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use super::columns::Col;
use super::plan::Plan;
use super::rng::Rng;
use crate::Error;
use crate::tpcds::{id_count, source_rows};

const CATALOGS_PER_YEAR: i64 = 18;
const CATALOG_YEARS: i64 = 6;

pub(super) struct Run<'a> {
    cancel: &'a AtomicBool,
    created: RefCell<Vec<PathBuf>>,
}

impl<'a> Run<'a> {
    pub(super) fn new(cancel: &'a AtomicBool) -> Self {
        Self {
            cancel,
            created: RefCell::new(Vec::new()),
        }
    }

    pub(super) fn check(&self) -> Result<(), Error> {
        if self.cancel.load(Ordering::Relaxed) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }

    pub(super) fn record(&self, path: &Path) {
        self.created.borrow_mut().push(path.to_path_buf());
    }

    pub(super) fn remove_created(&self) {
        for path in self.created.borrow().iter() {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(super) struct Ctx<'a> {
    pub(super) run: &'a Run<'a>,
    pub(super) rng: Rng,
    pub(super) plan: Plan,
    pub(super) scale: f64,
    pub(super) text: RandomTextGenerator,
}

impl<'a> Ctx<'a> {
    pub(super) fn new(run: &'a Run<'a>, scale: f64, update: i32, seed: i32) -> Self {
        let mut rng = Rng::new(seed);
        let plan = Plan::new(scale, update, &mut rng);
        Self {
            run,
            rng,
            plan,
            scale,
            text: RandomTextGenerator::new(),
        }
    }

    pub(super) fn rows(&self, table: &str) -> i64 {
        source_rows(table, self.scale) as i64
    }

    pub(super) fn ids(&self, table: &str) -> i64 {
        id_count(table, self.scale) as i64
    }

    pub(super) fn gen_text(&mut self, column: Col, min: i32, max: i32) -> String {
        self.text.random_text(min, max, self.rng.stream(column))
    }

    pub(super) fn join_uniform(&mut self, column: Col, table: &str) -> i64 {
        let rows = self.rows(table);
        self.rng.key(column, 1, rows)
    }

    pub(super) fn join_scd(&mut self, column: Col, table: &str) -> i64 {
        let ids = self.ids(table);
        let rows = self.rows(table);
        let unique = self.rng.key(column, 1, ids);
        let sk = match unique % 3 {
            1 => (unique / 3) * 6 + 1,
            2 => (unique / 3) * 6 + 2,
            _ => (unique / 3) * 6 - 2,
        }
        .min(rows);
        if sk > rows { -1 } else { sk }
    }

    pub(super) fn join_time(&mut self, column: Col) -> i64 {
        let hour =
            HoursDistribution::pick_random_hour(HoursWeights::Uniform, self.rng.stream(column));
        let seconds = self.rng.int(column, 0, 3599);
        i64::from(hour) * 3600 + i64::from(seconds)
    }

    pub(super) fn pages_per_catalog(&self) -> i64 {
        (self.rows("catalog_page") / CATALOGS_PER_YEAR) / CATALOG_YEARS
    }

    pub(super) fn join_catalog_page(&mut self, column: Col, julian: i32) -> i64 {
        let per_catalog = self.pages_per_catalog();
        let kind =
            CatalogPageTypesDistribution::pick_random_catalog_page_type(self.rng.stream(column));
        let page = i64::from(self.rng.int(column, 1, per_catalog as i32));
        let offset = i64::from(julian) - i64::from(super::calendar::DATA_START_DATE) - 1;
        let mut count = (offset / 365) * CATALOGS_PER_YEAR;
        let day = offset % 365;
        match kind.as_str() {
            "bi-annual" => {
                if day > 183 {
                    count += 1;
                }
            }
            "quarterly" => count += day / 91,
            "monthly" => count += day / 31,
            other => unreachable!("unknown catalog page type {other:?}"),
        }
        count * per_catalog + page
    }

    pub(super) fn catalog_number(&self, page: i64) -> i64 {
        page / self.pages_per_catalog()
    }
}

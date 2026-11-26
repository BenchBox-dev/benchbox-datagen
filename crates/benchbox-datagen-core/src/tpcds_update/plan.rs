use super::calendar::{
    CALENDAR_HIGH, CALENDAR_LOW, CALENDAR_MEDIUM, CALENDAR_SALES, calendar,
    day_number, day_of_week, is_leap, jtodt,
};
use super::columns::Col;
use super::rng::Rng;
use crate::tpcds::{id_count, source_rows};

const ZONES: [usize; 3] = [CALENDAR_LOW, CALENDAR_MEDIUM, CALENDAR_HIGH];
const STREAM_ZERO: Col = Col {
    number: 0,
    per_row: 0,
};

#[derive(Clone, Copy, Debug)]
pub(super) enum Channel {
    Store,
    Catalog,
    Web,
    Inventory,
}

pub(super) struct Plan {
    pub(super) scale: f64,
    pub(super) update: i32,
    pub(super) update_dates: [i32; 6],
    pub(super) inventory_dates: [i32; 6],
    store: DayRows,
    catalog: DayRows,
    web: DayRows,
    inventory: DayRows,
}

#[derive(Default, Clone, Copy)]
struct DayRows {
    cumulative: [i64; 6],
}

impl DayRows {
    fn total(&self) -> i64 {
        self.cumulative[5]
    }

    fn index(&self, row: i64) -> usize {
        let mut index = 0;
        while row > self.cumulative[index] {
            if index == 5 {
                break;
            }
            index += 1;
        }
        index
    }
}

impl Plan {
    pub(super) fn new(scale: f64, update: i32, rng: &mut Rng) -> Self {
        let (update_dates, inventory_dates) = set_update_dates(rng, update);
        let mut plan = Self {
            scale,
            update,
            update_dates,
            inventory_dates,
            store: DayRows::default(),
            catalog: DayRows::default(),
            web: DayRows::default(),
            inventory: DayRows::default(),
        };
        plan.store = plan.day_rows(Channel::Store);
        plan.catalog = plan.day_rows(Channel::Catalog);
        plan.web = plan.day_rows(Channel::Web);
        plan.inventory = plan.day_rows(Channel::Inventory);
        plan
    }

    fn day_rows(&self, channel: Channel) -> DayRows {
        let mut rows = DayRows::default();
        let mut total = 0;
        for (slot, date) in self.update_dates.iter().enumerate() {
            total += date_scaling(self.scale, channel, *date);
            rows.cumulative[slot] = total;
        }
        rows
    }

    pub(super) fn rows(&self, channel: Channel) -> i64 {
        match channel {
            Channel::Store => self.store.total(),
            Channel::Catalog => self.catalog.total(),
            Channel::Web => self.web.total(),
            Channel::Inventory => {
                (id_count("item", self.scale) * source_rows("warehouse", self.scale) * 6) as i64
            }
        }
    }

    pub(super) fn update_date(&self, channel: Channel, row: i64) -> i32 {
        match channel {
            Channel::Store => self.update_dates[self.store.index(row)],
            Channel::Catalog => self.update_dates[self.catalog.index(row)],
            Channel::Web => self.update_dates[self.web.index(row)],
            Channel::Inventory => self.inventory_dates[self.inventory.index(row)],
        }
    }
}

pub(super) fn date_scaling(scale: f64, channel: Channel, julian: i32) -> i64 {
    let (year, _, _) = jtodt(julian);
    let (mut rows, mut weight_set) = match channel {
        Channel::Store => (source_rows("store_sales", scale) as i64, CALENDAR_SALES),
        Channel::Catalog => (source_rows("catalog_sales", scale) as i64, CALENDAR_SALES),
        Channel::Web => (source_rows("web_sales", scale) as i64, CALENDAR_SALES),
        Channel::Inventory => {
            return (source_rows("warehouse", scale) * id_count("item", scale)) as i64;
        }
    };
    if is_leap(year) {
        weight_set += 1;
    }
    let calendar = calendar();
    let calendar_total = i64::from(calendar.max(weight_set)) * 5;
    let day_weight = i64::from(calendar.weight(day_number(julian), weight_set));
    rows *= day_weight;
    rows += calendar_total / 2;
    rows / calendar_total
}

fn weight_flag(julian: i32, offset: i32, set: usize) -> bool {
    calendar().weight(day_number(julian) + offset, set) != 0
}

fn set_update_dates(rng: &mut Rng, update: i32) -> ([i32; 6], [i32; 6]) {
    let calendar = calendar();
    let mut dates = [0_i32; 6];
    let mut inventory = [0_i32; 6];
    for _ in 0..update {
        for (zone, set) in ZONES.iter().enumerate() {
            let first = 2 * zone;
            dates[first] = calendar.skewed_julian_date(rng, *set, STREAM_ZERO);
            dates[first + 1] = if weight_flag(dates[first], 1, *set) {
                dates[first] + 1
            } else {
                dates[first] - 1
            };
            let thursday = dates[first] + (4 - day_of_week(dates[first]));
            inventory[first] = thursday;
            if !weight_flag(thursday, 0, *set) {
                let earlier = thursday - 7;
                inventory[first] = earlier;
                if !weight_flag(earlier, 0, *set) {
                    inventory[first] += 14;
                }
            }
            inventory[first + 1] = inventory[first] + 7;
            let offset = if zone == 0 { 1 } else { 0 };
            if !weight_flag(inventory[first + 1], offset, *set) {
                inventory[first + 1] -= 14;
            }
        }
    }
    (dates, inventory)
}

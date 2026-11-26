use std::sync::OnceLock;

use tpcdsgen::distribution::DistributionFileLoader;

use super::columns::Col;
use super::rng::Rng;

pub(super) const CALENDAR_UNIFORM: usize = 1;
pub(super) const CALENDAR_SALES: usize = 3;
pub(super) const CALENDAR_LOW: usize = 8;
pub(super) const CALENDAR_MEDIUM: usize = 9;
pub(super) const CALENDAR_HIGH: usize = 10;

pub(super) const YEAR_MINIMUM: i32 = 1998;
pub(super) const YEAR_MAXIMUM: i32 = 2002;
pub(super) const DATA_START_DATE: i32 = 2_450_815;
pub(super) const DATA_END_DATE: i32 = 2_453_005;
pub(super) const TODAYS_DATE: i32 = 2_452_648;

const M_DAYS: [[i32; 13]; 2] = [
    [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];

pub(super) struct Calendar {
    day: Vec<i32>,
    month: Vec<i32>,
    cumulative: Vec<Vec<i32>>,
}

pub(super) fn calendar() -> &'static Calendar {
    static CALENDAR: OnceLock<Calendar> = OnceLock::new();
    CALENDAR.get_or_init(|| {
        let lines = DistributionFileLoader::load_distribution_file("calendar.dst")
            .expect("calendar.dst is embedded");
        let mut day = Vec::new();
        let mut month = Vec::new();
        let mut cumulative = vec![Vec::new(); 10];
        let mut totals = [0_i32; 10];
        for (values, weights) in lines {
            day.push(values[2].parse().expect("calendar day"));
            month.push(values[4].parse().expect("calendar month"));
            for (set, weight) in weights.iter().enumerate() {
                totals[set] += weight.parse::<i32>().expect("calendar weight");
                cumulative[set].push(totals[set]);
            }
        }
        Calendar {
            day,
            month,
            cumulative,
        }
    })
}

impl Calendar {
    pub(super) fn weight(&self, entry: i32, set: usize) -> i32 {
        let sums = &self.cumulative[set - 1];
        let index = (entry - 1) as usize;
        if index >= sums.len() {
            0
        } else if index == 0 {
            sums[0]
        } else {
            sums[index] - sums[index - 1]
        }
    }

    pub(super) fn max(&self, set: usize) -> i32 {
        *self.cumulative[set - 1].last().expect("calendar weights")
    }

    pub(super) fn pick(&self, rng: &mut Rng, column: Col, set: usize) -> i32 {
        let sums = &self.cumulative[set - 1];
        let level = rng.int(column, 1, *sums.last().expect("calendar weights"));
        (sums.partition_point(|&sum| sum < level) + 1) as i32
    }

    pub(super) fn skewed_julian_date(&self, rng: &mut Rng, set: usize, column: Col) -> i32 {
        let entry = self.pick(rng, column, set);
        let year = rng.int(column, YEAR_MINIMUM, YEAR_MAXIMUM);
        dttoj(
            year,
            self.month[(entry - 1) as usize],
            self.day[(entry - 1) as usize],
        )
    }
}

pub(super) fn is_leap(year: i32) -> bool {
    if year % 100 == 0 {
        (year % 400) % 2 == 0
    } else {
        year % 4 == 0
    }
}

pub(super) fn dttoj(year: i32, month: i32, day: i32) -> i32 {
    let (mut y, mut m) = (year, month);
    if m <= 2 {
        m += 12;
        y -= 1;
    }
    day + (153 * m - 457) / 5 + 365 * y + y.div_euclid(4) - y.div_euclid(100)
        + y.div_euclid(400)
        + 1_721_118
        + 1
}

pub(super) fn jtodt(julian: i32) -> (i32, i32, i32) {
    let mut l = i64::from(julian) + 68_569;
    let n = (4 * l) / 146_097;
    l -= (146_097 * n + 3) / 4;
    let i = (4000 * (l + 1)) / 1_461_001;
    l = l - (1461 * i) / 4 + 31;
    let j = (80 * l) / 2447;
    let day = l - (2447 * j) / 80;
    l = j / 11;
    let month = j + 2 - 12 * l;
    let year = 100 * (n - 49) + i + l;
    (year as i32, month as i32, day as i32)
}

pub(super) fn day_number(julian: i32) -> i32 {
    let (year, month, day) = jtodt(julian);
    M_DAYS[usize::from(is_leap(year))][month as usize] + day
}

pub(super) fn day_of_week(julian: i32) -> i32 {
    let (year, month, day) = jtodt(julian);
    let mut known = [0, 3, 0, 0, 4, 9, 6, 11, 8, 5, 10, 7, 12];
    if is_leap(year) {
        known[1] = 4;
        known[2] = 1;
    }
    let doomsday = [3, 2, 0, 5];
    let mut anchor = (year / 100 - 15) % 4;
    anchor = doomsday[anchor as usize];
    let q = year % 100;
    let r = q % 12;
    anchor += q / 12 + r + r / 4;
    anchor %= 7;
    let mut res = day - known[month as usize];
    while res < 0 {
        res += 7;
    }
    while res > 6 {
        res -= 7;
    }
    (res + anchor) % 7
}

pub(super) fn date_string(julian: i32) -> String {
    let (year, month, day) = jtodt(julian);
    format!("{year:04}-{month:02}-{day:02}")
}

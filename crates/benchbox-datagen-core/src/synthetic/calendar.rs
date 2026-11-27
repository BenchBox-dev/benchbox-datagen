pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

pub(crate) fn weekday_from_monday(days: i64) -> i64 {
    (days + 3).rem_euclid(7)
}

pub(crate) fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

pub(crate) fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn iso_long_year(year: i64) -> bool {
    let p = |y: i64| (y + y / 4 - y / 100 + y / 400).rem_euclid(7);
    p(year) == 4 || p(year - 1) == 3
}

pub(crate) fn iso_week(days: i64) -> i64 {
    let (year, _, _) = civil_from_days(days);
    let ordinal = days - days_from_civil(year, 1, 1) + 1;
    let weekday = weekday_from_monday(days) + 1;
    let week = (ordinal - weekday + 10) / 7;
    if week < 1 {
        if iso_long_year(year - 1) { 53 } else { 52 }
    } else if week == 53 && !iso_long_year(year) {
        1
    } else {
        week
    }
}

pub(crate) fn date_key(days: i64) -> i64 {
    let (year, month, day) = civil_from_days(days);
    year * 10_000 + month * 100 + day
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_conversion_round_trips_and_matches_known_dates() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1992, 1, 1), 8_035);
        for days in (-1_000..40_000).step_by(7) {
            let (year, month, day) = civil_from_days(days);
            assert_eq!(days_from_civil(year, month, day), days);
        }
        assert_eq!(civil_from_days(days_from_civil(1996, 2, 29)), (1996, 2, 29));
        assert_eq!(weekday_from_monday(days_from_civil(1992, 1, 1)), 2);
    }

    #[test]
    fn iso_weeks_match_the_standard_at_year_boundaries() {
        let week = |y, m, d| iso_week(days_from_civil(y, m, d));
        assert_eq!(week(1992, 1, 1), 1);
        assert_eq!(week(1993, 1, 1), 53);
        assert_eq!(week(1993, 1, 4), 1);
        assert_eq!(week(1998, 12, 31), 53);
        assert_eq!(week(1996, 12, 30), 1);
        assert_eq!(week(1994, 1, 2), 52);
        assert_eq!(week(1995, 6, 15), 24);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Date {
    pub(crate) year: i64,
    pub(crate) month: i64,
    pub(crate) day: i64,
}

pub(crate) fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 => 28 + i64::from(is_leap(year)),
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn scan_number(text: &str, from: usize, limit: usize) -> Option<(i64, usize)> {
    let bytes = text.as_bytes();
    let mut at = from;
    while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
        at += 1;
    }
    let start = at;
    if matches!(bytes.get(at), Some(b'-' | b'+')) {
        at += 1;
    }
    let digits = at;
    while bytes.get(at).is_some_and(u8::is_ascii_digit) && at - start < limit {
        at += 1;
    }
    if at == digits {
        return None;
    }
    text[start..at].parse().ok().map(|value| (value, at))
}

impl Date {
    pub(crate) fn julian(self) -> i64 {
        let mut year = self.year;
        let mut month = self.month;
        if month <= 2 {
            month += 12;
            year -= 1;
        }
        self.day + (153 * month - 457) / 5 + 365 * year + year.div_euclid(4) - year.div_euclid(100)
            + year.div_euclid(400)
            + 1_721_119
    }

    pub(crate) fn from_julian(julian: i64) -> Option<Self> {
        if julian < 0 {
            return None;
        }
        let mut l = julian + 68_569;
        let n = (4 * l) / 146_097;
        l -= (146_097 * n + 3) / 4;
        let i = (4000 * (l + 1)) / 1_461_001;
        l = l - (1461 * i) / 4 + 31;
        let j = (80 * l) / 2447;
        let day = l - (2447 * j) / 80;
        let carry = j / 11;
        Some(Self {
            year: 100 * (n - 49) + i + carry,
            month: j + 2 - 12 * carry,
            day,
        })
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        let bytes = text.as_bytes();
        let (year, at) = scan_number(text, 0, 4)?;
        if bytes.get(at) != Some(&b'-') {
            return None;
        }
        let (month, at) = scan_number(text, at + 1, usize::MAX)?;
        if bytes.get(at) != Some(&b'-') {
            return None;
        }
        let (day, _) = scan_number(text, at + 1, usize::MAX)?;
        if !(1..=12).contains(&month) || !(1..=days_in_month(year, month)).contains(&day) {
            return None;
        }
        Some(Self { year, month, day })
    }

    pub(crate) fn format(self) -> String {
        format!("{:4}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_real_calendar_dates_parse() {
        assert_eq!(
            Date::parse("2000-02-29"),
            Some(Date {
                year: 2000,
                month: 2,
                day: 29
            })
        );
        for text in [
            "2001-02-29",
            "2000-00-10",
            "2000-13-01",
            "2000-04-31",
            "2000-01-00",
            "2000-01-32",
            "2000-01-999999999999",
            "2000-01-99999999999999999999",
        ] {
            assert_eq!(Date::parse(text), None, "{text}");
        }
    }
}

use thiserror::Error;

const MODULUS: i64 = 2_147_483_647;
const MULTIPLIER: i64 = 16_807;
const QUERIES_PER_SET: usize = 22;
const Q11_FRACTION: f64 = 0.0001;
const Q3_DATE_OFFSET: i64 = 1155;
const MAX_DISTINCT_ATTEMPTS: usize = 100_000;
const MAX_PARAMETERS: usize = 10;

const DEFAULTS: [&[&str]; 22] = [
    &["90"],
    &["15", "BRASS", "EUROPE"],
    &["BUILDING", "1995-03-15"],
    &["1993-07-01"],
    &["ASIA", "1994-01-01"],
    &["1994-01-01", ".06", "24"],
    &["FRANCE", "GERMANY"],
    &["BRAZIL", "AMERICA", "ECONOMY ANODIZED STEEL"],
    &["green"],
    &["1993-10-01"],
    &["GERMANY", "0.0001"],
    &["MAIL", "SHIP", "1994-01-01"],
    &["special", "requests"],
    &["1995-09-01"],
    &["1996-01-01"],
    &[
        "Brand#45",
        "MEDIUM POLISHED",
        "49",
        "14",
        "23",
        "45",
        "19",
        "3",
        "36",
        "9",
    ],
    &["Brand#23", "MED BOX"],
    &["300"],
    &["Brand#12", "Brand#23", "Brand#34", "1", "10", "20"],
    &["forest", "1994-01-01", "CANADA"],
    &["SAUDI ARABIA"],
    &["13", "31", "23", "29", "30", "18", "17"],
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QgenError {
    #[error("unsupported TPC-H query {0:?}")]
    UnsupportedQuery(String),
    #[error(
        "a seed or default_params=true is required; qgen's time-based seeding is not reproducible"
    )]
    SeedRequired,
    #[error("seed {0} is outside the supported range 0..=2147483646")]
    SeedOutOfRange(i64),
    #[error("distribution {0:?} was not found in the supplied distributions text")]
    MissingDistribution(String),
    #[error("distribution {0:?} is malformed")]
    MalformedDistribution(String),
    #[error(
        "qgen does not terminate for query {query} with seed {seed}: its distinct-value draw never changes"
    )]
    NonTerminating { query: u32, seed: i64 },
    #[error("scale factor must be finite and positive")]
    InvalidScale,
}

#[derive(Debug, Clone)]
pub struct QueryRequest<'a> {
    pub query: &'a str,
    pub template: &'a str,
    pub distributions: &'a str,
    pub scale_factor: f64,
    pub seed: Option<i64>,
    pub stream: Option<u32>,
    pub default_params: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedQuery {
    pub text: String,
    pub params: Vec<String>,
}

struct Distribution {
    entries: Vec<(String, i64)>,
}

impl Distribution {
    fn parse(source: &str, name: &str) -> Result<Self, QgenError> {
        let mut inside = false;
        let mut count = None;
        let mut entries: Vec<(String, i64)> = Vec::new();
        let mut max = 0_i64;
        for raw in source.lines() {
            let line = raw.split('#').next().unwrap_or("");
            if line.trim().is_empty() {
                continue;
            }
            if !inside {
                let mut words = line.split_whitespace();
                if words
                    .next()
                    .is_some_and(|word| word.eq_ignore_ascii_case("begin"))
                    && words
                        .next()
                        .is_some_and(|word| word.eq_ignore_ascii_case(name))
                {
                    inside = true;
                }
                continue;
            }
            if line
                .get(..3)
                .is_some_and(|word| word.eq_ignore_ascii_case("end"))
            {
                break;
            }
            let Some((token, weight)) = line.split_once('|') else {
                continue;
            };
            let Ok(weight) = weight.trim().parse::<i64>() else {
                continue;
            };
            if token.eq_ignore_ascii_case("count") {
                count = Some(weight as usize);
                continue;
            }
            max += weight;
            entries.push((token.to_owned(), max));
        }
        if !inside {
            return Err(QgenError::MissingDistribution(name.to_owned()));
        }
        if entries.is_empty() || count != Some(entries.len()) {
            return Err(QgenError::MalformedDistribution(name.to_owned()));
        }
        Ok(Self { entries })
    }

    fn max(&self) -> i64 {
        self.entries.last().map_or(0, |entry| entry.1)
    }
}

struct Distributions {
    categories: Distribution,
    colors: Distribution,
    types: Distribution,
    nations: Distribution,
    nations2: Distribution,
    regions: Distribution,
    ship_modes: Distribution,
    segments: Distribution,
    q13a: Distribution,
    q13b: Distribution,
}

impl Distributions {
    fn parse(source: &str) -> Result<Self, QgenError> {
        Ok(Self {
            categories: Distribution::parse(source, "p_cntr")?,
            colors: Distribution::parse(source, "colors")?,
            types: Distribution::parse(source, "p_types")?,
            nations: Distribution::parse(source, "nations")?,
            nations2: Distribution::parse(source, "nations2")?,
            regions: Distribution::parse(source, "regions")?,
            ship_modes: Distribution::parse(source, "smode")?,
            segments: Distribution::parse(source, "msegmnt")?,
            q13a: Distribution::parse(source, "Q13a")?,
            q13b: Distribution::parse(source, "Q13b")?,
        })
    }
}

struct Rng {
    state: i64,
}

impl Rng {
    fn for_query(seed: i64, query: u32) -> Self {
        let mut state = if seed < 0 { seed + MODULUS } else { seed };
        for _ in 0..query {
            state = next_rand(state);
        }
        Self { state }
    }

    fn uniform(&mut self, low: i64, high: i64) -> i64 {
        let range = (high - low + 1) as f64;
        self.state = next_rand(self.state);
        low + ((self.state as f64 / MODULUS as f64) * range) as i64
    }

    fn pick<'a>(&mut self, distribution: &'a Distribution) -> (usize, &'a str) {
        let target = self.uniform(1, distribution.max());
        let index = distribution
            .entries
            .iter()
            .position(|entry| entry.1 >= target)
            .unwrap_or(distribution.entries.len() - 1);
        (index, &distribution.entries[index].0)
    }

    fn permute(&mut self, values: &mut [i64]) {
        let last = values.len() as i64 - 1;
        for index in 0..values.len() {
            let source = self.uniform(index as i64, last) as usize;
            values.swap(source, index);
        }
    }
}

fn next_rand(state: i64) -> i64 {
    (state * MULTIPLIER) % MODULUS
}

fn brand(rng: &mut Rng) -> String {
    let first = rng.uniform(1, 5);
    let second = rng.uniform(1, 5);
    format!("Brand#{first}{second}")
}

fn year_start(rng: &mut Rng) -> String {
    format!("19{:02}-01-01", rng.uniform(93, 97))
}

fn month_start(rng: &mut Rng, low: i64, high: i64) -> String {
    let offset = rng.uniform(low, high);
    format!("19{:02}-{:02}-01", 93 + offset / 12, offset % 12 + 1)
}

fn civil_from_days(days_since_1992: i64) -> (i64, i64, i64) {
    let z = days_since_1992 + 8035 + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn last_word_dropped(text: &str) -> String {
    text.rsplit_once(' ')
        .map_or_else(String::new, |(head, _)| head.to_owned())
}

fn distinct_pick(
    rng: &mut Rng,
    distribution: &Distribution,
    avoid: usize,
    query: u32,
    seed: i64,
) -> Result<String, QgenError> {
    for _ in 0..MAX_DISTINCT_ATTEMPTS {
        let (index, text) = rng.pick(distribution);
        if index != avoid {
            return Ok(text.to_owned());
        }
    }
    Err(QgenError::NonTerminating { query, seed })
}

fn generate_parameters(
    query: u32,
    seed: i64,
    scale_factor: f64,
    dists: &Distributions,
) -> Result<Vec<String>, QgenError> {
    let mut rng = Rng::for_query(seed, query);
    let rng = &mut rng;
    let params = match query {
        1 => vec![rng.uniform(60, 120).to_string()],
        2 => {
            let size = rng.uniform(1, 50).to_string();
            let kind = rng.pick(&dists.types).1.to_owned();
            let suffix = kind.rsplit(' ').next().unwrap_or("").to_owned();
            let region = rng.pick(&dists.regions).1.to_owned();
            vec![size, suffix, region]
        }
        3 => {
            let segment = rng.pick(&dists.segments).1.to_owned();
            let offset = rng.uniform(0, 30);
            let (year, month, day) = civil_from_days(Q3_DATE_OFFSET + offset);
            vec![segment, format!("{year:04}-{month:02}-{day:02}")]
        }
        4 => vec![month_start(rng, 0, 57)],
        5 => {
            let region = rng.pick(&dists.regions).1.to_owned();
            vec![region, year_start(rng)]
        }
        6 => {
            let date = year_start(rng);
            let discount = format!("0.{:02}", rng.uniform(2, 9));
            vec![date, discount, rng.uniform(24, 25).to_string()]
        }
        7 => {
            let (first_index, first) = rng.pick(&dists.nations2);
            let first = first.to_owned();
            let second = distinct_pick(rng, &dists.nations2, first_index, query, seed)?;
            vec![first, second]
        }
        8 => {
            let (index, nation) = rng.pick(&dists.nations2);
            let nation = nation.to_owned();
            let region = dists
                .nations
                .entries
                .get(index)
                .and_then(|entry| usize::try_from(entry.1).ok())
                .and_then(|region_index| dists.regions.entries.get(region_index))
                .map(|entry| entry.0.clone())
                .ok_or_else(|| QgenError::MalformedDistribution("nations".to_owned()))?;
            let kind = rng.pick(&dists.types).1.to_owned();
            vec![nation, region, kind]
        }
        9 => vec![rng.pick(&dists.colors).1.to_owned()],
        10 => vec![month_start(rng, 1, 24)],
        11 => {
            let nation = rng.pick(&dists.nations2).1.to_owned();
            vec![nation, format!("{:11.10}", Q11_FRACTION / scale_factor)]
        }
        12 => {
            let (first_index, first) = rng.pick(&dists.ship_modes);
            let first = first.to_owned();
            let second = distinct_pick(rng, &dists.ship_modes, first_index, query, seed)?;
            vec![first, second, year_start(rng)]
        }
        13 => {
            let first = rng.pick(&dists.q13a).1.to_owned();
            let second = rng.pick(&dists.q13b).1.to_owned();
            vec![first, second]
        }
        14 => vec![month_start(rng, 0, 59)],
        15 => vec![month_start(rng, 0, 57)],
        16 => {
            let brand = brand(rng);
            let kind = last_word_dropped(rng.pick(&dists.types).1);
            let mut sizes: Vec<i64> = (1..=50).collect();
            rng.permute(&mut sizes);
            let mut params = vec![brand, kind];
            params.extend(sizes.iter().take(8).map(i64::to_string));
            params
        }
        17 => {
            let brand = brand(rng);
            let container = rng.pick(&dists.categories).1.to_owned();
            vec![brand, container]
        }
        18 => vec![rng.uniform(312, 315).to_string()],
        19 => {
            let first = brand(rng);
            let second = brand(rng);
            let third = brand(rng);
            vec![
                first,
                second,
                third,
                rng.uniform(1, 10).to_string(),
                rng.uniform(10, 20).to_string(),
                rng.uniform(20, 30).to_string(),
            ]
        }
        20 => {
            let color = rng.pick(&dists.colors).1.to_owned();
            let date = year_start(rng);
            let nation = rng.pick(&dists.nations2).1.to_owned();
            vec![color, date, nation]
        }
        21 => vec![rng.pick(&dists.nations2).1.to_owned()],
        22 => {
            let mut codes: Vec<i64> = (0..25).collect();
            rng.permute(&mut codes);
            codes
                .iter()
                .take(7)
                .map(|code| (10 + code).to_string())
                .collect()
        }
        _ => unreachable!("query validated by the caller"),
    };
    Ok(params)
}

fn default_parameters(query: u32) -> Vec<String> {
    let mut params: Vec<String> = DEFAULTS[query as usize - 1]
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
    if query == 11 {
        params[1] = "0.0001".to_owned();
    }
    params
}

struct CommentStripper {
    in_comment: bool,
}

impl CommentStripper {
    fn strip(&mut self, line: &str) -> String {
        let mut text = line.to_owned();
        if !self.in_comment
            && let Some(dash) = text.find('-')
            && text.as_bytes().get(dash + 1) == Some(&b'-')
        {
            text.truncate(dash);
        }
        loop {
            if !self.in_comment {
                let Some(open) = text.find('{') else {
                    return text;
                };
                self.in_comment = true;
                match text[open + 1..].find('}') {
                    Some(close) => {
                        text.replace_range(..open + 1 + close + 1, "");
                        self.in_comment = false;
                    }
                    None => {
                        text.clear();
                        return text;
                    }
                }
            } else {
                match text.find('}') {
                    Some(close) => {
                        text.replace_range(..=close, "");
                        self.in_comment = false;
                    }
                    None => {
                        text.clear();
                        return text;
                    }
                }
            }
        }
    }
}

fn leading_number(text: &str) -> (i64, usize) {
    let trimmed = text.trim_start();
    let skipped = text.len() - trimmed.len();
    let mut end = 0;
    let bytes = trimmed.as_bytes();
    if matches!(bytes.first(), Some(b'-' | b'+')) {
        end = 1;
    }
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    (trimmed[..end].parse().unwrap_or(0), skipped + end)
}

fn render(template: &str, query: &str, stream: u32, params: &[String]) -> String {
    let mut stripper = CommentStripper { in_comment: false };
    let mut out = String::with_capacity(template.len());
    for raw in template.split_inclusive('\n') {
        let line = stripper.strip(raw);
        let mut rest = line.as_str();
        'line: loop {
            let Some(position) = rest.find(':') else {
                out.push_str(rest);
                break;
            };
            let prefix = &rest[..position];
            out.push_str(prefix);
            let after = &rest[position + 1..];
            let mut chars = after.chars();
            let tag = chars.next();
            match tag {
                Some('n' | 'N') => {
                    let (count, _) = leading_number(&after[1..]);
                    out.push_str(&format!("where rownum <= {count};\n"));
                    out.push_str(prefix);
                    break 'line;
                }
                Some('q' | 'Q') => {
                    out.push_str(query);
                    rest = &after[1..];
                }
                Some('s' | 'S') => {
                    out.push_str(&stream.to_string());
                    rest = &after[1..];
                }
                Some('b' | 'B' | 'c' | 'C' | 'e' | 'E' | 'o' | 'O' | 'x' | 'X') => {
                    rest = &after[1..];
                }
                Some(digit) if digit.is_ascii_digit() && digit != '0' => {
                    let digits = after.bytes().take_while(u8::is_ascii_digit).count();
                    let index: usize = after[..digits].parse().unwrap_or(0);
                    if let Some(value) = params.get(index.wrapping_sub(1)) {
                        out.push_str(value);
                    }
                    rest = &after[digits..];
                }
                Some(other) => {
                    rest = &after[other.len_utf8()..];
                }
                None => {
                    rest = "";
                }
            }
        }
    }
    out
}

pub fn generate_query(request: &QueryRequest<'_>) -> Result<GeneratedQuery, QgenError> {
    let number = request
        .query
        .trim_end_matches(|character: char| character.is_ascii_alphabetic())
        .parse::<u32>()
        .ok()
        .filter(|number| (1..=QUERIES_PER_SET as u32).contains(number))
        .ok_or_else(|| QgenError::UnsupportedQuery(request.query.to_owned()))?;
    if !request.scale_factor.is_finite() || request.scale_factor <= 0.0 {
        return Err(QgenError::InvalidScale);
    }
    let params = if request.default_params {
        default_parameters(number)
    } else {
        let seed = request.seed.ok_or(QgenError::SeedRequired)?;
        if !(0..MODULUS).contains(&seed) {
            return Err(QgenError::SeedOutOfRange(seed));
        }
        let dists = Distributions::parse(request.distributions)?;
        generate_parameters(number, seed, request.scale_factor, &dists)?
    };
    debug_assert!(params.len() <= MAX_PARAMETERS);
    let mut substitutions = params.clone();
    if request.default_params && number == 11 {
        substitutions[1] = format!("{:11.10}", Q11_FRACTION / request.scale_factor);
    }
    let text = render(
        request.template,
        request.query,
        request.stream.unwrap_or(0),
        &substitutions,
    );
    Ok(GeneratedQuery { text, params })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates_start_at_1992() {
        assert_eq!(civil_from_days(0), (1992, 1, 1));
        assert_eq!(civil_from_days(Q3_DATE_OFFSET), (1995, 3, 1));
        assert_eq!(civil_from_days(Q3_DATE_OFFSET + 14), (1995, 3, 15));
    }

    #[test]
    fn lcg_matches_qgen_recurrence() {
        assert_eq!(next_rand(1), 16_807);
        assert_eq!(next_rand(0), 0);
    }

    #[test]
    fn seed_or_defaults_are_required() {
        let request = QueryRequest {
            query: "6",
            template: "",
            distributions: "",
            scale_factor: 1.0,
            seed: None,
            stream: None,
            default_params: false,
        };
        assert_eq!(generate_query(&request), Err(QgenError::SeedRequired));
    }

    #[test]
    fn comments_drop_from_line_start_through_the_closing_brace() {
        let mut stripper = CommentStripper { in_comment: false };
        assert_eq!(stripper.strip("keep { drop } tail\n"), " tail\n");
        assert!(!stripper.in_comment);
        assert_eq!(stripper.strip("before { open\n"), "");
        assert!(stripper.in_comment);
        assert_eq!(stripper.strip("still inside\n"), "");
        assert_eq!(stripper.strip("gone } after\n"), " after\n");
        assert!(!stripper.in_comment);
        assert_eq!(stripper.strip("a -- note { b\n"), "a ");
        assert!(!stripper.in_comment);
    }

    #[test]
    fn only_the_first_dash_is_checked_and_each_comment_on_a_line_is_removed() {
        let mut stripper = CommentStripper { in_comment: false };
        assert_eq!(stripper.strip("a - b -- c\n"), "a - b -- c\n");
        assert_eq!(stripper.strip("a { x } b { y } c\n"), " c\n");
        assert!(!stripper.in_comment);
        assert_eq!(stripper.strip("a { -- } b\n"), "");
        assert!(stripper.in_comment);
    }

    #[test]
    fn malformed_distributions_are_errors_not_panics() {
        let malformed = [
            "begin colors\ncount|2\na|1\n",
            "begin colors\n\u{e9}\u{e9}\nen",
            "begin colors\nen\n",
            "begin colors\ncount|0\nend\n",
            "begin colors\ncount|-1\na|1\nend\n",
        ];
        for source in malformed {
            assert_eq!(
                Distribution::parse(source, "colors").err(),
                Some(QgenError::MalformedDistribution("colors".to_owned())),
                "{source:?}"
            );
        }
        let multibyte = Distribution::parse("begin colors\ncount|1\n\u{e9}|1\nend\n", "colors");
        assert_eq!(multibyte.unwrap().entries, [("\u{e9}".to_owned(), 1)]);
    }

    #[test]
    fn distributions_text_never_panics_for_any_query() {
        for query in 1..=22 {
            for text in ["", "begin colors\n\u{e9}", "begin p_cntr\ncount|1\n"] {
                let request = QueryRequest {
                    query: &query.to_string(),
                    template: "",
                    distributions: text,
                    scale_factor: 1.0,
                    seed: Some(1),
                    stream: None,
                    default_params: false,
                };
                assert!(generate_query(&request).is_err());
            }
        }
    }

    #[test]
    fn default_parameters_need_no_distributions() {
        let request = QueryRequest {
            query: "6",
            template: "x :1 :2 :3\n:n -1\n",
            distributions: "",
            scale_factor: 1.0,
            seed: None,
            stream: None,
            default_params: true,
        };
        let query = generate_query(&request).unwrap();
        assert_eq!(query.params, ["1994-01-01", ".06", "24"]);
        assert_eq!(query.text, "x 1994-01-01 .06 24\nwhere rownum <= -1;\n");
    }
}

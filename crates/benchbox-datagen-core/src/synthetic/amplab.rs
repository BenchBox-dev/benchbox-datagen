use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int64Array, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::calendar::days_from_civil;
use super::rng::{Draw, Stream};
use super::{BLOCK_ROWS, Generator, Table, block_len};
use crate::generate::KeyRange;
use crate::schema::{self, Column, Date, Decimal128, Int, Text};

const BENCHMARK: &str = "amplab";
const RANKINGS_PER_SCALE: u64 = 250_000;
const USERVISITS_PER_SCALE: u64 = 2_500_000;
const DOCUMENTS_PER_SCALE: u64 = 125_000;
const EXTRA_DESTINATIONS: i64 = 50_000;
const VISIT_DAYS: i64 = 90;
const MIN_RANK: i64 = 10;
const MAX_RANK: i64 = 10_000;
const PARETO_SHAPE: f64 = 1.16;

const DOMAINS: [&str; 15] = [
    "example.com",
    "website.org",
    "portal.net",
    "site.co",
    "page.info",
    "blog.me",
    "news.tv",
    "shop.biz",
    "forum.us",
    "wiki.edu",
    "social.io",
    "tech.ai",
    "data.ml",
    "cloud.dev",
    "app.xyz",
];
const PATHS: [&str; 6] = ["page", "article", "post", "item", "content", "view"];
const COUNTRIES: [&str; 20] = [
    "USA", "CHN", "IND", "BRA", "RUS", "JPN", "DEU", "GBR", "FRA", "ITA", "CAN", "KOR", "ESP",
    "AUS", "MEX", "IDN", "NLD", "SAU", "TUR", "CHE",
];
const LANGUAGES: [&str; 14] = [
    "en-US", "zh-CN", "hi-IN", "pt-BR", "ru-RU", "ja-JP", "de-DE", "en-GB", "fr-FR", "it-IT",
    "en-CA", "ko-KR", "es-ES", "en-AU",
];
const USER_AGENTS: [&str; 5] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)",
    "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0",
];
const SEARCH_WORDS: [&str; 22] = [
    "database",
    "analytics",
    "web",
    "data",
    "search",
    "engine",
    "query",
    "information",
    "processing",
    "system",
    "technology",
    "software",
    "application",
    "service",
    "platform",
    "solution",
    "business",
    "science",
    "machine",
    "learning",
    "artificial",
    "intelligence",
];
const CONTENT_WORDS: [&str; 24] = [
    "web",
    "data",
    "system",
    "information",
    "technology",
    "computer",
    "internet",
    "digital",
    "online",
    "software",
    "application",
    "service",
    "platform",
    "solution",
    "business",
    "analysis",
    "processing",
    "query",
    "database",
    "search",
    "engine",
    "algorithm",
    "machine",
    "learning",
];

const COLUMNS: [(&str, &[Column]); 3] = [
    (
        "rankings",
        &[
            ("pageURL", Text, true),
            ("pageRank", Int, true),
            ("avgDuration", Int, true),
        ],
    ),
    (
        "documents",
        &[("url", Text, true), ("contents", Text, true)],
    ),
    (
        "uservisits",
        &[
            ("sourceIP", Text, true),
            ("destURL", Text, true),
            ("visitDate", Date, true),
            ("adRevenue", Decimal128(8, 2), true),
            ("userAgent", Text, true),
            ("countryCode", Text, true),
            ("languageCode", Text, true),
            ("searchWord", Text, true),
            ("duration", Int, true),
        ],
    ),
];

static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> =
    LazyLock::new(|| schema::build(&COLUMNS));

static RANK_CDF: LazyLock<Vec<u64>> = LazyLock::new(rank_cdf);

fn amplab_schema(table: &str) -> SchemaRef {
    SCHEMAS
        .get(table)
        .cloned()
        .expect("every AMPLab table has a schema")
}

pub(super) struct Amplab;

impl Generator for Amplab {
    fn name(&self) -> &'static str {
        "amplab"
    }

    fn tables(&self) -> &'static [&'static str] {
        &["rankings", "documents", "uservisits"]
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        SCHEMAS.get(table).cloned()
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        row_count(table, scale_factor)
    }

    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
        let rows = row_count(table, scale_factor);
        match table {
            "rankings" => Arc::new(Rankings { rows, seed }),
            "documents" => Arc::new(Documents { rows, seed }),
            "uservisits" => Arc::new(Uservisits {
                rows,
                rankings: row_count("rankings", scale_factor),
                seed,
            }),
            other => unreachable!("table {other:?} was validated by the caller"),
        }
    }

    fn key_ranges(&self, _scale_factor: f64) -> Vec<KeyRange> {
        Vec::new()
    }
}

fn row_count(table: &str, scale_factor: f64) -> u64 {
    let base = match table {
        "rankings" => RANKINGS_PER_SCALE,
        "documents" => DOCUMENTS_PER_SCALE,
        "uservisits" => USERVISITS_PER_SCALE,
        other => unreachable!("table {other:?} was validated by the caller"),
    };
    (base as f64 * scale_factor) as u64
}

fn ln(x: f64) -> f64 {
    let bits = x.to_bits();
    let mut exponent = ((bits >> 52) & 0x7ff) as i64 - 1023;
    let mut mantissa = f64::from_bits((bits & 0x000f_ffff_ffff_ffff) | (1023 << 52));
    if mantissa > std::f64::consts::SQRT_2 {
        mantissa *= 0.5;
        exponent += 1;
    }
    let s = (mantissa - 1.0) / (mantissa + 1.0);
    let s2 = s * s;
    let (mut term, mut sum) = (s, 0.0);
    for k in 0..30 {
        sum += term / (2 * k + 1) as f64;
        term *= s2;
    }
    exponent as f64 * std::f64::consts::LN_2 + 2.0 * sum
}

fn exp(y: f64) -> f64 {
    let n = (y / std::f64::consts::LN_2).round();
    let r = y - n * std::f64::consts::LN_2;
    let (mut term, mut sum) = (1.0, 1.0);
    for k in 1..30 {
        term *= r / k as f64;
        sum += term;
    }
    sum * f64::from_bits(((n as i64 + 1023) as u64) << 52)
}

fn rank_cdf() -> Vec<u64> {
    let scale = 18_446_744_073_709_551_616.0_f64;
    (MIN_RANK..MAX_RANK)
        .map(|rank| {
            let tail = exp(PARETO_SHAPE * ln(MIN_RANK as f64 / (rank + 1) as f64));
            ((1.0 - tail) * scale) as u64
        })
        .collect()
}

fn page_rank(draw: &mut Draw) -> i64 {
    let value = draw.u64();
    MIN_RANK + RANK_CDF.partition_point(|upper| *upper <= value) as i64
}

fn url(draw: &mut Draw, id: i64, out: &mut String) {
    let domain = draw.pick(&DOMAINS);
    let path = draw.pick(&PATHS);
    write!(out, "http://{domain}/{path}/{id}").expect("string write");
}

fn int(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn text(mut builder: StringBuilder) -> ArrayRef {
    Arc::new(builder.finish())
}

fn batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("generated columns match the schema")
}

#[derive(Debug)]
struct Rankings {
    rows: u64,
    seed: u64,
}

impl Table for Rankings {
    fn schema(&self) -> SchemaRef {
        amplab_schema("rankings")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut draw = Stream::new(BENCHMARK, "rankings", "fields", self.seed).block(index);
        let first = (index * BLOCK_ROWS) as i64 + 1;
        let mut urls = StringBuilder::new();
        let mut ranks = Vec::with_capacity(rows);
        let mut durations = Vec::with_capacity(rows);
        let mut scratch = String::new();
        for id in first..first + rows as i64 {
            scratch.clear();
            url(&mut draw, id, &mut scratch);
            urls.append_value(&scratch);
            ranks.push(page_rank(&mut draw));
            durations.push(draw.range(30, 600));
        }
        batch(self.schema(), vec![text(urls), int(ranks), int(durations)])
    }
}

#[derive(Debug)]
struct Documents {
    rows: u64,
    seed: u64,
}

impl Table for Documents {
    fn schema(&self) -> SchemaRef {
        amplab_schema("documents")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut draw = Stream::new(BENCHMARK, "documents", "fields", self.seed).block(index);
        let first = (index * BLOCK_ROWS) as i64 + 1;
        let mut urls = StringBuilder::new();
        let mut contents = StringBuilder::new();
        let mut scratch = String::new();
        for id in first..first + rows as i64 {
            scratch.clear();
            url(&mut draw, id, &mut scratch);
            urls.append_value(&scratch);
            let words = if draw.below(10) == 0 {
                draw.range(500, 2000)
            } else if draw.below(10) < 3 {
                draw.range(200, 500)
            } else {
                draw.range(50, 200)
            };
            scratch.clear();
            draw.words(&CONTENT_WORDS, words as usize, " ", &mut scratch);
            contents.append_value(&scratch);
        }
        batch(self.schema(), vec![text(urls), text(contents)])
    }
}

#[derive(Debug)]
struct Uservisits {
    rows: u64,
    rankings: u64,
    seed: u64,
}

impl Table for Uservisits {
    fn schema(&self) -> SchemaRef {
        amplab_schema("uservisits")
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let rows = block_len(self.rows, index);
        let mut draw = Stream::new(BENCHMARK, "uservisits", "fields", self.seed).block(index);
        let first_day = days_from_civil(2000, 1, 1);
        let ranked = self.rankings as i64;
        let mut ips = StringBuilder::new();
        let mut destinations = StringBuilder::new();
        let mut dates = Vec::with_capacity(rows);
        let mut revenues = Vec::with_capacity(rows);
        let mut agents = StringBuilder::new();
        let mut countries = StringBuilder::new();
        let mut languages = StringBuilder::new();
        let mut words = StringBuilder::new();
        let mut durations = Vec::with_capacity(rows);
        let mut scratch = String::new();
        for _ in 0..rows {
            scratch.clear();
            write!(
                scratch,
                "{}.{}.{}.{}",
                draw.range(1, 255),
                draw.range(0, 255),
                draw.range(0, 255),
                draw.range(1, 254)
            )
            .expect("string write");
            ips.append_value(&scratch);
            let id = if draw.below(10) < 8 && ranked > 0 {
                draw.range(1, ranked)
            } else {
                draw.range(ranked + 1, ranked + EXTRA_DESTINATIONS)
            };
            scratch.clear();
            url(&mut draw, id, &mut scratch);
            destinations.append_value(&scratch);
            dates.push((first_day + draw.range(0, VISIT_DAYS)) as i32);
            revenues.push(if draw.below(10) == 0 {
                i128::from((draw.below(998) + 3) / 2)
            } else {
                0
            });
            agents.append_value(draw.pick(&USER_AGENTS));
            countries.append_value(draw.pick(&COUNTRIES));
            languages.append_value(draw.pick(&LANGUAGES));
            if draw.below(2) == 0 {
                words.append_value(draw.pick(&SEARCH_WORDS));
            } else {
                words.append_null();
            }
            durations.push(draw.range(10, 1800));
        }
        let revenue = Decimal128Array::from(revenues)
            .with_precision_and_scale(8, 2)
            .expect("revenue fits the declared precision");
        batch(
            self.schema(),
            vec![
                text(ips),
                text(destinations),
                Arc::new(Date32Array::from(dates)),
                Arc::new(revenue),
                text(agents),
                text(countries),
                text(languages),
                text(words),
                int(durations),
            ],
        )
    }
}

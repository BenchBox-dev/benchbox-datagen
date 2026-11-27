use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Date32Array, Int64Array, StringBuilder, TimestampMicrosecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::calendar::days_from_civil;
use super::delimited::TextFormat;
use super::rng::{Draw, Stream};
use super::{BLOCK_ROWS, Generator, Table, block_len};
use crate::generate::KeyRange;
use crate::schema::{self, Column, Date, Int, Text, Timestamp};

const BENCHMARK: &str = "clickbench";
const HITS_PER_SCALE: u64 = 1_000_000;
const FIRST_DAY: (i64, i64, i64) = (2013, 7, 1);
const EVENT_DAYS: i64 = 30;
const SECONDS_PER_DAY: i64 = 86_400;
const MICROS_PER_SECOND: i64 = 1_000_000;

const SEARCH_PHRASES: [&str; 17] = [
    "google",
    "facebook",
    "youtube",
    "amazon",
    "news",
    "weather",
    "sports",
    "shopping",
    "travel",
    "music",
    "movies",
    "games",
    "finance",
    "health",
    "education",
    "technology",
    "business",
];
const SEARCH_PHRASE_SLOTS: u64 = 68;
const MOBILE_MODELS: [&str; 9] = [
    "iPhone",
    "Samsung Galaxy",
    "Google Pixel",
    "OnePlus",
    "Huawei",
    "Xiaomi",
    "LG",
    "Sony",
    "Nokia",
];
const MOBILE_MODEL_SLOTS: u64 = 30;
const REFERERS: [&str; 6] = [
    "https://google.com/",
    "https://facebook.com/",
    "https://twitter.com/",
    "https://linkedin.com/",
    "https://reddit.com/",
    "https://stackoverflow.com/",
];
const REFERER_SLOTS: u64 = 17;
const URLS: [&str; 8] = [
    "https://example.com/",
    "https://example.com/home",
    "https://example.com/about",
    "https://example.com/products",
    "https://example.com/contact",
    "https://google.com/search",
    "https://facebook.com/feed",
    "https://youtube.com/watch",
];
const TITLES: [&str; 13] = [
    "Home Page",
    "About Us",
    "Products",
    "Services",
    "Contact",
    "News",
    "Blog",
    "Support",
    "Login",
    "Register",
    "Search Results",
    "Product Details",
    "Shopping Cart",
];
const LANGUAGES: [&str; 8] = ["en", "ru", "zh", "es", "fr", "de", "ja", "pt"];
const COUNTRIES: [&str; 8] = ["US", "RU", "CN", "DE", "GB", "FR", "IN", "BR"];
const RESOLUTIONS: [(i64, i64); 8] = [
    (1920, 1080),
    (1366, 768),
    (1280, 720),
    (1440, 900),
    (1024, 768),
    (1600, 900),
    (1280, 1024),
    (1920, 1200),
];
const COLOR_DEPTHS: [i64; 3] = [16, 24, 32];
const HTTP_ERRORS: [i64; 4] = [0, 200, 404, 500];
const HIT_COLORS: [&str; 2] = ["S", "F"];
const CURRENCIES: [&str; 4] = ["USD", "EUR", "RUB", ""];

const COLUMNS: [(&str, &[Column]); 1] = [(
    "hits",
    &[
        ("WatchID", Int, false),
        ("JavaEnable", Int, false),
        ("Title", Text, false),
        ("GoodEvent", Int, false),
        ("EventTime", Timestamp, false),
        ("EventDate", Date, false),
        ("CounterID", Int, false),
        ("ClientIP", Int, false),
        ("RegionID", Int, false),
        ("UserID", Int, false),
        ("CounterClass", Int, false),
        ("OS", Int, false),
        ("UserAgent", Int, false),
        ("URL", Text, false),
        ("Referer", Text, false),
        ("IsRefresh", Int, false),
        ("RefererCategoryID", Int, false),
        ("RefererRegionID", Int, false),
        ("URLCategoryID", Int, false),
        ("URLRegionID", Int, false),
        ("ResolutionWidth", Int, false),
        ("ResolutionHeight", Int, false),
        ("ResolutionDepth", Int, false),
        ("FlashMajor", Int, false),
        ("FlashMinor", Int, false),
        ("FlashMinor2", Text, false),
        ("NetMajor", Int, false),
        ("NetMinor", Int, false),
        ("UserAgentMajor", Int, false),
        ("UserAgentMinor", Text, false),
        ("CookieEnable", Int, false),
        ("JavascriptEnable", Int, false),
        ("IsMobile", Int, false),
        ("MobilePhone", Int, false),
        ("MobilePhoneModel", Text, false),
        ("Params", Text, false),
        ("IPNetworkID", Int, false),
        ("TraficSourceID", Int, false),
        ("SearchEngineID", Int, false),
        ("SearchPhrase", Text, false),
        ("AdvEngineID", Int, false),
        ("IsArtifical", Int, false),
        ("WindowClientWidth", Int, false),
        ("WindowClientHeight", Int, false),
        ("ClientTimeZone", Int, false),
        ("ClientEventTime", Timestamp, false),
        ("SilverlightVersion1", Int, false),
        ("SilverlightVersion2", Int, false),
        ("SilverlightVersion3", Int, false),
        ("SilverlightVersion4", Int, false),
        ("PageCharset", Text, false),
        ("CodeVersion", Int, false),
        ("IsLink", Int, false),
        ("IsDownload", Int, false),
        ("IsNotBounce", Int, false),
        ("FUniqID", Int, false),
        ("OriginalURL", Text, false),
        ("HID", Int, false),
        ("IsOldCounter", Int, false),
        ("IsEvent", Int, false),
        ("IsParameter", Int, false),
        ("DontCountHits", Int, false),
        ("WithHash", Int, false),
        ("HitColor", Text, false),
        ("LocalEventTime", Timestamp, false),
        ("Age", Int, false),
        ("Sex", Int, false),
        ("Income", Int, false),
        ("Interests", Int, false),
        ("Robotness", Int, false),
        ("RemoteIP", Int, false),
        ("WindowName", Int, false),
        ("OpenerName", Int, false),
        ("HistoryLength", Int, false),
        ("BrowserLanguage", Text, false),
        ("BrowserCountry", Text, false),
        ("SocialNetwork", Text, false),
        ("SocialAction", Text, false),
        ("HTTPError", Int, false),
        ("SendTiming", Int, false),
        ("DNSTiming", Int, false),
        ("ConnectTiming", Int, false),
        ("ResponseStartTiming", Int, false),
        ("ResponseEndTiming", Int, false),
        ("FetchTiming", Int, false),
        ("SocialSourceNetworkID", Int, false),
        ("SocialSourcePage", Text, false),
        ("ParamPrice", Int, false),
        ("ParamOrderID", Text, false),
        ("ParamCurrency", Text, false),
        ("ParamCurrencyID", Int, false),
        ("OpenstatServiceName", Text, false),
        ("OpenstatCampaignID", Text, false),
        ("OpenstatAdID", Text, false),
        ("OpenstatSourceID", Text, false),
        ("UTMSource", Text, false),
        ("UTMMedium", Text, false),
        ("UTMCampaign", Text, false),
        ("UTMContent", Text, false),
        ("UTMTerm", Text, false),
        ("FromTag", Text, false),
        ("HasGCLID", Int, false),
        ("RefererHash", Int, false),
        ("URLHash", Int, false),
        ("CLID", Int, false),
    ],
)];

static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> =
    LazyLock::new(|| schema::build(&COLUMNS));

fn hits_schema() -> SchemaRef {
    SCHEMAS
        .get("hits")
        .cloned()
        .expect("clickbench has a hits schema")
}

pub(super) struct Clickbench;

impl Generator for Clickbench {
    fn name(&self) -> &'static str {
        BENCHMARK
    }

    fn tables(&self) -> &'static [&'static str] {
        &["hits"]
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        SCHEMAS.get(table).cloned()
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        row_count(table, scale_factor)
    }

    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
        Arc::new(Hits {
            rows: row_count(table, scale_factor),
            stream: Stream::new(BENCHMARK, "hits", "fields", seed),
        })
    }

    fn key_ranges(&self, _scale_factor: f64) -> Vec<KeyRange> {
        Vec::new()
    }

    fn text_format(&self, _table: &str) -> TextFormat {
        TextFormat::csv(b'|', false)
    }
}

fn row_count(table: &str, scale_factor: f64) -> u64 {
    match table {
        "hits" => ((HITS_PER_SCALE as f64 * scale_factor) as u64).max(1),
        other => unreachable!("table {other:?} was validated by the caller"),
    }
}

#[derive(Debug)]
struct Hits {
    rows: u64,
    stream: Stream,
}

fn ints(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn uniform(draw: &mut Draw, rows: usize, low: i64, high: i64) -> Vec<i64> {
    (0..rows).map(|_| draw.range(low, high)).collect()
}

fn one_of(draw: &mut Draw, rows: usize, values: &[i64]) -> Vec<i64> {
    (0..rows).map(|_| *draw.pick(values)).collect()
}

fn constant(rows: usize, value: i64) -> Vec<i64> {
    vec![value; rows]
}

fn slots(draw: &mut Draw, rows: usize, slots: u64) -> Vec<usize> {
    (0..rows).map(|_| draw.below(slots) as usize).collect()
}

fn text_of(picks: &[usize], word: impl Fn(usize) -> &'static str) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(picks.len(), picks.len() * 16);
    for pick in picks {
        builder.append_value(word(*pick));
    }
    Arc::new(builder.finish())
}

fn from_list(picks: &[usize], list: &'static [&'static str]) -> ArrayRef {
    text_of(picks, |pick| list[pick])
}

fn padded(picks: &[usize], list: &'static [&'static str]) -> ArrayRef {
    text_of(picks, |pick| {
        if (1..=list.len()).contains(&pick) {
            list[pick - 1]
        } else {
            ""
        }
    })
}

fn constant_text(rows: usize, value: &str) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(rows, rows * value.len());
    for _ in 0..rows {
        builder.append_value(value);
    }
    Arc::new(builder.finish())
}

impl Table for Hits {
    fn schema(&self) -> SchemaRef {
        hits_schema()
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        debug_assert!(index * BLOCK_ROWS < self.rows);
        let rows = block_len(self.rows, index);
        let mut stream = self.stream.block(index);
        let draw = &mut stream;
        let first_day = days_from_civil(FIRST_DAY.0, FIRST_DAY.1, FIRST_DAY.2);
        let mut seconds = Vec::with_capacity(rows);
        for _ in 0..rows {
            let day = draw.range(0, EVENT_DAYS);
            let hour = draw.range(0, 23);
            let minute = draw.range(0, 59);
            let second = draw.range(0, 59);
            seconds.push((first_day + day) * SECONDS_PER_DAY + hour * 3_600 + minute * 60 + second);
        }
        let timestamps = || -> ArrayRef {
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                seconds.iter().map(|value| value * MICROS_PER_SECOND),
            ))
        };
        let event_dates: ArrayRef = Arc::new(Date32Array::from_iter_values(
            seconds
                .iter()
                .map(|value| value.div_euclid(SECONDS_PER_DAY) as i32),
        ));
        let regions = uniform(draw, rows, 1, 1_000);
        let resolutions = slots(draw, rows, RESOLUTIONS.len() as u64);
        let mobile = uniform(draw, rows, 0, 1);
        let model_picks = slots(draw, rows, MOBILE_MODEL_SLOTS);
        let url_picks = slots(draw, rows, URLS.len() as u64);
        let title_picks = slots(draw, rows, TITLES.len() as u64);
        let referer_picks = slots(draw, rows, REFERER_SLOTS);
        let phrase_picks = slots(draw, rows, SEARCH_PHRASE_SLOTS);
        let models: Vec<usize> = mobile
            .iter()
            .zip(&model_picks)
            .map(|(mobile, pick)| if *mobile == 1 { *pick } else { 0 })
            .collect();
        let width = |picks: &[usize]| -> Vec<i64> {
            picks.iter().map(|pick| RESOLUTIONS[*pick].0).collect()
        };
        let height = |picks: &[usize]| -> Vec<i64> {
            picks.iter().map(|pick| RESOLUTIONS[*pick].1).collect()
        };
        let urls = from_list(&url_picks, &URLS);
        let columns: Vec<ArrayRef> = vec![
            ints(uniform(draw, rows, 1, 10_000_000)),
            ints(uniform(draw, rows, 0, 1)),
            from_list(&title_picks, &TITLES),
            ints(constant(rows, 1)),
            timestamps(),
            event_dates,
            ints(uniform(draw, rows, 1, 1_000)),
            ints(uniform(draw, rows, 1, 4_294_967_295)),
            ints(regions.clone()),
            ints(uniform(draw, rows, 1, 1_000_000)),
            ints(constant(rows, 0)),
            ints(uniform(draw, rows, 1, 50)),
            ints(uniform(draw, rows, 1, 1_000)),
            urls.clone(),
            padded(&referer_picks, &REFERERS),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 100)),
            ints(regions.clone()),
            ints(uniform(draw, rows, 0, 100)),
            ints(regions),
            ints(width(&resolutions)),
            ints(height(&resolutions)),
            ints(one_of(draw, rows, &COLOR_DEPTHS)),
            ints(uniform(draw, rows, 0, 20)),
            ints(uniform(draw, rows, 0, 20)),
            constant_text(rows, ""),
            ints(uniform(draw, rows, 0, 10)),
            ints(uniform(draw, rows, 0, 20)),
            ints(uniform(draw, rows, 1, 100)),
            constant_text(rows, ""),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            ints(mobile.clone()),
            ints(mobile),
            padded(&models, &MOBILE_MODELS),
            constant_text(rows, ""),
            ints(uniform(draw, rows, 1, 65_535)),
            ints(uniform(draw, rows, -1, 10)),
            ints(uniform(draw, rows, 0, 50)),
            padded(&phrase_picks, &SEARCH_PHRASES),
            ints(uniform(draw, rows, 0, 20)),
            ints(uniform(draw, rows, 0, 1)),
            ints(width(&resolutions)),
            ints(height(&resolutions)),
            ints(uniform(draw, rows, -12, 12)),
            timestamps(),
            ints(uniform(draw, rows, 0, 5)),
            ints(uniform(draw, rows, 0, 50)),
            ints(uniform(draw, rows, 0, 50_000)),
            ints(uniform(draw, rows, 0, 100)),
            constant_text(rows, "UTF-8"),
            ints(uniform(draw, rows, 1, 1_000)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 1, 999_999_999_999_999_999)),
            urls,
            ints(uniform(draw, rows, 1, 2_147_483_647)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 0, 1)),
            from_list(&slots(draw, rows, HIT_COLORS.len() as u64), &HIT_COLORS),
            timestamps(),
            ints(uniform(draw, rows, 18, 65)),
            ints(uniform(draw, rows, 0, 2)),
            ints(uniform(draw, rows, 0, 10)),
            ints(uniform(draw, rows, 0, 65_535)),
            ints(uniform(draw, rows, 0, 255)),
            ints(uniform(draw, rows, 1, 4_294_967_295)),
            ints(uniform(draw, rows, 1, 2_147_483_647)),
            ints(uniform(draw, rows, 1, 2_147_483_647)),
            ints(uniform(draw, rows, 1, 100)),
            from_list(&slots(draw, rows, LANGUAGES.len() as u64), &LANGUAGES),
            from_list(&slots(draw, rows, COUNTRIES.len() as u64), &COUNTRIES),
            constant_text(rows, ""),
            constant_text(rows, ""),
            ints(one_of(draw, rows, &HTTP_ERRORS)),
            ints(uniform(draw, rows, 0, 10_000)),
            ints(uniform(draw, rows, 0, 1_000)),
            ints(uniform(draw, rows, 0, 5_000)),
            ints(uniform(draw, rows, 0, 10_000)),
            ints(uniform(draw, rows, 0, 15_000)),
            ints(uniform(draw, rows, 0, 20_000)),
            ints(uniform(draw, rows, 0, 50)),
            constant_text(rows, ""),
            ints(uniform(draw, rows, 0, 1_000_000)),
            constant_text(rows, ""),
            from_list(&slots(draw, rows, CURRENCIES.len() as u64), &CURRENCIES),
            ints(uniform(draw, rows, 0, 10)),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            constant_text(rows, ""),
            ints(uniform(draw, rows, 0, 1)),
            ints(uniform(draw, rows, 1, i64::MAX)),
            ints(uniform(draw, rows, 1, i64::MAX)),
            ints(uniform(draw, rows, 0, 2_147_483_647)),
        ];
        RecordBatch::try_new(self.schema(), columns).expect("generated columns match the schema")
    }
}

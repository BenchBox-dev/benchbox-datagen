use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Int64Array, StringBuilder, new_null_array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::delimited::TextFormat;
use super::rng::{Draw, Stream};
use super::{BLOCK_ROWS, Generator, Table, block_len};
use crate::generate::{KeyRange, key};
use crate::schema::{self, Column, Int, Text};

const BENCHMARK: &str = "joinorder_synthetic";
const GOLDEN: u64 = 60;

const LOOKUPS: [(&str, &[&str]); 6] = [
    (
        "kind_type",
        &[
            "movie",
            "tv series",
            "tv movie",
            "video movie",
            "tv mini series",
            "video game",
            "episode",
        ],
    ),
    (
        "company_type",
        &[
            "distributors",
            "production companies",
            "special effects companies",
            "miscellaneous companies",
        ],
    ),
    (
        "info_type",
        &[
            "rating",
            "votes",
            "genres",
            "languages",
            "countries",
            "release dates",
            "running times",
            "locations",
            "budget",
            "gross",
            "keywords",
            "plot",
            "goofs",
            "trivia",
            "quotes",
            "soundtrack",
            "technical",
            "color info",
            "sound mix",
            "certificates",
            "filming locations",
            "production dates",
            "top 250 rank",
            "bottom 10 rank",
        ],
    ),
    (
        "role_type",
        &[
            "actor",
            "actress",
            "producer",
            "writer",
            "cinematographer",
            "composer",
            "costume designer",
            "director",
            "editor",
            "miscellaneous crew",
            "production designer",
            "guest",
        ],
    ),
    (
        "comp_cast_type",
        &["cast", "crew", "complete", "incomplete"],
    ),
    (
        "link_type",
        &[
            "follows",
            "followed by",
            "remake of",
            "remade as",
            "references",
            "referenced in",
            "spoofs",
            "spoofed in",
            "features",
            "featured in",
            "spin off from",
            "spin off",
            "version of",
            "similar to",
            "edited into",
            "edited from",
            "alternate language version of",
            "unknown link",
        ],
    ),
];

const TABLES: [&str; 21] = [
    "kind_type",
    "company_type",
    "info_type",
    "role_type",
    "comp_cast_type",
    "link_type",
    "title",
    "name",
    "company_name",
    "keyword",
    "char_name",
    "cast_info",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "person_info",
    "complete_cast",
    "aka_name",
    "aka_title",
];

const TITLE_PREFIXES: [&str; 4] = ["The", "A", "An", ""];
const TITLE_WORDS: [&str; 8] = [
    "Adventure",
    "Mystery",
    "Romance",
    "Comedy",
    "Drama",
    "Action",
    "Horror",
    "Thriller",
];
const FIRST_NAMES: [&str; 10] = [
    "John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Mary", "James", "Jennifer",
];
const LAST_NAMES: [&str; 10] = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
];
const SEED_NAMES: [&str; 3] = ["Robert Downey", "Downey Robert", "Angelina Smith"];
const COMPANY_TYPES: [&str; 6] = [
    "Studios",
    "Productions",
    "Pictures",
    "Films",
    "Entertainment",
    "Media",
];
const COMPANY_NAMES: [&str; 8] = [
    "Universal",
    "Warner",
    "Disney",
    "Sony",
    "Paramount",
    "Fox",
    "MGM",
    "Columbia",
];
const COUNTRIES: [&str; 10] = [
    "[us]", "[uk]", "[de]", "[fr]", "[jp]", "[ca]", "[au]", "[it]", "[es]", "[in]",
];
const SEED_COMPANIES: [&str; 3] = ["Warner Bros", "Warner Films", "Universal Films"];
const SEED_COUNTRIES: [&str; 4] = ["[us]", "[us]", "[ru]", "[pl]"];
const KEYWORDS: [&str; 23] = [
    "action",
    "adventure",
    "comedy",
    "drama",
    "horror",
    "thriller",
    "romance",
    "sequel",
    "superhero",
    "character-name-in-title",
    "based-on-novel",
    "violence",
    "murder",
    "love",
    "friendship",
    "betrayal",
    "revenge",
    "family",
    "war",
    "crime",
    "mystery",
    "fantasy",
    "sci-fi",
];
const SEED_KEYWORDS: [&str; 2] = ["marvel-cinematic-universe", "sequel"];
const GOLDEN_KEYWORDS: [&str; 7] = [
    "marvel-cinematic-universe",
    "murder",
    "blood",
    "violence",
    "superhero",
    "sequel",
    "character-name-in-title",
];
const CHARACTERS: [&str; 6] = [
    "John Doe",
    "Jane Smith",
    "The Hero",
    "The Villain",
    "Detective Brown",
    "Dr. Johnson",
];
const SEED_NOTES: [&str; 4] = ["(producer)", "(producer)", "(co-production)", "(presents)"];
const SEED_ROLES: [i64; 3] = [3, 3, 1];
const GOLDEN_CAST_NOTES: [&str; 3] = ["(voice) (uncredited)", "(voice) (uncredited)", "(producer)"];
const COMPANY_NOTES: [&str; 5] = [
    "(2002) (USA) (theatrical)",
    "(2005) (worldwide) (theatrical)",
    "(USA) (VHS)",
    "(Japan) (Blu-ray)",
    "(France) (theatrical)",
];
const SEED_INFO: [&str; 4] = ["Sweden", "Germany", "Norwegian", "USA"];
const INFO_VALUES: [&[&str]; 5] = [
    &["8.5", "7.2", "6.8", "9.1", "5.5"],
    &["1000", "5000", "50000", "100000"],
    &["Drama", "Comedy", "Action", "Horror", "Romance"],
    &["English", "Spanish", "French", "German", "Japanese"],
    &["USA", "UK", "Germany", "France", "Japan"],
];
const INDEXED_INFO_TYPES: [i64; 4] = [1, 2, 23, 24];
const PERSON_NOTES: [Option<&str>; 5] = [
    Some("Born in USA"),
    Some("Studied at university"),
    Some("Award winner"),
    Some("Director known for drama"),
    None,
];
const AKA_FIRST: [&str; 8] = ["Al", "Bob", "Chris", "Dan", "Ed", "Frank", "George", "Hank"];
const AKA_LAST: [&str; 7] = [
    "Anderson", "Baker", "Clark", "Davis", "Evans", "Foster", "Green",
];
const AKA_TITLE_WORDS: [&str; 7] = ["Journey", "Return", "Rise", "Fall", "Dawn", "Dusk", "Storm"];

const COLUMNS: [(&str, &[Column]); 21] = [
    ("kind_type", &[("id", Int, true), ("kind", Text, false)]),
    ("company_type", &[("id", Int, true), ("kind", Text, false)]),
    ("info_type", &[("id", Int, true), ("info", Text, false)]),
    ("role_type", &[("id", Int, true), ("role", Text, false)]),
    (
        "comp_cast_type",
        &[("id", Int, true), ("kind", Text, false)],
    ),
    ("link_type", &[("id", Int, true), ("link", Text, false)]),
    (
        "title",
        &[
            ("id", Int, true),
            ("title", Text, false),
            ("imdb_index", Text, true),
            ("kind_id", Int, false),
            ("production_year", Int, true),
            ("imdb_id", Int, true),
            ("phonetic_code", Text, true),
            ("episode_of_id", Int, true),
            ("season_nr", Int, true),
            ("episode_nr", Int, true),
            ("series_years", Text, true),
            ("md5sum", Text, true),
        ],
    ),
    (
        "name",
        &[
            ("id", Int, true),
            ("name", Text, false),
            ("imdb_index", Text, true),
            ("imdb_id", Int, true),
            ("gender", Text, true),
            ("name_pcode_cf", Text, true),
            ("name_pcode_nf", Text, true),
            ("surname_pcode", Text, true),
            ("md5sum", Text, true),
        ],
    ),
    (
        "company_name",
        &[
            ("id", Int, true),
            ("name", Text, false),
            ("country_code", Text, true),
            ("imdb_id", Int, true),
            ("name_pcode_nf", Text, true),
            ("name_pcode_sf", Text, true),
            ("md5sum", Text, true),
        ],
    ),
    (
        "keyword",
        &[
            ("id", Int, true),
            ("keyword", Text, false),
            ("phonetic_code", Text, true),
        ],
    ),
    (
        "char_name",
        &[
            ("id", Int, true),
            ("name", Text, false),
            ("imdb_index", Text, true),
            ("imdb_id", Int, true),
            ("name_pcode_nf", Text, true),
            ("surname_pcode", Text, true),
            ("md5sum", Text, true),
        ],
    ),
    (
        "cast_info",
        &[
            ("id", Int, true),
            ("person_id", Int, false),
            ("movie_id", Int, false),
            ("person_role_id", Int, true),
            ("note", Text, true),
            ("nr_order", Int, true),
            ("role_id", Int, false),
        ],
    ),
    (
        "movie_companies",
        &[
            ("id", Int, true),
            ("movie_id", Int, false),
            ("company_id", Int, false),
            ("company_type_id", Int, false),
            ("note", Text, true),
        ],
    ),
    (
        "movie_info",
        &[
            ("id", Int, true),
            ("movie_id", Int, false),
            ("info_type_id", Int, false),
            ("info", Text, false),
            ("note", Text, true),
        ],
    ),
    (
        "movie_info_idx",
        &[
            ("id", Int, true),
            ("movie_id", Int, false),
            ("info_type_id", Int, false),
            ("info", Text, false),
            ("note", Text, true),
        ],
    ),
    (
        "movie_keyword",
        &[
            ("id", Int, true),
            ("movie_id", Int, false),
            ("keyword_id", Int, false),
        ],
    ),
    (
        "movie_link",
        &[
            ("id", Int, true),
            ("movie_id", Int, false),
            ("linked_movie_id", Int, false),
            ("link_type_id", Int, false),
        ],
    ),
    (
        "person_info",
        &[
            ("id", Int, true),
            ("person_id", Int, false),
            ("info_type_id", Int, false),
            ("info", Text, false),
            ("note", Text, true),
        ],
    ),
    (
        "complete_cast",
        &[
            ("id", Int, true),
            ("movie_id", Int, true),
            ("subject_id", Int, false),
            ("status_id", Int, false),
        ],
    ),
    (
        "aka_name",
        &[
            ("id", Int, true),
            ("person_id", Int, false),
            ("name", Text, false),
            ("imdb_index", Text, true),
            ("name_pcode_cf", Text, true),
            ("name_pcode_nf", Text, true),
            ("surname_pcode", Text, true),
            ("md5sum", Text, true),
        ],
    ),
    (
        "aka_title",
        &[
            ("id", Int, true),
            ("movie_id", Int, false),
            ("title", Text, false),
            ("imdb_index", Text, true),
            ("kind_id", Int, false),
            ("production_year", Int, true),
            ("phonetic_code", Text, true),
            ("episode_of_id", Int, true),
            ("season_nr", Int, true),
            ("episode_nr", Int, true),
            ("note", Text, true),
            ("md5sum", Text, true),
        ],
    ),
];

static SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> =
    LazyLock::new(|| schema::build(&COLUMNS));

fn table_schema(table: &str) -> SchemaRef {
    SCHEMAS
        .get(table)
        .cloned()
        .expect("every joinorder_synthetic table has a schema")
}

fn scaled(base: u64, scale_factor: f64) -> u64 {
    ((base as f64 * scale_factor) as u64).max(1)
}

#[derive(Debug, Clone, Copy)]
struct Sizes {
    titles: u64,
    names: u64,
    companies: u64,
    keywords: u64,
    characters: u64,
}

impl Sizes {
    fn new(scale_factor: f64) -> Self {
        Self {
            titles: scaled(500_000, scale_factor),
            names: scaled(800_000, scale_factor),
            companies: scaled(60_000, scale_factor),
            keywords: scaled(24_000, scale_factor),
            characters: scaled(600_000, scale_factor),
        }
    }

    fn golden_title(&self, index: u64) -> i64 {
        (self.titles + 1 + index) as i64
    }

    fn golden_name(&self, index: u64) -> i64 {
        (self.names + 1 + index) as i64
    }

    fn golden_company(&self, index: u64) -> i64 {
        (self.companies + 1 + index) as i64
    }

    fn golden_keyword(&self, index: u64) -> i64 {
        (self.keywords + 1 + index) as i64
    }
}

fn base_rows(table: &str, scale_factor: f64) -> u64 {
    let base = match table {
        "title" => 500_000,
        "name" => 800_000,
        "company_name" => 60_000,
        "keyword" => 24_000,
        "char_name" => 600_000,
        "cast_info" => 7_000_000,
        "movie_companies" => 520_000,
        "movie_info" => 3_000_000,
        "movie_info_idx" => 280_000,
        "movie_keyword" => 1_000_000,
        "movie_link" => 6_000,
        "person_info" => 600_000,
        "complete_cast" => 30_000,
        "aka_name" => 180_000,
        "aka_title" => 80_000,
        other => unreachable!("table {other:?} was validated by the caller"),
    };
    scaled(base, scale_factor)
}

fn lookup(table: &str) -> Option<&'static [&'static str]> {
    LOOKUPS
        .iter()
        .find(|(name, _)| *name == table)
        .map(|(_, values)| *values)
}

fn row_count(table: &str, scale_factor: f64) -> u64 {
    if let Some(values) = lookup(table) {
        return values.len() as u64;
    }
    let rows = base_rows(table, scale_factor);
    match table {
        "title" | "name" | "company_name" | "keyword" => rows + GOLDEN,
        _ => rows,
    }
}

fn golden_bound(parent: u64, child_rows: u64) -> i64 {
    (parent + GOLDEN.min(child_rows)) as i64
}

pub(super) struct JoinorderSynthetic;

impl Generator for JoinorderSynthetic {
    fn name(&self) -> &'static str {
        BENCHMARK
    }

    fn tables(&self) -> &'static [&'static str] {
        &TABLES
    }

    fn schema(&self, table: &str) -> Option<SchemaRef> {
        SCHEMAS.get(table).cloned()
    }

    fn row_count(&self, table: &str, scale_factor: f64) -> u64 {
        row_count(table, scale_factor)
    }

    fn open(&self, table: &str, scale_factor: f64, seed: u64) -> Arc<dyn Table> {
        let table = TABLES
            .iter()
            .find(|name| **name == table)
            .expect("table was validated by the caller");
        Arc::new(Joined {
            table,
            rows: row_count(table, scale_factor),
            sizes: Sizes::new(scale_factor),
            stream: Stream::new(BENCHMARK, table, "fields", seed),
        })
    }

    fn text_format(&self, _table: &str) -> TextFormat {
        TextFormat::csv(b',', false)
    }

    fn key_ranges(&self, scale_factor: f64) -> Vec<KeyRange> {
        let rows = |table| row_count(table, scale_factor);
        let sizes = Sizes::new(scale_factor);
        let mut ranges: Vec<KeyRange> = TABLES
            .iter()
            .map(|table| key(table, "id", 1, rows(table) as i64))
            .collect();
        let titles = |child| golden_bound(sizes.titles, rows(child));
        let names = |child| golden_bound(sizes.names, rows(child));
        ranges.extend([
            key("cast_info", "movie_id", 1, titles("cast_info")),
            key("cast_info", "person_id", 1, names("cast_info")),
            key("cast_info", "person_role_id", 1, sizes.characters as i64),
            key("cast_info", "role_id", 1, 12),
            key("movie_companies", "movie_id", 1, titles("movie_companies")),
            key(
                "movie_companies",
                "company_id",
                1,
                golden_bound(sizes.companies, rows("movie_companies")),
            ),
            key("movie_companies", "company_type_id", 1, 4),
            key("movie_info", "movie_id", 1, titles("movie_info")),
            key("movie_info", "info_type_id", 1, 24),
            key("movie_info_idx", "movie_id", 1, sizes.titles as i64),
            key("movie_info_idx", "info_type_id", 1, 24),
            key("movie_keyword", "movie_id", 1, titles("movie_keyword")),
            key(
                "movie_keyword",
                "keyword_id",
                1,
                golden_bound(sizes.keywords, rows("movie_keyword")),
            ),
            key("movie_link", "movie_id", 1, titles("movie_link")),
            key("movie_link", "linked_movie_id", 1, titles("movie_link")),
            key("movie_link", "link_type_id", 1, 18),
            key("person_info", "person_id", 1, sizes.names as i64),
            key("person_info", "info_type_id", 1, 113),
            key("complete_cast", "movie_id", 1, titles("complete_cast")),
            key("complete_cast", "subject_id", 1, 4),
            key("complete_cast", "status_id", 1, 4),
            key("aka_name", "person_id", 1, names("aka_name")),
            key("aka_title", "movie_id", 1, sizes.titles as i64),
            key("aka_title", "kind_id", 1, 7),
            key("title", "kind_id", 1, 7),
        ]);
        ranges
    }
}

fn int(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn optional_int(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn text(mut builder: StringBuilder) -> ArrayRef {
    Arc::new(builder.finish())
}

fn null_int(rows: usize) -> ArrayRef {
    new_null_array(&DataType::Int64, rows)
}

fn null_text(rows: usize) -> ArrayRef {
    new_null_array(&DataType::Utf8, rows)
}

fn chance(draw: &mut Draw, numerator: u64, denominator: u64) -> bool {
    draw.below(denominator) < numerator
}

fn pick<'a>(draw: &mut Draw, items: &[&'a str]) -> &'a str {
    draw.pick(items)
}

#[derive(Debug)]
struct Joined {
    table: &'static str,
    rows: u64,
    sizes: Sizes,
    stream: Stream,
}

struct Block {
    rows: usize,
    first: u64,
    draw: Draw,
}

impl Block {
    fn range(&self) -> std::ops::Range<u64> {
        self.first..self.first + self.rows as u64
    }
}

impl Table for Joined {
    fn schema(&self) -> SchemaRef {
        table_schema(self.table)
    }

    fn rows(&self) -> u64 {
        self.rows
    }

    fn block(&self, index: u64) -> RecordBatch {
        let mut block = Block {
            rows: block_len(self.rows, index),
            first: index * BLOCK_ROWS,
            draw: self.stream.block(index),
        };
        let columns = match self.table {
            "title" => self.titles(&mut block),
            "name" => self.names(&mut block),
            "company_name" => self.companies(&mut block),
            "keyword" => self.keywords(&mut block),
            "char_name" => self.characters(&mut block),
            "cast_info" => self.cast_info(&mut block),
            "movie_companies" => self.movie_companies(&mut block),
            "movie_info" => self.movie_info(&mut block),
            "movie_info_idx" => self.movie_info_idx(&mut block),
            "movie_keyword" => self.movie_keyword(&mut block),
            "movie_link" => self.movie_link(&mut block),
            "person_info" => self.person_info(&mut block),
            "complete_cast" => self.complete_cast(&mut block),
            "aka_name" => self.aka_names(&mut block),
            "aka_title" => self.aka_titles(&mut block),
            lookup_table => Self::lookup(lookup_table),
        };
        RecordBatch::try_new(self.schema(), columns).expect("generated columns match the schema")
    }
}

impl Joined {
    fn lookup(table: &str) -> Vec<ArrayRef> {
        let values = lookup(table).expect("lookup table");
        let mut names = StringBuilder::new();
        for value in values {
            names.append_value(value);
        }
        vec![int((1..=values.len() as i64).collect()), text(names)]
    }

    fn titles(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let draw = &mut block.draw;
        let mut ids = Vec::with_capacity(rows);
        let mut kinds = Vec::with_capacity(rows);
        let mut years = Vec::with_capacity(rows);
        let mut episodes = Vec::with_capacity(rows);
        let mut titles = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.first..block.first + rows as u64 {
            ids.push(row as i64 + 1);
            scratch.clear();
            if row < self.sizes.titles {
                let prefix = pick(draw, &TITLE_PREFIXES);
                if !prefix.is_empty() {
                    scratch.push_str(prefix);
                    scratch.push(' ');
                }
                let (first, second) = (pick(draw, &TITLE_WORDS), pick(draw, &TITLE_WORDS));
                write!(scratch, "{first} {second}").expect("string write");
                if chance(draw, 3, 10) {
                    kinds.push(if chance(draw, 3, 4) { 1 } else { 2 });
                    years.push(Some(draw.range(1990, 2010)));
                } else {
                    kinds.push(draw.range(1, 7));
                    years.push(if chance(draw, 9, 10) {
                        Some(draw.range(1950, 2023))
                    } else {
                        None
                    });
                }
                episodes.push(None);
            } else {
                let offset = (row - self.sizes.titles) as i64;
                write!(scratch, "Golden Adventure {offset}").expect("string write");
                kinds.push(1);
                years.push(Some(2005 + offset % 6));
                episodes.push((offset % 3 == 0).then_some(50 + offset % 50));
            }
            titles.append_value(&scratch);
        }
        vec![
            int(ids),
            text(titles),
            null_text(rows),
            int(kinds),
            optional_int(years),
            null_int(rows),
            null_text(rows),
            null_int(rows),
            null_int(rows),
            optional_int(episodes),
            null_text(rows),
            null_text(rows),
        ]
    }

    fn names(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let draw = &mut block.draw;
        let mut ids = Vec::with_capacity(rows);
        let mut names = StringBuilder::new();
        let mut genders = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.first..block.first + rows as u64 {
            ids.push(row as i64 + 1);
            if row < self.sizes.names {
                if chance(draw, 8, 100) {
                    names.append_value(pick(draw, &SEED_NAMES));
                } else {
                    scratch.clear();
                    let (first, last) = (pick(draw, &FIRST_NAMES), pick(draw, &LAST_NAMES));
                    write!(scratch, "{first} {last}").expect("string write");
                    names.append_value(&scratch);
                }
                if chance(draw, 9, 10) {
                    genders.append_value(if chance(draw, 1, 2) { "m" } else { "f" });
                } else {
                    genders.append_null();
                }
            } else {
                let offset = row - self.sizes.names;
                names.append_value(if offset.is_multiple_of(2) {
                    "Downey Robert"
                } else {
                    "Angelina Smith"
                });
                genders.append_null();
            }
        }
        vec![
            int(ids),
            text(names),
            null_text(rows),
            null_int(rows),
            text(genders),
            null_text(rows),
            null_text(rows),
            null_text(rows),
            null_text(rows),
        ]
    }

    fn companies(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let draw = &mut block.draw;
        let mut ids = Vec::with_capacity(rows);
        let mut names = StringBuilder::new();
        let mut countries = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.first..block.first + rows as u64 {
            ids.push(row as i64 + 1);
            if row < self.sizes.companies {
                if chance(draw, 1, 10) {
                    names.append_value(pick(draw, &SEED_COMPANIES));
                    countries.append_value(pick(draw, &SEED_COUNTRIES));
                } else {
                    scratch.clear();
                    let (name, kind) = (pick(draw, &COMPANY_NAMES), pick(draw, &COMPANY_TYPES));
                    write!(scratch, "{name} {kind}").expect("string write");
                    names.append_value(&scratch);
                    countries.append_value(pick(draw, &COUNTRIES));
                }
            } else {
                let offset = (row - self.sizes.companies) % 4;
                names.append_value(match offset {
                    0 => "Warner Films",
                    1 => "Universal Films",
                    _ => "Moscow Films",
                });
                countries.append_value(if offset >= 2 { "[ru]" } else { "[us]" });
            }
        }
        vec![
            int(ids),
            text(names),
            text(countries),
            null_int(rows),
            null_text(rows),
            null_text(rows),
            null_text(rows),
        ]
    }

    fn keywords(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let draw = &mut block.draw;
        let mut ids = Vec::with_capacity(rows);
        let mut keywords = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.first..block.first + rows as u64 {
            ids.push(row as i64 + 1);
            if row < self.sizes.keywords {
                if chance(draw, 1, 10) {
                    keywords.append_value(pick(draw, &SEED_KEYWORDS));
                } else if let Some(keyword) = KEYWORDS.get(row as usize) {
                    keywords.append_value(keyword);
                } else {
                    scratch.clear();
                    write!(scratch, "keyword_{}", row + 1).expect("string write");
                    keywords.append_value(&scratch);
                }
            } else {
                let offset = (row - self.sizes.keywords) as usize;
                keywords.append_value(GOLDEN_KEYWORDS[offset % GOLDEN_KEYWORDS.len()]);
            }
        }
        vec![int(ids), text(keywords), null_text(rows)]
    }

    fn characters(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut names = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.first..block.first + rows as u64 {
            ids.push(row as i64 + 1);
            if let Some(name) = CHARACTERS.get(row as usize) {
                names.append_value(name);
            } else {
                scratch.clear();
                write!(scratch, "Character {}", row + 1).expect("string write");
                names.append_value(&scratch);
            }
        }
        vec![
            int(ids),
            text(names),
            null_text(rows),
            null_int(rows),
            null_text(rows),
            null_text(rows),
            null_text(rows),
        ]
    }

    fn cast_info(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let sizes = &self.sizes;
        let mut ids = Vec::with_capacity(rows);
        let mut people = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut characters = Vec::with_capacity(rows);
        let mut notes = StringBuilder::new();
        let mut orders = Vec::with_capacity(rows);
        let mut roles = Vec::with_capacity(rows);
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let person = draw.range(1, sizes.names as i64);
            let movie = draw.range(1, sizes.titles as i64);
            characters.push(chance(draw, 7, 10).then(|| draw.range(1, sizes.characters as i64)));
            let role = if chance(draw, 15, 100) {
                SEED_ROLES[draw.below(3) as usize]
            } else {
                draw.range(1, 12)
            };
            let note = chance(draw, 12, 100).then(|| pick(draw, &SEED_NOTES));
            orders.push(chance(draw, 1, 2).then(|| draw.range(1, 20)));
            if row < GOLDEN {
                people.push(sizes.golden_name(row));
                movies.push(sizes.golden_title(row));
                roles.push(if row % 2 == 0 { 3 } else { 1 });
                notes.append_value(GOLDEN_CAST_NOTES[row as usize % 3]);
            } else {
                people.push(person);
                movies.push(movie);
                roles.push(role);
                notes.append_option(note);
            }
        }
        vec![
            int(ids),
            int(people),
            int(movies),
            optional_int(characters),
            text(notes),
            optional_int(orders),
            int(roles),
        ]
    }

    fn movie_companies(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let sizes = &self.sizes;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut companies = Vec::with_capacity(rows);
        let mut types = Vec::with_capacity(rows);
        let mut notes = StringBuilder::new();
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let (movie, company) = (
                draw.range(1, sizes.titles as i64),
                draw.range(1, sizes.companies as i64),
            );
            types.push(draw.range(1, 4));
            let note = chance(draw, 12, 100).then(|| pick(draw, &COMPANY_NOTES));
            if row < GOLDEN {
                movies.push(sizes.golden_title(row));
                companies.push(sizes.golden_company(row));
                notes.append_value("(2005) (USA) (theatrical)");
            } else {
                movies.push(movie);
                companies.push(company);
                notes.append_option(note);
            }
        }
        vec![
            int(ids),
            int(movies),
            int(companies),
            int(types),
            text(notes),
        ]
    }

    fn movie_info(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut types = Vec::with_capacity(rows);
        let mut infos = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let movie = draw.range(1, self.sizes.titles as i64);
            let info_type = draw.range(1, 24);
            types.push(info_type);
            scratch.clear();
            let seeded = chance(draw, 15, 100);
            if seeded {
                scratch.push_str(pick(draw, &SEED_INFO));
            } else if let Some(values) = INFO_VALUES.get(info_type as usize - 1) {
                scratch.push_str(pick(draw, values));
            } else {
                write!(scratch, "info_{}", row + 1).expect("string write");
            }
            if row < GOLDEN {
                movies.push(self.sizes.golden_title(row));
                infos.append_value(if row % 2 == 0 { "Sweden" } else { "Germany" });
            } else {
                movies.push(movie);
                infos.append_value(&scratch);
            }
        }
        vec![
            int(ids),
            int(movies),
            int(types),
            text(infos),
            null_text(rows),
        ]
    }

    fn movie_info_idx(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut types = Vec::with_capacity(rows);
        let mut infos = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            movies.push(draw.range(1, self.sizes.titles as i64));
            let info_type = *draw.pick(&INDEXED_INFO_TYPES);
            types.push(info_type);
            scratch.clear();
            match info_type {
                1 => {
                    let tenths = 10 + draw.below(180).div_ceil(2);
                    write!(scratch, "{}.{}", tenths / 10, tenths % 10).expect("string write");
                }
                2 => write!(scratch, "{}", draw.range(100, 1_000_000)).expect("string write"),
                23 => write!(scratch, "{}", draw.range(1, 250)).expect("string write"),
                _ => write!(scratch, "{}", draw.range(1, 10)).expect("string write"),
            }
            infos.append_value(&scratch);
        }
        vec![
            int(ids),
            int(movies),
            int(types),
            text(infos),
            null_text(rows),
        ]
    }

    fn movie_keyword(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut keywords = Vec::with_capacity(rows);
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let (movie, keyword) = (
                draw.range(1, self.sizes.titles as i64),
                draw.range(1, self.sizes.keywords as i64),
            );
            if row < GOLDEN {
                movies.push(self.sizes.golden_title(row));
                keywords.push(self.sizes.golden_keyword(row));
            } else {
                movies.push(movie);
                keywords.push(keyword);
            }
        }
        vec![int(ids), int(movies), int(keywords)]
    }

    fn movie_link(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut linked = Vec::with_capacity(rows);
        let mut types = Vec::with_capacity(rows);
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let (movie, other) = (
                draw.range(1, self.sizes.titles as i64),
                draw.range(1, self.sizes.titles as i64),
            );
            let link_type = draw.range(1, 18);
            if row < GOLDEN {
                movies.push(self.sizes.golden_title(row));
                linked.push(self.sizes.golden_title((row + 1) % GOLDEN));
                types.push(1);
            } else {
                movies.push(movie);
                linked.push(other);
                types.push(link_type);
            }
        }
        vec![int(ids), int(movies), int(linked), int(types)]
    }

    fn person_info(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut people = Vec::with_capacity(rows);
        let mut types = Vec::with_capacity(rows);
        let mut infos = StringBuilder::new();
        let mut notes = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            people.push(draw.range(1, self.sizes.names as i64));
            types.push(draw.range(1, 113));
            scratch.clear();
            write!(scratch, "Info text {}", (row + 1) % 1000).expect("string write");
            infos.append_value(&scratch);
            notes.append_option(*draw.pick(&PERSON_NOTES));
        }
        vec![int(ids), int(people), int(types), text(infos), text(notes)]
    }

    fn complete_cast(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut subjects = Vec::with_capacity(rows);
        let mut statuses = Vec::with_capacity(rows);
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let movie = draw.range(1, self.sizes.titles as i64);
            let (subject, status) = (draw.range(1, 4), draw.range(1, 4));
            if row < GOLDEN {
                movies.push(Some(self.sizes.golden_title(row)));
                subjects.push(1);
                statuses.push(3);
            } else {
                movies.push(Some(movie));
                subjects.push(subject);
                statuses.push(status);
            }
        }
        vec![int(ids), optional_int(movies), int(subjects), int(statuses)]
    }

    fn aka_names(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut people = Vec::with_capacity(rows);
        let mut names = StringBuilder::new();
        let mut scratch = String::new();
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            let person = draw.range(1, self.sizes.names as i64);
            scratch.clear();
            let (first, last) = (pick(draw, &AKA_FIRST), pick(draw, &AKA_LAST));
            write!(scratch, "{first} {last}").expect("string write");
            if row < GOLDEN {
                people.push(self.sizes.golden_name(row));
                names.append_value("Golden Alias");
            } else {
                people.push(person);
                names.append_value(&scratch);
            }
        }
        vec![
            int(ids),
            int(people),
            text(names),
            null_text(rows),
            null_text(rows),
            null_text(rows),
            null_text(rows),
            null_text(rows),
        ]
    }

    fn aka_titles(&self, block: &mut Block) -> Vec<ArrayRef> {
        let rows = block.rows;
        let mut ids = Vec::with_capacity(rows);
        let mut movies = Vec::with_capacity(rows);
        let mut titles = StringBuilder::new();
        let mut kinds = Vec::with_capacity(rows);
        let mut years = Vec::with_capacity(rows);
        let mut scratch = String::new();
        for row in block.range() {
            let draw = &mut block.draw;
            ids.push(row as i64 + 1);
            movies.push(draw.range(1, self.sizes.titles as i64));
            scratch.clear();
            let word = pick(draw, &AKA_TITLE_WORDS);
            write!(scratch, "Alternative {word} {}", (row + 1) % 10_000).expect("string write");
            titles.append_value(&scratch);
            kinds.push(draw.range(1, 7));
            years.push(chance(draw, 9, 10).then(|| draw.range(1950, 2023)));
        }
        vec![
            int(ids),
            int(movies),
            text(titles),
            null_text(rows),
            int(kinds),
            optional_int(years),
            null_text(rows),
            null_int(rows),
            null_int(rows),
            null_int(rows),
            null_text(rows),
            null_text(rows),
        ]
    }
}

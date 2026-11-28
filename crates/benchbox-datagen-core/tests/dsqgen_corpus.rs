use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use benchbox_datagen_core::{
    DsqgenError, DsqgenQueryRequest, StreamsRequest, TemplateSource, generate_streams,
    generate_tpcds_query, split_statements, substitute_parameters,
};
use flate2::read::GzDecoder;
use serde_json::Value;
use sha2::{Digest, Sha256};

const MULTI_PART: [&str; 4] = ["14", "23", "24", "39"];
const SUBSCALE_OFFICIAL: &str = "define REASON= dist(return_reasons, 1, 1);";
const SUBSCALE_OVERRIDE: &str =
    "define REASON= distmember(return_reasons, random(1, rowcount(\"reason\"), uniform), 1);";

struct Inputs {
    corpus: PathBuf,
    templates: PathBuf,
    variants: PathBuf,
    dialect: String,
    distributions: Vec<u8>,
}

impl Inputs {
    fn load(root: &Path) -> Self {
        let tpcds = root.join("benchbox/_binaries/tpc-ds");
        Self {
            corpus: root.join("tests/fixtures/tpc_query_corpus"),
            templates: tpcds.join("templates/query_templates"),
            variants: tpcds.join("templates/query_variants"),
            dialect: fs::read_to_string(tpcds.join("templates/query_templates/netezza.tpl"))
                .unwrap(),
            distributions: fs::read(tpcds.join("linux-x86_64/tpcds.idx")).unwrap(),
        }
    }

    fn template_path(&self, query: &str) -> (String, PathBuf) {
        let (base, variant) = match query.strip_suffix(['a', 'b']) {
            Some(base) => (base, query.chars().last()),
            None => (query, None),
        };
        let multi_part = MULTI_PART.contains(&base) && variant.is_some();
        let name = if multi_part {
            format!("query{base}.tpl")
        } else {
            format!(
                "query{base}{}.tpl",
                variant.map(String::from).unwrap_or_default()
            )
        };
        let in_templates = self.templates.join(&name);
        if variant.is_some() && !multi_part && self.variants.join(&name).exists()
            || !in_templates.exists()
        {
            (name.clone(), self.variants.join(name))
        } else {
            (name, in_templates)
        }
    }

    fn template(&self, query: &str, scale: f64) -> (String, String) {
        let (name, path) = self.template_path(query);
        let mut text = fs::read_to_string(path).unwrap();
        if scale < 1.0 && name == "query93.tpl" {
            assert_eq!(text.matches(SUBSCALE_OFFICIAL).count(), 1);
            text = text.replacen(SUBSCALE_OFFICIAL, SUBSCALE_OVERRIDE, 1);
        }
        (name, text)
    }

    fn single(
        &self,
        query: &str,
        seed: Option<i64>,
        scale: f64,
        stream: Option<u32>,
        parameters: Option<&[(String, String)]>,
    ) -> Result<(String, BTreeMap<String, String>, bool), DsqgenError> {
        let (name, template) = self.template(query, scale);
        generate_tpcds_query(&DsqgenQueryRequest {
            query,
            template: &template,
            template_name: Some(&name),
            dialect: &self.dialect,
            distributions: &self.distributions,
            scale_factor: scale,
            seed,
            stream,
            parameters,
        })
        .map(|generated| {
            (
                generated.text,
                generated.parameters.into_iter().collect(),
                generated.wrapped,
            )
        })
    }
}

fn checkout() -> Option<PathBuf> {
    let root = std::env::var_os("BENCHBOX_CHECKOUT").map(PathBuf::from);
    if root.is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "BENCHBOX_CHECKOUT is required in CI for the TPC-DS corpus replay"
        );
        eprintln!("BENCHBOX_CHECKOUT is not set; skipping the TPC-DS corpus replay");
    }
    root
}

fn read_records(directory: &Path, stem: &str) -> Vec<Value> {
    let mut shards: Vec<PathBuf> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(&format!("{stem}-")) && name.ends_with(".jsonl.gz")
                })
        })
        .collect();
    shards.sort();
    let mut records = Vec::new();
    for shard in shards {
        let mut text = String::new();
        GzDecoder::new(fs::File::open(shard).unwrap())
            .read_to_string(&mut text)
            .unwrap();
        records.extend(
            text.lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap()),
        );
    }
    records
}

fn digest(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn string_map(value: &Value) -> BTreeMap<String, String> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
        .collect()
}

fn ordered_pairs(value: &Value) -> Vec<(String, String)> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
        .collect()
}

struct Tally {
    matched: usize,
    excluded: usize,
    excluded_matching: usize,
    mismatches: Vec<String>,
}

impl Tally {
    fn new() -> Self {
        Self {
            matched: 0,
            excluded: 0,
            excluded_matching: 0,
            mismatches: Vec::new(),
        }
    }

    fn exclude(&mut self, matches: bool) {
        self.excluded += 1;
        self.excluded_matching += usize::from(matches);
    }

    fn check(&mut self, ok: bool, label: impl FnOnce() -> String) {
        if ok {
            self.matched += 1;
        } else {
            self.mismatches.push(label());
        }
    }
}

fn nullable_scale(value: &Value) -> f64 {
    value.as_f64().unwrap()
}

#[test]
fn replays_single_template_records() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    let records = read_records(&inputs.corpus, "tpcds_single");
    let mut tally = Tally::new();
    let mut skipped = 0;
    for record in &records {
        let query = record["q"].as_str().unwrap();
        let seed = record["seed"].as_i64();
        let scale = nullable_scale(&record["sf"]);
        if record.get("s").is_some_and(|status| !status.is_null()) {
            skipped += 1;
            continue;
        }
        let outcome = inputs.single(query, seed, scale, None, None);
        let matches = matches!(&outcome, Ok((text, params, _))
            if digest(text) == record["h"].as_str().unwrap() && *params == string_map(&record["p"]));
        if matches!(outcome, Ok((_, _, true))) {
            tally.exclude(matches);
            continue;
        }
        tally.check(matches, || {
            format!(
                "q{query} seed {seed:?} sf {scale}: {:?}",
                outcome.map(|(text, _, _)| digest(&text))
            )
        });
    }
    eprintln!(
        "single: {} records, matched {}, excluded {} (would match anyway: {}), flagged by the oracle {skipped}",
        records.len(),
        tally.matched,
        tally.excluded,
        tally.excluded_matching
    );
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches: {:?}",
        tally.mismatches.len(),
        &tally.mismatches[..tally.mismatches.len().min(12)]
    );
}

#[test]
fn replays_stream_template_records() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    let records = read_records(&inputs.corpus, "tpcds_stream_template");
    let mut tally = Tally::new();
    for record in &records {
        let query = record["q"].as_str().unwrap();
        let seed = record["seed"].as_i64();
        let stream = record["stream"].as_u64().unwrap() as u32;
        let outcome = inputs.single(query, seed, 1.0, Some(stream), None);
        let matches = matches!(&outcome, Ok((text, params, _))
            if digest(text) == record["h"].as_str().unwrap() && *params == string_map(&record["p"]));
        if matches!(outcome, Ok((_, _, true))) {
            tally.exclude(matches);
            continue;
        }
        tally.check(matches, || {
            format!(
                "q{query} seed {seed:?} stream {stream}: {:?}",
                outcome.map(|(text, _, _)| digest(&text))
            )
        });
    }
    eprintln!(
        "stream_template: {} records, matched {}, excluded {} (would match anyway: {})",
        records.len(),
        tally.matched,
        tally.excluded,
        tally.excluded_matching
    );
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches: {:?}",
        tally.mismatches.len(),
        &tally.mismatches[..tally.mismatches.len().min(12)]
    );
}

#[test]
fn replays_stream_all_records() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    let records = read_records(&inputs.corpus, "tpcds_stream_all");
    let names: Vec<String> = fs::read_to_string(inputs.templates.join("templates.lst"))
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .map(str::to_owned)
        .collect();
    let full_texts: Vec<String> = names
        .iter()
        .map(|name| fs::read_to_string(inputs.templates.join(name)).unwrap())
        .collect();
    let sub_texts: Vec<String> = names
        .iter()
        .zip(&full_texts)
        .map(|(name, text)| {
            if name == "query93.tpl" {
                text.replacen(SUBSCALE_OFFICIAL, SUBSCALE_OVERRIDE, 1)
            } else {
                text.clone()
            }
        })
        .collect();
    let mut groups: BTreeMap<(String, String), Vec<&Value>> = BTreeMap::new();
    for record in &records {
        groups
            .entry((record["seed"].to_string(), record["sf"].to_string()))
            .or_default()
            .push(record);
    }
    let mut tally = Tally::new();
    for ((seed, scale), group) in groups {
        let seed = record_seed(&seed);
        let scale: f64 = scale.parse().unwrap();
        let sources: Vec<TemplateSource> = names
            .iter()
            .zip(if scale < 1.0 { &sub_texts } else { &full_texts })
            .map(|(name, text)| TemplateSource { name, text })
            .collect();
        let output = generate_streams(&StreamsRequest {
            templates: &sources,
            dialect: &inputs.dialect,
            distributions: &inputs.distributions,
            scale_factor: scale,
            seed,
            streams: 21,
            qualify: false,
        })
        .unwrap();
        for record in group {
            let stream = record["stream"].as_u64().unwrap() as usize;
            let generated = &output.streams[stream];
            let expected_order: Vec<String> = record["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect();
            let expected_hashes = record["h"].as_array().unwrap();
            let expected_templates = record["templates"].as_array().unwrap();
            let mut order = Vec::new();
            let mut cursor = 0;
            for (position, query) in generated.queries.iter().enumerate() {
                let id = query
                    .template
                    .trim_start_matches("query")
                    .trim_end_matches(".tpl");
                let statements = split_statements(&query.text);
                if MULTI_PART.contains(&id) {
                    order.push(format!("{id}a"));
                    order.push(format!("{id}b"));
                } else {
                    order.push(id.to_owned());
                }
                let hashes_ok = statements.iter().enumerate().all(|(offset, statement)| {
                    expected_hashes
                        .get(cursor + offset)
                        .and_then(Value::as_str)
                        .is_none_or(|want| want == digest(statement))
                });
                let params = &expected_templates[position][1];
                let params_ok = !params.is_object()
                    || query.parameters.iter().cloned().collect::<BTreeMap<_, _>>()
                        == string_map(params);
                if query.wrapped {
                    tally.exclude(hashes_ok && params_ok);
                } else {
                    tally.check(hashes_ok, || {
                        format!("seed {seed:?} sf {scale} stream {stream} q{id}: text")
                    });
                    tally.check(params_ok, || {
                        format!("seed {seed:?} sf {scale} stream {stream} q{id}: parameters")
                    });
                }
                cursor += statements.len();
            }
            tally.check(
                order == expected_order && cursor == expected_hashes.len(),
                || {
                    format!(
                        "seed {seed:?} sf {scale} stream {stream}: order or statement count differs"
                    )
                },
            );
        }
    }
    eprintln!(
        "stream_all: {} stream records ({} statements), (stream, template) units excluded {} (would match anyway: {}), checks matched {}",
        records.len(),
        records
            .iter()
            .map(|record| record["h"].as_array().unwrap().len())
            .sum::<usize>(),
        tally.excluded,
        tally.excluded_matching,
        tally.matched
    );
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches: {:?}",
        tally.mismatches.len(),
        &tally.mismatches[..tally.mismatches.len().min(12)]
    );
}

fn record_seed(text: &str) -> Option<i64> {
    text.parse().ok()
}

#[test]
fn replays_explicit_parameter_records() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    let records = read_records(&inputs.corpus, "tpcds_explicit");
    let mut tally = Tally::new();
    for record in &records {
        let query = record["q"].as_str().unwrap();
        let seed = record["seed"].as_i64();
        let scale = nullable_scale(&record["sf"]);
        let parameters = ordered_pairs(&record["p"]);
        let seeded = inputs.single(query, seed, scale, None, None);
        let (_, template) = inputs.template(query, scale);
        let rewritten = substitute_parameters(&template, &parameters, query);
        let explicit = inputs.single(query, seed, scale, None, Some(&parameters));
        let alternate = inputs.single(query, Some(99), scale, None, Some(&parameters));
        let wrapped = matches!(seeded, Ok((_, _, true)));
        let round_trip = record.get("r").and_then(Value::as_str).is_none_or(
            |round_trip| matches!(&seeded, Ok((text, _, _)) if digest(text) == round_trip),
        );
        let results = [
            matches!(&rewritten, Ok(text) if digest(text) == record["t"].as_str().unwrap()),
            matches!(&explicit, Ok((text, _, _)) if digest(text) == record["h"].as_str().unwrap()),
            record
                .get("h_alt")
                .and_then(Value::as_str)
                .is_none_or(|alt| matches!(&alternate, Ok((text, _, _)) if digest(text) == alt)),
        ];
        for (ok, what) in
            results
                .iter()
                .zip(["rewritten template", "explicit text", "alternate-seed text"])
        {
            tally.check(*ok, || {
                format!("q{query} seed {seed:?} sf {scale}: {what} differs")
            });
        }
        if wrapped {
            tally.exclude(round_trip);
        } else {
            tally.check(round_trip, || {
                format!("q{query} seed {seed:?} sf {scale}: seeded text differs")
            });
        }
    }
    eprintln!(
        "explicit: {} records, checks matched {}, seeded round-trip checks excluded {} (would match anyway: {})",
        records.len(),
        tally.matched,
        tally.excluded,
        tally.excluded_matching
    );
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches: {:?}",
        tally.mismatches.len(),
        &tally.mismatches[..tally.mismatches.len().min(12)]
    );
}

fn parameters_of(
    inputs: &Inputs,
    query: &str,
    seed: i64,
    scale: f64,
    stream: Option<u32>,
    prefix: &str,
) -> Vec<String> {
    let (_, params, wrapped) = inputs
        .single(query, Some(seed), scale, stream, None)
        .unwrap();
    assert!(wrapped);
    let mut values: Vec<(&String, &String)> = params
        .iter()
        .filter(|(name, _)| name.starts_with(prefix))
        .collect();
    values.sort();
    values.into_iter().map(|(_, value)| value.clone()).collect()
}

#[test]
fn a_ulist_longer_than_its_permutation_wraps_in_order() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    for (query, scale, prefix, held) in [
        ("46", 1.0, "CITYNUMBER", 2),
        ("34", 10.0, "COUNTYNUMBER", 3),
    ] {
        for seed in [1, 42, 1_339_962_585] {
            let values = parameters_of(&inputs, query, seed, scale, None, prefix);
            assert!(values.len() > held);
            for (index, value) in values.iter().enumerate() {
                assert_eq!(
                    *value,
                    values[index % held],
                    "q{query} seed {seed} entry {index}"
                );
            }
            let mut head: Vec<&String> = values[..held].iter().collect();
            head.sort();
            head.dedup();
            assert_eq!(head.len(), held);
            let again = parameters_of(&inputs, query, seed, scale, None, prefix);
            assert_eq!(values, again);
        }
    }
}

#[test]
fn wrapped_draws_do_not_depend_on_the_stream_position() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    let (_, plain, _) = inputs.single("34", Some(42), 10.0, None, None).unwrap();
    let (_, streamed, wrapped) = inputs.single("34", Some(42), 10.0, Some(0), None).unwrap();
    assert!(wrapped);
    assert_eq!(plain, streamed);
    for stream in [1, 5] {
        let values = parameters_of(&inputs, "34", 42, 10.0, Some(stream), "COUNTYNUMBER");
        for (index, value) in values.iter().enumerate() {
            assert_eq!(*value, values[index % 3]);
        }
    }
}

#[test]
fn q9_generates_where_the_key_count_truncates_to_a_small_modulus() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    for scale in [89_478.0, 89_479.0] {
        let (text, params, _) = inputs.single("9", Some(1), scale, None, None).unwrap();
        assert!(!text.is_empty() && !params.is_empty(), "sf {scale}");
    }
}

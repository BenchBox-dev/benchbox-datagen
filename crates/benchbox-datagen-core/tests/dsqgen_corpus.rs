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
const HEAP_READ_TEMPLATES: [&str; 4] = ["16", "27", "34", "73"];
const HEAP_READ_ROUND_TRIPS: [(&str, i64); 4] =
    [("16", 1001), ("27", 1001), ("36", 1001), ("73", 1001)];
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
        log_run: bool,
    ) -> Result<(String, BTreeMap<String, String>), DsqgenError> {
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
            log_run,
        })
        .map(|generated| (generated.text, generated.parameters.into_iter().collect()))
    }
}

fn checkout() -> Option<PathBuf> {
    std::env::var_os("BENCHBOX_CHECKOUT").map(PathBuf::from)
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
    mismatches: Vec<String>,
}

impl Tally {
    fn new() -> Self {
        Self {
            matched: 0,
            mismatches: Vec::new(),
        }
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
        let outcome = inputs.single(query, seed, scale, None, None, true);
        tally.check(
            matches!(&outcome, Ok((text, params))
                if digest(text) == record["h"].as_str().unwrap() && *params == string_map(&record["p"])),
            || format!("q{query} seed {seed:?} sf {scale}: {:?}", outcome.map(|(text, _)| digest(&text))),
        );
    }
    eprintln!(
        "single matched {} of {} (flagged {skipped})",
        tally.matched,
        records.len()
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
        let outcome = inputs.single(query, seed, 1.0, Some(stream), None, true);
        tally.check(
            matches!(&outcome, Ok((text, params))
                if digest(text) == record["h"].as_str().unwrap() && *params == string_map(&record["p"])),
            || format!("q{query} seed {seed:?} stream {stream}: {:?}", outcome.map(|(text, _)| digest(&text))),
        );
    }
    eprintln!(
        "stream_template matched {} of {}",
        tally.matched,
        records.len()
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
    let mut excluded_checks = 0;
    let mut excluded_failed = 0;
    let mut failed_templates = std::collections::BTreeSet::new();
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
            log_run: true,
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
                let excluded = seed.is_none() && scale == 10.0 && HEAP_READ_TEMPLATES.contains(&id);
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
                if excluded {
                    excluded_checks += 2;
                    excluded_failed += usize::from(!hashes_ok) + usize::from(!params_ok);
                    if !hashes_ok {
                        failed_templates.insert(format!("q{id}"));
                    }
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
        "stream_all checks matched {} excluded {excluded_checks} (of which {excluded_failed} differ; templates with differing text {failed_templates:?})",
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
        let (_, template) = inputs.template(query, scale);
        let rewritten = substitute_parameters(&template, &parameters, query);
        tally.check(
            matches!(&rewritten, Ok(text) if digest(text) == record["t"].as_str().unwrap()),
            || format!("q{query} sf {scale}: rewritten template hash differs"),
        );
        let outcome = inputs.single(query, seed, scale, None, Some(&parameters), false);
        tally.check(
            matches!(&outcome, Ok((text, _)) if digest(text) == record["h"].as_str().unwrap()),
            || {
                format!(
                    "q{query} seed {seed:?} sf {scale}: explicit text differs {:?}",
                    outcome.map(|(text, _)| digest(&text))
                )
            },
        );
        if let Some(alt) = record.get("h_alt").and_then(Value::as_str) {
            let alternate = inputs.single(query, Some(99), scale, None, Some(&parameters), false);
            tally.check(
                matches!(&alternate, Ok((text, _)) if digest(text) == alt),
                || format!("q{query} sf {scale}: alternate-seed text differs"),
            );
        }
        if let Some(round_trip) = record.get("r").and_then(Value::as_str)
            && !(scale == 10.0
                && seed.is_some_and(|seed| HEAP_READ_ROUND_TRIPS.contains(&(query, seed))))
        {
            let seeded = inputs.single(query, seed, scale, None, None, true);
            tally.check(
                matches!(&seeded, Ok((text, _)) if digest(text) == round_trip),
                || format!("q{query} seed {seed:?} sf {scale}: seeded text differs"),
            );
        }
    }
    eprintln!("explicit checks matched {}", tally.matched);
    assert!(
        tally.mismatches.is_empty(),
        "{} mismatches: {:?}",
        tally.mismatches.len(),
        &tally.mismatches[..tally.mismatches.len().min(12)]
    );
}

#[test]
fn replays_nondeterministic_query_34_cases() {
    let Some(root) = checkout() else { return };
    let inputs = Inputs::load(&root);
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(inputs.corpus.join("manifest.json")).unwrap())
            .unwrap();
    let cases = manifest["tpcds"]["nondeterministic_cases"]["cases"]
        .as_array()
        .unwrap();
    let mut tally = Tally::new();
    for case in cases {
        let seed = case["seed"].as_i64();
        let scale = nullable_scale(&case["sf"]);
        for (mode, log_run) in [("plain", false), ("log", true)] {
            let outcome = inputs.single("34", seed, scale, None, None, log_run);
            let wanted = case[mode]["h"].as_array().unwrap();
            tally.check(
                matches!(&outcome, Ok((text, _)) if wanted.iter().all(|hash| hash.as_str() == Some(digest(text).as_str()))),
                || format!("q34 seed {seed:?} sf {scale} {mode}"),
            );
        }
    }
    eprintln!(
        "nondeterministic cases matched {} of {}",
        tally.matched,
        cases.len() * 2
    );
    assert!(tally.mismatches.is_empty(), "{:?}", tally.mismatches);
}

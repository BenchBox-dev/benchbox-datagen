use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use benchbox_datagen_core::{QgenError, QueryRequest, generate_query};
use flate2::read::GzDecoder;
use serde_json::Value;

fn checkout() -> Option<PathBuf> {
    std::env::var_os("BENCHBOX_CHECKOUT").map(PathBuf::from)
}

struct Inputs {
    root: PathBuf,
    distributions: String,
}

impl Inputs {
    fn load(root: PathBuf) -> Self {
        let distributions =
            fs::read_to_string(root.join("benchbox/_binaries/tpc-h/templates/dists.dss")).unwrap();
        Self {
            root,
            distributions,
        }
    }

    fn template(&self, query: &str) -> String {
        let directory = if query == "15a" {
            "variants"
        } else {
            "queries"
        };
        let path = self.root.join(format!(
            "benchbox/_binaries/tpc-h/templates/{directory}/{query}.sql"
        ));
        fs::read_to_string(path).unwrap()
    }

    fn run(&self, query: &str, seed: Option<i64>, scale: f64) -> Result<Vec<String>, QgenError> {
        let template = self.template(query);
        generate_query(&QueryRequest {
            query,
            template: &template,
            distributions: &self.distributions,
            scale_factor: scale,
            seed,
            stream: None,
            default_params: seed.is_none(),
        })
        .map(|generated| generated.params)
    }
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

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn replays_the_conformance_corpus() {
    let Some(root) = checkout() else {
        assert!(
            std::env::var_os("CI").is_none(),
            "BENCHBOX_CHECKOUT is required in CI for the TPC-H corpus replay"
        );
        eprintln!("BENCHBOX_CHECKOUT is not set; skipping the TPC-H corpus replay");
        return;
    };
    let corpus = root.join("tests/fixtures/tpc_query_corpus");
    let inputs = Inputs::load(root);
    let mut mismatches = Vec::new();
    let mut matched = 0_usize;
    let mut timeouts = 0_usize;

    let named = read_records(&corpus, "tpch_named");
    for case in &named {
        let query = case["q"].as_str().unwrap();
        let seed = case["seed"].as_i64();
        let scale = case["sf"].as_f64().unwrap();
        let outcome = inputs.run(query, seed, scale);
        if case.get("s").and_then(Value::as_str) == Some("timeout") {
            match outcome {
                Err(QgenError::NonTerminating { .. }) => timeouts += 1,
                other => mismatches.push(format!(
                    "q{query} seed {seed:?} expected non-termination, got {other:?}"
                )),
            }
            continue;
        }
        match outcome {
            Ok(params) if params == strings(&case["p"]) => matched += 1,
            other => mismatches.push(format!(
                "q{query} seed {seed:?} sf {scale}: {other:?} vs {}",
                case["p"]
            )),
        }
    }

    let random = read_records(&corpus, "tpch_random");
    let mut random_matched = 0_usize;
    for record in &random {
        let seed = record["seed"].as_i64().unwrap();
        for (query, expected) in record["p"].as_object().unwrap() {
            match inputs.run(query, Some(seed), 1.0) {
                Ok(params) if expected.is_array() && params == strings(expected) => {
                    random_matched += 1
                }
                other => mismatches.push(format!("q{query} seed {seed}: {other:?} vs {expected}")),
            }
        }
    }

    eprintln!(
        "named matched {matched}/{} (timeouts {timeouts}); random matched {random_matched}/{}",
        named.len(),
        random.len() * 23
    );
    assert!(
        mismatches.is_empty(),
        "{} mismatches, first: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(10)]
    );
}

fn synthetic_distributions() -> String {
    let mut text = String::new();
    for name in [
        "p_cntr", "colors", "p_types", "nations", "nations2", "regions", "smode", "msegmnt",
        "Q13a", "Q13b",
    ] {
        text.push_str(&format!(
            "begin {name}\ncount|2\nfirst|1\nsecond|1\nend {name}\n"
        ));
    }
    text
}

fn synthetic(query: &str, seed: Option<i64>, default_params: bool) -> Result<(), QgenError> {
    let distributions = synthetic_distributions();
    generate_query(&QueryRequest {
        query,
        template: "select :1;\n",
        distributions: &distributions,
        scale_factor: 1.0,
        seed,
        stream: None,
        default_params,
    })
    .map(|_| ())
}

#[test]
fn seed_zero_queries_that_never_terminate_are_refused() {
    for query in ["7", "12"] {
        assert!(matches!(
            synthetic(query, Some(0), false),
            Err(QgenError::NonTerminating { .. })
        ));
    }
    assert!(synthetic("6", Some(0), false).is_ok());
}

#[test]
fn a_seed_or_default_parameters_are_required() {
    assert_eq!(synthetic("6", None, false), Err(QgenError::SeedRequired));
}

#[test]
fn seeds_outside_the_generator_range_are_refused() {
    for seed in [-1, 2_147_483_647] {
        assert_eq!(
            synthetic("6", Some(seed), false),
            Err(QgenError::SeedOutOfRange(seed))
        );
    }
    assert!(synthetic("6", Some(2_147_483_646), false).is_ok());
}

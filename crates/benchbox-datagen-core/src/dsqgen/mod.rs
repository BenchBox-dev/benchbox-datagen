mod date;
mod eval;
mod idx;
mod lexer;
mod parser;
mod rng;

use thiserror::Error;

use eval::Run;
use idx::DistIndex;
use parser::{Arena, Environment, parse_template};

const DEFAULT_SEED: i64 = 19_620_718;
const PERMUTATION_SEED: u64 = 19_620_718;
const MAX_SEED: i64 = 2_147_483_646;
const MAX_SCALE: f64 = 100_000.0;
const MULTI_PART_QUERIES: [u32; 4] = [14, 23, 24, 39];

#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum DsqgenError {
    #[error("seed {0} is outside the supported range 0..=2147483646")]
    SeedOutOfRange(i64),
    #[error(
        "scale factor must be finite and positive, and below 100000 (TPC-DS scale factors of 1 or more must be whole numbers)"
    )]
    InvalidScale,
    #[error("malformed distribution index: {0}")]
    Distributions(String),
    #[error("template error at line {line}: {message}")]
    Template { line: u32, message: String },
    #[error("evaluation error: {0}")]
    Evaluation(String),
    #[error("unsupported TPC-DS query {0:?}")]
    UnsupportedQuery(String),
    #[error("query {query} has no substitution for parameter {name:?}")]
    UnknownParameter { query: String, name: String },
    #[error("parameter name must look like 'YEAR.01' (as dsqgen -LOG writes it), got {0:?}")]
    InvalidParameterName(String),
    #[error("at least one template and one stream are required")]
    NothingToGenerate,
}

#[derive(Debug, Clone, Copy)]
pub struct TemplateSource<'a> {
    pub name: &'a str,
    pub text: &'a str,
}

#[derive(Debug, Clone)]
pub struct StreamsRequest<'a> {
    pub templates: &'a [TemplateSource<'a>],
    pub dialect: &'a str,
    pub distributions: &'a [u8],
    pub scale_factor: f64,
    pub seed: Option<i64>,
    pub streams: u32,
    pub qualify: bool,
    pub log_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedStatement {
    pub template: String,
    pub text: String,
    pub parameters: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOutput {
    pub stream: u32,
    pub queries: Vec<GeneratedStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsOutput {
    pub preamble: String,
    pub streams: Vec<StreamOutput>,
    pub log: String,
}

#[derive(Debug, Clone)]
pub struct DsqgenQueryRequest<'a> {
    pub query: &'a str,
    pub template: &'a str,
    pub template_name: Option<&'a str>,
    pub dialect: &'a str,
    pub distributions: &'a [u8],
    pub scale_factor: f64,
    pub seed: Option<i64>,
    pub stream: Option<u32>,
    pub parameters: Option<&'a [(String, String)]>,
    pub log_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DsqgenQuery {
    pub text: String,
    pub parameters: Vec<(String, String)>,
}

fn validate_scale(scale_factor: f64) -> Result<(), DsqgenError> {
    if !scale_factor.is_finite()
        || scale_factor <= 0.0
        || scale_factor > MAX_SCALE
        || (scale_factor >= 1.0 && scale_factor.fract() != 0.0)
    {
        return Err(DsqgenError::InvalidScale);
    }
    Ok(())
}

fn table_rowcount(
    dists: &DistIndex<'_>,
    scale_factor: f64,
    name: &str,
) -> Result<Option<i64>, DsqgenError> {
    if let Some(rowcounts) = dists.find("rowcounts")? {
        for row in 1..=rowcounts.length as i64 {
            if rowcounts.member(row, 1)?.eq_ignore_ascii_case(name) {
                return crate::tpcds::dsqgen_rowcount(name, scale_factor)
                    .and_then(|rows| i64::try_from(rows).ok())
                    .map(Some)
                    .ok_or_else(|| {
                        DsqgenError::Evaluation(format!("rowcount({name:?}) is not modelled"))
                    });
            }
        }
    }
    Ok(dists.find(name)?.map(|dist| dist.length as i64))
}

pub fn generate_streams(request: &StreamsRequest<'_>) -> Result<StreamsOutput, DsqgenError> {
    if request.templates.is_empty() || request.streams == 0 {
        return Err(DsqgenError::NothingToGenerate);
    }
    validate_scale(request.scale_factor)?;
    let seed = request.seed.unwrap_or(DEFAULT_SEED);
    if !(0..=MAX_SEED).contains(&seed) {
        return Err(DsqgenError::SeedOutOfRange(seed));
    }
    let dists = DistIndex::new(request.distributions)?;
    let scale_factor = request.scale_factor;
    let rowcount = |name: &str| table_rowcount(&dists, scale_factor, name);
    let env = Environment {
        scale_factor,
        rowcount: &rowcount,
    };
    let mut arena = Arena::new();
    let mut parsed = Vec::with_capacity(request.templates.len());
    let mut preamble = String::new();
    for source in request.templates {
        let template = parse_template(&mut arena, &env, source.name, source.text, request.dialect)?;
        if template.warnings > 0 {
            preamble.push_str(&format!(
                "WARNING: {} warnings encountered parsing {}\nWARNING: Query output may not be correct!\n",
                template.warnings, source.name
            ));
        }
        parsed.push(template);
    }
    let mut run = Run::new(&dists, arena, seed as u64, request.log_run);
    for template in parsed {
        run.add_template(template);
    }
    let count = request.templates.len();
    let mut order: Vec<u32> = (0..count as u32).collect();
    let mut log = Some(String::new());
    let mut streams = Vec::with_capacity(request.streams as usize);
    for stream in 0..request.streams {
        run.reset_permutation_rng(PERMUTATION_SEED);
        run.shuffle_order(&mut order)?;
        run.stream_number = i64::from(stream);
        if let Some(log) = &mut log {
            log.push_str(&format!("BEGIN STREAM {stream}\n"));
        }
        let mut queries = Vec::with_capacity(count);
        for position in 1..=count {
            run.query_number = position as i64;
            let id = if request.qualify {
                position - 1
            } else {
                order[position - 1] as usize
            };
            let (text, parameters) = run.generate(id, &mut log)?;
            if let Some(log) = &mut log {
                log.push('\n');
            }
            queries.push(GeneratedStatement {
                template: request.templates[id].name.to_owned(),
                text,
                parameters,
            });
        }
        if let Some(log) = &mut log {
            log.push_str(&format!("END STREAM {stream}\n"));
        }
        streams.push(StreamOutput { stream, queries });
    }
    Ok(StreamsOutput {
        preamble,
        streams,
        log: log.unwrap_or_default(),
    })
}

fn parse_parameter_name(key: &str) -> Option<(&str, u64)> {
    let (name, position) = key.rsplit_once('.')?;
    let mut characters = name.chars();
    let first = characters.next()?;
    if !(first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
        || position.is_empty()
        || !position.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((name, position.parse().ok()?))
}

fn replace_literal(text: &str, patterns: &[String], value: &str) -> (String, usize) {
    let mut out = String::with_capacity(text.len());
    let mut count = 0;
    let mut at = 0;
    while at < text.len() {
        match patterns
            .iter()
            .find(|pattern| text[at..].starts_with(pattern.as_str()))
        {
            Some(pattern) => {
                out.push_str(value);
                at += pattern.len();
                count += 1;
            }
            None => {
                let character = text[at..].chars().next().unwrap_or('\u{fffd}');
                out.push(character);
                at += character.len_utf8();
            }
        }
    }
    (out, count)
}

fn defines_name(template: &str, name: &str) -> bool {
    let mut from = 0;
    while let Some(found) = template[from..].find("define") {
        let start = from + found;
        from = start + 6;
        let boundary = template[..start]
            .chars()
            .next_back()
            .is_none_or(|before| !(before == '_' || before.is_alphanumeric()));
        if !boundary {
            continue;
        }
        let rest = &template[from..];
        let trimmed = rest.trim_start();
        if trimmed.len() == rest.len() {
            continue;
        }
        if let Some(after) = trimmed.strip_prefix(name)
            && after.trim_start().starts_with('=')
        {
            return true;
        }
    }
    false
}

pub fn substitute_parameters(
    template: &str,
    parameters: &[(String, String)],
    query: &str,
) -> Result<String, DsqgenError> {
    let mut text = template.to_owned();
    for (key, value) in parameters {
        let (name, position) = parse_parameter_name(key)
            .ok_or_else(|| DsqgenError::InvalidParameterName(key.clone()))?;
        let mut patterns = vec![format!("[{name}.{position}]")];
        if position == 1 {
            patterns.push(format!("[{name}]"));
        }
        let (replaced, count) = replace_literal(&text, &patterns, value);
        if count == 0 && !defines_name(&replaced, name) {
            return Err(DsqgenError::UnknownParameter {
                query: query.to_owned(),
                name: key.clone(),
            });
        }
        text = replaced;
    }
    Ok(text)
}

pub fn split_statements(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let bytes = trimmed.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b';' && bytes.get(at + 1) == Some(&b'\n') {
            let mut after = at + 1;
            while bytes.get(after) == Some(&b'\n') {
                after += 1;
            }
            if trimmed[after..]
                .chars()
                .next()
                .is_some_and(|next| !next.is_whitespace())
            {
                statements.push(trimmed[start..=at].trim().to_owned());
                start = after;
                at = after;
                continue;
            }
        }
        at += 1;
    }
    statements.push(trimmed[start..].trim().to_owned());
    statements.retain(|statement| !statement.is_empty());
    statements
}

fn split_query_id(query: &str) -> Result<(u32, Option<char>), DsqgenError> {
    let unsupported = || DsqgenError::UnsupportedQuery(query.to_owned());
    let (digits, variant) = match query.chars().last() {
        Some(last @ ('a' | 'b')) => (&query[..query.len() - 1], Some(last)),
        _ => (query, None),
    };
    let number = digits
        .parse::<u32>()
        .ok()
        .filter(|number| (1..=99).contains(number))
        .ok_or_else(unsupported)?;
    Ok((number, variant))
}

pub fn generate_query(request: &DsqgenQueryRequest<'_>) -> Result<DsqgenQuery, DsqgenError> {
    let (number, variant) = split_query_id(request.query)?;
    let multi_part = MULTI_PART_QUERIES.contains(&number) && variant.is_some();
    let default_name = format!("query{number}.tpl");
    let template_name = request.template_name.unwrap_or(&default_name);
    let rewritten;
    let template = match request.parameters {
        Some(parameters) => {
            rewritten = substitute_parameters(request.template, parameters, request.query)?;
            rewritten.as_str()
        }
        None => request.template,
    };
    let output = generate_streams(&StreamsRequest {
        templates: &[TemplateSource {
            name: template_name,
            text: template,
        }],
        dialect: request.dialect,
        distributions: request.distributions,
        scale_factor: request.scale_factor,
        seed: request.seed,
        streams: request.stream.map_or(1, |stream| stream + 1),
        qualify: false,
        log_run: request.log_run,
    })?;
    let last = output
        .streams
        .last()
        .and_then(|stream| stream.queries.first())
        .ok_or(DsqgenError::NothingToGenerate)?;
    let text = if request.stream.is_some() {
        split_statements(&last.text).join("\n\n")
    } else {
        let whole = format!("{}{}", output.preamble, last.text);
        let whole = whole.trim().to_owned();
        if multi_part {
            let parts: Vec<&str> = whole
                .split(';')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .collect();
            let index = usize::from(variant == Some('b'));
            parts
                .get(index)
                .map(|part| (*part).to_owned())
                .ok_or_else(|| DsqgenError::UnsupportedQuery(request.query.to_owned()))?
        } else {
            whole
        }
    };
    Ok(DsqgenQuery {
        text,
        parameters: last.parameters.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect()
    }

    #[test]
    fn explicit_parameters_replace_bracketed_names() {
        let template = "define YEAR = random(1, 2, uniform);\nselect [YEAR] [YEAR.1] [LIST.2];";
        let rewritten = substitute_parameters(
            template,
            &pairs(&[("YEAR.01", "2000"), ("LIST.02", "x")]),
            "1",
        )
        .unwrap();
        assert_eq!(
            rewritten,
            "define YEAR = random(1, 2, uniform);\nselect 2000 2000 x;"
        );
    }

    #[test]
    fn explicit_parameters_without_a_target_are_errors() {
        assert!(matches!(
            substitute_parameters("select 1;", &pairs(&[("YEAR.01", "1")]), "1"),
            Err(DsqgenError::UnknownParameter { .. })
        ));
        assert!(matches!(
            substitute_parameters("select 1;", &pairs(&[("YEAR", "1")]), "1"),
            Err(DsqgenError::InvalidParameterName(_))
        ));
        assert!(substitute_parameters("define YEAR =1;", &pairs(&[("YEAR.01", "1")]), "1").is_ok());
    }

    #[test]
    fn statements_split_after_a_semicolon_and_newlines() {
        assert_eq!(
            split_statements("\nselect 1;\n\nselect 2;\n\n"),
            ["select 1;", "select 2;"]
        );
        assert_eq!(split_statements("a;\n b"), ["a;\n b"]);
    }

    #[test]
    fn malformed_index_files_are_errors() {
        for bytes in [&b""[..], &[0, 0, 0, 9][..], &[255; 64][..]] {
            assert!(DistIndex::new(bytes).is_err());
        }
    }

    #[test]
    fn invalid_requests_are_refused() {
        let request = |scale_factor: f64, seed: Option<i64>| StreamsRequest {
            templates: &[TemplateSource {
                name: "q.tpl",
                text: "select 1;",
            }],
            dialect: "",
            distributions: &[],
            scale_factor,
            seed,
            streams: 1,
            qualify: false,
            log_run: false,
        };
        assert_eq!(
            generate_streams(&request(0.0, None)),
            Err(DsqgenError::InvalidScale)
        );
        assert_eq!(
            generate_streams(&request(1.5, None)),
            Err(DsqgenError::InvalidScale)
        );
        assert_eq!(
            generate_streams(&request(1.0, Some(-1))),
            Err(DsqgenError::SeedOutOfRange(-1))
        );
        assert!(matches!(
            generate_streams(&request(1.0, None)),
            Err(DsqgenError::Distributions(_))
        ));
    }
}

use benchbox_datagen_core::{DsqgenError, DsqgenQuery, DsqgenQueryRequest, generate_tpcds_query};

const EMPTY_INDEX: [u8; 4] = [0; 4];
const DIALECT: &str = "define _END = \"\";\n";

fn run_with(
    query: &str,
    template: &str,
    distributions: &[u8],
    scale_factor: f64,
    stream: Option<u32>,
) -> Result<DsqgenQuery, DsqgenError> {
    generate_tpcds_query(&DsqgenQueryRequest {
        query,
        template,
        template_name: None,
        dialect: DIALECT,
        distributions,
        scale_factor,
        seed: Some(7),
        stream,
        parameters: None,
    })
}

fn run(template: &str) -> Result<DsqgenQuery, DsqgenError> {
    run_with("1", template, &EMPTY_INDEX, 1.0, None)
}

fn index_with_distribution(weight_width: i32, length: i32) -> Vec<u8> {
    let mut bytes = 1_i32.to_be_bytes().to_vec();
    let mut name = [0_u8; 20];
    name[..3].copy_from_slice(b"bad");
    bytes.extend_from_slice(&name);
    for field in [0, 4, 0, length, weight_width, 0, 0] {
        bytes.extend_from_slice(&field.to_be_bytes());
    }
    bytes
}

#[test]
fn a_huge_list_count_is_an_error() {
    let template = "define A = ulist(random(1,10,uniform), 10000000000000);\nselect [A.1];";
    assert!(matches!(run(template), Err(DsqgenError::Template { .. })));
    let template = "define A = list(random(1,10,uniform), 10000000000000);\nselect [A.1];";
    assert!(matches!(run(template), Err(DsqgenError::Template { .. })));
}

#[test]
fn a_huge_use_count_is_an_error() {
    let template = "define A = ulist(random(1,10,uniform), 10000);\nselect [A9999999999999];";
    assert!(matches!(run(template), Err(DsqgenError::Evaluation(_))));
}

#[test]
fn deeply_nested_expressions_are_errors() {
    let template = format!(
        "define A = {}1{};\nselect [A];",
        "random(".repeat(100_000),
        ")".repeat(100_000)
    );
    assert!(matches!(run(&template), Err(DsqgenError::Template { .. })));
    let template = format!("define A = 1{};\nselect [A];", "+1".repeat(100_000));
    assert!(matches!(run(&template), Err(DsqgenError::Template { .. })));
}

#[test]
fn long_substitution_chains_are_errors() {
    let mut template = String::from("define XA = 1;\n");
    for index in 1..100_000 {
        template.push_str(&format!(
            "define X{} = [X{}];\n",
            name(index),
            name(index - 1)
        ));
    }
    template.push_str(&format!("select [X{}];", name(99_999)));
    assert!(run(&template).is_err());
    let mut template = String::from("define XA = 1;\n");
    for index in 1..300 {
        template.push_str(&format!(
            "define X{} = [X{}];\n",
            name(index),
            name(index - 1)
        ));
    }
    template.push_str(&format!("select [X{}];", name(299)));
    assert!(matches!(run(&template), Err(DsqgenError::Evaluation(_))));
}

fn name(mut index: usize) -> String {
    let mut letters = String::new();
    loop {
        letters.push((b'A' + (index % 26) as u8) as char);
        index /= 26;
        if index == 0 {
            return letters;
        }
    }
}

#[test]
fn hostile_dates_are_errors() {
    for end in [
        "2000-01-999999999999",
        "2000-13-01",
        "2000-00-10",
        "2001-02-29",
        "2000-04-31",
    ] {
        for distribution in ["sales", "returns", "uniform"] {
            let template =
                format!("define D = date(\"2000-01-01\",\"{end}\",{distribution});\nselect [D];");
            assert!(
                matches!(run(&template), Err(DsqgenError::Evaluation(_))),
                "{end} {distribution}"
            );
        }
    }
    let template = "define D = date(\"2000-02-29\",\"2000-03-01\",uniform);\nselect [D];";
    assert!(run(template).is_ok());
}

#[test]
fn an_oversized_distribution_width_is_an_error() {
    for (weight_width, length) in [(i32::MAX, 1), (i32::MAX, 0), (3, i32::MAX), (1, 1_000_000)] {
        let bytes = index_with_distribution(weight_width, length);
        let template = "define A = dist(bad, 1, 1);\nselect [A];";
        assert!(
            matches!(
                run_with("1", template, &bytes, 1.0, None),
                Err(DsqgenError::Distributions(_))
            ),
            "{weight_width} {length}"
        );
    }
}

#[test]
fn overflowing_arithmetic_is_an_error_or_wraps() {
    let templates = [
        "define A = random(-9223372036854775808, 9223372036854775807, uniform);\nselect [A];",
        "define A = ulist(random(-9223372036854775808, 9223372036854775807, uniform), 2);\nselect [A.1];",
        "define A = text({\"a\", 9223372036854775807}, {\"b\", 9223372036854775807});\nselect [A];",
    ];
    for template in templates {
        assert!(
            matches!(run(template), Err(DsqgenError::Evaluation(_))),
            "{template}"
        );
    }
    let wrapping = "define A = 9223372036854775807 + 1;\ndefine B = -9223372036854775808 / -1;\nselect [A] [B];";
    assert!(run(wrapping).is_ok());
}

#[test]
fn a_ulist_over_a_single_key_does_not_wrap() {
    let one = "define A = ulist(random(1,1,uniform), 3);\nselect [A.1] [A.2] [A.3];";
    let generated = run(one).unwrap();
    assert!(!generated.wrapped);
    assert!(generated.text.contains("1 1 1"));
    let two = "define A = ulist(random(1,2,uniform), 3);\nselect [A.1] [A.2] [A.3];";
    let generated = run(two).unwrap();
    assert!(generated.wrapped);
    let values: Vec<&str> = generated
        .text
        .trim()
        .trim_start_matches("select ")
        .trim_end_matches(';')
        .split(' ')
        .collect();
    assert_eq!(values[0], values[2]);
    assert_ne!(values[0], values[1]);
}

#[test]
fn stream_numbers_that_overflow_are_errors() {
    assert_eq!(
        run_with("1", "select 1;", &EMPTY_INDEX, 1.0, Some(u32::MAX)),
        Err(DsqgenError::StreamOutOfRange(u32::MAX))
    );
}

#[test]
fn the_default_template_name_keeps_the_variant() {
    let template = "select '[_TEMPLATE]';";
    let text = |query| {
        run_with(query, template, &EMPTY_INDEX, 1.0, None)
            .unwrap()
            .text
    };
    assert!(text("27a").contains("query27a.tpl"));
    assert!(text("27").contains("query27.tpl"));
    assert!(text("14a").contains("query14.tpl"));
}

#[test]
fn the_scale_limit_message_is_accurate() {
    let error = run_with("1", "select 1;", &EMPTY_INDEX, 100_001.0, None).unwrap_err();
    assert_eq!(error, DsqgenError::InvalidScale);
    assert!(error.to_string().contains("at most 100000"));
    assert!(run_with("1", "select 1;", &EMPTY_INDEX, 100_000.0, None).is_ok());
}

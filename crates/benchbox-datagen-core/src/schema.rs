use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Kind {
    Int,
    Int32,
    Decimal,
    Decimal128(u8, i8),
    Float32,
    Float64,
    Fixed(u8),
    Bool,
    Timestamp,
    Date,
    Text,
    Vector(i32),
}

impl Kind {
    pub(crate) fn field(self, name: &str, nullable: bool) -> Field {
        let field = Field::new(name, self.data_type(), nullable);
        match self {
            Kind::Fixed(scale) => field.with_metadata(HashMap::from([(
                FIXED_SCALE_KEY.to_owned(),
                scale.to_string(),
            )])),
            _ => field,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Kind::Int => DataType::Int64,
            Kind::Int32 => DataType::Int32,
            Kind::Decimal => DataType::Decimal128(15, 2),
            Kind::Decimal128(precision, scale) => DataType::Decimal128(precision, scale),
            Kind::Float32 => DataType::Float32,
            Kind::Float64 | Kind::Fixed(_) => DataType::Float64,
            Kind::Bool => DataType::Boolean,
            Kind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Kind::Date => DataType::Date32,
            Kind::Text => DataType::Utf8,
            Kind::Vector(dimensions) => DataType::FixedSizeList(
                Arc::new(Field::new_list_field(DataType::Float32, true)),
                dimensions,
            ),
        }
    }
}

#[allow(unused_imports)]
pub(crate) use Kind::*;

pub(crate) const FIXED_SCALE_KEY: &str = "benchbox.fixed_scale";

pub(crate) type Column = (&'static str, Kind, bool);

const TPCH: [(&str, &[Column]); 8] = [
    (
        "nation",
        &[
            ("n_nationkey", Int, false),
            ("n_name", Text, false),
            ("n_regionkey", Int, false),
            ("n_comment", Text, true),
        ],
    ),
    (
        "region",
        &[
            ("r_regionkey", Int, false),
            ("r_name", Text, false),
            ("r_comment", Text, true),
        ],
    ),
    (
        "part",
        &[
            ("p_partkey", Int, false),
            ("p_name", Text, false),
            ("p_mfgr", Text, false),
            ("p_brand", Text, false),
            ("p_type", Text, false),
            ("p_size", Int, false),
            ("p_container", Text, false),
            ("p_retailprice", Decimal, false),
            ("p_comment", Text, false),
        ],
    ),
    (
        "supplier",
        &[
            ("s_suppkey", Int, false),
            ("s_name", Text, false),
            ("s_address", Text, false),
            ("s_nationkey", Int, false),
            ("s_phone", Text, false),
            ("s_acctbal", Decimal, false),
            ("s_comment", Text, false),
        ],
    ),
    (
        "partsupp",
        &[
            ("ps_partkey", Int, false),
            ("ps_suppkey", Int, false),
            ("ps_availqty", Int, false),
            ("ps_supplycost", Decimal, false),
            ("ps_comment", Text, false),
        ],
    ),
    (
        "customer",
        &[
            ("c_custkey", Int, false),
            ("c_name", Text, false),
            ("c_address", Text, false),
            ("c_nationkey", Int, false),
            ("c_phone", Text, false),
            ("c_acctbal", Decimal, false),
            ("c_mktsegment", Text, false),
            ("c_comment", Text, false),
        ],
    ),
    (
        "orders",
        &[
            ("o_orderkey", Int, false),
            ("o_custkey", Int, false),
            ("o_orderstatus", Text, false),
            ("o_totalprice", Decimal, false),
            ("o_orderdate", Date, false),
            ("o_orderpriority", Text, false),
            ("o_clerk", Text, false),
            ("o_shippriority", Int, false),
            ("o_comment", Text, false),
        ],
    ),
    (
        "lineitem",
        &[
            ("l_orderkey", Int, false),
            ("l_partkey", Int, false),
            ("l_suppkey", Int, false),
            ("l_linenumber", Int, false),
            ("l_quantity", Decimal, false),
            ("l_extendedprice", Decimal, false),
            ("l_discount", Decimal, false),
            ("l_tax", Decimal, false),
            ("l_returnflag", Text, false),
            ("l_linestatus", Text, false),
            ("l_shipdate", Date, false),
            ("l_commitdate", Date, false),
            ("l_receiptdate", Date, false),
            ("l_shipinstruct", Text, false),
            ("l_shipmode", Text, false),
            ("l_comment", Text, false),
        ],
    ),
];

pub(crate) fn build(tables: &[(&'static str, &[Column])]) -> HashMap<&'static str, SchemaRef> {
    tables
        .iter()
        .map(|(table, columns)| {
            let fields: Vec<Field> = columns
                .iter()
                .map(|(name, kind, nullable)| kind.field(name, *nullable))
                .collect();
            (*table, Arc::new(Schema::new(fields)) as SchemaRef)
        })
        .collect()
}

static TPCH_SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> = LazyLock::new(|| {
    TPCH.iter()
        .map(|(table, columns)| {
            let fields: Vec<Field> = columns
                .iter()
                .map(|(name, kind, nullable)| Field::new(*name, kind.data_type(), *nullable))
                .collect();
            (*table, Arc::new(Schema::new(fields)))
        })
        .collect()
});

pub(crate) fn tpch(table: &str) -> Option<SchemaRef> {
    TPCH_SCHEMAS.get(table).cloned()
}

const TPCDS_SPEC: &str = include_str!("tpcds_schema.txt");

fn parse_type(token: &str) -> DataType {
    match token {
        "int64" => DataType::Int64,
        "string" => DataType::Utf8,
        "date32" => DataType::Date32,
        decimal => {
            let (precision, scale) = decimal
                .strip_prefix("decimal128(")
                .and_then(|rest| rest.strip_suffix(')'))
                .and_then(|inner| inner.split_once(','))
                .expect("the TPC-DS schema spec only uses int64, string, date32 and decimal128");
            DataType::Decimal128(
                precision.parse().expect("decimal precision is a number"),
                scale.parse().expect("decimal scale is a number"),
            )
        }
    }
}

static TPCDS_SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> = LazyLock::new(|| {
    let mut tables: HashMap<&'static str, Vec<Field>> = HashMap::new();
    for line in TPCDS_SPEC.lines() {
        let mut parts = line.split_whitespace();
        let (Some(table), Some(name), Some(data_type), Some(nullability)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        tables.entry(table).or_default().push(Field::new(
            name,
            parse_type(data_type),
            nullability == "null",
        ));
    }
    tables
        .into_iter()
        .map(|(table, fields)| (table, Arc::new(Schema::new(fields))))
        .collect()
});

pub(crate) fn tpcds(table: &str) -> Option<SchemaRef> {
    TPCDS_SCHEMAS.get(table).cloned()
}

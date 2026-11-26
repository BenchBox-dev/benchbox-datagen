use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

#[derive(Clone, Copy)]
enum Kind {
    Int,
    Decimal,
    Date,
    Text,
}

impl Kind {
    fn data_type(self) -> DataType {
        match self {
            Kind::Int => DataType::Int64,
            Kind::Decimal => DataType::Decimal128(15, 2),
            Kind::Date => DataType::Date32,
            Kind::Text => DataType::Utf8,
        }
    }
}

use Kind::{Date, Decimal, Int, Text};

type Column = (&'static str, Kind, bool);

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

const SSB: [(&str, &[Column]); 5] = [
    (
        "date",
        &[
            ("d_datekey", Int, true),
            ("d_date", Text, true),
            ("d_dayofweek", Text, true),
            ("d_month", Text, true),
            ("d_year", Int, true),
            ("d_yearmonthnum", Int, true),
            ("d_yearmonth", Text, true),
            ("d_daynuminweek", Int, true),
            ("d_daynuminmonth", Int, true),
            ("d_daynuminyear", Int, true),
            ("d_monthnuminyear", Int, true),
            ("d_weeknuminyear", Int, true),
            ("d_sellingseason", Text, true),
            ("d_lastdayinweekfl", Int, true),
            ("d_lastdayinmonthfl", Int, true),
            ("d_holidayfl", Int, true),
            ("d_weekdayfl", Int, true),
        ],
    ),
    (
        "customer",
        &[
            ("c_custkey", Int, true),
            ("c_name", Text, true),
            ("c_address", Text, true),
            ("c_city", Text, true),
            ("c_nation", Text, true),
            ("c_region", Text, true),
            ("c_phone", Text, true),
            ("c_mktsegment", Text, true),
        ],
    ),
    (
        "supplier",
        &[
            ("s_suppkey", Int, true),
            ("s_name", Text, true),
            ("s_address", Text, true),
            ("s_city", Text, true),
            ("s_nation", Text, true),
            ("s_region", Text, true),
            ("s_phone", Text, true),
        ],
    ),
    (
        "part",
        &[
            ("p_partkey", Int, true),
            ("p_name", Text, true),
            ("p_mfgr", Text, true),
            ("p_category", Text, true),
            ("p_brand1", Text, true),
            ("p_color", Text, true),
            ("p_type", Text, true),
            ("p_size", Int, true),
            ("p_container", Text, true),
        ],
    ),
    (
        "lineorder",
        &[
            ("lo_orderkey", Int, true),
            ("lo_linenumber", Int, true),
            ("lo_custkey", Int, true),
            ("lo_partkey", Int, true),
            ("lo_suppkey", Int, true),
            ("lo_orderdate", Int, false),
            ("lo_orderpriority", Text, true),
            ("lo_shippriority", Int, true),
            ("lo_quantity", Int, true),
            ("lo_extendedprice", Int, true),
            ("lo_ordtotalprice", Int, true),
            ("lo_discount", Int, true),
            ("lo_revenue", Int, true),
            ("lo_supplycost", Int, true),
            ("lo_tax", Int, true),
            ("lo_commitdate", Int, true),
            ("lo_shipmode", Text, true),
        ],
    ),
];

fn build(tables: &[(&'static str, &[Column])]) -> HashMap<&'static str, SchemaRef> {
    tables
        .iter()
        .map(|(table, columns)| {
            let fields: Vec<Field> = columns
                .iter()
                .map(|(name, kind, nullable)| Field::new(*name, kind.data_type(), *nullable))
                .collect();
            (*table, Arc::new(Schema::new(fields)) as SchemaRef)
        })
        .collect()
}

static SSB_SCHEMAS: LazyLock<HashMap<&'static str, SchemaRef>> = LazyLock::new(|| build(&SSB));

pub(crate) fn ssb(table: &str) -> Option<SchemaRef> {
    SSB_SCHEMAS.get(table).cloned()
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

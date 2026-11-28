use std::fmt::Write as _;
use std::io;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::calendar::civil_from_days;
use crate::schema::{FIXED_SCALE_KEY, TRIMMED_DECIMAL_KEY};

const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;
const FLOAT_SIGNIFICANT_DIGITS: usize = 6;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TextFormat {
    pub(crate) delimiter: u8,
    pub(crate) header: bool,
    pub(crate) quote: bool,
    pub(crate) extension: &'static str,
}

#[allow(dead_code)]
impl TextFormat {
    pub(crate) fn tbl() -> Self {
        Self {
            delimiter: b'|',
            header: false,
            quote: false,
            extension: "tbl",
        }
    }

    pub(crate) fn csv(delimiter: u8, header: bool) -> Self {
        Self {
            delimiter,
            header,
            quote: true,
            extension: "csv",
        }
    }
}

enum Column<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Date(&'a Date32Array),
    Text(&'a StringArray),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Fixed(&'a Float64Array, usize),
    Bool(&'a BooleanArray),
    Timestamp(&'a TimestampMicrosecondArray),
    Decimal(&'a Decimal128Array, usize),
    TrimmedDecimal(&'a Decimal128Array, usize),
    Vector(&'a FixedSizeListArray, &'a Float32Array, usize),
}

fn unsupported(name: &str, data_type: &DataType) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("delimited text cannot encode column {name} of type {data_type}"),
    )
}

fn column<'a>(field: &Field, array: &'a dyn Array) -> io::Result<Column<'a>> {
    let name = field.name();
    let data_type = array.data_type();
    let fixed = match field.metadata().get(FIXED_SCALE_KEY) {
        None => None,
        Some(scale) => Some(
            scale
                .parse::<usize>()
                .map_err(|_| unsupported(name, data_type))?,
        ),
    };
    if fixed.is_some() && *data_type != DataType::Float64 {
        return Err(unsupported(name, data_type));
    }
    let trimmed = field.metadata().contains_key(TRIMMED_DECIMAL_KEY);
    if trimmed && !matches!(data_type, DataType::Decimal128(_, scale) if *scale >= 0) {
        return Err(unsupported(name, data_type));
    }
    let any = array.as_any();
    let column = match data_type {
        DataType::Int32 => any.downcast_ref().map(Column::Int32),
        DataType::Int64 => any.downcast_ref().map(Column::Int64),
        DataType::Date32 => any.downcast_ref().map(Column::Date),
        DataType::Utf8 => any.downcast_ref().map(Column::Text),
        DataType::Float32 => any.downcast_ref().map(Column::Float32),
        DataType::Float64 => any
            .downcast_ref()
            .map(|array| fixed.map_or(Column::Float64(array), |scale| Column::Fixed(array, scale))),
        DataType::Boolean => any.downcast_ref().map(Column::Bool),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            any.downcast_ref().map(Column::Timestamp)
        }
        DataType::Decimal128(_, scale) if *scale >= 0 => any.downcast_ref().map(|array| {
            if trimmed {
                Column::TrimmedDecimal(array, *scale as usize)
            } else {
                Column::Decimal(array, *scale as usize)
            }
        }),
        DataType::FixedSizeList(item, length)
            if *item.data_type() == DataType::Float32 && *length > 0 =>
        {
            any.downcast_ref::<FixedSizeListArray>().and_then(|list| {
                list.values()
                    .as_any()
                    .downcast_ref()
                    .map(|values| Column::Vector(list, values, *length as usize))
            })
        }
        _ => None,
    };
    column.ok_or_else(|| unsupported(name, data_type))
}

fn columns(batch: &RecordBatch) -> io::Result<Vec<Column<'_>>> {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| column(field, array.as_ref()))
        .collect()
}

pub(crate) fn check(schema: &arrow::datatypes::Schema) -> io::Result<()> {
    schema.fields().iter().try_for_each(|field| {
        let empty = arrow::array::new_empty_array(field.data_type());
        column(field, empty.as_ref()).map(drop)
    })
}

fn push_padded(out: &mut Vec<u8>, value: i64, width: usize) {
    let mut digits = itoa::Buffer::new();
    let digits = digits.format(value).as_bytes();
    out.extend(std::iter::repeat_n(
        b'0',
        width.saturating_sub(digits.len()),
    ));
    out.extend_from_slice(digits);
}

fn push_date(out: &mut Vec<u8>, days: i64) {
    let (year, month, day) = civil_from_days(days);
    push_padded(out, year, 4);
    out.push(b'-');
    push_padded(out, month, 2);
    out.push(b'-');
    push_padded(out, day, 2);
}

fn push_timestamp(out: &mut Vec<u8>, micros: i64) {
    let seconds = micros.div_euclid(MICROS_PER_SECOND);
    let fraction = micros.rem_euclid(MICROS_PER_SECOND);
    let time = seconds.rem_euclid(SECONDS_PER_DAY);
    push_date(out, seconds.div_euclid(SECONDS_PER_DAY));
    out.push(b' ');
    push_padded(out, time / 3_600, 2);
    out.push(b':');
    push_padded(out, time / 60 % 60, 2);
    out.push(b':');
    push_padded(out, time % 60, 2);
    if fraction != 0 {
        out.push(b'.');
        push_padded(out, fraction, 6);
    }
}

fn push_decimal(out: &mut Vec<u8>, value: i128, scale: usize) {
    if value < 0 {
        out.push(b'-');
    }
    let mut digits = itoa::Buffer::new();
    let digits = digits.format(value.unsigned_abs()).as_bytes();
    if scale == 0 {
        out.extend_from_slice(digits);
        return;
    }
    if digits.len() > scale {
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        out.extend_from_slice(whole);
        out.push(b'.');
        out.extend_from_slice(fraction);
    } else {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat_n(b'0', scale - digits.len()));
        out.extend_from_slice(digits);
    }
}

fn push_trimmed_decimal(out: &mut Vec<u8>, scratch: &mut Scratch, value: i128, scale: usize) {
    let mut itoa = itoa::Buffer::new();
    let digits = itoa.format(value.unsigned_abs()).as_bytes();
    let exponent = if value == 0 {
        0
    } else {
        digits.len() as i32 - 1 - scale as i32
    };
    scratch.digits.clear();
    scratch.digits.extend_from_slice(digits);
    while scratch.digits.len() > 1 && scratch.digits.last() == Some(&b'0') {
        scratch.digits.pop();
    }
    push_digits(out, value < 0, &scratch.digits, exponent);
}

fn push_non_finite(out: &mut Vec<u8>, value: f64) -> bool {
    if value.is_nan() {
        out.extend_from_slice(b"nan");
    } else if value.is_infinite() {
        out.extend_from_slice(if value < 0.0 { b"-inf" } else { b"inf" });
    } else {
        return false;
    }
    true
}

fn split_scientific(text: &str, digits: &mut Vec<u8>) -> (bool, i32) {
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text, "0"));
    digits.clear();
    digits.extend(mantissa.bytes().filter(u8::is_ascii_digit));
    (negative, exponent.parse().unwrap_or(0))
}

struct Scratch {
    text: String,
    digits: Vec<u8>,
}

impl Scratch {
    fn new() -> Self {
        Self {
            text: String::new(),
            digits: Vec::new(),
        }
    }
}

fn push_repr(out: &mut Vec<u8>, scratch: &mut Scratch, shortest: std::fmt::Arguments<'_>) {
    scratch.text.clear();
    scratch
        .text
        .write_fmt(shortest)
        .expect("writing to a String cannot fail");
    let (negative, exponent) = split_scientific(&scratch.text, &mut scratch.digits);
    push_digits(out, negative, &scratch.digits, exponent);
}

fn push_digits(out: &mut Vec<u8>, negative: bool, digits: &[u8], exponent: i32) {
    if negative {
        out.push(b'-');
    }
    let point = exponent + 1;
    if point > -4 && point <= 16 {
        if point <= 0 {
            out.extend_from_slice(b"0.");
            out.extend(std::iter::repeat_n(b'0', (-point) as usize));
            out.extend_from_slice(digits);
        } else if point as usize >= digits.len() {
            out.extend_from_slice(digits);
            out.extend(std::iter::repeat_n(b'0', point as usize - digits.len()));
            out.extend_from_slice(b".0");
        } else {
            out.extend_from_slice(&digits[..point as usize]);
            out.push(b'.');
            out.extend_from_slice(&digits[point as usize..]);
        }
    } else {
        out.push(digits[0]);
        if digits.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        out.push(if exponent < 0 { b'-' } else { b'+' });
        push_padded(out, i64::from(exponent.abs()), 2);
    }
}

fn push_float64(out: &mut Vec<u8>, scratch: &mut Scratch, value: f64) {
    if !push_non_finite(out, value) {
        push_repr(out, scratch, format_args!("{value:e}"));
    }
}

fn push_float32(out: &mut Vec<u8>, scratch: &mut Scratch, value: f32) {
    if !push_non_finite(out, f64::from(value)) {
        push_repr(out, scratch, format_args!("{value:e}"));
    }
}

fn push_fixed(out: &mut Vec<u8>, scratch: &mut Scratch, value: f64, scale: usize) {
    if push_non_finite(out, value) {
        return;
    }
    scratch.text.clear();
    scratch
        .text
        .write_fmt(format_args!("{value:.scale$}"))
        .expect("writing to a String cannot fail");
    out.extend_from_slice(scratch.text.as_bytes());
}

fn push_general(out: &mut Vec<u8>, scratch: &mut Scratch, value: f32) {
    let value = f64::from(value);
    if push_non_finite(out, value) {
        return;
    }
    scratch.text.clear();
    scratch
        .text
        .write_fmt(format_args!(
            "{value:.precision$e}",
            precision = FLOAT_SIGNIFICANT_DIGITS - 1
        ))
        .expect("writing to a String cannot fail");
    let (negative, exponent) = split_scientific(&scratch.text, &mut scratch.digits);
    let digits = &mut scratch.digits;
    while digits.len() > 1 && digits.last() == Some(&b'0') {
        digits.pop();
    }
    if negative {
        out.push(b'-');
    }
    if (-4..FLOAT_SIGNIFICANT_DIGITS as i32).contains(&exponent) {
        if exponent < 0 {
            out.extend_from_slice(b"0.");
            out.extend(std::iter::repeat_n(b'0', (-exponent - 1) as usize));
            out.extend_from_slice(digits);
        } else {
            let whole = exponent as usize + 1;
            if digits.len() <= whole {
                out.extend_from_slice(digits);
                out.extend(std::iter::repeat_n(b'0', whole - digits.len()));
            } else {
                out.extend_from_slice(&digits[..whole]);
                out.push(b'.');
                out.extend_from_slice(&digits[whole..]);
            }
        }
    } else {
        out.push(digits[0]);
        if digits.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        out.push(if exponent < 0 { b'-' } else { b'+' });
        push_padded(out, i64::from(exponent.abs()), 2);
    }
}

fn push_vector(
    out: &mut Vec<u8>,
    scratch: &mut Scratch,
    values: &Float32Array,
    row: usize,
    length: usize,
) -> io::Result<()> {
    out.push(b'[');
    for index in 0..length {
        let position = row * length + index;
        if values.is_null(position) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a vector element is null",
            ));
        }
        if index > 0 {
            out.push(b',');
        }
        push_general(out, scratch, values.value(position));
    }
    out.push(b']');
    Ok(())
}

fn needs_quotes(text: &[u8], delimiter: u8) -> bool {
    text.iter()
        .any(|&byte| byte == delimiter || matches!(byte, b'"' | b'\n' | b'\r'))
}

fn push_text(out: &mut Vec<u8>, text: &str, format: &TextFormat) {
    let bytes = text.as_bytes();
    if !format.quote {
        out.extend_from_slice(bytes);
    } else if bytes.is_empty() {
        out.extend_from_slice(b"\"\"");
    } else if needs_quotes(bytes, format.delimiter) {
        out.push(b'"');
        for &byte in bytes {
            if byte == b'"' {
                out.push(b'"');
            }
            out.push(byte);
        }
        out.push(b'"');
    } else {
        out.extend_from_slice(bytes);
    }
}

fn upper_bound(columns: &[Column<'_>], rows: usize, format: &TextFormat) -> usize {
    let values: usize = columns
        .iter()
        .map(|column| match column {
            Column::Int32(_) => rows * 11,
            Column::Int64(_) => rows * 20,
            Column::Date(_) => rows * 10,
            Column::Text(array) => {
                if format.quote {
                    array.value_data().len() * 2 + rows * 2
                } else {
                    array.value_data().len()
                }
            }
            Column::Float32(_) => rows * 16,
            Column::Float64(_) => rows * 25,
            Column::Fixed(_, scale) => rows * (24 + scale),
            Column::Bool(_) => rows * 5,
            Column::Timestamp(_) => rows * 27,
            Column::Decimal(array, _) | Column::TrimmedDecimal(array, _) => {
                rows * (array.precision() as usize + 6)
            }
            Column::Vector(_, _, length) => rows * (length * 14 + 4),
        })
        .sum();
    values + rows * columns.len()
}

pub(crate) fn encode_header(
    schema: &arrow::datatypes::Schema,
    format: &TextFormat,
    out: &mut Vec<u8>,
) {
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            out.push(format.delimiter);
        }
        push_text(out, field.name(), format);
    }
    out.push(b'\n');
}

pub(crate) fn encode(
    batch: &RecordBatch,
    format: &TextFormat,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    let columns = columns(batch)?;
    out.reserve(upper_bound(&columns, batch.num_rows(), format));
    let mut digits = itoa::Buffer::new();
    let mut scratch = Scratch::new();
    for row in 0..batch.num_rows() {
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(format.delimiter);
            }
            match column {
                Column::Int32(array) if array.is_valid(row) => {
                    out.extend_from_slice(digits.format(array.value(row)).as_bytes());
                }
                Column::Int64(array) if array.is_valid(row) => {
                    out.extend_from_slice(digits.format(array.value(row)).as_bytes());
                }
                Column::Date(array) if array.is_valid(row) => {
                    push_date(out, i64::from(array.value(row)));
                }
                Column::Text(array) if array.is_valid(row) => {
                    push_text(out, array.value(row), format);
                }
                Column::Float32(array) if array.is_valid(row) => {
                    push_float32(out, &mut scratch, array.value(row));
                }
                Column::Float64(array) if array.is_valid(row) => {
                    push_float64(out, &mut scratch, array.value(row));
                }
                Column::Fixed(array, scale) if array.is_valid(row) => {
                    push_fixed(out, &mut scratch, array.value(row), *scale);
                }
                Column::Bool(array) if array.is_valid(row) => {
                    out.extend_from_slice(if array.value(row) { b"true" } else { b"false" });
                }
                Column::Timestamp(array) if array.is_valid(row) => {
                    push_timestamp(out, array.value(row));
                }
                Column::Decimal(array, scale) if array.is_valid(row) => {
                    push_decimal(out, array.value(row), *scale);
                }
                Column::TrimmedDecimal(array, scale) if array.is_valid(row) => {
                    push_trimmed_decimal(out, &mut scratch, array.value(row), *scale);
                }
                Column::Vector(list, values, length) if list.is_valid(row) => {
                    let quoted = format.quote && format.delimiter == b',' && *length > 1;
                    if quoted {
                        out.push(b'"');
                    }
                    push_vector(out, &mut scratch, values, row, *length)?;
                    if quoted {
                        out.push(b'"');
                    }
                }
                _ => {}
            }
        }
        out.push(b'\n');
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};

    use super::*;
    use crate::schema::Kind;

    fn one(array: ArrayRef, nullable: bool, format: &TextFormat) -> String {
        let field = Field::new("c", array.data_type().clone(), nullable);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let mut out = Vec::new();
        encode(&batch, format, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn lines(array: ArrayRef) -> Vec<String> {
        one(array, true, &TextFormat::tbl())
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn rows_are_pipe_separated_with_nulls_empty_and_dates_iso() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b c")])),
            Arc::new(Date32Array::from(vec![8_035, 0])),
        ];
        let schema = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, false),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        let mut out = Vec::new();
        encode(&batch, &TextFormat::tbl(), &mut out).unwrap();
        assert_eq!(out, b"1|a|1992-01-01\n|b c|1970-01-01\n");
    }

    #[test]
    fn integers_are_plain_decimal() {
        assert_eq!(
            lines(Arc::new(Int32Array::from(vec![
                Some(-7),
                None,
                Some(i32::MAX)
            ]))),
            ["-7", "", "2147483647"]
        );
        assert_eq!(
            lines(Arc::new(Int64Array::from(vec![i64::MIN, 0]))),
            ["-9223372036854775808", "0"]
        );
    }

    #[test]
    fn float64_is_the_shortest_round_trip_in_python_repr_layout() {
        let values = vec![
            0.0,
            -0.0,
            1.0,
            0.1,
            1.5,
            -73.98,
            123456.789,
            0.0001,
            0.00001,
            1e15,
            1e16,
            1.2345678901234568e17,
            1.5e-7,
            1e100,
            f64::MAX,
            5e-324,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        assert_eq!(
            lines(Arc::new(Float64Array::from(values))),
            [
                "0.0",
                "-0.0",
                "1.0",
                "0.1",
                "1.5",
                "-73.98",
                "123456.789",
                "0.0001",
                "1e-05",
                "1000000000000000.0",
                "1e+16",
                "1.2345678901234568e+17",
                "1.5e-07",
                "1e+100",
                "1.7976931348623157e+308",
                "5e-324",
                "nan",
                "inf",
                "-inf",
            ]
        );
    }

    #[test]
    fn float32_is_the_shortest_round_trip_of_the_single_precision_value() {
        assert_eq!(
            lines(Arc::new(Float32Array::from(vec![
                Some(0.1_f32),
                Some(3.5),
                Some(1e-5),
                Some(16_777_216.0),
                None,
                Some(f32::NAN),
            ]))),
            ["0.1", "3.5", "1e-05", "16777216.0", "", "nan"]
        );
    }

    #[test]
    fn fixed_scale_floats_match_python_fixed_formatting() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.005),
            Some(2.675),
            Some(0.125),
            Some(0.375),
            Some(-0.001),
            Some(12.5),
            Some(1e7),
            None,
            Some(f64::NAN),
        ]));
        let field = Kind::Fixed(2).field("c", true);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let mut out = Vec::new();
        encode(&batch, &TextFormat::tbl(), &mut out).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap().lines().collect::<Vec<_>>(),
            [
                "0.00",
                "1.00",
                "2.67",
                "0.12",
                "0.38",
                "-0.00",
                "12.50",
                "10000000.00",
                "",
                "nan"
            ]
        );
    }

    #[test]
    fn fixed_scale_on_a_non_float64_column_is_an_error() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let field = Field::new("c", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([(FIXED_SCALE_KEY.to_owned(), "2".to_owned())]),
        );
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        assert!(encode(&batch, &TextFormat::tbl(), &mut Vec::new()).is_err());
        assert!(check(batch.schema().as_ref()).is_err());
    }

    #[test]
    fn booleans_are_lowercase_words() {
        assert_eq!(
            lines(Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None
            ]))),
            ["true", "false", ""]
        );
    }

    #[test]
    fn timestamps_omit_the_fraction_when_it_is_zero() {
        let micros = |days: i64, seconds: i64, fraction: i64| {
            (days * SECONDS_PER_DAY + seconds) * MICROS_PER_SECOND + fraction
        };
        let array = TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(micros(8_035, 3_661, 0)),
            Some(micros(8_035, 86_399, 120_000)),
            Some(micros(8_035, 0, 1)),
            Some(-1),
            None,
        ]);
        assert_eq!(
            lines(Arc::new(array)),
            [
                "1970-01-01 00:00:00",
                "1992-01-01 01:01:01",
                "1992-01-01 23:59:59.120000",
                "1992-01-01 00:00:00.000001",
                "1969-12-31 23:59:59.999999",
                "",
            ]
        );
    }

    #[test]
    fn timestamps_with_a_time_zone_or_other_unit_are_errors() {
        for data_type in [
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            let array = arrow::array::new_null_array(&data_type, 1);
            let field = Field::new("c", data_type, true);
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
            assert!(encode(&batch, &TextFormat::tbl(), &mut Vec::new()).is_err());
        }
    }

    #[test]
    fn decimals_carry_exactly_their_scale() {
        let array =
            Decimal128Array::from(vec![Some(123_456), Some(-5), Some(0), Some(-123_456), None])
                .with_precision_and_scale(15, 2)
                .unwrap();
        assert_eq!(
            lines(Arc::new(array)),
            ["1234.56", "-0.05", "0.00", "-1234.56", ""]
        );
        let whole = Decimal128Array::from(vec![42, -7])
            .with_precision_and_scale(10, 0)
            .unwrap();
        assert_eq!(lines(Arc::new(whole)), ["42", "-7"]);
        let wide = Decimal128Array::from(vec![i128::from(-73_980_000_000_000_i64)])
            .with_precision_and_scale(18, 14)
            .unwrap();
        assert_eq!(lines(Arc::new(wide)), ["-0.73980000000000"]);
    }

    #[test]
    fn trimmed_decimals_match_python_float_repr() {
        let cases: [(i128, i8, &str); 17] = [
            (-12_345_678_901_234_567, 16, "-1.2345678901234567"),
            (9_007_199_254_740_993, 0, "9007199254740993.0"),
            (-92_233_720_368_547_758, 17, "-0.92233720368547758"),
            (0, 2, "0.0"),
            (250, 2, "2.5"),
            (-5, 2, "-0.05"),
            (100, 2, "1.0"),
            (-73_912_345_000_000, 14, "-0.73912345"),
            (-7_391_234_512_345_678, 14, "-73.91234512345678"),
            (1, 5, "1e-05"),
            (-1, 5, "-1e-05"),
            (10, 5, "0.0001"),
            (1, 4, "0.0001"),
            (12_345_678_901_234_500, 2, "123456789012345.0"),
            (1_000_000_000_000_000_000, 2, "1e+16"),
            (42, 0, "42.0"),
            (123_456, 2, "1234.56"),
        ];
        let field = Kind::DecimalTrimmed(38, 0).field("c", true);
        assert_eq!(field.metadata().get(TRIMMED_DECIMAL_KEY).unwrap(), "true");
        for (value, scale, expected) in cases {
            let array = Decimal128Array::from(vec![Some(value), None])
                .with_precision_and_scale(38, scale)
                .unwrap();
            let field = Kind::DecimalTrimmed(38, scale).field("c", true);
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)])
                    .unwrap();
            let mut out = Vec::new();
            encode(&batch, &TextFormat::tbl(), &mut out).unwrap();
            assert_eq!(String::from_utf8(out).unwrap(), format!("{expected}\n\n"));
        }
    }

    #[test]
    fn trimmed_decimal_on_a_non_decimal_column_is_an_error() {
        let field = Field::new("c", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([(TRIMMED_DECIMAL_KEY.to_owned(), "true".to_owned())]),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(encode(&batch, &TextFormat::tbl(), &mut Vec::new()).is_err());
    }

    #[test]
    fn negative_scale_decimals_are_errors() {
        let array = Decimal128Array::from(vec![1])
            .with_precision_and_scale(10, -2)
            .unwrap();
        let field = Field::new("c", array.data_type().clone(), false);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)])
            .unwrap();
        assert!(encode(&batch, &TextFormat::tbl(), &mut Vec::new()).is_err());
    }

    #[test]
    fn vectors_are_bracketed_six_significant_digit_lists() {
        let values = Float32Array::from(vec![
            0.5,
            -0.25,
            1.0,
            0.123_456_79,
            1e-5,
            123_456.7,
            1_234_567.0,
            0.0,
        ]);
        let item = Arc::new(Field::new_list_field(DataType::Float32, true));
        let list = FixedSizeListArray::new(item, 4, Arc::new(values), None);
        assert_eq!(
            lines(Arc::new(list)),
            ["[0.5,-0.25,1,0.123457]", "[1e-05,123457,1.23457e+06,0]"]
        );
    }

    #[test]
    fn vector_nulls_are_empty_and_null_elements_are_errors() {
        let item = Arc::new(Field::new_list_field(DataType::Float32, true));
        let values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let nulls = arrow::buffer::NullBuffer::from(vec![true, false]);
        let list = FixedSizeListArray::new(item.clone(), 2, Arc::new(values), Some(nulls));
        assert_eq!(lines(Arc::new(list)), ["[1,2]", ""]);

        let values = Float32Array::from(vec![Some(1.0), None]);
        let list = FixedSizeListArray::new(item, 2, Arc::new(values), None);
        let field = Field::new("c", list.data_type().clone(), false);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(list)]).unwrap();
        assert!(encode(&batch, &TextFormat::tbl(), &mut Vec::new()).is_err());
    }

    #[test]
    fn other_types_are_errors_not_panics() {
        let array: ArrayRef = Arc::new(arrow::array::UInt8Array::from(vec![1]));
        let field = Field::new("c", DataType::UInt8, false);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let error = encode(&batch, &TextFormat::tbl(), &mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("UInt8"));
        assert!(check(batch.schema().as_ref()).is_err());
    }

    #[test]
    fn csv_quotes_only_what_needs_it_and_distinguishes_empty_from_null() {
        let format = TextFormat::csv(b',', true);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("plain"),
            Some("a,b"),
            Some("say \"hi\""),
            Some("two\nlines"),
            Some(""),
            None,
        ]));
        assert_eq!(
            one(array, true, &format),
            "plain\n\"a,b\"\n\"say \"\"hi\"\"\"\n\"two\nlines\"\n\"\"\n\n"
        );
    }

    #[test]
    fn csv_vectors_are_quoted_only_when_the_delimiter_occurs_in_them() {
        let item = Arc::new(Field::new_list_field(DataType::Float32, true));
        let values = Float32Array::from(vec![1.0, 2.0]);
        let list = FixedSizeListArray::new(item, 2, Arc::new(values), None);
        let array: ArrayRef = Arc::new(list);
        assert_eq!(
            one(array.clone(), false, &TextFormat::csv(b',', false)),
            "\"[1,2]\"\n"
        );
        assert_eq!(one(array, false, &TextFormat::csv(b'|', false)), "[1,2]\n");
    }

    #[test]
    fn headers_use_the_table_delimiter() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a,b", DataType::Utf8, false),
        ]);
        let mut out = Vec::new();
        encode_header(&schema, &TextFormat::csv(b',', true), &mut out);
        assert_eq!(out, b"id,\"a,b\"\n");
        out.clear();
        encode_header(&schema, &TextFormat::csv(b'|', true), &mut out);
        assert_eq!(out, b"id|a,b\n");
    }
}

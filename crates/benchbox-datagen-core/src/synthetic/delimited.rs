use arrow::array::{Array, Date32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use super::calendar::civil_from_days;

enum Column<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Date(&'a Date32Array),
    Text(&'a StringArray),
}

fn columns(batch: &RecordBatch) -> Vec<Column<'_>> {
    batch
        .columns()
        .iter()
        .map(|array| match array.data_type() {
            DataType::Int32 => Column::Int32(array.as_any().downcast_ref().expect("int32")),
            DataType::Int64 => Column::Int64(array.as_any().downcast_ref().expect("int64")),
            DataType::Date32 => Column::Date(array.as_any().downcast_ref().expect("date32")),
            DataType::Utf8 => Column::Text(array.as_any().downcast_ref().expect("utf8")),
            other => unreachable!("synthetic tables never emit {other}"),
        })
        .collect()
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

fn upper_bound(columns: &[Column<'_>], rows: usize) -> usize {
    let per_row = columns.len();
    let values: usize = columns
        .iter()
        .map(|column| match column {
            Column::Int32(_) => rows * 11,
            Column::Int64(_) => rows * 20,
            Column::Date(_) => rows * 10,
            Column::Text(array) => array.value_data().len(),
        })
        .sum();
    values + rows * per_row
}

pub(crate) fn encode(batch: &RecordBatch, out: &mut Vec<u8>) {
    let columns = columns(batch);
    out.reserve(upper_bound(&columns, batch.num_rows()));
    let mut digits = itoa::Buffer::new();
    for row in 0..batch.num_rows() {
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(b'|');
            }
            match column {
                Column::Int32(array) if array.is_valid(row) => {
                    out.extend_from_slice(digits.format(array.value(row)).as_bytes());
                }
                Column::Int64(array) if array.is_valid(row) => {
                    out.extend_from_slice(digits.format(array.value(row)).as_bytes());
                }
                Column::Date(array) if array.is_valid(row) => {
                    let (year, month, day) = civil_from_days(i64::from(array.value(row)));
                    push_padded(out, year, 4);
                    out.push(b'-');
                    push_padded(out, month, 2);
                    out.push(b'-');
                    push_padded(out, day, 2);
                }
                Column::Text(array) if array.is_valid(row) => {
                    out.extend_from_slice(array.value(row).as_bytes());
                }
                _ => {}
            }
        }
        out.push(b'\n');
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};

    use super::*;

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
        encode(&batch, &mut out);
        assert_eq!(out, b"1|a|1992-01-01\n|b c|1970-01-01\n");
    }
}

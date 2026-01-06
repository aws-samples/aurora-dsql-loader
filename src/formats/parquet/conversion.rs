//! Conversion from Arrow RecordBatch to row-based Records.
//!
//! This module converts Arrow's columnar format to the string-based Record format
//! used throughout the loader. All values are converted to strings, with nulls
//! represented as empty strings. The worker will later parse these strings back
//! to typed values based on the target schema.

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;

use crate::formats::reader::Record;

/// Convert an Arrow RecordBatch to a vector of Records
///
/// This bridges Arrow's columnar format to the row-based Record format expected by workers.
/// All values are converted to strings, with nulls represented as empty strings.
pub fn record_batch_to_records(batch: &RecordBatch) -> Result<Vec<Record>> {
    let num_rows = batch.num_rows();
    let num_columns = batch.num_columns();

    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Convert each column to strings
    let mut column_strings: Vec<Vec<String>> = Vec::with_capacity(num_columns);
    for col_idx in 0..num_columns {
        let array = batch.column(col_idx);
        let strings = array_to_strings(array).with_context(|| {
            format!(
                "Failed to convert column {} ({:?}) to strings",
                col_idx,
                array.data_type()
            )
        })?;
        column_strings.push(strings);
    }

    // Transpose to rows
    let mut records = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let fields = column_strings
            .iter()
            .map(|col| col[row_idx].clone())
            .collect();

        records.push(Record { fields });
    }

    Ok(records)
}

/// Convert an Arrow array to a vector of string representations
fn array_to_strings(array: &dyn Array) -> Result<Vec<String>> {
    let mut strings = Vec::with_capacity(array.len());

    match array.data_type() {
        DataType::Boolean => {
            let arr = as_boolean_array(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    arr.value(i).to_string()
                });
            }
        }
        DataType::Int8 => convert_primitive::<Int8Type>(array, &mut strings),
        DataType::Int16 => convert_primitive::<Int16Type>(array, &mut strings),
        DataType::Int32 => convert_primitive::<Int32Type>(array, &mut strings),
        DataType::Int64 => convert_primitive::<Int64Type>(array, &mut strings),
        DataType::UInt8 => convert_primitive::<UInt8Type>(array, &mut strings),
        DataType::UInt16 => convert_primitive::<UInt16Type>(array, &mut strings),
        DataType::UInt32 => convert_primitive::<UInt32Type>(array, &mut strings),
        DataType::UInt64 => convert_primitive::<UInt64Type>(array, &mut strings),
        DataType::Float32 => convert_primitive::<Float32Type>(array, &mut strings),
        DataType::Float64 => convert_primitive::<Float64Type>(array, &mut strings),
        DataType::Utf8 => {
            let arr = as_string_array(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    arr.value(i).to_string()
                });
            }
        }
        DataType::LargeUtf8 => {
            let arr = as_largestring_array(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    arr.value(i).to_string()
                });
            }
        }
        DataType::Binary => {
            let arr = as_generic_binary_array::<i32>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    // Convert binary to hex string
                    hex::encode(arr.value(i))
                });
            }
        }
        DataType::LargeBinary => {
            let arr = as_generic_binary_array::<i64>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    // Convert binary to hex string
                    hex::encode(arr.value(i))
                });
            }
        }
        DataType::Date32 => {
            let arr = as_primitive_array::<Date32Type>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let days = arr.value(i);
                    // Convert days since epoch to date string YYYY-MM-DD
                    let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_signed(chrono::Duration::days(days as i64))
                        .context("Invalid date")?;
                    date.format("%Y-%m-%d").to_string()
                });
            }
        }
        DataType::Date64 => {
            let arr = as_primitive_array::<Date64Type>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let millis = arr.value(i);
                    // Convert millis since epoch to date string
                    let datetime =
                        chrono::DateTime::from_timestamp_millis(millis).context("Invalid date")?;
                    datetime.format("%Y-%m-%d").to_string()
                });
            }
        }
        DataType::Timestamp(unit, _) => {
            convert_timestamp(array, unit, &mut strings)?;
        }
        DataType::Decimal128(_, scale) => {
            let arr = as_primitive_array::<Decimal128Type>(array);
            let scale = *scale as u32;
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let value = arr.value(i);
                    format_decimal128(value, scale)
                });
            }
        }
        DataType::Decimal256(_, _scale) => {
            let arr = as_primitive_array::<Decimal256Type>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    // Convert to string representation
                    // Decimal256 is stored as i256, which is not a standard Rust type
                    // For now, just format it as a string
                    format!("{}", arr.value(i))
                });
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported array type for conversion: {:?}",
                array.data_type()
            ));
        }
    }

    Ok(strings)
}

/// Helper to convert primitive arrays
fn convert_primitive<T: ArrowPrimitiveType>(array: &dyn Array, strings: &mut Vec<String>)
where
    T::Native: std::fmt::Display,
{
    let arr = as_primitive_array::<T>(array);
    for i in 0..arr.len() {
        strings.push(if arr.is_null(i) {
            String::new()
        } else {
            arr.value(i).to_string()
        });
    }
}

/// Convert timestamp arrays to strings
fn convert_timestamp(array: &dyn Array, unit: &TimeUnit, strings: &mut Vec<String>) -> Result<()> {
    match unit {
        TimeUnit::Second => {
            let arr = as_primitive_array::<TimestampSecondType>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let seconds = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp(seconds, 0)
                        .context("Invalid timestamp")?;
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                });
            }
        }
        TimeUnit::Millisecond => {
            let arr = as_primitive_array::<TimestampMillisecondType>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let millis = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp_millis(millis)
                        .context("Invalid timestamp")?;
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                });
            }
        }
        TimeUnit::Microsecond => {
            let arr = as_primitive_array::<TimestampMicrosecondType>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let micros = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp_micros(micros)
                        .context("Invalid timestamp")?;
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                });
            }
        }
        TimeUnit::Nanosecond => {
            let arr = as_primitive_array::<TimestampNanosecondType>(array);
            for i in 0..arr.len() {
                strings.push(if arr.is_null(i) {
                    String::new()
                } else {
                    let nanos = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp_nanos(nanos);
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                });
            }
        }
    }
    Ok(())
}

/// Format a Decimal128 value with the given scale
fn format_decimal128(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }

    let divisor = 10_i128.pow(scale);
    let int_part = value / divisor;
    let frac_part = (value % divisor).abs();

    // Format with proper padding
    format!("{}.{:0width$}", int_part, frac_part, width = scale as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::f64;
    use std::sync::Arc;

    #[test]
    fn test_record_batch_to_records_integers() {
        let schema = Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]);

        let int32_array = Int32Array::from(vec![1, 2, 3]);
        let int64_array = Int64Array::from(vec![100, 200, 300]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(int32_array), Arc::new(int64_array)],
        )
        .unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].fields, vec!["1", "100"]);
        assert_eq!(records[1].fields, vec!["2", "200"]);
        assert_eq!(records[2].fields, vec!["3", "300"]);
    }

    #[test]
    fn test_record_batch_to_records_strings() {
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let string_array = StringArray::from(vec![Some("Alice"), None, Some("Bob")]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(string_array)]).unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].fields, vec!["Alice"]);
        assert_eq!(records[1].fields, vec![""]); // Null becomes empty string
        assert_eq!(records[2].fields, vec!["Bob"]);
    }

    #[test]
    fn test_record_batch_to_records_floats() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, false)]);

        let float_array = Float64Array::from(vec![1.5, 2.7, f64::consts::PI]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(float_array)]).unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].fields, vec!["1.5"]);
        assert_eq!(records[1].fields, vec!["2.7"]);
        assert_eq!(records[2].fields, vec![f64::consts::PI.to_string()]);
    }

    #[test]
    fn test_record_batch_to_records_booleans() {
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, false)]);

        let bool_array = BooleanArray::from(vec![true, false, true]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(bool_array)]).unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].fields, vec!["true"]);
        assert_eq!(records[1].fields, vec!["false"]);
        assert_eq!(records[2].fields, vec!["true"]);
    }

    #[test]
    fn test_record_batch_to_records_dates() {
        let schema = Schema::new(vec![Field::new("date", DataType::Date32, false)]);

        // Date32 is days since epoch
        // 0 = 1970-01-01
        // 18993 = 2022-01-01
        let date_array = Date32Array::from(vec![0, 18993]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(date_array)]).unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].fields, vec!["1970-01-01"]);
        assert_eq!(records[1].fields, vec!["2022-01-01"]);
    }

    #[test]
    fn test_record_batch_to_records_mixed_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("balance", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob")]);
        let balance_array = Float64Array::from(vec![Some(100.50), None]);
        let active_array = BooleanArray::from(vec![true, false]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(balance_array),
                Arc::new(active_array),
            ],
        )
        .unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].fields, vec!["1", "Alice", "100.5", "true"]);
        assert_eq!(records[1].fields, vec!["2", "Bob", "", "false"]);
    }

    #[test]
    fn test_format_decimal128() {
        assert_eq!(format_decimal128(12345, 2), "123.45");
        assert_eq!(format_decimal128(1, 2), "0.01");
        assert_eq!(format_decimal128(100, 2), "1.00");
        assert_eq!(format_decimal128(-12345, 2), "-123.45");
        assert_eq!(format_decimal128(12345, 0), "12345");
    }

    #[test]
    fn test_record_batch_to_records_empty() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let int32_array = Int32Array::from(vec![] as Vec<i32>);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int32_array)]).unwrap();

        let records = record_batch_to_records(&batch).unwrap();

        assert_eq!(records.len(), 0);
    }
}

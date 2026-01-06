use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::coordination::manifest::{ColumnJson, SchemaJson};

/// Field values from a record (as strings from CSV/TSV)
/// Used for schema inference - just the raw field values without metadata
pub type FieldValues = Vec<String>;

/// SQL data type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
    Numeric,
    Text,
    Varchar,
    Char,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Interval,
    Uuid,
    Bytea,
}

impl SqlType {
    /// Returns the Postgres type name
    pub fn to_postgres(&self) -> &'static str {
        match self {
            SqlType::Boolean => "BOOLEAN",
            SqlType::SmallInt => "SMALLINT",
            SqlType::Integer => "INTEGER",
            SqlType::BigInt => "BIGINT",
            SqlType::Real => "REAL",
            SqlType::DoublePrecision => "DOUBLE PRECISION",
            SqlType::Numeric => "NUMERIC",
            SqlType::Text => "TEXT",
            SqlType::Varchar => "VARCHAR",
            SqlType::Char => "CHAR",
            SqlType::Timestamp => "TIMESTAMP",
            SqlType::TimestampTz => "TIMESTAMP WITH TIME ZONE",
            SqlType::Date => "DATE",
            SqlType::Time => "TIME",
            SqlType::Interval => "INTERVAL",
            SqlType::Uuid => "UUID",
            SqlType::Bytea => "BYTEA",
        }
    }

    /// Find the most specific common type that accommodates both types
    pub fn common_type(&self, other: &SqlType) -> SqlType {
        if self == other {
            return self.clone();
        }

        // Type promotions
        let type_promotion = |a: &SqlType, b: &SqlType| -> Option<SqlType> {
            use SqlType::*;
            match (a, b) {
                // Boolean can promote to any numeric type
                (Boolean, SmallInt) | (SmallInt, Boolean) => Some(SmallInt),
                (Boolean, Integer) | (Integer, Boolean) => Some(Integer),
                (Boolean, BigInt) | (BigInt, Boolean) => Some(BigInt),
                (Boolean, Numeric) | (Numeric, Boolean) => Some(Numeric),

                // Integer type promotions
                (SmallInt, Integer) | (Integer, SmallInt) => Some(Integer),
                (SmallInt, BigInt) | (BigInt, SmallInt) => Some(BigInt),
                (Integer, BigInt) | (BigInt, Integer) => Some(BigInt),

                // Integer to Numeric promotions
                (SmallInt | Integer | BigInt, Numeric) | (Numeric, SmallInt | Integer | BigInt) => {
                    Some(Numeric)
                }

                // Float type promotions
                (Real, DoublePrecision) | (DoublePrecision, Real) => Some(DoublePrecision),

                // Numeric to float promotions
                (Numeric, Real) | (Real, Numeric) => Some(DoublePrecision),
                (Numeric, DoublePrecision) | (DoublePrecision, Numeric) => Some(DoublePrecision),

                // Integer to float promotions
                (SmallInt | Integer | BigInt, Real) | (Real, SmallInt | Integer | BigInt) => {
                    Some(Real)
                }
                (SmallInt | Integer | BigInt, DoublePrecision)
                | (DoublePrecision, SmallInt | Integer | BigInt) => Some(DoublePrecision),

                // String type promotions - all character types promote to Text
                (Text, Varchar) | (Varchar, Text) => Some(Text),
                (Text, Char) | (Char, Text) => Some(Text),
                (Varchar, Char) | (Char, Varchar) => Some(Text),

                // Date/Time/Timestamp promotions
                (Date, Timestamp) | (Timestamp, Date) => Some(Timestamp),
                (Date, TimestampTz) | (TimestampTz, Date) => Some(TimestampTz),
                (Timestamp, TimestampTz) | (TimestampTz, Timestamp) => Some(TimestampTz),

                _ => None,
            }
        };

        type_promotion(self, other).unwrap_or(SqlType::Text)
    }
}

/// A column in a schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
}

/// A database schema (collection of columns)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}

/// Conversion from rich Column type to JSON serialization format
impl From<Column> for ColumnJson {
    fn from(col: Column) -> Self {
        ColumnJson {
            name: col.name,
            col_type: col.sql_type.to_postgres().to_string(),
        }
    }
}

/// Conversion from rich Schema type to JSON serialization format
impl From<Schema> for SchemaJson {
    fn from(schema: Schema) -> Self {
        SchemaJson {
            columns: schema.columns.into_iter().map(|c| c.into()).collect(),
        }
    }
}

/// Query the schema of an existing table from the database
pub async fn query_table_schema(pool: &super::pool::Pool, table_name: &str) -> Result<Schema> {
    // Query PostgreSQL information_schema to get column types
    let query = r#"
        SELECT
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
    "#;

    let rows = pool
        .fetch_all_with_bind(query, table_name)
        .await
        .context("Failed to query table schema")?;

    if rows.is_empty() {
        anyhow::bail!("Table '{}' not found or has no columns", table_name);
    }

    let mut columns = Vec::new();

    for (column_name, data_type, is_nullable) in rows {
        let sql_type = match data_type.to_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => SqlType::Boolean,
            "SMALLINT" | "INT2" => SqlType::SmallInt,
            "INTEGER" | "INT" | "INT4" => SqlType::Integer,
            "BIGINT" | "INT8" => SqlType::BigInt,
            "REAL" | "FLOAT4" => SqlType::Real,
            "DOUBLE PRECISION" | "FLOAT8" => SqlType::DoublePrecision,
            "NUMERIC" | "DECIMAL" => SqlType::Numeric,
            "CHARACTER VARYING" | "VARCHAR" => SqlType::Varchar,
            "CHARACTER" | "CHAR" | "BPCHAR" => SqlType::Char,
            "TEXT" => SqlType::Text,
            "DATE" => SqlType::Date,
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => SqlType::Timestamp,
            "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => SqlType::TimestampTz,
            "TIME" | "TIME WITHOUT TIME ZONE" => SqlType::Time,
            "INTERVAL" => SqlType::Interval,
            "UUID" => SqlType::Uuid,
            "BYTEA" => SqlType::Bytea,
            _ => SqlType::Text, // Default to TEXT for unsupported types
        };

        let nullable = is_nullable.eq_ignore_ascii_case("YES");

        columns.push(Column {
            name: column_name,
            sql_type,
            nullable,
        });
    }

    Ok(Schema { columns })
}

/// Schema inferrer for analyzing data and generating DDL
pub struct SchemaInferrer {
    /// Infer header names from first row if true
    pub has_header: bool,
}

impl SchemaInferrer {
    /// Infer the type of a single value
    fn infer_value_type(value: &str) -> Option<SqlType> {
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return None; // Null value
        }

        // Boolean
        if trimmed.eq_ignore_ascii_case("true")
            || trimmed.eq_ignore_ascii_case("false")
            || trimmed.eq_ignore_ascii_case("t")
            || trimmed.eq_ignore_ascii_case("f")
            || trimmed == "0"
            || trimmed == "1"
        {
            return Some(SqlType::Boolean);
        }

        // Try parsing as integer
        if let Ok(val) = trimmed.parse::<i64>() {
            return Some(if val >= i16::MIN as i64 && val <= i16::MAX as i64 {
                SqlType::SmallInt
            } else if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                SqlType::Integer
            } else {
                SqlType::BigInt
            });
        }

        // Try parsing as float
        if trimmed.parse::<f64>().is_ok() {
            // Check for scientific notation or many decimal places
            if trimmed.contains('e') || trimmed.contains('E') {
                return Some(SqlType::DoublePrecision);
            }

            // Count decimal places
            if let Some(decimal_pos) = trimmed.find('.') {
                let decimals = trimmed.len() - decimal_pos - 1;
                if decimals > 7 {
                    return Some(SqlType::DoublePrecision);
                }
            }

            return Some(SqlType::Real);
        }

        // Try parsing as date - validate with chrono to ensure it's a real date
        // Support multiple common date formats
        if let Some(date_type) = Self::try_parse_as_date(trimmed) {
            return Some(date_type);
        }

        // Try parsing as timestamp - use stricter validation
        if Self::is_valid_timestamp(trimmed) {
            return Some(SqlType::Timestamp);
        }

        // Default to text
        Some(SqlType::Text)
    }

    /// Try to parse a value as a date, validating that it's actually valid
    /// Supports common date formats: YYYY-MM-DD, MM/DD/YYYY, DD-MM-YYYY, DD/MM/YYYY
    fn try_parse_as_date(value: &str) -> Option<SqlType> {
        use chrono::NaiveDate;

        // Try YYYY-MM-DD format (ISO 8601)
        if NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() {
            return Some(SqlType::Date);
        }

        // Try MM/DD/YYYY (US format)
        if NaiveDate::parse_from_str(value, "%m/%d/%Y").is_ok() {
            return Some(SqlType::Date);
        }

        // Try DD-MM-YYYY (European format with dashes)
        if NaiveDate::parse_from_str(value, "%d-%m-%Y").is_ok() {
            return Some(SqlType::Date);
        }

        // Try DD/MM/YYYY (European format with slashes)
        if NaiveDate::parse_from_str(value, "%d/%m/%Y").is_ok() {
            return Some(SqlType::Date);
        }

        None
    }

    /// Check if a value looks like a valid timestamp
    /// More strict than before - requires digits, proper separators, and time components
    fn is_valid_timestamp(value: &str) -> bool {
        use chrono::NaiveDateTime;

        // Common timestamp formats PostgreSQL supports
        let formats = [
            "%Y-%m-%d %H:%M:%S",    // 2025-01-01 12:34:56
            "%Y-%m-%dT%H:%M:%S",    // 2025-01-01T12:34:56 (ISO 8601)
            "%Y-%m-%d %H:%M:%S%.f", // With fractional seconds
            "%Y-%m-%dT%H:%M:%S%.f", // ISO 8601 with fractional seconds
            "%Y-%m-%d %H:%M",       // Without seconds
            "%Y-%m-%dT%H:%M",       // ISO 8601 without seconds
            "%m/%d/%Y %H:%M:%S",    // US format with time
            "%d-%m-%Y %H:%M:%S",    // European format with time
            "%d/%m/%Y %H:%M:%S",    // European format with time
        ];

        for format in &formats {
            if NaiveDateTime::parse_from_str(value, format).is_ok() {
                return true;
            }
        }

        false
    }

    /// Infer column types from multiple values
    fn infer_column_type(values: &[&str]) -> (SqlType, bool) {
        let mut inferred_type: Option<SqlType> = None;
        let mut has_nulls = false;

        for value in values {
            match Self::infer_value_type(value) {
                Some(val_type) => {
                    inferred_type = Some(match inferred_type {
                        None => val_type,
                        Some(current) => current.common_type(&val_type),
                    });
                }
                None => {
                    has_nulls = true;
                }
            }
        }

        (inferred_type.unwrap_or(SqlType::Text), has_nulls)
    }
}

impl SchemaInferrer {
    /// Infer schema from sample records
    pub fn infer_from_data(&self, records: &[FieldValues]) -> Result<Schema> {
        if records.is_empty() {
            anyhow::bail!("Cannot infer schema from empty dataset");
        }

        let (header_names, data_start_idx) = if self.has_header {
            (records[0].clone(), 1)
        } else {
            // Generate default column names
            let num_cols = records[0].len();
            let names = (0..num_cols).map(|i| format!("column_{}", i + 1)).collect();
            (names, 0)
        };

        let data_rows = &records[data_start_idx..];
        if data_rows.is_empty() {
            anyhow::bail!("No data rows available for type inference");
        }

        let num_columns = header_names.len();
        let mut columns = Vec::with_capacity(num_columns);

        // Infer type for each column
        for (col_idx, name) in header_names.iter().enumerate() {
            // Collect values from this column across all rows
            let column_values: Vec<&str> = data_rows
                .iter()
                .filter_map(|row| row.get(col_idx).map(|s| s.as_str()))
                .collect();

            let (sql_type, nullable) = Self::infer_column_type(&column_values);

            columns.push(Column {
                name: name.clone(),
                sql_type,
                nullable,
            });
        }

        Ok(Schema { columns })
    }

    /// Generate DDL statement for creating a table
    pub fn generate_ddl(&self, table_name: &str, schema: &Schema) -> String {
        let mut ddl = format!("CREATE TABLE \"{}\" (\n", table_name);

        let column_defs: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let nullable_clause = if col.nullable { "" } else { " NOT NULL" };
                format!(
                    "  \"{}\" {}{}",
                    col.name,
                    col.sql_type.to_postgres(),
                    nullable_clause
                )
            })
            .collect();

        ddl.push_str(&column_defs.join(",\n"));
        ddl.push_str("\n);");

        ddl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_value_types() {
        assert_eq!(
            SchemaInferrer::infer_value_type("42"),
            Some(SqlType::SmallInt)
        );
        assert_eq!(
            SchemaInferrer::infer_value_type("100000"),
            Some(SqlType::Integer)
        );
        assert_eq!(
            SchemaInferrer::infer_value_type("9999999999"),
            Some(SqlType::BigInt)
        );
        assert_eq!(
            SchemaInferrer::infer_value_type("3.14"),
            Some(SqlType::Real)
        );
        assert_eq!(
            SchemaInferrer::infer_value_type("true"),
            Some(SqlType::Boolean)
        );
        assert_eq!(
            SchemaInferrer::infer_value_type("hello"),
            Some(SqlType::Text)
        );
        assert_eq!(
            SchemaInferrer::infer_value_type("2025-12-03"),
            Some(SqlType::Date)
        );
        assert_eq!(SchemaInferrer::infer_value_type(""), None);
    }

    #[test]
    fn test_type_promotion() {
        assert_eq!(
            SqlType::SmallInt.common_type(&SqlType::Integer),
            SqlType::Integer
        );
        assert_eq!(
            SqlType::Integer.common_type(&SqlType::BigInt),
            SqlType::BigInt
        );
        assert_eq!(SqlType::Integer.common_type(&SqlType::Real), SqlType::Real);
        assert_eq!(
            SqlType::Real.common_type(&SqlType::DoublePrecision),
            SqlType::DoublePrecision
        );
        assert_eq!(
            SqlType::Date.common_type(&SqlType::Timestamp),
            SqlType::Timestamp
        );
        assert_eq!(SqlType::Integer.common_type(&SqlType::Text), SqlType::Text);
    }

    #[test]
    fn test_infer_schema_with_header() {
        let records = vec![
            vec!["id".to_string(), "name".to_string(), "age".to_string()],
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
        ];

        let inferrer = SchemaInferrer { has_header: true };
        let schema = inferrer.infer_from_data(&records).unwrap();

        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].sql_type, SqlType::SmallInt);
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[1].sql_type, SqlType::Text);
        assert_eq!(schema.columns[2].name, "age");
        assert_eq!(schema.columns[2].sql_type, SqlType::SmallInt);
    }

    #[test]
    fn test_infer_schema_without_header() {
        let records = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
        ];

        let inferrer = SchemaInferrer { has_header: false };
        let schema = inferrer.infer_from_data(&records).unwrap();

        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "column_1");
        assert_eq!(schema.columns[1].name, "column_2");
        assert_eq!(schema.columns[2].name, "column_3");
    }

    #[test]
    fn test_nullable_detection() {
        let records = vec![
            vec!["id".to_string(), "value".to_string()],
            vec!["1".to_string(), "100".to_string()],
            vec!["2".to_string(), "".to_string()], // Null value
            vec!["3".to_string(), "300".to_string()],
        ];

        let inferrer = SchemaInferrer { has_header: true };
        let schema = inferrer.infer_from_data(&records).unwrap();

        assert!(!schema.columns[0].nullable); // id has no nulls
        assert!(schema.columns[1].nullable); // value has null
    }

    #[test]
    fn test_generate_ddl() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    sql_type: SqlType::Integer,
                    nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    sql_type: SqlType::Text,
                    nullable: true,
                },
                Column {
                    name: "balance".to_string(),
                    sql_type: SqlType::DoublePrecision,
                    nullable: true,
                },
            ],
        };

        let inferrer = SchemaInferrer { has_header: true };
        let ddl = inferrer.generate_ddl("customers", &schema);

        assert!(ddl.contains("CREATE TABLE \"customers\""));
        assert!(ddl.contains("\"id\" INTEGER NOT NULL"));
        assert!(ddl.contains("\"name\" TEXT"));
        assert!(!ddl.contains("\"name\" TEXT NOT NULL"));
        assert!(ddl.contains("\"balance\" DOUBLE PRECISION"));
    }

    #[test]
    fn test_mixed_types_promote_to_text() {
        let records = vec![
            vec!["value".to_string()],
            vec!["123".to_string()],
            vec!["hello".to_string()],
            vec!["456".to_string()],
        ];

        let inferrer = SchemaInferrer { has_header: true };
        let schema = inferrer.infer_from_data(&records).unwrap();

        // Mixed numeric and text should promote to Text
        assert_eq!(schema.columns[0].sql_type, SqlType::Text);
    }

    #[test]
    fn test_numeric_promotion() {
        let records = vec![
            vec!["value".to_string()],
            vec!["1".to_string()],      // SmallInt
            vec!["100000".to_string()], // Integer
            vec!["3.14".to_string()],   // Real
        ];

        let inferrer = SchemaInferrer { has_header: true };
        let schema = inferrer.infer_from_data(&records).unwrap();

        // Should promote through SmallInt -> Integer -> Real
        assert_eq!(schema.columns[0].sql_type, SqlType::Real);
    }

    #[test]
    fn test_schema_to_json_conversion() {
        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    sql_type: SqlType::Integer,
                    nullable: false,
                },
                Column {
                    name: "balance".to_string(),
                    sql_type: SqlType::DoublePrecision,
                    nullable: true,
                },
            ],
        };

        let json_schema: SchemaJson = schema.into();

        assert_eq!(json_schema.columns.len(), 2);
        assert_eq!(json_schema.columns[0].name, "id");
        assert_eq!(json_schema.columns[0].col_type, "INTEGER");
        assert_eq!(json_schema.columns[1].name, "balance");
        assert_eq!(json_schema.columns[1].col_type, "DOUBLE PRECISION");
    }

    // DATE AND TIMESTAMP VALIDATION TESTS

    #[test]
    fn test_date_parsing() {
        // Table-driven test for date parsing
        let test_cases = [
            // (input, expected_type, description)
            // Valid dates in various formats
            ("2025-01-01", SqlType::Date, "ISO date"),
            ("2024-12-31", SqlType::Date, "ISO date end of year"),
            ("12/25/2025", SqlType::Date, "US format MM/DD/YYYY"),
            ("25-12-2025", SqlType::Date, "European format DD-MM-YYYY"),
            ("25/12/2025", SqlType::Date, "European format DD/MM/YYYY"),
            ("1970-01-01", SqlType::Date, "Unix epoch"),
            ("2099-12-31", SqlType::Date, "Far future date"),
            ("2025-04-30", SqlType::Date, "Valid April 30"),
            // Leap year handling
            ("2024-02-29", SqlType::Date, "Valid leap year"),
            ("02/29/2024", SqlType::Date, "Leap year US format"),
            ("2025-02-29", SqlType::Text, "Invalid non-leap year Feb 29"),
            // Invalid dates
            ("2025-13-01", SqlType::Text, "Invalid month 13"),
            ("2025-01-99", SqlType::Text, "Invalid day 99"),
            ("2025-02-30", SqlType::Text, "February 30 doesn't exist"),
            ("9999-99-99", SqlType::Text, "Completely invalid date"),
            ("0000-00-00", SqlType::Text, "Invalid zero date"),
            ("13/32/2025", SqlType::Text, "Invalid US format"),
            ("32-13-2025", SqlType::Text, "Invalid European format"),
            ("2025-04-31", SqlType::Text, "April 31 doesn't exist"),
        ];

        for (input, expected, description) in test_cases {
            assert_eq!(
                SchemaInferrer::infer_value_type(input),
                Some(expected.clone()),
                "Failed: {} - input '{}'",
                description,
                input
            );
        }
    }

    #[test]
    fn test_timestamp_parsing() {
        // Table-driven test for timestamp parsing
        let test_cases = [
            // (input, expected_type, description)
            // Valid timestamps
            ("2025-01-01T12:34:56", SqlType::Timestamp, "ISO 8601"),
            ("2025-01-01 12:34:56", SqlType::Timestamp, "SQL format"),
            ("2025-01-01 12:34", SqlType::Timestamp, "Without seconds"),
            (
                "2025-01-01 12:34:56.123",
                SqlType::Timestamp,
                "With fractional seconds",
            ),
            (
                "12/25/2025 14:30:00",
                SqlType::Timestamp,
                "US format with time",
            ),
            (
                "25/12/2025 14:30:00",
                SqlType::Timestamp,
                "European format with time",
            ),
            // Invalid timestamps (false positives)
            ("path/to/file:123", SqlType::Text, "File path"),
            ("error-code:T1234", SqlType::Text, "Error code"),
            ("some-text:value", SqlType::Text, "Random text"),
            ("http://example.com:8080", SqlType::Text, "URL"),
        ];

        for (input, expected, description) in test_cases {
            assert_eq!(
                SchemaInferrer::infer_value_type(input),
                Some(expected.clone()),
                "Failed: {} - input '{}'",
                description,
                input
            );
        }
    }
}

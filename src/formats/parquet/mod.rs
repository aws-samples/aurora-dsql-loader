//! Parquet file format support for the DSQL data loader.
//!
//! This module provides functionality to read Apache Parquet files and load them into DSQL tables.
//! It includes:
//! - Schema mapping and validation between Arrow and SQL types
//! - Conversion from Arrow RecordBatches to row-based Records
//! - ByteReader adapter for Arrow's AsyncRead+AsyncSeek interface
//! - GenericParquetReader that implements the FileReader trait

mod adapter;
mod conversion;
mod reader;
mod schema;

pub use reader::GenericParquetReader;

//! File format parsers and readers

pub mod delimited;
pub mod parquet;
pub mod reader;

pub use reader::{DelimitedConfig, FileReader, Format, Partition, ReaderFactory, Record};

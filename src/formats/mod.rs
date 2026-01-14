//! File format parsers and readers

pub mod delimited;
pub mod parquet;
pub mod reader;

pub use reader::{Chunk, DelimitedConfig, FileReader, Format, ReaderFactory, Record};

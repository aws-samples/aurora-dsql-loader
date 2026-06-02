//! File format parsers and readers

pub mod delimited;
pub mod parquet;
pub mod pgdump;
pub mod reader;

pub use reader::{Chunk, DelimitedConfig, FileReader, Format, ReaderFactory, Record};

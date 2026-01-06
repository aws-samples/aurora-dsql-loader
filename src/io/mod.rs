//! I/O abstraction layer for reading bytes from different sources

pub mod byte_reader;
pub mod local_reader;
pub mod s3_reader;
pub mod uri;

pub use byte_reader::{ByteReader, estimate_rows_in_range, find_next_record_boundary};
pub use local_reader::LocalFileByteReader;
pub use s3_reader::S3ByteReader;
pub use uri::SourceUri;

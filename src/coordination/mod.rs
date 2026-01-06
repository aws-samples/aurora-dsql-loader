//! Distributed work coordination using manifest-based approach

pub mod coordinator;
pub mod manifest;
pub mod worker;

pub use coordinator::{Coordinator, LoadConfig};
pub use manifest::{DsqlConfig, FileFormat};

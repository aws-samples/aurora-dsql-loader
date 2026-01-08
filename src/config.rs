//! Configuration constants for the data loader
//!
//! This module centralizes all tunable parameters and constants used throughout
//! the application.

use std::time::Duration;

// ============================================================================
// Connection Pool Configuration
// ============================================================================

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(45);

pub const PING_TIMEOUT: Duration = Duration::from_secs(5);

pub const TOKEN_VALIDITY_DURATION: Duration = Duration::from_secs(900); // 15 minutes

// ============================================================================
// Worker Configuration
// ============================================================================

pub const MAX_RETRIES: u32 = 5;

/// Timeout for individual database queries
///
/// Set to 3 minutes because INSERT operations with large batches can take time,
/// especially when the table has indexes or constraints that need to be validated.
/// This prevents queries from hanging indefinitely while allowing legitimate
/// long-running operations to complete.
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(180); // 3 minutes

// ============================================================================
// I/O Configuration
// ============================================================================

/// Size of chunks when reading files in streaming fashion
///
/// Set to 8KB (standard memory page size) for efficient I/O operations.
/// This aligns with OS-level buffering and provides good balance between
/// memory usage and throughput for streaming file reads.
pub const CHUNK_SIZE: usize = 8192; // 8 KB

/// Size of sample to read when inferring file schema
///
/// Set to 8KB to provide enough data for accurate type inference without
/// reading large portions of the file. This typically captures hundreds to
/// thousands of rows depending on record size, sufficient for statistical
/// type detection.
pub const SAMPLE_SIZE: usize = 8192; // 8 KB

/// Buffer size for parquet async I/O adapter
///
/// Set to 256KB to optimize for parquet's columnar format and compression.
/// Larger buffers improve throughput for compressed parquet files by reducing
/// the number of I/O operations and allowing better utilization of CPU for
/// decompression.
pub const PARQUET_BUFFER_SIZE: usize = 256 * 1024; // 256 KB

//! Configuration constants for the data loader
//!
//! This module centralizes all tunable parameters and constants used throughout
//! the application. Adjust these values to tune performance and behavior.

use std::time::Duration;

// ============================================================================
// Connection Pool Configuration
// ============================================================================

/// Timeout for establishing new database connections
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(45);

/// Timeout for database connection health checks
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// Duration for which IAM authentication tokens remain valid
/// Tokens expire after 15 minutes, we refresh every 15 minutes before expiry
pub const TOKEN_VALIDITY_DURATION: Duration = Duration::from_secs(900); // 15 minutes

// ============================================================================
// Worker Configuration
// ============================================================================

/// Maximum number of retry attempts for transient database errors
pub const MAX_RETRIES: u32 = 10;

/// Timeout for individual database queries
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(180); // 3 minutes

// ============================================================================
// I/O Configuration
// ============================================================================

/// Size of chunks when reading files in streaming fashion
pub const CHUNK_SIZE: usize = 8192; // 8 KB

/// Size of sample to read when inferring file schema
pub const SAMPLE_SIZE: usize = 8192; // 8 KB

/// Buffer size for parquet async I/O adapter
pub const PARQUET_BUFFER_SIZE: usize = 256 * 1024; // 256 KB

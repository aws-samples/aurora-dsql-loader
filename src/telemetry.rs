/// Telemetry events sent from workers to coordinator for progress tracking
#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    /// Worker started processing a chunk
    ChunkStarted,
    /// Batch of records was successfully loaded
    BatchLoaded {
        records_loaded: u64,
        bytes_processed: u64,
        duration_ms: u64,
    },
    /// Worker completed processing a chunk
    ChunkCompleted { records_failed: u64 },
}

/// Statistics aggregated from telemetry events
#[derive(Debug, Default, Clone)]
pub struct ProgressStats {
    pub chunks_started: usize,
    pub chunks_completed: usize,
    pub records_loaded: u64,
    pub records_failed: u64,
    pub bytes_processed: u64,
    pub batch_durations_ms: Vec<u64>,
}

impl ProgressStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update stats with a telemetry event
    pub fn update(&mut self, event: &TelemetryEvent) {
        match event {
            TelemetryEvent::ChunkStarted => {
                self.chunks_started += 1;
            }
            TelemetryEvent::BatchLoaded {
                records_loaded,
                bytes_processed,
                duration_ms,
            } => {
                self.records_loaded += records_loaded;
                self.bytes_processed += bytes_processed;
                self.batch_durations_ms.push(*duration_ms);
            }
            TelemetryEvent::ChunkCompleted { records_failed } => {
                self.chunks_completed += 1;
                self.records_failed += records_failed;
                // Note: records_loaded and bytes_processed are already counted via BatchLoaded events
            }
        }
    }

    /// Calculate percentile from batch durations
    pub fn percentile(&self, p: f64) -> Option<u64> {
        if self.batch_durations_ms.is_empty() {
            return None;
        }

        let mut sorted = self.batch_durations_ms.clone();
        sorted.sort_unstable();

        // saturating_sub guards p=0.0 (ceil → 0, would underflow on `- 1`).
        let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
        let index = rank.saturating_sub(1).min(sorted.len() - 1);

        Some(sorted[index])
    }

    /// Get p50, p90, p99 percentiles
    pub fn get_percentiles(&self) -> (Option<u64>, Option<u64>, Option<u64>) {
        (
            self.percentile(50.0),
            self.percentile(90.0),
            self.percentile(99.0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_zero_does_not_underflow() {
        // p=0.0 → ceil(0)=0; without saturating_sub this panics in debug
        // / wraps to usize::MAX in release.
        let mut stats = ProgressStats::new();
        stats.batch_durations_ms = vec![10, 20, 30];
        assert_eq!(stats.percentile(0.0), Some(10));
    }

    #[test]
    fn percentile_empty_is_none() {
        assert_eq!(ProgressStats::new().percentile(50.0), None);
    }

    #[test]
    fn percentile_hundred_is_max() {
        let mut stats = ProgressStats::new();
        stats.batch_durations_ms = vec![10, 20, 30];
        assert_eq!(stats.percentile(100.0), Some(30));
    }
}

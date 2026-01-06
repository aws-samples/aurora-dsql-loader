/// Telemetry events sent from workers to coordinator for progress tracking
#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    /// Worker started processing a partition
    PartitionStarted,
    /// Batch of records was successfully loaded
    BatchLoaded {
        records_loaded: u64,
        bytes_processed: u64,
        duration_ms: u64,
    },
    /// Worker completed processing a partition
    PartitionCompleted { records_failed: u64 },
}

/// Statistics aggregated from telemetry events
#[derive(Debug, Default, Clone)]
pub struct ProgressStats {
    pub partitions_started: usize,
    pub partitions_completed: usize,
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
            TelemetryEvent::PartitionStarted => {
                self.partitions_started += 1;
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
            TelemetryEvent::PartitionCompleted { records_failed } => {
                self.partitions_completed += 1;
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

        let index = ((p / 100.0) * sorted.len() as f64).ceil() as usize - 1;
        let index = index.min(sorted.len() - 1);

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

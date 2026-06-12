//! Progress bar construction and lifecycle for both load and migrate flows.
//!
//! Bar layout during migrate (top → bottom):
//! ```text
//! [elapsed] Tables:     [bar] N/M (P%)              ← MigrateProgress, persistent
//! [elapsed] Dump Bytes: [bar] X/Y (P%) | rate ETA Z ← MigrateProgress, persistent
//! [elapsed] Chunks:     [bar] n/m (P%)              ← LoadProgress, per-table
//! [elapsed] Rows:       [bar] r/total (P%) | rate   ← LoadProgress, per-table (when row count is known)
//! [elapsed] Bytes:      [bar] b/size (P%) | rate    ← LoadProgress, per-table
//! [elapsed] Batch Time: p50/p90/p99                 ← LoadProgress, per-table
//! ```
//!
//! Bar lifecycle (per table): workers send telemetry → channel closes →
//! pump drains → bars finalize. Finalizing while the pump is still
//! writing corrupts the next render (see `LoadProgress::finish_and_clear`).

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

use crate::formats::pgdump::CopyBlock;
use crate::formats::reader::FileMetadata;
use crate::telemetry::{ProgressStats, TelemetryEvent};

/// Whole-dump progress view: two persistent bars (Tables, Dump Bytes)
/// plus the `MultiProgress` that per-table [`LoadProgress`] attach to.
pub struct MigrateProgress {
    multi: MultiProgress,
    tables_bar: ProgressBar,
    bytes_bar: ProgressBar,
}

impl MigrateProgress {
    pub fn new(blocks: &[CopyBlock]) -> Self {
        Self::with_multi(blocks, MultiProgress::new())
    }

    /// Test seam: pass a `ProgressDrawTarget::hidden()`-backed
    /// `MultiProgress` so `cargo test` doesn't stream bars to stderr.
    pub fn with_multi(blocks: &[CopyBlock], multi: MultiProgress) -> Self {
        let total_tables = blocks.len() as u64;
        let total_bytes: u64 = blocks.iter().map(|b| b.data_end - b.data_start).sum();

        let tables_bar = multi.add(ProgressBar::new(total_tables));
        tables_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] Tables:     [{bar:30.magenta/blue}] {pos}/{len} ({percent}%)",
                )
                .unwrap()
                .progress_chars("=>-"),
        );

        let bytes_bar = multi.add(ProgressBar::new(total_bytes));
        bytes_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] Dump Bytes: [{bar:30.yellow/blue}] {bytes}/{total_bytes} ({percent}%) | {bytes_per_sec} ETA {eta}",
                )
                .unwrap()
                .progress_chars("=>-"),
        );

        Self {
            multi,
            tables_bar,
            bytes_bar,
        }
    }

    /// Handle for per-table [`LoadProgress`] instances to attach to.
    pub fn multi(&self) -> MultiProgress {
        self.multi.clone()
    }

    pub fn record_table_loaded(&self, block_bytes: u64) {
        self.tables_bar.inc(1);
        self.bytes_bar.inc(block_bytes);
    }

    /// Finish the persistent bars "done", leaving them visible. Call
    /// before any trailing stdout so the live render doesn't corrupt it.
    pub fn finish_visible(&self) {
        self.tables_bar.finish_with_message("done");
        self.bytes_bar.finish_with_message("done");
    }

    /// Freeze both persistent bars at their current position. The
    /// halting table's per-load bars are finalized inside
    /// `Coordinator::run_load` before this is called.
    pub fn finish_halted(&self, reason: &str) {
        self.tables_bar
            .abandon_with_message(format!("halted: {reason}"));
        self.bytes_bar
            .abandon_with_message(format!("halted: {reason}"));
    }
}

impl Drop for MigrateProgress {
    /// Safety net for `?`-unwind out of the migrate loop: abandon any
    /// still-live bar so it doesn't interleave with the printed error.
    /// No-op once `finish_visible`/`finish_halted` has run.
    fn drop(&mut self) {
        if !self.tables_bar.is_finished() {
            self.tables_bar.abandon_with_message("interrupted");
        }
        if !self.bytes_bar.is_finished() {
            self.bytes_bar.abandon_with_message("interrupted");
        }
    }
}

/// Per-load progress: 4 bars (chunks / rows / bytes / batch-time) plus
/// the telemetry pump task that drives them. Keeps its `MultiProgress`
/// so the embedded path can `remove` finished bars (clearing alone
/// leaves zombies that pile up across a multi-table migrate).
pub struct LoadProgress {
    multi: MultiProgress,
    chunk_bar: ProgressBar,
    rows_bar: Option<ProgressBar>,
    bytes_bar: ProgressBar,
    stats_bar: ProgressBar,
    pump: tokio::task::JoinHandle<()>,
}

impl LoadProgress {
    /// Standalone-load path: own `MultiProgress`, leave bars visible
    /// after `finish_standalone`.
    pub fn new(
        file_metadata: &FileMetadata,
        total_chunks: u64,
        telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Self {
        Self::with_multi(
            MultiProgress::new(),
            file_metadata,
            total_chunks,
            telemetry_rx,
        )
    }

    /// Attach 4 bars to an existing `MultiProgress`; bars render in
    /// `add` order, so the dump-wide bars must already be inserted.
    pub fn within(
        parent: MultiProgress,
        file_metadata: &FileMetadata,
        total_chunks: u64,
        telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Self {
        Self::with_multi(parent, file_metadata, total_chunks, telemetry_rx)
    }

    fn with_multi(
        multi: MultiProgress,
        file_metadata: &FileMetadata,
        total_chunks: u64,
        mut telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Self {
        let (chunk_bar, rows_bar, bytes_bar, stats_bar) =
            build_load_bars(&multi, file_metadata, total_chunks);

        let chunk_bar_pump = chunk_bar.clone();
        let rows_bar_pump = rows_bar.clone();
        let bytes_bar_pump = bytes_bar.clone();
        let stats_bar_pump = stats_bar.clone();

        let pump = tokio::spawn(async move {
            let mut stats = ProgressStats::new();
            while let Some(event) = telemetry_rx.recv().await {
                stats.update(&event);
                chunk_bar_pump.set_position(stats.chunks_completed as u64);
                if let Some(ref bar) = rows_bar_pump {
                    bar.set_position(stats.records_loaded);
                }
                bytes_bar_pump.set_position(stats.bytes_processed);
                let (p50, p90, p99) = stats.get_percentiles();
                if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                    stats_bar_pump
                        .set_message(format!("p50: {}ms, p90: {}ms, p99: {}ms", p50, p90, p99));
                }
            }
        });

        Self {
            multi,
            chunk_bar,
            rows_bar,
            bytes_bar,
            stats_bar,
            pump,
        }
    }

    /// Drain the pump, then clear and remove each bar (embedded/migrate
    /// path). Caller must have closed the channel first.
    pub async fn finish_and_clear(mut self) {
        self.join_pump().await;
        for bar in self.bars() {
            bar.finish_and_clear();
            self.multi.remove(&bar);
        }
    }

    /// Same pump-wait ordering as `finish_and_clear`, but leave bars
    /// visible so the operator keeps the final state on screen.
    pub async fn finish_standalone(mut self) {
        self.join_pump().await;
        self.chunk_bar.finish_with_message("All chunks completed");
        if let Some(bar) = &self.rows_bar {
            bar.finish();
        }
        self.bytes_bar.finish();
        self.stats_bar.finish();
    }

    fn bars(&self) -> Vec<ProgressBar> {
        let mut v = vec![self.chunk_bar.clone()];
        v.extend(self.rows_bar.clone());
        v.push(self.bytes_bar.clone());
        v.push(self.stats_bar.clone());
        v
    }

    /// Await the pump, surfacing a panic instead of swallowing it — a
    /// panicked pump means bar positions silently stopped advancing.
    async fn join_pump(&mut self) {
        if let Err(e) = (&mut self.pump).await
            && e.is_panic()
        {
            tracing::error!(error = %e, "progress pump panicked; bar positions may under-count");
        }
    }
}

fn build_load_bars(
    multi: &MultiProgress,
    file_metadata: &FileMetadata,
    total_chunks: u64,
) -> (ProgressBar, Option<ProgressBar>, ProgressBar, ProgressBar) {
    let estimated_total_rows = file_metadata.estimated_rows.unwrap_or(0);

    let chunk_bar = multi.add(ProgressBar::new(total_chunks));
    chunk_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] Chunks:     [{bar:30.cyan/blue}] {pos}/{len} ({percent}%)",
            )
            .unwrap()
            .progress_chars("=>-"),
    );

    let rows_bar = if estimated_total_rows > 0 {
        let bar = multi.add(ProgressBar::new(estimated_total_rows));
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] Rows:       [{bar:30.green/blue}] {human_pos}/{human_len} ({percent}%) | {per_sec}",
                )
                .unwrap()
                .progress_chars("=>-"),
        );
        Some(bar)
    } else {
        None
    };

    let bytes_bar = multi.add(ProgressBar::new(file_metadata.file_size_bytes));
    bytes_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] Bytes:      [{bar:30.yellow/blue}] {bytes}/{total_bytes} ({percent}%) | {bytes_per_sec}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );

    let stats_bar = multi.add(ProgressBar::new(0));
    stats_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] Batch Time: {msg}")
            .unwrap(),
    );

    (chunk_bar, rows_bar, bytes_bar, stats_bar)
}

#[cfg(test)]
mod tests {
    use super::*;
    use indicatif::ProgressDrawTarget;

    fn mk_block(schema: &str, table: &str, data_start: u64, data_end: u64) -> CopyBlock {
        CopyBlock {
            schema: schema.into(),
            table: table.into(),
            header_start: data_start.saturating_sub(1),
            data_start,
            data_end,
            block_end: data_end + 2,
            columns: vec!["id".into()],
        }
    }

    /// Pins: tables = `blocks.len()`, bytes = `sum(data_end - data_start)`.
    #[test]
    fn migrate_progress_totals_from_blocks() {
        let blocks = vec![
            mk_block("public", "a", 100, 200),
            mk_block("public", "b", 300, 1300),
            mk_block("public", "c", 1400, 1450),
        ];
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let mp = MigrateProgress::with_multi(&blocks, multi);
        assert_eq!(mp.tables_bar.length(), Some(3));
        assert_eq!(mp.bytes_bar.length(), Some(100 + 1000 + 50));
    }

    #[test]
    fn migrate_progress_records_advance_both_bars() {
        let blocks = vec![
            mk_block("public", "a", 0, 100),
            mk_block("public", "b", 100, 300),
        ];
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let mp = MigrateProgress::with_multi(&blocks, multi);
        mp.record_table_loaded(100);
        assert_eq!(mp.tables_bar.position(), 1);
        assert_eq!(mp.bytes_bar.position(), 100);
        mp.record_table_loaded(200);
        assert_eq!(mp.tables_bar.position(), 2);
        assert_eq!(mp.bytes_bar.position(), 300);
    }

    /// Pins freeze-on-halt: bars stay at the last `record_table_loaded`
    /// position rather than advancing to total.
    #[test]
    fn migrate_progress_finish_halted_freezes_position() {
        let blocks = vec![
            mk_block("public", "a", 0, 100),
            mk_block("public", "b", 100, 300),
        ];
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let mp = MigrateProgress::with_multi(&blocks, multi);
        mp.record_table_loaded(100);
        mp.finish_halted("1 row failed");
        assert_eq!(mp.tables_bar.position(), 1);
        assert_eq!(mp.bytes_bar.position(), 100);
    }

    /// Smoke: build → send event → drop tx → `finish_and_clear` must not
    /// panic. Catches regressions in the pump-drain-then-finalize order.
    #[tokio::test]
    async fn load_progress_within_lifecycle_finishes_cleanly() {
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let metadata = FileMetadata {
            file_size_bytes: 1000,
            estimated_rows: Some(50),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let lp = LoadProgress::within(multi, &metadata, 5, rx);

        tx.send(TelemetryEvent::ChunkStarted).unwrap();
        tx.send(TelemetryEvent::BatchLoaded {
            records_loaded: 10,
            bytes_processed: 200,
            duration_ms: 42,
        })
        .unwrap();
        drop(tx);

        lp.finish_and_clear().await;
    }

    /// Standalone path (own `MultiProgress`, `finish_standalone`) — the
    /// entire non-migrate `load` surface, otherwise only covered via
    /// `within`.
    #[tokio::test]
    async fn load_progress_standalone_lifecycle_finishes_cleanly() {
        let metadata = FileMetadata {
            file_size_bytes: 1000,
            estimated_rows: Some(50),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let lp = LoadProgress::new(&metadata, 5, rx);

        tx.send(TelemetryEvent::BatchLoaded {
            records_loaded: 10,
            bytes_processed: 200,
            duration_ms: 42,
        })
        .unwrap();
        drop(tx);

        lp.finish_standalone().await;
    }

    /// `estimated_rows: None` → no rows bar; finalizers must skip the
    /// absent bar without panicking.
    #[tokio::test]
    async fn load_progress_without_rows_bar_finishes_cleanly() {
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let metadata = FileMetadata {
            file_size_bytes: 1000,
            estimated_rows: None,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let lp = LoadProgress::within(multi, &metadata, 5, rx);
        assert!(lp.rows_bar.is_none());

        tx.send(TelemetryEvent::BatchLoaded {
            records_loaded: 10,
            bytes_processed: 200,
            duration_ms: 42,
        })
        .unwrap();
        drop(tx);

        lp.finish_and_clear().await;
    }
}

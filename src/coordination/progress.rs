//! Progress bar construction and lifecycle for both load and migrate flows.
//!
//! Two consumers:
//! - `Coordinator::run_load` builds a [`LoadProgress`] either standalone
//!   (its own `MultiProgress`) or `within` a parent `MultiProgress`
//!   supplied by the migrate orchestrator.
//! - `migrate::orchestrator::run_migrate` builds one [`MigrateProgress`]
//!   for the whole-dump view and threads its `multi` handle into each
//!   per-table load via `LoadConfig.parent_multi`.
//!
//! Bar layout during migrate (top → bottom):
//! ```text
//! [elapsed] Tables:     [bar] N/M (P%)              ← MigrateProgress, persistent
//! [elapsed] Bytes:      [bar] X/Y (P%) | rate ETA Z ← MigrateProgress, persistent
//! [elapsed] Chunks:     [bar] n/m (P%)              ← LoadProgress, per-table
//! [elapsed] Rows:       [bar] r/total (P%) | rate   ← LoadProgress, per-table (parquet only)
//! [elapsed] Bytes:      [bar] b/size (P%) | rate    ← LoadProgress, per-table
//! [elapsed] Batch Time: p50/p90/p99                 ← LoadProgress, per-table
//! ```
//!
//! Bar lifecycle (per table):
//! 1. `LoadProgress::within` adds 4 bars below the 2 persistent ones.
//! 2. Workers send `TelemetryEvent`s; the pump task updates the bars.
//! 3. After the load returns, the channel is closed; the pump exits.
//! 4. `LoadProgress::finish_clean` / `finish_halted` awaits the pump,
//!    then either clears the per-table bars (clean) or freezes them
//!    in place (halt) — see method docs for the ordering invariant.

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::formats::pgdump::CopyBlock;
use crate::formats::reader::FileMetadata;
use crate::telemetry::{ProgressStats, TelemetryEvent};

/// Whole-dump progress view used by the migrate orchestrator.
///
/// Holds two persistent bars (Tables, Bytes) plus a shared
/// `MultiProgress` handle that per-table [`LoadProgress`] instances
/// attach to. Constructed once at the top of `run_migrate` and lives
/// for the duration of the per-table loop.
pub struct MigrateProgress {
    multi: Arc<MultiProgress>,
    tables_bar: ProgressBar,
    bytes_bar: ProgressBar,
}

impl MigrateProgress {
    /// Build the migrate-wide progress view from the resolved
    /// `CopyBlock` list. `total_bytes` sums each block's data payload
    /// (`data_end - data_start`) — already produced by `list_copy_blocks`.
    pub fn new(blocks: &[CopyBlock]) -> Self {
        Self::with_multi(blocks, MultiProgress::new())
    }

    /// Test/internal constructor: callers can supply a `MultiProgress`
    /// with `ProgressDrawTarget::hidden()` so `cargo test` doesn't
    /// stream bars to stderr.
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
            multi: Arc::new(multi),
            tables_bar,
            bytes_bar,
        }
    }

    /// Shared `MultiProgress` handle for per-table [`LoadProgress`]
    /// instances to attach to via [`LoadProgress::within`].
    pub fn multi(&self) -> Arc<MultiProgress> {
        Arc::clone(&self.multi)
    }

    /// Advance the persistent bars after a clean per-table load.
    pub fn record_table_loaded(&self, block_bytes: u64) {
        self.tables_bar.inc(1);
        self.bytes_bar.inc(block_bytes);
    }

    /// Close the run cleanly: finish both persistent bars at full
    /// position so the live render is closed before the orchestrator's
    /// caller (e.g. `main.rs`'s "Loaded tables:" summary) prints.
    pub fn finish_clean(&self) {
        self.tables_bar.finish_with_message("done");
        self.bytes_bar.finish_with_message("done");
    }

    /// Close the run on a halting iteration: freeze both persistent
    /// bars at their current position with a "halted" message. The
    /// per-table `LoadProgress` for the halting table is finalized
    /// independently via [`LoadProgress::finish_halted`].
    pub fn finish_halted(&self, reason: &str) {
        self.tables_bar
            .abandon_with_message(format!("halted: {reason}"));
        self.bytes_bar
            .abandon_with_message(format!("halted: {reason}"));
    }
}

/// Per-load progress: 4 bars (chunks / rows / bytes / batch-time) plus
/// the telemetry pump task that drives them. Constructed by
/// `Coordinator::run_load` either standalone or attached to a parent
/// `MultiProgress` from [`MigrateProgress`].
pub struct LoadProgress {
    chunk_bar: ProgressBar,
    rows_bar: Option<ProgressBar>,
    bytes_bar: ProgressBar,
    stats_bar: ProgressBar,
    pump: tokio::task::JoinHandle<()>,
}

impl LoadProgress {
    /// Standalone-load path: build a fresh `MultiProgress` and 4 bars,
    /// spawn the pump task. Equivalent to the previous
    /// `setup_progress_tracking` body.
    pub fn new(
        file_metadata: &FileMetadata,
        total_chunks: u64,
        telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Self {
        Self::with_multi(
            &MultiProgress::new(),
            file_metadata,
            total_chunks,
            telemetry_rx,
        )
    }

    /// Migrate path: attach 4 bars to the caller's `MultiProgress` so
    /// they render below the persistent dump-wide bars.
    pub fn within(
        parent: &MultiProgress,
        file_metadata: &FileMetadata,
        total_chunks: u64,
        telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Self {
        Self::with_multi(parent, file_metadata, total_chunks, telemetry_rx)
    }

    fn with_multi(
        multi: &MultiProgress,
        file_metadata: &FileMetadata,
        total_chunks: u64,
        mut telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Self {
        let (chunk_bar, rows_bar, bytes_bar, stats_bar) =
            build_load_bars(multi, file_metadata, total_chunks);

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
            chunk_bar,
            rows_bar,
            bytes_bar,
            stats_bar,
            pump,
        }
    }

    /// Migrate clean path: await the pump (channel must already be
    /// closed by the caller dropping its `telemetry_tx`), then
    /// `finish_and_clear` the per-table bars so the next iteration's
    /// bars render in the same screen position. Bars must NOT be
    /// finalized while the pump is still writing to them — corrupts
    /// the next bar's render.
    pub async fn finish_clean(self) {
        let _ = self.pump.await;
        self.chunk_bar.finish_and_clear();
        if let Some(bar) = self.rows_bar {
            bar.finish_and_clear();
        }
        self.bytes_bar.finish_and_clear();
        self.stats_bar.finish_and_clear();
    }

    /// Standalone-load termination: same pump-wait ordering as
    /// `finish_clean`, but leave the bars visible (not cleared) so
    /// the operator keeps the final state on screen. This is the
    /// historical behavior of `setup_progress_tracking`'s pump exit.
    pub async fn finish_standalone(self) {
        let _ = self.pump.await;
        self.chunk_bar.finish_with_message("All chunks completed");
        if let Some(bar) = self.rows_bar {
            bar.finish();
        }
        self.bytes_bar.finish();
        self.stats_bar.finish();
    }
}

/// Add the four per-load bars to `multi` and return them. Shared
/// between [`LoadProgress::new`] and [`LoadProgress::within`] so the
/// templates live in one place.
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

    /// `MigrateProgress::new` derives the dump totals straight from the
    /// `CopyBlock` list — total tables = blocks.len(), total bytes =
    /// sum of (data_end - data_start). Pinning so a future refactor of
    /// `CopyBlock` field semantics surfaces here.
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

    /// `record_table_loaded` advances both persistent bars in lockstep.
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

    /// `finish_halted` does NOT advance the bars — they freeze at the
    /// last `record_table_loaded` position. Pin so a future "set to
    /// total on halt" refactor surfaces.
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

    /// `LoadProgress::within` smoke: build, send one telemetry event,
    /// drop the channel, await `finish_clean`. Exercises the full
    /// lifecycle (insert → pump → drop → finalize) without any real
    /// load. Pinned so a future change to the pump's drain semantics
    /// or finish ordering surfaces.
    #[tokio::test]
    async fn load_progress_within_lifecycle_finishes_cleanly() {
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let metadata = FileMetadata {
            file_size_bytes: 1000,
            estimated_rows: Some(50),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let lp = LoadProgress::within(&multi, &metadata, 5, rx);

        tx.send(TelemetryEvent::ChunkStarted).unwrap();
        tx.send(TelemetryEvent::BatchLoaded {
            records_loaded: 10,
            bytes_processed: 200,
            duration_ms: 42,
        })
        .unwrap();
        drop(tx);

        lp.finish_clean().await;
    }
}

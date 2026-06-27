//! # zradar-parquet
//!
//! Parquet storage layer for zradar telemetry data.
//!
//! ## Modules
//!
//! - `schema` — Arrow schemas and RecordBatch ↔ domain struct conversions
//! - `writer` — `ParquetFileWriter`: write Parquet files to local disk and register in file_list
//! - `recovery` — M07-03: startup crash recovery (removes orphaned `.par` temp files)
//! - `write_buffer` — M07-04: `WriteBuffer`: in-memory accumulator keyed by (tenant, signal, hour)
//! - `flush_worker` — M07-04: `FlushWorker`: background worker that drains `WriteBuffer` to Parquet
//! - `telemetry_writer` — `ParquetTelemetryWriter`: implements `TelemetryWriter` trait
//! - `reader` — `ParquetFileReader`: query Parquet files via DataFusion (local + S3 via DiskCache)
//! - `telemetry_reader` — `ParquetTelemetryReader`: implements `TelemetryReader` trait
//! - `disk_cache` — `DiskCache`: LRU + TTL local cache for S3-backed Parquet files
//! - `file_mover` — `FileMover`: background job that promotes local files to S3
//! - `util` — Small shared helpers (SQL identifier quoting, etc.)
//!
//! Physical deletion of expired/compacted Parquet files is handled by the
//! single lease-aware `FileReclaimer` in `zradar-retention`. Compaction here
//! only *soft-deletes* originals (`mark_deleted`); it never unlinks files.

pub mod compactor;
pub mod disk_cache;
pub mod engine;
pub mod file_mover;
pub mod flush_worker;
pub mod memory_cache;
pub mod reader;
pub mod recovery;
pub mod schema;
pub mod telemetry_reader;
pub mod telemetry_writer;
pub mod util;
pub mod write_buffer;
pub mod writer;

pub use compactor::Compactor;
pub use disk_cache::DiskCache;
pub use engine::SharedEngine;
// The lease registry lives in `zradar-traits` so both the reader (here) and the
// `FileReclaimer` (zradar-retention) can share one instance without a crate
// cycle. Re-exported for ergonomic `zradar_parquet::FileLeaseRegistry` access.
pub use file_mover::FileMover;
pub use flush_worker::FlushWorker;
pub use memory_cache::MemoryCache;
pub use reader::ParquetFileReader;
pub use recovery::recover_incomplete_writes;
pub use telemetry_reader::ParquetTelemetryReader;
pub use telemetry_writer::ParquetTelemetryWriter;
pub use util::quote_identifier;
pub use write_buffer::WriteBuffer;
pub use writer::{ParquetFileWriter, WriterConfig};
pub use zradar_traits::file_lease::{FileLease, FileLeaseRegistry};

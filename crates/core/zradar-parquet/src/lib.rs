//! # zradar-parquet
//!
//! Parquet storage layer for zradar telemetry data.
//!
//! ## Modules
//!
//! - `schema` — Arrow schemas and RecordBatch ↔ domain struct conversions
//! - `writer` — `ParquetFileWriter`: write Parquet files to local disk and register in file_list
//! - `telemetry_writer` — `ParquetTelemetryWriter`: implements `TelemetryWriter` trait
//! - `reader` — `ParquetFileReader`: query Parquet files via DataFusion
//! - `telemetry_reader` — `ParquetTelemetryReader`: implements `TelemetryReader` trait

pub mod reader;
pub mod schema;
pub mod telemetry_reader;
pub mod telemetry_writer;
pub mod writer;

pub use reader::ParquetFileReader;
pub use telemetry_reader::ParquetTelemetryReader;
pub use telemetry_writer::ParquetTelemetryWriter;
pub use writer::ParquetFileWriter;

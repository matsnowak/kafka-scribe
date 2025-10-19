//! Message format implementations
//!
//! This module contains implementations of the `MessageFormat` trait for various
//! message formats such as JSON, Avro, Protobuf, Binary, and String.

pub mod json;
pub mod json_hybrid;

pub use json::JsonFormat;
pub use json_hybrid::{JsonHybridFormat, BinaryEncoding};
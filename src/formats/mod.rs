//! Message format implementations
//!
//! This module contains implementations of the `MessageFormat` trait for various
//! message formats such as JSON, Avro, Protobuf, Binary, and String.

pub mod json;

pub use json::JsonFormat;
//! Integration tests for the kafka-scribe CLI tool.
//!
//! These tests execute the actual compiled binary rather than just testing
//! individual modules, ensuring end-to-end functionality validation.

pub mod store_command_tests;

pub use integration::*;
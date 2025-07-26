//! Integration tests for the kafka-scribe CLI tool.
//!
//! These tests execute the actual compiled binary rather than just testing
//! individual modules, ensuring end-to-end functionality validation.

pub mod store_command_tests;
pub mod replay_tests;
pub mod stats_tests;
pub mod e2e_tests;
pub mod performance_tests;
pub mod schema_validation_tests;
pub mod fixture_manager_test;

pub use integration::*;

//! Integration tests for the kafka-scribe CLI tool.
//!
//! These tests execute the actual compiled binary rather than just testing
//! individual modules, ensuring end-to-end functionality validation.

// Import the common module for shared test utilities
mod common;

// Import the store_command_tests module directly
#[path = "integration/store_command_tests.rs"]
mod store_command_tests;

// Re-export all tests from the store_command_tests module
pub use store_command_tests::*;

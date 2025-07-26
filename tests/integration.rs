//! Integration tests for the kafka-scribe CLI tool.
//!
//! These tests execute the actual compiled binary rather than just testing
//! individual modules, ensuring end-to-end functionality validation.

// Import the common module for shared test utilities
mod common;

// Import all test modules directly
#[path = "integration/store_command_tests.rs"]
mod store_command_tests;

#[path = "integration/replay_tests.rs"]
mod replay_tests;

#[path = "integration/stats_tests.rs"]
mod stats_tests;

#[path = "integration/e2e_tests.rs"]
mod e2e_tests;

#[path = "integration/performance_tests.rs"]
mod performance_tests;

#[path = "integration/schema_validation_tests.rs"]
mod schema_validation_tests;

// Re-export all tests from all test modules
pub use store_command_tests::*;
pub use replay_tests::*;
pub use stats_tests::*;
pub use e2e_tests::*;
pub use performance_tests::*;
pub use schema_validation_tests::*;

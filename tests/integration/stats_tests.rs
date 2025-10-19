//! Integration tests for the stats command.
//!
//! These tests verify that the stats command correctly calculates and displays
//! statistics about stored Kafka messages.

use std::sync::Once;
use std::fs;
use std::path::Path;

use anyhow::Result;
use serde_json::Value;
use tokio::runtime::Runtime;
use tracing::{debug, info};

use super::common::cli_helpers::{
    run_store_command, validate_success, validate_output_contains, 
    validate_output_does_not_contain, TestDirectory, CliExecutor
};
use super::common::kafka_setup::KafkaTestContext;
use super::common::test_data::{
    generate_test_messages, generate_binary_test_messages, 
    generate_key_filtered_test_messages, generate_header_filtered_test_messages
};

// Initialize test environment once
static INIT: Once = Once::new();
fn init_test_environment() -> &'static () {
    static DOCKER: &() = &();
    INIT.call_once(|| {
        // Initialize logging for tests
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();
    });
    DOCKER
}

/// Test basic statistics generation
#[tokio::test]
async fn test_basic_stats() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-basic-stats";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command to store messages
    info!("Running store command to store messages for stats test");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Run the stats command
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &[],
    ).await?;

    // Validate command output
    validate_success(&stats_output)?;

    // Verify output contains expected statistics
    let stdout = String::from_utf8_lossy(&stats_output.stdout);
    validate_output_contains(&stats_output, "Total messages:")?;
    validate_output_contains(&stats_output, "10")?; // 10 messages
    validate_output_contains(&stats_output, "Partitions:")?;
    validate_output_contains(&stats_output, "Message size statistics")?;

    Ok(())
}

/// Test stats command with JSON output format
#[tokio::test]
async fn test_stats_json_format() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-stats-json";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command to store messages
    info!("Running store command to store messages for stats test");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Run the stats command with JSON output
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &["--format", "json"],
    ).await?;

    // Validate command output
    validate_success(&stats_output)?;

    // Verify output is valid JSON
    let stdout = String::from_utf8_lossy(&stats_output.stdout);
    let json_result: Result<Value, _> = serde_json::from_str(&stdout);
    assert!(json_result.is_ok(), "Output is not valid JSON: {}", stdout);

    // Verify JSON contains expected fields
    let json = json_result.unwrap();
    assert!(json.get("total_messages").is_some(), "JSON missing total_messages field");
    assert_eq!(json["total_messages"], 10, "Expected 10 total messages");
    assert!(json.get("partitions").is_some(), "JSON missing partitions field");
    assert!(json.get("size_stats").is_some(), "JSON missing size_stats field");

    Ok(())
}

/// Test stats command with CSV output format
#[tokio::test]
async fn test_stats_csv_format() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-stats-csv";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command to store messages
    info!("Running store command to store messages for stats test");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Run the stats command with CSV output
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &["--format", "csv"],
    ).await?;

    // Validate command output
    validate_success(&stats_output)?;

    // Verify output contains CSV headers and data
    let stdout = String::from_utf8_lossy(&stats_output.stdout);
    assert!(stdout.contains("topic,partition,count"), "CSV missing headers");
    assert!(stdout.contains("test-stats-csv,0,10"), "CSV missing expected data");

    Ok(())
}

/// Test stats command with different storage backends (single file)
#[tokio::test]
async fn test_stats_with_file_storage() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-stats-file";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;
    let output_file = temp_dir.path().join("messages.json");

    // Run the store command to store messages to a single file
    info!("Running store command to store messages to a file for stats test");
    let cli = CliExecutor::new();
    let store_output = cli.execute(&[
        "store", 
        topic, 
        "--bootstrap-servers", 
        kafka.bootstrap_servers(),
        "--to-file", 
        output_file.to_str().unwrap(),
        "--from-beginning",
    ]).await?;
    validate_success(&store_output)?;

    // Run the stats command on the file
    let stats_output = cli.execute(&[
        "stats",
        "--from-file",
        output_file.to_str().unwrap(),
    ]).await?;

    // Validate command output
    validate_success(&stats_output)?;

    // Verify output contains expected statistics
    validate_output_contains(&stats_output, "Total messages:")?;
    validate_output_contains(&stats_output, "10")?; // 10 messages

    Ok(())
}

/// Test stats command with edge case: empty store
#[tokio::test]
async fn test_stats_empty_store() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Create empty temporary directory
    let temp_dir = TestDirectory::new()?;

    // Run the stats command on empty directory
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &[],
    ).await?;

    // Validate command output
    validate_success(&stats_output)?;

    // Verify output shows zero messages
    validate_output_contains(&stats_output, "Total messages: 0")?;

    Ok(())
}

/// Test stats command with edge case: very large messages
#[tokio::test]
async fn test_stats_large_messages() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-stats-large";
    kafka.create_topic(topic, 1).await?;

    // Produce large binary test messages
    let messages = generate_binary_test_messages(5);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command to store messages
    info!("Running store command to store large messages for stats test");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Run the stats command
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &[],
    ).await?;

    // Validate command output
    validate_success(&stats_output)?;

    // Verify output contains expected statistics
    validate_output_contains(&stats_output, "Total messages: 5")?;
    validate_output_contains(&stats_output, "Max size:")?;

    Ok(())
}
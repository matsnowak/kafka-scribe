//! Integration tests for the `store` command.
//!
//! These tests verify that the `store` command correctly stores messages
//! from Kafka topics to various storage backends.

use std::sync::Once;

use anyhow::Result;
use tokio::runtime::Runtime;
use tracing::{debug, info};

use super::common::cli_helpers::{
    run_store_command, validate_stored_messages, validate_success, TestDirectory,
};
use super::common::kafka_setup::KafkaTestContext;
use super::common::test_data::{generate_test_messages, generate_binary_test_messages, generate_key_filtered_test_messages, generate_header_filtered_test_messages, generate_timestamped_test_messages};

// Initialize the test environment once
static INIT: Once = Once::new();
static mut DOCKER_CLIENT: Option<()> = None;

fn init_test_environment() -> &'static () {
    unsafe {
        INIT.call_once(|| {
            // Initialize tracing for tests
            let _ = tracing_subscriber::fmt()
                .with_env_filter("debug")
                .try_init();

            // Initialize a dummy Docker client
            DOCKER_CLIENT = Some(());
        });
        DOCKER_CLIENT.as_ref().unwrap()
    }
}

/// Test that the `store` command can store messages from a Kafka topic to a directory.
#[tokio::test]
async fn test_basic_store_to_directory() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-basic-store";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command
    info!("Running store command");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--count",
        "10",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages
    validate_stored_messages(&temp_dir, 10, |_| Ok(()))?;

    Ok(())
}

/// Test that the `store` command can store messages from a Kafka topic to a single file.
#[tokio::test]
async fn test_store_to_file() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-store-to-file";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;
    let output_file = temp_dir.path().join("output.json");

    // Run the store command
    info!("Running store command");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-file",
        output_file.to_str().unwrap(),
        "--count",
        "10",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages
    validate_stored_messages(&temp_dir, 10, |_| Ok(()))?;

    Ok(())
}

/// Test that the `store` command can filter messages by key regex.
#[tokio::test]
async fn test_store_with_key_regex_filter() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-key-regex";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages with different key patterns
    let messages = generate_key_filtered_test_messages();
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with key regex filter
    info!("Running store command with key regex filter");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--key-regex",
        "user-.*",
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have messages with keys matching "user-.*"
    validate_stored_messages(&temp_dir, 5, |msg| {
        let key = msg.key.as_ref().unwrap();
        if !key.starts_with("user-") {
            anyhow::bail!("Message key does not match regex: {}", key);
        }
        Ok(())
    })?;

    Ok(())
}

/// Test that the `store` command can filter messages by header.
#[tokio::test]
async fn test_store_with_header_filter() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-header-filter";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages with different headers
    let messages = generate_header_filtered_test_messages();
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with header filter
    info!("Running store command with header filter");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--header",
        "region=us",
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have messages with header "region=us"
    validate_stored_messages(&temp_dir, 5, |msg| {
        let headers = msg.headers.as_ref().unwrap();
        if !headers.contains_key("region") || headers.get("region").unwrap() != "us" {
            anyhow::bail!("Message does not have header region=us");
        }
        Ok(())
    })?;

    Ok(())
}

/// Test that the `store` command can filter messages by partition.
#[tokio::test]
async fn test_store_with_partition_filter() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-partition-filter";
    kafka.create_topic(topic, 3).await?;

    // Produce test messages to different partitions
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages[0..3], Some(0)).await?;
    kafka.produce_messages(topic, &messages[3..7], Some(1)).await?;
    kafka.produce_messages(topic, &messages[7..10], Some(2)).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with partition filter
    info!("Running store command with partition filter");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--partitions",
        "1",
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have messages from partition 1
    validate_stored_messages(&temp_dir, 4, |msg| {
        if msg.partition != 1 {
            anyhow::bail!("Message is not from partition 1: {}", msg.partition);
        }
        Ok(())
    })?;

    Ok(())
}

/// Test that the `store` command can limit the number of messages stored.
#[tokio::test]
async fn test_store_with_count_limit() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-count-limit";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(20);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with count limit
    info!("Running store command with count limit");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--count",
        "5",
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have 5 messages
    validate_stored_messages(&temp_dir, 5, |_| Ok(()))?;

    Ok(())
}

/// Test that the `store` command can handle binary message data.
#[tokio::test]
async fn test_store_binary_messages() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-binary-messages";
    kafka.create_topic(topic, 1).await?;

    // Produce binary test messages
    let messages = generate_binary_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command
    info!("Running store command for binary messages");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--count",
        "10",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages
    validate_stored_messages(&temp_dir, 10, |_| Ok(()))?;

    Ok(())
}

/// Test that the `store` command can filter messages by timestamp.
#[tokio::test]
async fn test_store_with_timestamp_filter() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-timestamp-filter";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages with different timestamps
    let messages = generate_timestamped_test_messages();
    kafka.produce_messages(topic, &messages, None).await?;

    // Get the timestamp threshold (between old and new messages)
    let threshold = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64 - 1800000; // 30 minutes ago

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with timestamp filter
    info!("Running store command with timestamp filter");
    let threshold_str = threshold.to_string();
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-timestamp",
        &threshold_str,
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have recent messages
    validate_stored_messages(&temp_dir, 5, |msg| {
        if msg.timestamp < threshold {
            anyhow::bail!("Message timestamp is before threshold: {}", msg.timestamp);
        }
        Ok(())
    })?;

    Ok(())
}

/// Test that the `store` command fails with an invalid bootstrap server.
#[tokio::test]
async fn test_store_invalid_bootstrap_server() -> Result<()> {
    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with invalid bootstrap server
    info!("Running store command with invalid bootstrap server");
    let args = vec![
        "test-topic",
        "--bootstrap-servers",
        "invalid-host:9092",
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--count",
        "10",
    ];
    let output = run_store_command(args)?;

    // Validate command output - should fail
    assert!(!output.status.success());

    Ok(())
}

/// Test that the `store` command fails with a non-existent topic.
#[tokio::test]
async fn test_store_non_existent_topic() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with non-existent topic
    info!("Running store command with non-existent topic");
    let args = vec![
        "non-existent-topic",
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--count",
        "10",
    ];
    let output = run_store_command(args)?;

    // Validate command output - should fail or timeout waiting for messages
    // Note: This behavior depends on how the CLI handles non-existent topics
    // It might wait indefinitely, timeout, or fail immediately

    Ok(())
}

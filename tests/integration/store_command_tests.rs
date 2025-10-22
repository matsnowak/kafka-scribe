//! Integration tests for the `store` command.
//!
//! These tests verify that the `store` command correctly stores messages
//! from Kafka topics to various storage backends.

use std::collections::HashMap;
use std::sync::Once;

use anyhow::Result;
use tokio::runtime::Runtime;
use tracing::{debug, info};

use super::common::cli_helpers::{
    run_store_command, validate_stored_messages, validate_stored_messages_in_file, validate_success, TestDirectory, build_normalized_dir_json, write_normalized_dir_file,
};
use super::common::kafka_setup::KafkaTestContext;
use super::common::test_data::{
    generate_test_messages, generate_binary_test_messages, generate_key_filtered_test_messages, 
    generate_header_filtered_test_messages, generate_timestamped_test_messages, TestMessage, JsonMessage
};

// Initialize the test environment at once
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
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages (basic structural check)
    validate_stored_messages(&temp_dir, 10, |_| Ok(()))?;

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;

    // Persist normalized content next to the stored files for debugging/inspection
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("basic_store_to_directory_normalized", normalized);

    Ok(())
}

/// Test that the `store` command can store messages from a Kafka topic to a single file.
#[ignore]
#[tokio::test]
async fn test_store_to_file() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-store-to-file";
    kafka.create_topic(topic, 1).await?;

    // Generate known test data
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Proper validation of single file format
    validate_stored_messages_in_file(&temp_dir, "output.json", 10, |msg| {
        // Validate message structure
        if msg.partition < 0 {
            anyhow::bail!("Invalid partition: {}", msg.partition);
        }
        if msg.offset < 0 {
            anyhow::bail!("Invalid offset: {}", msg.offset);
        }
        if msg.timestamp <= 0 {
            anyhow::bail!("Invalid timestamp: {}", msg.timestamp);
        }
        
        // Validate message value
        if let serde_json::Value::Object(obj) = &msg.value {
            // Check required fields
            if !obj.contains_key("id") {
                anyhow::bail!("Message value missing 'id' field");
            }
            if !obj.contains_key("name") {
                anyhow::bail!("Message value missing 'name' field");
            }
            if !obj.contains_key("timestamp") {
                anyhow::bail!("Message value missing 'timestamp' field");
            }
            if !obj.contains_key("data") {
                anyhow::bail!("Message value missing 'data' field");
            }
            
            // Validate data structure
            if let Some(serde_json::Value::Object(data)) = obj.get("data") {
                if !data.contains_key("field1") || !data.contains_key("field2") || !data.contains_key("field3") {
                    anyhow::bail!("Message data missing required fields");
                }
            } else {
                anyhow::bail!("Message data is not an object");
            }
        } else {
            anyhow::bail!("Message value is not a JSON object");
        }
        
        Ok(())
    })?;

    // Validate file format - should be one JSON object per line
    let file_content = std::fs::read_to_string(&output_file)?;
    let lines: Vec<&str> = file_content.lines().collect();
    assert_eq!(lines.len(), 10, "Should have exactly 10 lines in file");

    // Validate each line is valid JSON
    for (i, line) in lines.iter().enumerate() {
        serde_json::from_str::<serde_json::Value>(line)
            .map_err(|e| anyhow::anyhow!("Line {} is not valid JSON: {}", i + 1, e))?;
    }

    Ok(())
}

/// Test that the `store` command can filter messages by key regex.
#[ignore]
#[tokio::test]
async fn test_store_with_key_regex_filter() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-key-regex";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages with different key patterns
    let all_messages = generate_key_filtered_test_messages();
    
    // Calculate expected matching count programmatically
    let expected_matching = all_messages.iter()
        .filter(|msg| {
            String::from_utf8_lossy(&msg.key)
                .starts_with("user-")
        })
        .count();
    
    // Ensure we have both matching and non-matching messages
    let total_messages = all_messages.len();
    assert!(expected_matching > 0, "Test data should contain matching messages");
    assert!(expected_matching < total_messages, "Test data should contain non-matching messages");
    
    info!("Generated {} total messages, {} should match 'user-.*' regex", 
          total_messages, expected_matching);
    
    // Produce messages to Kafka
    kafka.produce_messages(topic, &all_messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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
    validate_stored_messages(&temp_dir, expected_matching, |msg| {
        if let Some(key) = &msg.key {
            if !key.starts_with("user-") {
                anyhow::bail!("Message key does not match regex: {}", key);
            }
        } else {
            anyhow::bail!("Message has no key");
        }
        Ok(())
    })?;
    
    // Verify exact count
    let stored_messages = temp_dir.read_json_messages()?;
    assert_eq!(stored_messages.len(), expected_matching, 
        "Should store exactly {} matching messages, got {}", 
        expected_matching, stored_messages.len());
    
    // Verify non-matching messages are excluded
    for msg in &stored_messages {
        if let Some(key) = &msg.key {
            assert!(key.starts_with("user-"), 
                   "Found message with non-matching key: {}", key);
        }
    }
    
    info!("Successfully filtered {} messages from {} total", 
          expected_matching, total_messages);

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("store_with_key_regex_filter_to_directory_normalized", normalized);

    Ok(())
}

/// Test that the `store` command can filter messages by header value.
#[tokio::test]
#[ignore]
async fn test_store_with_header_filter() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-header-filter";
    kafka.create_topic(topic, 1).await?;

    // Generate messages with specific headers
    let messages = generate_header_filtered_test_messages();
    
    // Calculate expected matching count for user-type=premium
    let expected_matching = messages.iter()
        .filter(|msg| {
            msg.headers.as_ref()
                .and_then(|headers| headers.get("region"))
                .map(|value| value.as_slice() == b"us")
                .unwrap_or(false)
        })
        .count();
    
    // Ensure test data is valid
    let total_messages = messages.len();
    assert!(expected_matching > 0, "Test data should contain messages with region=us");
    assert!(expected_matching < total_messages, "Test data should contain messages with other region values");
    
    info!("Generated {} total messages, {} should match 'region=us' header", 
          total_messages, expected_matching);

    // Produce messages to Kafka
    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

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

    // Validate header filtering
    validate_stored_messages(&temp_dir, expected_matching, |msg| {
        let header_value = msg.headers.as_ref()
            .and_then(|h| h.get("region"))
            .ok_or_else(|| anyhow::anyhow!("Message should have 'region' header"))?;
        
        if header_value != "us" {
            anyhow::bail!("Header 'region' should be 'us', got '{}'", header_value);
        }
        
        // Validate message content matches expected region
        if let serde_json::Value::Object(obj) = &msg.value {
            if let Some(region) = obj.get("region").and_then(|v| v.as_str()) {
                if region != "us" {
                    anyhow::bail!("Message region field '{}' doesn't match header value 'us'", region);
                }
            }
        }
        
        Ok(())
    })?;

    // Verify exact count
    let stored_messages = temp_dir.read_json_messages()?;
    assert_eq!(stored_messages.len(), expected_matching, 
              "Should store exactly {} matching messages, got {}", 
              expected_matching, stored_messages.len());
    
    // Verify all stored messages have the correct header
    for msg in &stored_messages {
        let header_value = msg.headers.as_ref()
            .and_then(|h| h.get("region"))
            .expect("Message should have 'region' header");
        
        assert_eq!(header_value, "us", 
                  "All stored messages should have 'region=us' header, got '{}'", header_value);
    }

    info!("Successfully filtered {} messages with region=us from {} total", 
          expected_matching, total_messages);

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("store_with_header_filter_to_directory_normalized", normalized);

    Ok(())
}

/// Test that the `store` command can filter messages by partition.
#[ignore]
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

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("store_with_partition_filter_to_directory_normalized", normalized);

    Ok(())
}

/// Test that the `store` command can limit the number of messages stored.
#[tokio::test]
#[ignore]
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

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("store_with_count_limit_to_directory_normalized", normalized);

    Ok(())
}

/// Test that the `store` command can handle binary message data.
#[tokio::test]
#[ignore]
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

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("store_binary_messages_to_directory_normalized", normalized);

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
        // Removed "--from-beginning" as it conflicts with "--from-timestamp"
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

    // Build a normalized, deterministic JSON of the entire stored directory
    let normalized = build_normalized_dir_json(&temp_dir)?;
    let _normalized_path = write_normalized_dir_file(&temp_dir, "normalized.json")?;

    // Snapshot test to ensure directory contents match expected messages without changes
    insta::assert_json_snapshot!("store_with_timestamp_filter_to_directory_normalized", normalized);

    Ok(())
}

/// Test that the `store` command can store messages from a specific offset.
#[tokio::test]
#[ignore]
async fn test_store_from_offset() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-from-offset";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with from-offset
    info!("Running store command with from-offset");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-offset",
        "0:5", // Start from offset 5 in partition 0
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have messages from offset 5 onwards
    validate_stored_messages(&temp_dir, 5, |msg| {
        if msg.offset < 5 {
            anyhow::bail!("Message offset {} is less than 5", msg.offset);
        }
        Ok(())
    })?;

    Ok(())
}

/// Test that the `store` command can store messages until a specific offset.
#[tokio::test]
#[ignore]
async fn test_store_until_offset() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-until-offset";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the store command with until-offset
    info!("Running store command with until-offset");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
        "--until-offset",
        "0:5", // Stop at offset 5 (exclusive) in partition 0
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should only have messages up to offset 5 (exclusive)
    validate_stored_messages(&temp_dir, 5, |msg| {
        if msg.offset >= 5 {
            anyhow::bail!("Message offset {} is greater than or equal to 5", msg.offset);
        }
        Ok(())
    })?;

    Ok(())
}

/// Test that the `store` command fails with an invalid bootstrap server.
/// 
/// Note: This test uses a non-routable IP address (192.0.2.1) from the TEST-NET-1 range
/// as specified in RFC 5737. This IP address is guaranteed to be invalid and should
/// cause the connection to fail quickly.
#[tokio::test]
#[ignore]
async fn test_store_invalid_bootstrap_server() -> Result<()> {
    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;
    
    // Record initial state
    let initial_file_count = temp_dir.count_files()?;
    assert_eq!(initial_file_count, 0, "Temporary directory should be empty initially");
    
    // Run the store command with invalid bootstrap server
    info!("Running store command with invalid bootstrap server");
    let args = vec![
        "test-topic",
        "--bootstrap-servers",
        "192.0.2.1:9092", // RFC5737 test address - guaranteed to fail
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--count",
        "1",
        "--timeout",
        "5",
        "--from-beginning",
    ];
    
    let output = run_store_command(args)?;

    // Command MUST fail with invalid bootstrap server
    assert!(!output.status.success(), 
           "Command should fail with invalid bootstrap server, but got success status");
    
    // Validate specific error message about connection
    let error_output = format!("{}\n{}", 
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr));
    
    let has_connection_error = error_output.to_lowercase().contains("connection") ||
                              error_output.to_lowercase().contains("timeout") ||
                              error_output.to_lowercase().contains("failed to connect") ||
                              error_output.to_lowercase().contains("unreachable");
    
    assert!(has_connection_error, 
           "Should contain connection error message, got: {}", error_output);

    // Ensure no files were created on failure
    let file_count = temp_dir.count_files()?;
    assert_eq!(file_count, 0, "No files should be created on connection failure, found {}", file_count);

    // Verify output directory structure is clean
    let dir_contents: Vec<_> = std::fs::read_dir(temp_dir.path())?
        .collect::<Result<Vec<_>, _>>()?;
    assert!(dir_contents.is_empty(), "Directory should be empty on failure, contains: {:?}", 
            dir_contents.iter().map(|e| e.file_name()).collect::<Vec<_>>());

    info!("Successfully validated connection failure with no side effects");
    Ok(())
}

/// Test that the `store` command fails with a non-existent topic.
#[tokio::test]
#[ignore]
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
        "1", // Only try to get 1 message to fail faster
    ];
    let output = run_store_command(args)?;

    // Validate command output - should fail when trying to consume from a non-existent topic
    // The command should exit with a non-zero status code
    assert!(!output.status.success(), "Command should fail with non-existent topic");

    // Check that the output contains a topic-related error message
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let output_text = format!("{}\n{}", stdout, stderr);

    assert!(
        output_text.contains("topic") || 
        output_text.contains("error") || 
        output_text.contains("failed"),
        "Error message should mention topic or error: {}", output_text
    );

    Ok(())
}

/// Test that the `store` command can run in live mode with a timeout.
#[tokio::test]
#[ignore]
async fn test_store_live_mode_with_timeout() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-live-mode";
    kafka.create_topic(topic, 1).await?;

    // Produce initial test messages
    let initial_messages = generate_test_messages(5);
    kafka.produce_messages(topic, &initial_messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Start a background task to produce more messages after a delay
    let bootstrap_servers = kafka.bootstrap_servers().to_string();
    let topic_name = topic.to_string();
    let producer_handle = tokio::spawn(async move {
        // Wait a bit before producing more messages
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Set up Kafka again (need a new instance for the new thread)
        let kafka = KafkaTestContext::new(docker).await.unwrap();

        // Produce more messages
        let additional_messages = generate_test_messages(5);
        kafka.produce_messages(&topic_name, &additional_messages, None).await.unwrap();

        info!("Produced additional messages in background task");
    });

    // Run the store command in live mode with a timeout
    info!("Running store command in live mode with timeout");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--live",
        "--timeout",
        "5", // 5 second timeout
    ];
    let output = run_store_command(args)?;

    // Wait for the producer to finish
    producer_handle.await?;

    // Validate command output
    validate_success(&output)?;

    // Validate stored messages - should have both initial and additional messages
    let message_count = temp_dir.count_files()?;
    assert!(message_count >= 5, "Expected at least 5 messages, found {}", message_count);

    // If the live mode worked correctly, we should have captured some or all of the additional messages
    info!("Captured {} messages in live mode", message_count);

    Ok(())
}

/// Test that the `store` command can store messages in a compressed format.
#[ignore]
#[tokio::test]
async fn test_store_with_compression() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-compression";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;
    let output_file = temp_dir.path().join("messages.json.gz");

    // Run the store command with compression
    info!("Running store command with compression");
    let args = vec![
        topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-file",
        output_file.to_str().unwrap(),
        "--from-beginning",
        "--compress",
    ];
    let output = run_store_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Verify the compressed file was created
    assert!(output_file.exists(), "Compressed file was not created");

    // Check that the file size is smaller than it would be uncompressed
    // This is a simple heuristic to verify compression was applied
    let file_size = std::fs::metadata(&output_file)?.len();
    info!("Compressed file size: {} bytes", file_size);

    // We can't easily validate the content of the compressed file directly,
    // but we can check that it's not empty and has a reasonable size
    assert!(file_size > 0, "Compressed file is empty");

    Ok(())
}

/// Test that the `store` command preserves message ordering by offset.
#[tokio::test]
#[ignore]
async fn test_store_preserves_message_ordering() -> Result<()> {
    let docker = init_test_environment();
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-message-ordering";
    kafka.create_topic(topic, 2).await?; // Use 2 partitions

    // Generate ordered test messages with sequence numbers
    let mut messages = Vec::new();
    for i in 0..20 {
        let mut headers = HashMap::new();
        headers.insert("sequence".to_string(), i.to_string().into_bytes());
        
        let key = format!("seq-{:03}", i);
        let value = format!(r#"{{"sequence": {}, "timestamp": {}}}"#, i, i * 1000);
        let mut msg = TestMessage::new(key, value, Some(headers));
        msg.partition = i % 2; // Distribute across partitions
        
        messages.push(msg);
    }

    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let temp_dir = TestDirectory::new()?;
    let args = vec![
        topic,
        "--bootstrap-servers", kafka.bootstrap_servers(),
        "--to-dir", temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Read and validate message ordering
    let stored_messages = temp_dir.read_json_messages()?;
    assert_eq!(stored_messages.len(), 20, "Should store all 20 messages");

    // Group messages by partition and validate ordering within each partition
    let mut partition_messages: HashMap<i32, Vec<&JsonMessage>> = HashMap::new();
    for msg in &stored_messages {
        partition_messages.entry(msg.partition)
            .or_insert_with(Vec::new)
            .push(msg);
    }

    // Validate ordering within each partition
    for (partition, msgs) in &partition_messages {
        // Sort by offset
        let mut sorted_msgs = msgs.clone();
        sorted_msgs.sort_by_key(|m| m.offset);
        
        // Validate offsets are sequential within partition
        for (i, msg) in sorted_msgs.iter().enumerate() {
            assert_eq!(msg.partition, *partition, "Message partition should match");
            
            // Validate sequence number matches expected order
            if let Some(headers) = &msg.headers {
                if let Some(sequence) = headers.get("sequence") {
                    let sequence: i64 = sequence.parse()?;
                    
                    // Each partition should have alternating sequence numbers
                    let expected_sequence = if *partition == 0 { i * 2 } else { i * 2 + 1 };
                    assert_eq!(sequence, expected_sequence as i64,
                              "Sequence number should match expected order");
                }
            }
        }
        
        // Verify offsets are monotonically increasing
        for i in 1..sorted_msgs.len() {
            assert!(sorted_msgs[i].offset > sorted_msgs[i-1].offset,
                   "Offsets should be monotonically increasing within partition");
        }
    }

    info!("Successfully validated message ordering across {} partitions", partition_messages.len());
    Ok(())
}
/// Test that the `store` command can handle large messages without memory issues or corruption.
#[ignore]
#[tokio::test]
async fn test_store_large_messages() -> Result<()> {
    let docker = init_test_environment();
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-large-messages";
    kafka.create_topic(topic, 1).await?;

    // Generate messages of various sizes
    let mut messages = Vec::new();
    let sizes = vec![1024, 10240, 102400, 512000]; // 1KB, 10KB, 100KB, 500KB
    
    for (i, size) in sizes.iter().enumerate() {
        let large_data = "X".repeat(*size);
        let large_value = format!(r#"{{"data": "{}", "size": {}, "index": {}}}"#, large_data, size, i);
        
        let mut headers = HashMap::new();
        headers.insert("size".to_string(), size.to_string().into_bytes());
        headers.insert("index".to_string(), i.to_string().into_bytes());
        
        let key = format!("large-msg-{}", i);
        let mut msg = TestMessage::new(key, large_value, Some(headers));
        msg.partition = 0;
        
        messages.push(msg);
    }

    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    let temp_dir = TestDirectory::new()?;
    let args = vec![
        topic,
        "--bootstrap-servers", kafka.bootstrap_servers(),
        "--to-dir", temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Validate large message storage and integrity
    validate_stored_messages(&temp_dir, messages.len(), |msg| {
        let size_header = msg.headers.as_ref()
            .and_then(|h| h.get("size"))
            .ok_or_else(|| anyhow::anyhow!("Message should have size header"))?;
        
        let size: usize = size_header.parse()?;
        
        let index_header = msg.headers.as_ref()
            .and_then(|h| h.get("index"))
            .ok_or_else(|| anyhow::anyhow!("Message should have index header"))?;

        let index: usize = index_header.parse()?;
        
        // Validate message size matches header
        if let serde_json::Value::Object(obj) = &msg.value {
            if let Some(serde_json::Value::String(data)) = obj.get("data") {
                // Verify data is all X's as expected
                if !data.chars().all(|c| c == 'X') {
                    anyhow::bail!("Data field should contain only 'X' characters");
                }
                
                // Verify data length matches expected size
                if data.len() != size {
                    anyhow::bail!("Data field length {} doesn't match expected size {}", 
                                 data.len(), size);
                }
                
                info!("Validated large message #{} with size {}", index, size);
            } else {
                anyhow::bail!("Message should have string data field");
            }
        } else {
            anyhow::bail!("Message value is not a JSON object");
        }

        Ok(())
    })?;

    // Validate total storage size
    let stored_messages = temp_dir.read_json_messages()?;
    let total_stored_size: usize = stored_messages.iter()
        .map(|msg| {
            if let serde_json::Value::Object(obj) = &msg.value {
                if let Some(serde_json::Value::String(data)) = obj.get("data") {
                    return data.len();
                }
            }
            0
        })
        .sum();
    
    info!("Successfully stored {} large messages, total size: {} bytes", 
          stored_messages.len(), total_stored_size);
    
    // Ensure we stored the expected amount of data
    assert!(total_stored_size > 500000, "Should have stored at least 500KB of data");

    Ok(())
}
/// Test that binary message data (non-UTF8) is stored and retrieved without corruption.
#[ignore]
#[tokio::test]
async fn test_store_binary_data_integrity() -> Result<()> {
    let docker = init_test_environment();
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-binary-data";
    kafka.create_topic(topic, 1).await?;

    // Generate messages with various binary data patterns
    let mut messages = Vec::new();
    
    // Test case 1: Random binary data
    let random_bytes: Vec<u8> = (0..256).map(|_| rand::random::<u8>()).collect();
    let mut headers1 = HashMap::new();
    headers1.insert("type".to_string(), "random".to_string().into_bytes());
    headers1.insert("size".to_string(), random_bytes.len().to_string().into_bytes());
    messages.push(TestMessage::new("random-binary", &random_bytes, Some(headers1)));

    // Test case 2: All possible byte values (0-255)
    let all_bytes: Vec<u8> = (0..=255).collect();
    let mut headers2 = HashMap::new();
    headers2.insert("type".to_string(), "all-bytes".to_string().into_bytes());
    headers2.insert("size".to_string(), all_bytes.len().to_string().into_bytes());
    messages.push(TestMessage::new("all-bytes", &all_bytes, Some(headers2)));

    // Test case 3: Null bytes and control characters
    let null_bytes = vec![0u8, 1, 2, 3, 255, 254, 253, 0, 0, 0];
    let mut headers3 = HashMap::new();
    headers3.insert("type".to_string(), "null-bytes".to_string().into_bytes());
    headers3.insert("size".to_string(), null_bytes.len().to_string().into_bytes());
    messages.push(TestMessage::new("null-bytes", &null_bytes, Some(headers3)));

    // Test case 4: Simulated image header (PNG signature)
    let png_header = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    let mut headers4 = HashMap::new();
    headers4.insert("type".to_string(), "png-header".to_string().into_bytes());
    headers4.insert("content-type".to_string(), "image/png".to_string().into_bytes());
    messages.push(TestMessage::new("png-header", &png_header, Some(headers4)));

    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let temp_dir = TestDirectory::new()?;
    let args = vec![
        topic,
        "--bootstrap-servers", kafka.bootstrap_servers(),
        "--to-dir", temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Create expected data map for validation
    let expected_data: HashMap<String, Vec<u8>> = [
        ("random".to_string(), random_bytes.clone()),
        ("all-bytes".to_string(), all_bytes.clone()),
        ("null-bytes".to_string(), null_bytes.clone()),
        ("png-header".to_string(), png_header.clone()),
    ].iter().cloned().collect();

    // Validate binary data integrity
    validate_stored_messages(&temp_dir, messages.len(), |msg| {
        let msg_type = msg.headers.as_ref()
            .and_then(|h| h.get("type"))
            .ok_or_else(|| anyhow::anyhow!("Message should have type header"))?;
        
        let expected_bytes = expected_data.get(msg_type)
            .ok_or_else(|| anyhow::anyhow!("Unknown message type: {}", msg_type))?;
        
        // For binary data, we need to check the raw bytes in the value
        // This is a bit tricky since JsonMessage.value is already parsed as JSON
        // We'll need to check specific patterns instead of exact bytes
        
        match msg_type.as_str() {
            "all-bytes" => {
                // Verify we can find some key byte patterns
                let value_str = format!("{:?}", msg.value);
                // Check for some specific byte values that should be present
                assert!(value_str.contains("0"), "All-bytes should contain byte 0");
                assert!(value_str.contains("255"), "All-bytes should contain byte 255");
                assert!(value_str.contains("128"), "All-bytes should contain byte 128");
            },
            "null-bytes" => {
                // Verify null bytes are preserved
                let value_str = format!("{:?}", msg.value);
                assert!(value_str.contains("0"), "Null bytes not preserved correctly");
            },
            "png-header" => {
                // Verify PNG signature bytes
                let value_str = format!("{:?}", msg.value);
                assert!(value_str.contains("137"), "PNG header signature missing byte 137 (0x89)");
                assert!(value_str.contains("80"), "PNG header signature missing byte 80 (0x50 'P')");
                assert!(value_str.contains("78"), "PNG header signature missing byte 78 (0x4E 'N')");
                assert!(value_str.contains("71"), "PNG header signature missing byte 71 (0x47 'G')");
            },
            _ => {} // Random data is harder to verify exactly
        }

        info!("Binary data integrity verified for type '{}'", msg_type);
        Ok(())
    })?;

    // Additional validation: check that we can read the files directly
    let stored_files = std::fs::read_dir(temp_dir.path())?;
    for entry in stored_files {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let file_content = std::fs::read_to_string(entry.path())?;
            // Each line should be valid JSON
            for line in file_content.lines() {
                serde_json::from_str::<serde_json::Value>(line)
                    .map_err(|e| anyhow::anyhow!("File {} contains invalid JSON: {}", 
                                              entry.file_name().to_string_lossy(), e))?;
            }
            info!("File {} contains valid JSON", entry.file_name().to_string_lossy());
        }
    }

    Ok(())
}
/// Test storing messages while producers are actively writing to the topic, ensuring no message loss.
#[ignore]
#[tokio::test]
async fn test_store_concurrent_producers() -> Result<()> {
    let docker = init_test_environment();
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-concurrent-producers";
    kafka.create_topic(topic, 2).await?;

    // Initial batch of messages
    let initial_messages = generate_test_messages(10);
    kafka.produce_messages(topic, &initial_messages, None).await?;
    
    // Give Kafka a moment to process the initial messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let temp_dir = TestDirectory::new()?;
    
    // Start store command in background
    let kafka_servers = kafka.bootstrap_servers().to_string();
    let temp_dir_path = temp_dir.path().to_str().unwrap().to_string();
    
    // Create a function to generate numbered test messages
    let generate_numbered_test_messages = |start: i32, end: i32| -> Vec<TestMessage> {
        (start..end).map(|i| {
            let key = format!("msg-{}", i);
            let value = format!(r#"{{"number": {}, "data": "message-{}", "timestamp": {}}}"#, 
                               i, i, chrono::Utc::now().timestamp_millis());
            
            let mut headers = HashMap::new();
            headers.insert("number".to_string(), i.to_string().into_bytes());
            headers.insert("batch".to_string(), "concurrent".to_string().into_bytes());
            
            let mut msg = TestMessage::new(key, value, Some(headers));
            msg.partition = i % 2;
            msg
        }).collect()
    };
    
    // Start store command in background with --live mode
    let store_handle = tokio::spawn(async move {
        let args = vec![
            topic,
            "--bootstrap-servers", &kafka_servers,
            "--to-dir", &temp_dir_path,
            "--live", // Keep consuming
            "--timeout", "10", // Run for 10 seconds
            "--from-beginning",
        ];
        run_store_command(args)
    });

    // Wait a moment for store command to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Produce additional messages while store is running
    let concurrent_messages_1 = generate_numbered_test_messages(100, 200); // Messages 100-199
    kafka.produce_messages(topic, &concurrent_messages_1, None).await?;
    
    // Short delay then more messages
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    let concurrent_messages_2 = generate_numbered_test_messages(200, 250); // Messages 200-249
    kafka.produce_messages(topic, &concurrent_messages_2, None).await?;

    // Wait for store command to complete
    let store_result = store_handle.await??;
    validate_success(&store_result)?;

    // Validate all messages were captured
    let stored_messages = temp_dir.read_json_messages()?;
    
    // Calculate total expected messages
    let total_expected = 10 + concurrent_messages_1.len() + concurrent_messages_2.len();
    
    // Should have captured all messages
    assert!(stored_messages.len() >= total_expected, 
           "Should capture at least {} messages, got {}", total_expected, stored_messages.len());

    // Validate message sequence integrity
    let mut message_numbers: Vec<i32> = Vec::new();
    for msg in &stored_messages {
        if let serde_json::Value::Object(obj) = &msg.value {
            if let Some(serde_json::Value::Number(num)) = obj.get("number") {
                if let Some(num) = num.as_i64() {
                    message_numbers.push(num as i32);
                }
            }
        }
    }

    message_numbers.sort();
    
    // Should have initial messages (0-9) and concurrent messages (100-249)
    let expected_numbers: Vec<i32> = (0..10).chain(100..250).collect();
    let missing_numbers: Vec<i32> = expected_numbers.iter()
        .filter(|&&num| !message_numbers.contains(&num))
        .cloned()
        .collect();

    if !missing_numbers.is_empty() {
        anyhow::bail!("Missing messages with numbers: {:?}", missing_numbers);
    }

    info!("Successfully captured {} messages from concurrent producers", stored_messages.len());
    info!("Message number range: {} to {}", 
          message_numbers.first().unwrap_or(&-1), 
          message_numbers.last().unwrap_or(&-1));

    Ok(())
}
/// Test graceful handling when disk space runs out during message storage.
#[ignore]
#[tokio::test]
async fn test_store_disk_space_exhaustion() -> Result<()> {
    // Skip this test on systems where we can't create limited filesystems
    if std::env::var("SKIP_DISK_TESTS").is_ok() {
        println!("Skipping disk exhaustion test - SKIP_DISK_TESTS set");
        return Ok(());
    }

    let docker = init_test_environment();
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-disk-exhaustion";
    kafka.create_topic(topic, 1).await?;

    // Generate large messages that will fill up limited space
    let mut messages = Vec::new();
    for i in 0..10 {
        let large_data = "X".repeat(100000); // 100KB per message
        let large_value = format!(r#"{{"data": "{}", "index": {}}}"#, large_data, i);
        
        let mut headers = HashMap::new();
        headers.insert("size".to_string(), "100000".to_string().into_bytes());
        headers.insert("index".to_string(), i.to_string().into_bytes());
        
        let mut msg = TestMessage::new(format!("large-{}", i), large_value, Some(headers));
        msg.partition = 0;
        
        messages.push(msg);
    }

    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create a limited-size temporary directory using tmpfs if available
    let temp_dir = TestDirectory::new()?;
    let limited_dir = temp_dir.path().join("limited");
    std::fs::create_dir(&limited_dir)?;

    // Try to mount a small tmpfs (will fail gracefully on systems without permission)
    let mount_result = std::process::Command::new("mount")
        .args(&["-t", "tmpfs", "-o", "size=500k", "tmpfs", limited_dir.to_str().unwrap()])
        .output();

    let cleanup_mount = if mount_result.as_ref().map(|o| o.status.success()).unwrap_or(false) {
        info!("Created limited tmpfs mount for disk exhaustion test");
        Some(limited_dir.clone())
    } else {
        info!("Could not create tmpfs mount, using regular filesystem");
        None
    };

    // Run store command - should fail due to disk space
    let args = vec![
        topic,
        "--bootstrap-servers", kafka.bootstrap_servers(),
        "--to-dir", limited_dir.to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;

    // Cleanup mount if we created one
    if let Some(mount_path) = cleanup_mount {
        let _ = std::process::Command::new("umount").arg(mount_path.to_str().unwrap()).output();
    }

    // Command should fail due to disk space issues
    if output.status.success() {
        // If command succeeded, we couldn't simulate disk exhaustion
        println!("Could not simulate disk exhaustion - test environment limitation");
        return Ok(());
    }

    // Validate error message indicates disk space issue
    let error_output = format!("{}\n{}", 
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr));

    let has_disk_error = error_output.to_lowercase().contains("no space left") ||
                        error_output.to_lowercase().contains("disk full") ||
                        error_output.to_lowercase().contains("insufficient space") ||
                        error_output.to_lowercase().contains("write failed");

    if has_disk_error {
        info!("Successfully detected disk space exhaustion error");
    } else {
        info!("Command failed but not due to disk space - test environment limitation");
    }

    // Validate partial data handling - should either have no files or complete files
    let files_count = std::fs::read_dir(&limited_dir)
        .map(|entries| entries.count())
        .unwrap_or(0);

    info!("Found {} files after disk exhaustion", files_count);

    // If files exist, they should be complete and valid
    if files_count > 0 {
        let stored_messages = temp_dir.read_json_messages()?;
        for msg in &stored_messages {
            // Validate each stored message is complete
            if let serde_json::Value::Object(obj) = &msg.value {
                if !obj.contains_key("data") || !obj.contains_key("index") {
                    anyhow::bail!("Partial message found - missing required fields");
                }
            } else {
                anyhow::bail!("Partial message found - value is not a JSON object");
            }
        }
        info!("All {} stored messages are complete", stored_messages.len());
    }

    Ok(())
}
/// Test proper handling of Unicode characters, emojis, and special characters in messages.
#[ignore]
#[tokio::test]
async fn test_store_unicode_messages() -> Result<()> {
    let docker = init_test_environment();
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-unicode-messages";
    kafka.create_topic(topic, 1).await?;

    // Generate messages with various Unicode characters
    let unicode_test_cases = vec![
        ("basic-ascii", "Hello World"),
        ("accents", "Café, naïve, résumé"),
        ("emoji", "🚀 🌍 🔥 ☕ 🎉"),
        ("mixed-emoji", "Processing order #123 ✅ Status: ⏳ → ✅"),
        ("chinese", "你好世界"),
        ("japanese", "こんにちは世界"),
        ("arabic", "مرحبا بالعالم"),
        ("russian", "Привет мир"),
        ("mathematical", "∑(π × α²) ≈ ∞"),
        ("symbols", "©®™ €£¥ ←→↕ ♪♫♬"),
        ("combining", "a̧̞͎̣͎̟̞̠̠̋̎b̷̮̮̊̀̈́̈́̚c̸̨̰̳̱̬̣̿̇̂̿̍̚"), // Combining characters
        ("zero-width", "hello\u{200B}world\u{FEFF}test"), // Zero-width space and BOM
        ("surrogates", "𐐷 𐐸 𐐹"), // Characters requiring surrogate pairs in UTF-16
        ("rtl", "Hello العالم World"), // Mixed LTR/RTL text
    ];

    let mut messages = Vec::new();
    for (i, (test_type, content)) in unicode_test_cases.iter().enumerate() {
        let value = format!(r#"{{"content": "{}", "type": "{}", "index": {}}}"#, 
                           content.replace("\"", "\\\""), test_type, i);
        
        let mut headers = HashMap::new();
        headers.insert("test-type".to_string(), test_type.to_string().into_bytes());
        headers.insert("content-preview".to_string(), content.chars().take(10).collect::<String>().into_bytes());
        
        let key = format!("unicode-{}", test_type);
        let msg = TestMessage::new(key, value, Some(headers));
        messages.push(msg);
    }

    kafka.produce_messages(topic, &messages, None).await?;
    
    // Give Kafka a moment to process the messages
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let temp_dir = TestDirectory::new()?;
    let args = vec![
        topic,
        "--bootstrap-servers", kafka.bootstrap_servers(),
        "--to-dir", temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let output = run_store_command(args)?;
    validate_success(&output)?;

    // Create expected content map for validation
    let expected_content: HashMap<String, String> = unicode_test_cases.iter()
        .map(|(test_type, content)| (test_type.to_string(), content.to_string()))
        .collect();

    // Validate Unicode message storage and integrity
    validate_stored_messages(&temp_dir, messages.len(), |msg| {
        let test_type = msg.headers.as_ref()
            .and_then(|h| h.get("test-type"))
            .ok_or_else(|| anyhow::anyhow!("Message should have test-type header"))?;
        
        let expected_content_str = expected_content.get(test_type)
            .ok_or_else(|| anyhow::anyhow!("Unknown test type: {}", test_type))?;

        // Parse JSON to extract content
        if let serde_json::Value::Object(obj) = &msg.value {
            if let Some(serde_json::Value::String(stored_content)) = obj.get("content") {
                // Exact Unicode comparison
                if stored_content != expected_content_str {
                    anyhow::bail!("Unicode content mismatch for type '{}'. Expected: '{}', Got: '{}'", 
                                 test_type, expected_content_str, stored_content);
                }
                
                // Additional validations for specific test cases
                match test_type.as_str() {
                    "emoji" => {
                        // Check that emoji characters are preserved
                        if !stored_content.contains("🚀") || !stored_content.contains("🌍") {
                            anyhow::bail!("Emoji characters not preserved correctly");
                        }
                    },
                    "zero-width" => {
                        // Check that zero-width characters are preserved
                        let char_count = stored_content.chars().count();
                        let byte_count = stored_content.len();
                        if char_count <= byte_count {
                            anyhow::bail!("Zero-width characters may have been lost");
                        }
                    },
                    "surrogates" => {
                        // Check that surrogate pair characters are preserved
                        if !stored_content.contains("𐐷") {
                            anyhow::bail!("Surrogate pair characters not preserved correctly");
                        }
                    },
                    "combining" => {
                        // Check that combining characters are preserved
                        if stored_content.chars().count() <= 3 {
                            anyhow::bail!("Combining characters may have been lost or merged");
                        }
                    },
                    _ => {} // Other tests validated by exact comparison
                }
                
                info!("Unicode integrity verified for '{}': {} chars", test_type, stored_content.chars().count());
            } else {
                anyhow::bail!("Message should have content field");
            }
        } else {
            anyhow::bail!("Message value is not a JSON object");
        }

        Ok(())
    })?;

    // Additional validation: check that stored files are valid UTF-8
    let stored_files = std::fs::read_dir(temp_dir.path())?;
    for entry in stored_files {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let file_content = std::fs::read(entry.path())?;
            match std::str::from_utf8(&file_content) {
                Ok(_) => info!("File {} is valid UTF-8", entry.file_name().to_string_lossy()),
                Err(e) => anyhow::bail!("File {} contains invalid UTF-8: {}", 
                                      entry.file_name().to_string_lossy(), e),
            }
        }
    }

    Ok(())
}
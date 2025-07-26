//! End-to-end workflow tests for kafka-scribe.
//!
//! These tests verify complete workflows that combine multiple commands,
//! ensuring that the different components of kafka-scribe work together correctly.

use std::sync::Once;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::time::timeout;
use tracing::{debug, info};

use super::common::cli_helpers::{
    run_store_command, validate_success, validate_output_contains, 
    TestDirectory, CliExecutor
};
use super::common::kafka_setup::KafkaTestContext;
use super::common::test_data::{
    generate_test_messages, generate_binary_test_messages, 
    generate_key_filtered_test_messages, TestDataGenerator
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

/// Test a complete store-to-replay pipeline
#[tokio::test]
async fn test_store_to_replay_pipeline() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let source_topic = "test-e2e-source";
    let target_topic = "test-e2e-target";
    
    // Create topics
    kafka.create_topic(source_topic, 1).await?;
    kafka.create_topic(target_topic, 1).await?;

    // Produce test messages to source topic
    let messages = generate_test_messages(10);
    kafka.produce_messages(source_topic, &messages, None).await?;

    // Create temporary directory for storage
    let temp_dir = TestDirectory::new()?;

    // Step 1: Store messages from source topic
    info!("Running store command to capture messages from source topic");
    let args = vec![
        source_topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
    ];
    let store_output = run_store_command(args)?;
    validate_success(&store_output)?;

    // Step 2: Run stats command to analyze stored messages
    info!("Running stats command to analyze stored messages");
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &[],
    ).await?;
    validate_success(&stats_output)?;
    validate_output_contains(&stats_output, "Total messages: 10")?;

    // Step 3: Replay messages to target topic
    info!("Running replay command to send messages to target topic");
    let replay_output = cli.replay(
        temp_dir.path(),
        target_topic,
        kafka.bootstrap_servers(),
        &["--add-header", "replayed=true"],
    ).await?;
    validate_success(&replay_output)?;

    // Step 4: Verify messages were replayed correctly
    let consumed_messages = kafka.consume_messages(target_topic, 10, 10).await?;
    assert_eq!(consumed_messages.len(), 10, "Expected 10 messages in target topic");

    // Verify all messages have the added header
    for message in &consumed_messages {
        assert!(message.headers.contains_key("replayed"), "Replayed message missing 'replayed' header");
        assert_eq!(message.headers.get("replayed"), Some(&"true".to_string()), 
            "Header 'replayed' has incorrect value");
    }

    Ok(())
}

/// Test filtering and transformation workflow
#[tokio::test]
async fn test_filtering_and_transformation_workflow() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let source_topic = "test-filter-source";
    let target_topic = "test-filter-target";
    
    // Create topics
    kafka.create_topic(source_topic, 1).await?;
    kafka.create_topic(target_topic, 1).await?;

    // Produce test messages with different key patterns
    let messages = generate_key_filtered_test_messages();
    kafka.produce_messages(source_topic, &messages, None).await?;

    // Create temporary directory for storage
    let temp_dir = TestDirectory::new()?;

    // Step 1: Store messages with key filter
    info!("Running store command with key filter");
    let args = vec![
        source_topic,
        "--bootstrap-servers",
        kafka.bootstrap_servers(),
        "--to-dir",
        temp_dir.path().to_str().unwrap(),
        "--from-beginning",
        "--key-regex",
        "user-.*",
    ];
    let store_output = run_store_command(args)?;
    validate_success(&store_output)?;

    // Step 2: Verify filtered storage with stats
    let cli = CliExecutor::new();
    let stats_output = cli.stats(
        temp_dir.path(),
        &[],
    ).await?;
    validate_success(&stats_output)?;
    validate_output_contains(&stats_output, "Total messages: 5")?; // Only user-* keys

    // Step 3: Create a transformation script
    let script_path = temp_dir.path().join("transform.js");
    let script_content = r#"
    function transform(message) {
        // Add a transformed flag
        message.headers["transformed"] = "true";
        
        // If the message has a value and it's JSON, modify it
        if (message.value) {
            try {
                let value = JSON.parse(message.value);
                value.processed = true;
                message.value = JSON.stringify(value);
            } catch (e) {
                // Not JSON, leave as is
            }
        }
        
        return message;
    }
    "#;
    fs::write(&script_path, script_content)?;

    // Step 4: Replay with transformation
    let replay_output = cli.replay(
        temp_dir.path(),
        target_topic,
        kafka.bootstrap_servers(),
        &[
            "--transform-script", 
            script_path.to_str().unwrap(),
        ],
    ).await?;
    validate_success(&replay_output)?;

    // Step 5: Verify transformed messages
    let consumed_messages = kafka.consume_messages(target_topic, 5, 10).await?;
    assert_eq!(consumed_messages.len(), 5, "Expected 5 messages in target topic");

    // Verify all messages have the transformed header
    for message in &consumed_messages {
        assert!(message.headers.contains_key("transformed"), 
            "Transformed message missing 'transformed' header");
        assert_eq!(message.headers.get("transformed"), Some(&"true".to_string()), 
            "Header 'transformed' has incorrect value");
    }

    Ok(())
}

/// Test workflow with different message formats
#[tokio::test]
async fn test_different_message_formats_workflow() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let json_topic = "test-format-json";
    let binary_topic = "test-format-binary";
    let target_topic = "test-format-target";
    
    // Create topics
    kafka.create_topic(json_topic, 1).await?;
    kafka.create_topic(binary_topic, 1).await?;
    kafka.create_topic(target_topic, 1).await?;

    // Create test data generator
    let mut generator = TestDataGenerator::new(42);

    // Produce JSON messages
    let json_messages = generator.generate_message_batch(5);
    kafka.produce_messages(json_topic, &json_messages, None).await?;

    // Produce binary messages
    let binary_messages = generator.generate_filterable_messages("binary", 5);
    kafka.produce_messages(binary_topic, &binary_messages, None).await?;

    // Create temporary directories for storage
    let json_dir = TestDirectory::new()?;
    let binary_dir = TestDirectory::new()?;
    let combined_dir = TestDirectory::new()?;

    // Step 1: Store JSON messages
    info!("Running store command for JSON messages");
    let cli = CliExecutor::new();
    let json_store_output = cli.store(
        json_topic,
        kafka.bootstrap_servers(),
        json_dir.path(),
        &["--from-beginning"],
    ).await?;
    validate_success(&json_store_output)?;

    // Step 2: Store binary messages
    info!("Running store command for binary messages");
    let binary_store_output = cli.store(
        binary_topic,
        kafka.bootstrap_servers(),
        binary_dir.path(),
        &["--from-beginning"],
    ).await?;
    validate_success(&binary_store_output)?;

    // Step 3: Copy all files to combined directory
    for entry in fs::read_dir(json_dir.path())? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let target_path = combined_dir.path().join(path.file_name().unwrap());
            fs::copy(&path, &target_path)?;
        }
    }
    
    for entry in fs::read_dir(binary_dir.path())? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let target_path = combined_dir.path().join(path.file_name().unwrap());
            fs::copy(&path, &target_path)?;
        }
    }

    // Step 4: Verify combined storage with stats
    let stats_output = cli.stats(
        combined_dir.path(),
        &[],
    ).await?;
    validate_success(&stats_output)?;
    validate_output_contains(&stats_output, "Total messages: 10")?; // 5 JSON + 5 binary

    // Step 5: Replay all messages to target topic
    let replay_output = cli.replay(
        combined_dir.path(),
        target_topic,
        kafka.bootstrap_servers(),
        &["--add-header", "source=combined"],
    ).await?;
    validate_success(&replay_output)?;

    // Step 6: Verify all messages were replayed
    let consumed_messages = kafka.consume_messages(target_topic, 10, 10).await?;
    assert_eq!(consumed_messages.len(), 10, "Expected 10 messages in target topic");

    // Verify all messages have the added header
    for message in &consumed_messages {
        assert!(message.headers.contains_key("source"), 
            "Replayed message missing 'source' header");
        assert_eq!(message.headers.get("source"), Some(&"combined".to_string()), 
            "Header 'source' has incorrect value");
    }

    Ok(())
}

/// Test data integrity through multiple operations
#[tokio::test]
async fn test_data_integrity_through_operations() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let source_topic = "test-integrity-source";
    let intermediate_topic = "test-integrity-intermediate";
    let final_topic = "test-integrity-final";
    
    // Create topics
    kafka.create_topic(source_topic, 1).await?;
    kafka.create_topic(intermediate_topic, 1).await?;
    kafka.create_topic(final_topic, 1).await?;

    // Generate test messages with specific content for verification
    let mut messages = Vec::new();
    for i in 0..5 {
        let key = format!("key-{}", i);
        let value = json!({
            "id": i,
            "name": format!("Test {}", i),
            "value": i * 10,
            "active": i % 2 == 0
        }).to_string();
        
        let mut message = TestMessage::new(
            key.as_bytes(),
            value.as_bytes(),
            None
        );
        message.headers.insert("original".to_string(), "true".to_string());
        messages.push(message);
    }
    
    kafka.produce_messages(source_topic, &messages, None).await?;

    // Create temporary directories for each stage
    let stage1_dir = TestDirectory::new()?;
    let stage2_dir = TestDirectory::new()?;

    // Step 1: Store original messages
    info!("Running store command for original messages");
    let cli = CliExecutor::new();
    let store1_output = cli.store(
        source_topic,
        kafka.bootstrap_servers(),
        stage1_dir.path(),
        &["--from-beginning"],
    ).await?;
    validate_success(&store1_output)?;

    // Step 2: Replay to intermediate topic with header modification
    let replay1_output = cli.replay(
        stage1_dir.path(),
        intermediate_topic,
        kafka.bootstrap_servers(),
        &[
            "--add-header", "stage=intermediate",
            "--add-header", "processed=true"
        ],
    ).await?;
    validate_success(&replay1_output)?;

    // Step 3: Store messages from intermediate topic
    let store2_output = cli.store(
        intermediate_topic,
        kafka.bootstrap_servers(),
        stage2_dir.path(),
        &["--from-beginning"],
    ).await?;
    validate_success(&store2_output)?;

    // Step 4: Replay to final topic with key modification
    let replay2_output = cli.replay(
        stage2_dir.path(),
        final_topic,
        kafka.bootstrap_servers(),
        &[
            "--add-header", "stage=final",
            "--override-key", "final-key"
        ],
    ).await?;
    validate_success(&replay2_output)?;

    // Step 5: Verify final messages
    let final_messages = kafka.consume_messages(final_topic, 5, 10).await?;
    assert_eq!(final_messages.len(), 5, "Expected 5 messages in final topic");

    // Verify data integrity and transformations
    for message in &final_messages {
        // Check key was overridden
        assert_eq!(message.key, Some("final-key".as_bytes().to_vec()), 
            "Message key wasn't overridden to 'final-key'");
        
        // Check all headers are present
        assert!(message.headers.contains_key("original"), 
            "Message missing 'original' header");
        assert!(message.headers.contains_key("stage"), 
            "Message missing 'stage' header");
        assert!(message.headers.contains_key("processed"), 
            "Message missing 'processed' header");
        
        assert_eq!(message.headers.get("original"), Some(&"true".to_string()), 
            "Header 'original' has incorrect value");
        assert_eq!(message.headers.get("stage"), Some(&"final".to_string()), 
            "Header 'stage' has incorrect value");
        assert_eq!(message.headers.get("processed"), Some(&"true".to_string()), 
            "Header 'processed' has incorrect value");
        
        // Check value is still valid JSON
        if let Some(value_bytes) = &message.value {
            let value_str = String::from_utf8_lossy(value_bytes);
            let json_result: Result<Value, _> = serde_json::from_str(&value_str);
            assert!(json_result.is_ok(), "Message value is not valid JSON: {}", value_str);
            
            // Verify JSON structure is preserved
            let json = json_result.unwrap();
            assert!(json.get("id").is_some(), "JSON missing 'id' field");
            assert!(json.get("name").is_some(), "JSON missing 'name' field");
            assert!(json.get("value").is_some(), "JSON missing 'value' field");
            assert!(json.get("active").is_some(), "JSON missing 'active' field");
        } else {
            panic!("Message has no value");
        }
    }

    Ok(())
}

// Helper struct for test messages
struct TestMessage {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: HashMap<String, String>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl TestMessage {
    fn new(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>, headers: Option<HashMap<String, Vec<u8>>>) -> Self {
        let mut header_map = HashMap::new();
        if let Some(headers) = headers {
            for (k, v) in headers {
                header_map.insert(k, String::from_utf8_lossy(&v).to_string());
            }
        }
        
        Self {
            key: Some(key.as_ref().to_vec()),
            value: Some(value.as_ref().to_vec()),
            headers: header_map,
            topic: "".to_string(),
            partition: 0,
            offset: 0,
        }
    }
}
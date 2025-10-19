//! Performance tests for kafka-scribe.
//!
//! These tests measure the performance characteristics of kafka-scribe operations,
//! including throughput, batch processing, parallel processing, and memory usage.

use std::sync::Once;
use std::time::{Duration, Instant};
use std::process::Command;
use std::path::Path;
use std::fs;
use std::thread;

use anyhow::Result;
use tokio::time::timeout;
use tracing::{debug, info};
use serde_json::Value;

use super::common::cli_helpers::{
    TestDirectory, CliExecutor, validate_success
};
use super::common::kafka_setup::KafkaTestContext;
use super::common::test_data::{
    TestDataGenerator, generate_large_test_message_set
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

/// Measure throughput for store operation with different message counts
#[tokio::test]
async fn test_store_throughput() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-store-throughput";
    
    // Create topic
    kafka.create_topic(topic, 1).await?;

    // Define message counts to test
    let message_counts = [100, 1000, 5000];
    
    for &count in &message_counts {
        // Create temporary directory for storage
        let temp_dir = TestDirectory::new()?;
        
        // Generate and produce test messages
        info!("Generating and producing {} messages", count);
        let messages = kafka.generate_and_produce(topic, count, Some(42)).await?;
        
        // Create CLI executor
        let cli = CliExecutor::new();
        
        // Measure store operation throughput
        info!("Measuring store throughput for {} messages", count);
        let start = Instant::now();
        
        let result = cli.store(
            topic,
            kafka.bootstrap_servers(),
            temp_dir.path(),
            &["--from-beginning"],
        ).await?;
        
        let elapsed = start.elapsed();
        validate_success(&result)?;
        
        // Calculate throughput
        let throughput = count as f64 / elapsed.as_secs_f64();
        info!("Store throughput for {} messages: {:.2} messages/second", count, throughput);
        
        // Verify all messages were stored
        let stored_count = temp_dir.count_files()?;
        assert_eq!(stored_count, count, "Expected {} stored files, found {}", count, stored_count);
    }
    
    Ok(())
}

/// Measure throughput for replay operation with different message counts
#[tokio::test]
async fn test_replay_throughput() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let source_topic = "test-replay-source";
    let target_topic = "test-replay-throughput";
    
    // Create topics
    kafka.create_topic(source_topic, 1).await?;
    kafka.create_topic(target_topic, 1).await?;

    // Define message counts to test
    let message_counts = [100, 1000, 5000];
    
    for &count in &message_counts {
        // Create temporary directory for storage
        let temp_dir = TestDirectory::new()?;
        
        // Generate and produce test messages
        info!("Generating and producing {} messages", count);
        let messages = kafka.generate_and_produce(source_topic, count, Some(42)).await?;
        
        // Store messages first
        let cli = CliExecutor::new();
        let store_result = cli.store(
            source_topic,
            kafka.bootstrap_servers(),
            temp_dir.path(),
            &["--from-beginning"],
        ).await?;
        
        validate_success(&store_result)?;
        
        // Measure replay operation throughput
        info!("Measuring replay throughput for {} messages", count);
        let start = Instant::now();
        
        let replay_result = cli.replay(
            temp_dir.path(),
            target_topic,
            kafka.bootstrap_servers(),
            &[],
        ).await?;
        
        let elapsed = start.elapsed();
        validate_success(&replay_result)?;
        
        // Calculate throughput
        let throughput = count as f64 / elapsed.as_secs_f64();
        info!("Replay throughput for {} messages: {:.2} messages/second", count, throughput);
        
        // Verify all messages were replayed
        let consumed_messages = kafka.consume_messages(target_topic, count, 30).await?;
        assert_eq!(consumed_messages.len(), count, 
            "Expected {} consumed messages, found {}", count, consumed_messages.len());
    }
    
    Ok(())
}

/// Test performance with different batch sizes
#[tokio::test]
async fn test_batch_size_performance() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-batch-size";
    
    // Create topic
    kafka.create_topic(topic, 1).await?;

    // Define batch sizes to test
    let batch_sizes = [10, 50, 100, 500];
    
    // Fixed total message count
    let total_messages = 1000;
    
    // Generate and produce test messages
    info!("Generating and producing {} messages", total_messages);
    let messages = kafka.generate_and_produce(topic, total_messages, Some(42)).await?;
    
    for &batch_size in &batch_sizes {
        // Create temporary directory for storage
        let temp_dir = TestDirectory::new()?;
        
        // Create CLI executor
        let cli = CliExecutor::new();
        
        // Measure store operation with specific batch size
        info!("Measuring store performance with batch size {}", batch_size);
        let start = Instant::now();
        
        let result = cli.store(
            topic,
            kafka.bootstrap_servers(),
            temp_dir.path(),
            &["--from-beginning", "--batch-size", &batch_size.to_string()],
        ).await?;
        
        let elapsed = start.elapsed();
        validate_success(&result)?;
        
        // Calculate throughput
        let throughput = total_messages as f64 / elapsed.as_secs_f64();
        info!("Store throughput with batch size {}: {:.2} messages/second", 
            batch_size, throughput);
        
        // Verify all messages were stored
        let stored_count = temp_dir.count_files()?;
        assert_eq!(stored_count, total_messages, 
            "Expected {} stored files, found {}", total_messages, stored_count);
    }
    
    Ok(())
}

/// Test parallel processing performance
#[tokio::test]
async fn test_parallel_processing_performance() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-parallel-processing";
    
    // Create topic with multiple partitions to enable parallel processing
    let partition_counts = [1, 2, 4, 8];
    
    // Fixed message count
    let message_count = 2000;
    
    for &partitions in &partition_counts {
        // Create topic with specified number of partitions
        let partition_topic = format!("{}-{}", topic, partitions);
        kafka.create_topic(&partition_topic, partitions).await?;
        
        // Generate and produce test messages
        info!("Generating and producing {} messages to {} partitions", 
            message_count, partitions);
        let messages = kafka.generate_and_produce(&partition_topic, message_count, Some(42)).await?;
        
        // Create temporary directory for storage
        let temp_dir = TestDirectory::new()?;
        
        // Create CLI executor
        let cli = CliExecutor::new();
        
        // Measure store operation with parallel processing
        info!("Measuring store performance with {} partitions", partitions);
        let start = Instant::now();
        
        let result = cli.store(
            &partition_topic,
            kafka.bootstrap_servers(),
            temp_dir.path(),
            &["--from-beginning", "--parallel", &partitions.to_string()],
        ).await?;
        
        let elapsed = start.elapsed();
        validate_success(&result)?;
        
        // Calculate throughput
        let throughput = message_count as f64 / elapsed.as_secs_f64();
        info!("Store throughput with {} partitions: {:.2} messages/second", 
            partitions, throughput);
        
        // Verify all messages were stored
        let stored_count = temp_dir.count_files()?;
        assert_eq!(stored_count, message_count, 
            "Expected {} stored files, found {}", message_count, stored_count);
    }
    
    Ok(())
}

/// Monitor memory usage during operations
#[tokio::test]
async fn test_memory_usage_monitoring() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-memory-usage";
    
    // Create topic
    kafka.create_topic(topic, 1).await?;

    // Define message sizes to test
    let message_counts = [1000, 5000, 10000];
    
    for &count in &message_counts {
        // Create temporary directory for storage
        let temp_dir = TestDirectory::new()?;
        
        // Generate and produce test messages
        info!("Generating and producing {} messages", count);
        let messages = kafka.generate_and_produce(topic, count, Some(42)).await?;
        
        // Create CLI executor
        let cli = CliExecutor::new();
        
        // Function to get current memory usage
        let get_memory_usage = || -> Result<u64> {
            // This is a simplified approach - in a real implementation,
            // you might use a more sophisticated method to measure memory usage
            let output = Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()?;
            
            let memory_kb = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<u64>()
                .unwrap_or(0);
            
            Ok(memory_kb)
        };
        
        // Measure memory before operation
        let memory_before = get_memory_usage()?;
        info!("Memory usage before store operation: {} KB", memory_before);
        
        // Execute store operation
        let result = cli.store(
            topic,
            kafka.bootstrap_servers(),
            temp_dir.path(),
            &["--from-beginning"],
        ).await?;
        
        validate_success(&result)?;
        
        // Measure memory after operation
        let memory_after = get_memory_usage()?;
        info!("Memory usage after store operation: {} KB", memory_after);
        info!("Memory usage difference: {} KB", 
            if memory_after > memory_before { memory_after - memory_before } else { 0 });
        
        // Verify all messages were stored
        let stored_count = temp_dir.count_files()?;
        assert_eq!(stored_count, count, "Expected {} stored files, found {}", count, stored_count);
    }
    
    Ok(())
}
use clap::Args;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::core::models::KafkaMessage;
use crate::core::store_usecase::{
    StoreKafkaCommand, StoreKafkaFrom, StoreKafkaTo, StoreKafkaToStorageBackend,
};
use crate::kafka::consumer::{KafkaConsumer, KafkaConsumerConfig};
use crate::storage::files::directory::{DirectoryStorage, DirectoryStorageConfig};
use crate::storage::files::single_file::{SingleFileStorage, SingleFileStorageConfig};
use crate::storage::StorageBackend;

/// Store messages from a Kafka topic to a storage destination
///
/// Examples:
///
/// # Store 1000 messages from the 'orders' topic into a local directory
/// $ kscribe store orders --bootstrap-servers kafka-prod:9092 --count 1000 --to-dir ./orders_capture
///
/// # Store all messages for a specific user from the 'user-events' topic into a file
/// $ kscribe store user-events --bootstrap-servers kafka-prod:9092 --key-regex "user-123" --from-beginning --to-file ./user-events.json
///
/// # Store messages from the 'orders' topic starting from specific offsets
/// $ kscribe store orders --bootstrap-servers kafka-prod:9092 --from-offsets 0=1000,1=500 --to-file ./orders.json
#[derive(Args)]
pub struct StoreCommand {
    /// Topic name to store messages from
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Kafka bootstrap servers (comma-separated list of broker addresses)
    #[arg(long, value_name = "SERVERS")]
    pub bootstrap_servers: String,

    /// Store messages to a directory (one file per message)
    #[arg(long, value_name = "DIR", group = "destination")]
    pub to_dir: Option<String>,

    /// Store messages to a single file
    #[arg(long, value_name = "FILE", group = "destination")]
    pub to_file: Option<String>,

    /// Store messages to a database
    #[arg(long, value_name = "CONNECTION_STRING", group = "destination")]
    pub to_db: Option<String>,

    /// Table name for database storage (defaults to topic name)
    #[arg(long, value_name = "NAME")]
    pub table_name: Option<String>,

    /// Format to use for message values
    #[arg(long, value_name = "FORMAT", default_value = "json")]
    pub format: String,

    // Source Range Selection
    /// Start from the earliest offset
    #[arg(long, group = "start_position")]
    pub from_beginning: bool,

    /// Start from specific offsets (format: partition=offset,partition=offset,...)
    #[arg(long, value_name = "PARTITION=OFFSET", group = "start_position")]
    pub from_offsets: Option<Vec<String>>,

    /// Start from a specific timestamp (milliseconds since epoch)
    #[arg(long, value_name = "TIMESTAMP", group = "start_position")]
    pub from_timestamp: Option<i64>,

    /// Capture exactly N messages
    #[arg(long, value_name = "N", group = "end_position")]
    pub count: Option<u64>,

    /// Capture until a specific offset is reached
    #[arg(long, value_name = "OFFSET", group = "end_position")]
    pub until_offset: Option<u64>,

    /// Capture until a specific timestamp is reached (milliseconds since epoch)
    #[arg(long, value_name = "TIMESTAMP", group = "end_position")]
    pub until_timestamp: Option<i64>,

    /// Continue capturing messages indefinitely
    #[arg(long, group = "end_position")]
    pub live: bool,

    // Filtering
    /// Capture from specific partitions only (comma-separated list)
    #[arg(long, value_name = "PARTITIONS", value_delimiter = ',')]
    pub partitions: Option<Vec<i32>>,

    /// Filter messages by a regex on the key
    #[arg(long, value_name = "PATTERN")]
    pub key_regex: Option<String>,

    /// Filter messages by a specific header (format: key=value)
    #[arg(long, value_name = "KEY=VALUE")]
    pub header: Option<Vec<String>>,

    // Performance Options
    /// Number of messages to process in a batch
    #[arg(long, value_name = "N", default_value = "100")]
    pub batch_size: u32,

    /// Size of the internal buffer in messages
    #[arg(long, value_name = "N", default_value = "1000")]
    pub buffer_size: u32,

    /// Number of worker threads for parallel processing
    #[arg(long, value_name = "N")]
    pub threads: Option<u32>,

    /// Compression algorithm for stored messages
    #[arg(long, value_name = "ALGORITHM", default_value = "none")]
    pub compression: String,

    /// Timeout for the consume operation in seconds (0 means use default)
    #[arg(long, value_name = "SECONDS", default_value = "60")]
    pub timeout: u64,

    // Common Options
    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Quiet mode (minimal output)
    #[arg(short, long)]
    pub quiet: bool,

    /// Simulate execution without making changes
    #[arg(long)]
    pub dry_run: bool,
}

impl StoreCommand {
    // Parse from_offsets strings (format: "partition=offset") into a HashMap
    fn parse_from_offsets(&self) -> anyhow::Result<HashMap<i32, u64>> {
        let mut offsets = HashMap::new();

        if let Some(offset_strings) = &self.from_offsets {
            for offset_str in offset_strings {
                // Split by the equals sign
                let parts: Vec<&str> = offset_str.split('=').collect();
                if parts.len() != 2 {
                    return Err(anyhow::anyhow!(
                        "Invalid format for --from-offsets. Expected 'partition=offset', got '{}'",
                        offset_str
                    ));
                }

                // Parse partition and offset
                let partition = parts[0].trim().parse::<i32>().map_err(|_| {
                    anyhow::anyhow!("Invalid partition number in --from-offsets: '{}'", parts[0])
                })?;

                let offset = parts[1].trim().parse::<u64>().map_err(|_| {
                    anyhow::anyhow!("Invalid offset in --from-offsets: '{}'", parts[1])
                })?;

                offsets.insert(partition, offset);
            }
        }

        Ok(offsets)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        return self.execute_new().await;

        // The code below is unreachable but kept for reference
        // Validate that a destination is specified
        #[allow(unreachable_code)]
        if self.to_dir.is_none() && self.to_file.is_none() && self.to_db.is_none() {
            return Err(anyhow::anyhow!(
                "No destination specified. Use --to-dir, --to-file, or --to-db"
            ));
        }

        // Parse the from_offsets parameter when needed
        let partition_offsets = if self.from_offsets.is_some() {
            let offsets = self.parse_from_offsets()?;
            if !self.quiet {
                println!("Using custom partition offsets: {:?}", offsets);
            }
            Some(offsets)
        } else {
            None
        };

        // Parse headers if specified
        let headers = if let Some(header_strings) = &self.header {
            let mut headers_map = HashMap::new();
            for header_str in header_strings {
                let parts: Vec<&str> = header_str.split('=').collect();
                if parts.len() != 2 {
                    return Err(anyhow::anyhow!(
                        "Invalid format for --header. Expected 'key=value', got '{}'",
                        header_str
                    ));
                }
                headers_map.insert(parts[0].to_string(), parts[1].to_string());
            }
            Some(headers_map)
        } else {
            None
        };

        // Configure the Kafka consumer
        let consumer_config = KafkaConsumerConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            topic: self.topic.clone(),
            // TODO: determinism needed, allow to pass group_id
            group_id: format!("kafka-scribe-{}", uuid::Uuid::new_v4()),
            from_beginning: self.from_beginning,
            from_offsets: partition_offsets,
            from_timestamp: self.from_timestamp,
            count: self.count,
            until_offset: self.until_offset,
            until_timestamp: self.until_timestamp,
            live: self.live,
            partitions: self.partitions.clone(),
            key_regex: self.key_regex.clone(),
            headers,
            batch_size: self.batch_size,
            buffer_size: self.buffer_size,
            timeout_seconds: self.timeout,
        };

        if self.verbose {
            println!("Kafka consumer configuration:");
            println!("  Bootstrap servers: {}", consumer_config.bootstrap_servers);
            println!("  Topic: {}", consumer_config.topic);
            println!("  Group ID: {}", consumer_config.group_id);
            println!("  From beginning: {}", consumer_config.from_beginning);
            if let Some(offsets) = &consumer_config.from_offsets {
                println!("  From offsets: {:?}", offsets);
            }
            if let Some(timestamp) = consumer_config.from_timestamp {
                println!("  From timestamp: {}", timestamp);
            }
            if let Some(count) = consumer_config.count {
                println!("  Count limit: {}", count);
            }
            if let Some(offset) = consumer_config.until_offset {
                println!("  Until offset: {}", offset);
            }
            if let Some(timestamp) = consumer_config.until_timestamp {
                println!("  Until timestamp: {}", timestamp);
            }
            println!("  Live mode: {}", consumer_config.live);
            if let Some(partitions) = &consumer_config.partitions {
                println!("  Partitions: {:?}", partitions);
            }
            if let Some(regex) = &consumer_config.key_regex {
                println!("  Key regex: {}", regex);
            }
            if let Some(headers) = &consumer_config.headers {
                println!("  Headers filter: {:?}", headers);
            }
            println!("  Batch size: {}", consumer_config.batch_size);
            println!("  Buffer size: {}", consumer_config.buffer_size);
        }

        // If dry run, just return
        if self.dry_run {
            println!("Dry run completed. No messages were stored.");
            return Ok(());
        }

        // Create and initialize the storage backend
        let storage: Arc<dyn StorageBackend> = if let Some(dir_path) = &self.to_dir {
            let config = DirectoryStorageConfig {
                base_dir: PathBuf::from(dir_path),
                ..Default::default()
            };
            let storage = DirectoryStorage::new(config);
            Arc::new(storage)
        } else if let Some(file_path) = &self.to_file {
            let config = SingleFileStorageConfig {
                file_path: PathBuf::from(file_path),
                pretty_print: self.format == "json" && self.verbose, // Pretty print JSON if verbose
                ..Default::default()
            };
            let storage = SingleFileStorage::new(config);
            Arc::new(storage)
        } else if let Some(_db_conn) = &self.to_db {
            // Database storage is not implemented yet
            return Err(anyhow::anyhow!("Database storage is not implemented yet"));
        } else {
            unreachable!("Destination validation should have caught this");
        };

        storage
            .initialize()
            .await
            .context("Failed to initialize storage")?;

        // Create a channel for messages
        let (tx, mut rx) = mpsc::channel::<KafkaMessage>(self.buffer_size as usize);

        // Create and initialize the Kafka consumer
        let mut consumer =
            KafkaConsumer::new(consumer_config).context("Failed to create Kafka consumer")?;
        consumer
            .initialize()
            .await
            .context("Failed to initialize Kafka consumer")?;

        // Start the consumer in a separate task
        let mut consumer_handle = tokio::spawn(async move {
            if let Err(e) = consumer.consume_messages(tx).await {
                error!("Error consuming messages: {:?}", e);
                return Err(e);
            }
            Ok(consumer.message_count())
        });

        // Set up signal handling for graceful shutdown
        let mut ctrl_c = tokio::spawn(async {
            signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl-c signal");
        });

        let mut term_signal = tokio::spawn(async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to listen for terminate signal")
                .recv()
                .await;
        });

        // Process messages from the channel
        let start_time = Instant::now();
        let mut message_count = 0;
        let mut last_update = Instant::now();
        let update_interval = Duration::from_secs(1); // Update progress every second
        let mut consumer_task_completed = false;
        let mut consumer_result = None;

        loop {
            tokio::select! {
                // Check for signals
                _ = &mut ctrl_c => {
                    if !self.quiet {
                        println!("\nReceived interrupt signal, shutting down gracefully...");
                    }
                    break;
                }
                _ = &mut term_signal => {
                    if !self.quiet {
                        println!("\nReceived termination signal, shutting down gracefully...");
                    }
                    break;
                }
                // Check if consumer task has completed
                result = &mut consumer_handle, if !consumer_task_completed => {
                    debug!("Consumer task completed");
                    consumer_task_completed = true;
                    consumer_result = Some(result);
                    break;
                }
                // Process messages
                Some(message) = rx.recv() => {
                    // Store the message
                    if let Err(e) = storage.store_message(message).await {
                        error!("Failed to store message: {:?}", e);
                        // Continue processing other messages
                    }

                    message_count += 1;

                    // Update progress periodically
                    if !self.quiet && last_update.elapsed() >= update_interval {
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let rate = message_count as f64 / elapsed;
                        print!("\rStored {} messages ({:.2} msgs/sec)", message_count, rate);
                        std::io::Write::flush(&mut std::io::stdout()).ok();
                        last_update = Instant::now();
                    }
                }
                // Check if consumer is done
                else => {
                    debug!("Channel closed, no more messages");
                    break;
                }
            }
        }

        // Flush the storage to ensure all messages are written
        if !self.quiet {
            println!("\nFlushing storage...");
        }
        storage.flush().await.context("Failed to flush storage")?;

        // Close the storage
        storage.close().await.context("Failed to close storage")?;

        // Get the final message count from the consumer
        let consumer_count = match consumer_result {
            Some(Ok(count)) => count.context("Consumer task failed")?,
            Some(Err(e)) => return Err(anyhow::anyhow!("Consumer task failed: {:?}", e)),
            None => consumer_handle
                .await
                .context("Failed to join consumer task")?
                .context("Consumer task failed")?,
        };

        // Get storage stats
        let stats = storage.get_stats();

        if !self.quiet {
            println!("\nStorage operation completed:");
            println!("  Messages processed by consumer: {}", consumer_count);
            println!("  Messages stored: {}", stats.message_count);
            println!("  Total size: {} bytes", stats.total_size);
            println!("  Topic count: {}", stats.topic_count);
            println!("  Partition count: {}", stats.partition_count);
            if let Some(earliest) = stats.earliest_timestamp {
                println!("  Earliest message timestamp: {}", earliest);
            }
            if let Some(latest) = stats.latest_timestamp {
                println!("  Latest message timestamp: {}", latest);
            }
            let elapsed = start_time.elapsed().as_secs_f64();
            println!("  Elapsed time: {:.2} seconds", elapsed);
            println!(
                "  Average rate: {:.2} msgs/sec",
                message_count as f64 / elapsed
            );
        }

        Ok(())
    }

    async fn execute_new(&self) -> Result<()> {
        let random_group_id = format!("kafka-scribe-{}", uuid::Uuid::new_v4());
        info!("Using random group ID: {}", random_group_id);

        let storage_backend = if let Some(dir) = &self.to_dir {
            StoreKafkaToStorageBackend::Directory(dir.clone())
        } else {
            return Err(anyhow::anyhow!(
                "Destination not supported or unspecified. Only --to-dir is currently supported."
            ));
        };

        let from = if self.from_beginning {
            StoreKafkaFrom::FromBegining
        } else {
            //TODO: add parsing here
            StoreKafkaFrom::FromEnd
        };

        let command = StoreKafkaCommand {
            bootstrap_servers: self.bootstrap_servers.clone(),
            topic: self.topic.clone(),
            group_id: random_group_id,
            from,
            to: StoreKafkaTo::Live,
            store_to_storage: storage_backend,
        };
        crate::core::store_usecase::store(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test as async_test;

    #[test]
    fn test_parse_from_offsets_valid() {
        // Create a StoreCommand with valid from_offsets
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            from_offsets: Some(vec![
                "0=1000".to_string(),
                "1=500".to_string(),
                "2=750".to_string(),
            ]),
            // Set default values for other required fields
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: false,
            dry_run: false,
        };

        // Parse the from_offsets
        let result = cmd.parse_from_offsets();

        // Check that the result is Ok and contains the expected values
        assert!(result.is_ok());
        let offsets = result.unwrap();
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets.get(&0), Some(&1000));
        assert_eq!(offsets.get(&1), Some(&500));
        assert_eq!(offsets.get(&2), Some(&750));
    }

    #[test]
    fn test_parse_from_offsets_invalid_format() {
        // Create a StoreCommand with invalid from_offsets format (missing equals sign)
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            from_offsets: Some(vec!["0:1000".to_string()]), // Using colon instead of equals
            // Set default values for other required fields
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: false,
            dry_run: false,
        };

        // Parse the from_offsets
        let result = cmd.parse_from_offsets();

        // Check that the result is an error with the expected message
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid format for --from-offsets"));
        assert!(err.contains("0:1000"));
    }

    #[test]
    fn test_parse_from_offsets_invalid_partition() {
        // Create a StoreCommand with invalid partition number
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            from_offsets: Some(vec!["invalid=1000".to_string()]), // Non-numeric partition
            // Set default values for other required fields
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: false,
            dry_run: false,
        };

        // Parse the from_offsets
        let result = cmd.parse_from_offsets();

        // Check that the result is an error with the expected message
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid partition number"));
        assert!(err.contains("invalid"));
    }

    #[test]
    fn test_parse_from_offsets_invalid_offset() {
        // Create a StoreCommand with invalid offset value
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            from_offsets: Some(vec!["0=invalid".to_string()]), // Non-numeric offset
            // Set default values for other required fields
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: false,
            dry_run: false,
        };

        // Parse the from_offsets
        let result = cmd.parse_from_offsets();

        // Check that the result is an error with the expected message
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid offset"));
        assert!(err.contains("invalid"));
    }

    #[test]
    fn test_parse_from_offsets_empty() {
        // Create a StoreCommand with empty from_offsets
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            from_offsets: Some(vec![]),
            // Set default values for other required fields
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: false,
            dry_run: false,
        };

        // Parse the from_offsets
        let result = cmd.parse_from_offsets();

        // Check that the result is Ok and contains an empty HashMap
        assert!(result.is_ok());
        let offsets = result.unwrap();
        assert_eq!(offsets.len(), 0);
    }

    #[test]
    fn test_parse_from_offsets_none() {
        // Create a StoreCommand with None from_offsets
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            from_offsets: None,
            // Set default values for other required fields
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: false,
            dry_run: false,
        };

        // Parse the from_offsets
        let result = cmd.parse_from_offsets();

        // Check that the result is Ok and contains an empty HashMap
        assert!(result.is_ok());
        let offsets = result.unwrap();
        assert_eq!(offsets.len(), 0);
    }

    #[async_test]
    async fn test_execute_no_destination() {
        // Create a StoreCommand with no destination
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            to_dir: None,
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_offsets: None,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: true, // Quiet to avoid console output during tests
            dry_run: false,
        };

        // Execute the command
        let result = cmd.execute().await;

        // Check that the result is an error with the expected message
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No destination specified"));
    }

    #[async_test]
    async fn test_execute_dry_run() {
        // Create a StoreCommand with dry_run=true
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            to_dir: Some("./test-dir".to_string()),
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_offsets: None,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: None,
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: true, // Quiet to avoid console output during tests
            dry_run: true,
        };

        // Execute the command
        let result = cmd.execute().await;

        // Check that the result is Ok (dry run should succeed without doing anything)
        assert!(result.is_ok());
    }

    #[async_test]
    #[ignore]
    async fn test_execute_invalid_header_format() {
        // Create a StoreCommand with invalid header format
        let cmd = StoreCommand {
            topic: "test-topic".to_string(),
            bootstrap_servers: "localhost:9092".to_string(),
            to_dir: Some("./test-dir".to_string()),
            to_file: None,
            to_db: None,
            table_name: None,
            format: "json".to_string(),
            from_beginning: false,
            from_offsets: None,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            header: Some(vec!["invalid-header".to_string()]), // Missing equals sign
            batch_size: 100,
            buffer_size: 1000,
            threads: None,
            compression: "none".to_string(),
            timeout: 60,
            verbose: false,
            quiet: true, // Quiet to avoid console output during tests
            dry_run: false,
        };

        // Execute the command
        let result = cmd.execute().await;

        // Check that the result is an error with the expected message
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid format for --header"));
        assert!(err.contains("invalid-header"));
    }
}

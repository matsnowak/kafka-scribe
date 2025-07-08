use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use serde_json::to_string;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::core::errors::{StorageError, StorageResult};
use crate::core::models::{KafkaMessage, StorageStats};
use crate::storage::StorageBackend;

/// Configuration for the single file storage backend
#[derive(Debug, Clone)]
pub struct SingleFileStorageConfig {
    /// Path to the file where messages will be stored
    pub file_path: PathBuf,
    /// Whether to create the file if it doesn't exist
    pub create_if_missing: bool,
    /// Whether to append a newline after each message
    pub append_newline: bool,
    /// Whether to pretty-print JSON (more readable but larger files)
    pub pretty_print: bool,
}

impl Default for SingleFileStorageConfig {
    fn default() -> Self {
        Self {
            file_path: PathBuf::from("./kafka-messages.jsonl"),
            create_if_missing: true,
            append_newline: true,
            pretty_print: false,
        }
    }
}

/// Single file storage backend that stores all messages in one file
///
/// This backend stores Kafka messages in a single file, with one message per line
/// in JSON format. This is useful for storing large numbers of messages that
/// need to be processed together or analyzed with tools like jq.
///
/// # Examples
///
/// ```
/// use kafka_scribe::storage::files::single_file::{SingleFileStorage, SingleFileStorageConfig};
/// use kafka_scribe::storage::StorageBackend;
/// use kafka_scribe::core::models::KafkaMessage;
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a single file storage with default configuration
/// let storage = SingleFileStorage::new(SingleFileStorageConfig::default());
///
/// // Or with custom configuration
/// let storage = SingleFileStorage::new(SingleFileStorageConfig {
///     file_path: PathBuf::from("/tmp/kafka-messages.jsonl"),
///     create_if_missing: true,
///     append_newline: true,
///     pretty_print: false,
/// });
///
/// // Initialize the storage
/// storage.initialize().await?;
///
/// // Store a message
/// let message = KafkaMessage::new(
///     Some(b"key".to_vec()),
///     Some(b"value".to_vec()),
///     "topic".to_string(),
///     0,
///     123,
/// );
/// storage.store_message(message).await?;
///
/// // Get storage statistics
/// let stats = storage.get_stats();
/// println!("Stored {} messages", stats.message_count);
///
/// // Close the storage
/// storage.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct SingleFileStorage {
    config: SingleFileStorageConfig,
    stats: Arc<RwLock<StorageStats>>,
    // Track unique topics and partitions
    topics: Arc<Mutex<HashSet<String>>>,
    partitions: Arc<Mutex<HashMap<String, HashSet<i32>>>>,
    // File handle mutex for concurrent writes
    file_mutex: Arc<AsyncMutex<()>>,
}

impl SingleFileStorage {
    /// Create a new single file storage backend with the given configuration
    pub fn new(config: SingleFileStorageConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(StorageStats::default())),
            topics: Arc::new(Mutex::new(HashSet::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            file_mutex: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Update storage statistics with a new message
    fn update_stats(&self, message: &KafkaMessage, size: u64) {
        // Update message count and total size
        let mut stats = self.stats.write().unwrap();
        stats.message_count += 1;
        stats.total_size += size;

        // Update timestamps
        if let Some(timestamp) = message.timestamp {
            match stats.earliest_timestamp {
                Some(earliest) if timestamp < earliest => {
                    stats.earliest_timestamp = Some(timestamp)
                }
                None => stats.earliest_timestamp = Some(timestamp),
                _ => {}
            }

            match stats.latest_timestamp {
                Some(latest) if timestamp > latest => stats.latest_timestamp = Some(timestamp),
                None => stats.latest_timestamp = Some(timestamp),
                _ => {}
            }
        }

        // Update topic and partition tracking
        drop(stats); // Release the write lock before acquiring other locks

        // Update topics set
        let mut topics = self.topics.lock().unwrap();
        topics.insert(message.topic.clone());

        // Update partitions map
        let mut partitions = self.partitions.lock().unwrap();
        let topic_partitions = partitions
            .entry(message.topic.clone())
            .or_default();
        topic_partitions.insert(message.partition);

        // Update stats with new counts
        let mut stats = self.stats.write().unwrap();
        stats.topic_count = topics.len() as u32;
        stats.partition_count = partitions.values().map(|set| set.len() as u32).sum();
    }
}

#[async_trait]
impl StorageBackend for SingleFileStorage {
    /// Store a single message in the file
    async fn store_message(&self, message: KafkaMessage) -> StorageResult<()> {
        // Serialize the message to JSON
        let json = if self.config.pretty_print {
            serde_json::to_string_pretty(&message).map_err(|e| {
                StorageError::StoreFailed(format!("Failed to serialize message: {}", e))
            })?
        } else {
            to_string(&message).map_err(|e| {
                StorageError::StoreFailed(format!("Failed to serialize message: {}", e))
            })?
        };

        // Acquire the mutex to ensure exclusive access to the file
        let _guard = self.file_mutex.lock().await;

        // Open the file in append mode
        let mut file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.config.file_path)
            .await
            .map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to open file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;

        // Write the message to the file
        file.write_all(json.as_bytes()).await.map_err(|e| {
            StorageError::StoreFailed(format!(
                "Failed to write to file {}: {}",
                self.config.file_path.display(),
                e
            ))
        })?;

        // Append a newline if configured
        if self.config.append_newline {
            file.write_all(b"\n").await.map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to write newline to file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;
        }

        // Update statistics
        let size = json.len() as u64 + if self.config.append_newline { 1 } else { 0 };
        self.update_stats(&message, size);

        Ok(())
    }

    /// Store multiple messages in the file
    async fn store_messages(&self, messages: &[KafkaMessage]) -> StorageResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // Serialize all messages to JSON
        let mut _total_size = 0;
        let json_messages: Vec<String> = messages
            .iter()
            .map(|message| {
                if self.config.pretty_print {
                    serde_json::to_string_pretty(message)
                } else {
                    to_string(message)
                }
                .map_err(|e| {
                    StorageError::StoreFailed(format!("Failed to serialize message: {}", e))
                })
            })
            .collect::<Result<Vec<String>, StorageError>>()?;

        // Acquire the mutex to ensure exclusive access to the file
        let _guard = self.file_mutex.lock().await;

        // Open the file in append mode
        let mut file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.config.file_path)
            .await
            .map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to open file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;

        // Write all messages to the file
        for (i, json) in json_messages.iter().enumerate() {
            file.write_all(json.as_bytes()).await.map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to write to file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;

            // Append a newline if configured
            if self.config.append_newline {
                file.write_all(b"\n").await.map_err(|e| {
                    StorageError::StoreFailed(format!(
                        "Failed to write newline to file {}: {}",
                        self.config.file_path.display(),
                        e
                    ))
                })?;
            }

            // Update statistics for each message
            let size = json.len() as u64 + if self.config.append_newline { 1 } else { 0 };
            _total_size += size;
            self.update_stats(&messages[i], size);
        }

        Ok(())
    }

    /// Flush any buffered data to disk
    async fn flush(&self) -> StorageResult<()> {
        // Acquire the mutex to ensure exclusive access to the file
        let _guard = self.file_mutex.lock().await;

        // Open the file and flush it
        let file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.config.file_path)
            .await
            .map_err(|e| {
                StorageError::FlushFailed(format!(
                    "Failed to open file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;

        file.sync_all().await.map_err(|e| {
            StorageError::FlushFailed(format!(
                "Failed to flush file {}: {}",
                self.config.file_path.display(),
                e
            ))
        })?;

        Ok(())
    }

    /// Get storage statistics
    fn get_stats(&self) -> StorageStats {
        self.stats.read().unwrap().clone()
    }

    /// Initialize the storage backend
    async fn initialize(&self) -> StorageResult<()> {
        // Create parent directories if they don't exist
        if let Some(parent) = self.config.file_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await.map_err(|e| {
                    StorageError::InitializationFailed(format!(
                        "Failed to create directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
        }

        // Create the file if it doesn't exist and create_if_missing is true
        if self.config.create_if_missing && !self.config.file_path.exists() {
            fs::File::create(&self.config.file_path)
                .await
                .map_err(|e| {
                    StorageError::InitializationFailed(format!(
                        "Failed to create file {}: {}",
                        self.config.file_path.display(),
                        e
                    ))
                })?;
        } else if !self.config.file_path.exists() {
            return Err(StorageError::InitializationFailed(format!(
                "File {} does not exist",
                self.config.file_path.display()
            )));
        }

        Ok(())
    }

    /// Close the storage backend
    async fn close(&self) -> StorageResult<()> {
        // Flush any pending writes
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::NamedTempFile;
    use tokio::test;

    #[test]
    async fn test_single_file_storage_initialization() {
        // Create a temporary file for testing
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            pretty_print: false,
        };

        let storage = SingleFileStorage::new(config);
        assert!(storage.initialize().await.is_ok());
    }

    #[test]
    async fn test_single_file_storage_store_message() {
        // Create a temporary file for testing
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            pretty_print: false,
        };

        let storage = SingleFileStorage::new(config);
        storage.initialize().await.unwrap();

        // Create a test message
        let message = KafkaMessage::new(
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            "test-topic".to_string(),
            0,
            123,
        )
        .with_timestamp(1640995200000);

        // Store the message
        assert!(storage.store_message(message.clone()).await.is_ok());

        // Check the content of the file
        let content = fs::read_to_string(temp_file.path()).unwrap();
        let stored_message: KafkaMessage = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(stored_message, message);

        // Check the statistics
        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 1);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1640995200000));
        assert_eq!(stats.latest_timestamp, Some(1640995200000));
        assert_eq!(stats.topic_count, 1);
        assert_eq!(stats.partition_count, 1);
    }

    #[test]
    async fn test_single_file_storage_multiple_messages() {
        // Create a temporary file for testing
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            pretty_print: false,
        };

        let storage = SingleFileStorage::new(config);
        storage.initialize().await.unwrap();

        // Create and store multiple messages
        let messages = vec![
            KafkaMessage::new(
                Some(b"key1".to_vec()),
                Some(b"value1".to_vec()),
                "topic1".to_string(),
                0,
                1,
            )
            .with_timestamp(1640995200000),
            KafkaMessage::new(
                Some(b"key2".to_vec()),
                Some(b"value2".to_vec()),
                "topic1".to_string(),
                1,
                2,
            )
            .with_timestamp(1640995300000),
            KafkaMessage::new(
                Some(b"key3".to_vec()),
                Some(b"value3".to_vec()),
                "topic2".to_string(),
                0,
                3,
            )
            .with_timestamp(1640995400000),
        ];

        // Store messages individually
        for message in &messages {
            assert!(storage.store_message(message.clone()).await.is_ok());
        }

        // Check the content of the file
        let content = fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Parse each line and check against original messages
        for (i, line) in lines.iter().enumerate() {
            let stored_message: KafkaMessage = serde_json::from_str(line).unwrap();
            assert_eq!(stored_message, messages[i]);
        }

        // Check the statistics
        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 3);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1640995200000));
        assert_eq!(stats.latest_timestamp, Some(1640995400000));
        assert_eq!(stats.topic_count, 2);
        assert_eq!(stats.partition_count, 3);
    }

    #[test]
    async fn test_single_file_storage_batch_messages() {
        // Create a temporary file for testing
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            pretty_print: false,
        };

        let storage = SingleFileStorage::new(config);
        storage.initialize().await.unwrap();

        // Create messages
        let messages = vec![
            KafkaMessage::new(
                Some(b"key1".to_vec()),
                Some(b"value1".to_vec()),
                "topic1".to_string(),
                0,
                1,
            )
            .with_timestamp(1640995200000),
            KafkaMessage::new(
                Some(b"key2".to_vec()),
                Some(b"value2".to_vec()),
                "topic1".to_string(),
                1,
                2,
            )
            .with_timestamp(1640995300000),
            KafkaMessage::new(
                Some(b"key3".to_vec()),
                Some(b"value3".to_vec()),
                "topic2".to_string(),
                0,
                3,
            )
            .with_timestamp(1640995400000),
        ];

        // Store messages in batch
        assert!(storage.store_messages(&messages).await.is_ok());

        // Check the content of the file
        let content = fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Parse each line and check against original messages
        for (i, line) in lines.iter().enumerate() {
            let stored_message: KafkaMessage = serde_json::from_str(line).unwrap();
            assert_eq!(stored_message, messages[i]);
        }

        // Check the statistics
        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 3);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1640995200000));
        assert_eq!(stats.latest_timestamp, Some(1640995400000));
        assert_eq!(stats.topic_count, 2);
        assert_eq!(stats.partition_count, 3);
    }

    #[test]
    async fn test_single_file_storage_pretty_print() {
        // Create a temporary file for testing
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            pretty_print: true,
        };

        let storage = SingleFileStorage::new(config);
        storage.initialize().await.unwrap();

        // Create a test message
        let message = KafkaMessage::new(
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            "test-topic".to_string(),
            0,
            123,
        );

        // Store the message
        assert!(storage.store_message(message.clone()).await.is_ok());

        // Check the content of the file
        let content = fs::read_to_string(temp_file.path()).unwrap();

        // The pretty-printed JSON should contain newlines within the JSON itself
        assert!(content.contains("\n  "));

        // Parse the content and check against original message
        let content_without_trailing_newline = content.trim_end();
        let stored_message: KafkaMessage =
            serde_json::from_str(content_without_trailing_newline).unwrap();
        assert_eq!(stored_message, message);
    }
}

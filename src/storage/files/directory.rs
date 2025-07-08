use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use serde_json::to_string_pretty;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::core::errors::{StorageError, StorageResult};
use crate::core::models::{KafkaMessage, StorageStats};
use crate::storage::StorageBackend;

/// Configuration for the directory-based storage backend
#[derive(Debug, Clone)]
pub struct DirectoryStorageConfig {
    /// Base directory where messages will be stored
    pub base_dir: PathBuf,
    /// Whether to create the directory if it doesn't exist
    pub create_if_missing: bool,
    /// File extension to use for message files
    pub file_extension: String,
}

impl Default for DirectoryStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./kafka-messages"),
            create_if_missing: true,
            file_extension: "json".to_string(),
        }
    }
}

/// Directory-based storage backend that stores each message in a separate file
///
/// This backend stores each Kafka message as a separate file in a directory structure.
/// The directory structure is organized as follows:
/// ```
/// base_dir/
///   topic1/
///     partition1/
///       offset1.json
///       offset2.json
///     partition2/
///       offset1.json
///   topic2/
///     ...
/// ```
///
/// # Examples
///
/// ```
/// use kafka_scribe::storage::files::directory::{DirectoryStorage, DirectoryStorageConfig};
/// use kafka_scribe::storage::StorageBackend;
/// use kafka_scribe::core::models::KafkaMessage;
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a directory storage with default configuration
/// let storage = DirectoryStorage::new(DirectoryStorageConfig::default());
///
/// // Or with custom configuration
/// let storage = DirectoryStorage::new(DirectoryStorageConfig {
///     base_dir: PathBuf::from("/tmp/kafka-messages"),
///     create_if_missing: true,
///     file_extension: "json".to_string(),
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
pub struct DirectoryStorage {
    config: DirectoryStorageConfig,
    stats: Arc<RwLock<StorageStats>>,
    // Track unique topics and partitions
    topics: Arc<Mutex<HashSet<String>>>,
    partitions: Arc<Mutex<HashMap<String, HashSet<i32>>>>,
}

impl DirectoryStorage {
    /// Create a new directory-based storage backend with the given configuration
    pub fn new(config: DirectoryStorageConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(StorageStats::default())),
            topics: Arc::new(Mutex::new(HashSet::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the path for a message file
    fn get_message_path(&self, message: &KafkaMessage) -> PathBuf {
        let mut path = self.config.base_dir.clone();
        path.push(&message.topic);
        path.push(format!("partition-{}", message.partition));
        path.push(format!("{}.{}", message.offset, self.config.file_extension));
        path
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
                Some(earliest) if timestamp < earliest => stats.earliest_timestamp = Some(timestamp),
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
        let topic_partitions = partitions.entry(message.topic.clone()).or_insert_with(HashSet::new);
        topic_partitions.insert(message.partition);

        // Update stats with new counts
        let mut stats = self.stats.write().unwrap();
        stats.topic_count = topics.len() as u32;
        stats.partition_count = partitions.values().map(|set| set.len() as u32).sum();
    }
}

#[async_trait]
impl StorageBackend for DirectoryStorage {
    /// Store a single message in the directory
    async fn store_message(&self, message: KafkaMessage) -> StorageResult<()> {
        // Get the path for the message
        let path = self.get_message_path(&message);

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                StorageError::StoreFailed(format!("Failed to create directory {}: {}", parent.display(), e))
            })?;
        }

        // Serialize the message to JSON
        let json = to_string_pretty(&message).map_err(|e| {
            StorageError::StoreFailed(format!("Failed to serialize message: {}", e))
        })?;

        // Write the message to a file
        let mut file = fs::File::create(&path).await.map_err(|e| {
            StorageError::StoreFailed(format!("Failed to create file {}: {}", path.display(), e))
        })?;

        file.write_all(json.as_bytes()).await.map_err(|e| {
            StorageError::StoreFailed(format!("Failed to write to file {}: {}", path.display(), e))
        })?;

        // Update statistics
        self.update_stats(&message, json.len() as u64);

        Ok(())
    }

    /// Flush any buffered data to disk
    async fn flush(&self) -> StorageResult<()> {
        // No buffering in this implementation, so nothing to flush
        Ok(())
    }

    /// Get storage statistics
    fn get_stats(&self) -> StorageStats {
        self.stats.read().unwrap().clone()
    }

    /// Initialize the storage backend
    async fn initialize(&self) -> StorageResult<()> {
        // Create the base directory if it doesn't exist and create_if_missing is true
        if self.config.create_if_missing && !self.config.base_dir.exists() {
            fs::create_dir_all(&self.config.base_dir).await.map_err(|e| {
                StorageError::InitializationFailed(format!(
                    "Failed to create base directory {}: {}",
                    self.config.base_dir.display(),
                    e
                ))
            })?;
        } else if !self.config.base_dir.exists() {
            return Err(StorageError::InitializationFailed(format!(
                "Base directory {} does not exist",
                self.config.base_dir.display()
            )));
        } else if !self.config.base_dir.is_dir() {
            return Err(StorageError::InitializationFailed(format!(
                "Base path {} is not a directory",
                self.config.base_dir.display()
            )));
        }

        Ok(())
    }

    /// Close the storage backend
    async fn close(&self) -> StorageResult<()> {
        // No resources to release in this implementation
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use tokio::test;

    #[test]
    async fn test_directory_storage_initialization() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
        };

        let storage = DirectoryStorage::new(config);
        assert!(storage.initialize().await.is_ok());
    }

    #[test]
    async fn test_directory_storage_store_message() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
        };

        let storage = DirectoryStorage::new(config);
        storage.initialize().await.unwrap();

        // Create a test message
        let message = KafkaMessage::new(
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            "test-topic".to_string(),
            0,
            123,
        ).with_timestamp(1640995200000);

        // Store the message
        assert!(storage.store_message(message.clone()).await.is_ok());

        // Check that the file was created
        let expected_path = temp_dir.path()
            .join("test-topic")
            .join("partition-0")
            .join("123.json");
        assert!(expected_path.exists());

        // Check the content of the file
        let content = fs::read_to_string(expected_path).unwrap();
        let stored_message: KafkaMessage = serde_json::from_str(&content).unwrap();
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
    async fn test_directory_storage_multiple_messages() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
        };

        let storage = DirectoryStorage::new(config);
        storage.initialize().await.unwrap();

        // Create and store multiple messages
        let messages = vec![
            KafkaMessage::new(
                Some(b"key1".to_vec()),
                Some(b"value1".to_vec()),
                "topic1".to_string(),
                0,
                1,
            ).with_timestamp(1640995200000),
            KafkaMessage::new(
                Some(b"key2".to_vec()),
                Some(b"value2".to_vec()),
                "topic1".to_string(),
                1,
                2,
            ).with_timestamp(1640995300000),
            KafkaMessage::new(
                Some(b"key3".to_vec()),
                Some(b"value3".to_vec()),
                "topic2".to_string(),
                0,
                3,
            ).with_timestamp(1640995400000),
        ];

        for message in &messages {
            assert!(storage.store_message(message.clone()).await.is_ok());
        }

        // Check that all files were created
        assert!(temp_dir.path().join("topic1").join("partition-0").join("1.json").exists());
        assert!(temp_dir.path().join("topic1").join("partition-1").join("2.json").exists());
        assert!(temp_dir.path().join("topic2").join("partition-0").join("3.json").exists());

        // Check the statistics
        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 3);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1640995200000));
        assert_eq!(stats.latest_timestamp, Some(1640995400000));
        assert_eq!(stats.topic_count, 2);
        assert_eq!(stats.partition_count, 3);
    }
}
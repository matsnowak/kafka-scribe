use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::core::errors::{StorageError, StorageResult};
use crate::core::format::MessageFormat;
use crate::core::models::{KafkaMessage, StorageStats};
use crate::formats::JsonHybridFormat;
use crate::storage::StorageBackend;

/// Configuration for the directory-based storage backend.
///
/// The serialization `format` is a trait object so the same backend can host
/// any registered `MessageFormat` (Task 41 wiring). The default is
/// `JsonHybridFormat` with `Utf8WithFallback` encoding.
#[derive(Clone)]
pub struct DirectoryStorageConfig {
    /// Base directory where messages will be stored
    pub base_dir: PathBuf,
    /// Whether to create the directory if it doesn't exist
    pub create_if_missing: bool,
    /// File extension to use for message files
    pub file_extension: String,
    /// Serialization format (any `MessageFormat` impl)
    pub format: Arc<dyn MessageFormat + Send + Sync>,
}

impl fmt::Debug for DirectoryStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectoryStorageConfig")
            .field("base_dir", &self.base_dir)
            .field("create_if_missing", &self.create_if_missing)
            .field("file_extension", &self.file_extension)
            .field("format", &self.format.format_name())
            .finish()
    }
}

impl Default for DirectoryStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./kafka-messages"),
            create_if_missing: true,
            file_extension: "json".to_string(),
            format: Arc::new(JsonHybridFormat::default()),
        }
    }
}

/// Directory-based storage backend that stores each message in a separate file
///
/// Layout:
/// ```text
/// base_dir/
///   topic1/
///     partition-0/
///       1.json
///       2.json
///   topic2/
///     ...
/// ```
pub struct DirectoryStorage {
    config: DirectoryStorageConfig,
    stats: Arc<RwLock<StorageStats>>,
    topics: Arc<Mutex<HashSet<String>>>,
    partitions: Arc<Mutex<HashMap<String, HashSet<i32>>>>,
}

impl DirectoryStorage {
    pub fn new(config: DirectoryStorageConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(StorageStats::default())),
            topics: Arc::new(Mutex::new(HashSet::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_message_path(&self, message: &KafkaMessage) -> PathBuf {
        let mut path = self.config.base_dir.clone();
        path.push(&message.topic);
        path.push(format!("partition-{}", message.partition));
        path.push(format!("{}.{}", message.offset, self.config.file_extension));
        path
    }

    fn update_stats(&self, message: &KafkaMessage, size: u64) {
        let mut stats = self.stats.write().unwrap();
        stats.message_count += 1;
        stats.total_size += size;

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

        drop(stats);

        let mut topics = self.topics.lock().unwrap();
        topics.insert(message.topic.clone());

        let mut partitions = self.partitions.lock().unwrap();
        let topic_partitions = partitions.entry(message.topic.clone()).or_default();
        topic_partitions.insert(message.partition);

        let mut stats = self.stats.write().unwrap();
        stats.topic_count = topics.len() as u32;
        stats.partition_count = partitions.values().map(|set| set.len() as u32).sum();
    }
}

#[async_trait]
impl StorageBackend for DirectoryStorage {
    async fn store_message(&self, message: KafkaMessage) -> StorageResult<()> {
        let path = self.get_message_path(&message);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to create directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let serialized = self
            .config
            .format
            .serialize(&message)
            .await
            .map_err(|e| StorageError::StoreFailed(format!("Failed to serialize message: {}", e)))?;

        let mut file = fs::File::create(&path).await.map_err(|e| {
            StorageError::StoreFailed(format!("Failed to create file {}: {}", path.display(), e))
        })?;

        file.write_all(&serialized).await.map_err(|e| {
            StorageError::StoreFailed(format!("Failed to write to file {}: {}", path.display(), e))
        })?;

        // TODO: per-message flush is a known perf bottleneck — Task 30 batches it.
        file.flush().await.map_err(|e| {
            StorageError::StoreFailed(format!("Failed to flush file {}: {}", path.display(), e))
        })?;

        self.update_stats(&message, serialized.len() as u64);
        Ok(())
    }

    async fn flush(&self) -> StorageResult<()> {
        Ok(())
    }

    fn get_stats(&self) -> StorageStats {
        self.stats.read().unwrap().clone()
    }

    async fn initialize(&self) -> StorageResult<()> {
        if self.config.create_if_missing && !self.config.base_dir.exists() {
            fs::create_dir_all(&self.config.base_dir)
                .await
                .map_err(|e| {
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

    async fn close(&self) -> StorageResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::{BinaryEncoding, JsonFormat};
    use std::fs;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::test;

    fn json_format() -> Arc<dyn MessageFormat + Send + Sync> {
        Arc::new(JsonFormat::new())
    }

    fn json_hybrid_format(encoding: BinaryEncoding) -> Arc<dyn MessageFormat + Send + Sync> {
        Arc::new(JsonHybridFormat::with_encoding(encoding))
    }

    async fn wait_for_file_exists(path: &std::path::Path, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if path.exists() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[test]
    async fn test_directory_storage_initialization() {
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
            format: json_format(),
        };
        let storage = DirectoryStorage::new(config);
        assert!(storage.initialize().await.is_ok());
    }

    #[test]
    async fn test_directory_storage_store_message() {
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
            format: json_format(),
        };

        let storage = DirectoryStorage::new(config);
        storage.initialize().await.unwrap();

        let message = KafkaMessage::new(
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            "test-topic".to_string(),
            0,
            123,
        )
        .with_timestamp(1640995200000);

        assert!(storage.store_message(message.clone()).await.is_ok());

        let expected_path = temp_dir
            .path()
            .join("test-topic")
            .join("partition-0")
            .join("123.json");
        assert!(
            wait_for_file_exists(&expected_path, Duration::from_secs(5)).await,
            "File should exist at {:?}",
            expected_path
        );

        let content = fs::read_to_string(expected_path).unwrap();
        let stored_message: KafkaMessage = serde_json::from_str(&content).unwrap();
        assert_eq!(stored_message, message);

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
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
            format: json_format(),
        };

        let storage = DirectoryStorage::new(config);
        storage.initialize().await.unwrap();

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

        for message in &messages {
            assert!(storage.store_message(message.clone()).await.is_ok());
        }

        assert!(temp_dir
            .path()
            .join("topic1")
            .join("partition-0")
            .join("1.json")
            .exists());
        assert!(temp_dir
            .path()
            .join("topic1")
            .join("partition-1")
            .join("2.json")
            .exists());
        assert!(temp_dir
            .path()
            .join("topic2")
            .join("partition-0")
            .join("3.json")
            .exists());

        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 3);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1640995200000));
        assert_eq!(stats.latest_timestamp, Some(1640995400000));
        assert_eq!(stats.topic_count, 2);
        assert_eq!(stats.partition_count, 3);
    }

    #[test]
    async fn test_directory_storage_json_hybrid_format() {
        let temp_dir = tempdir().unwrap();
        let config = DirectoryStorageConfig {
            base_dir: temp_dir.path().to_path_buf(),
            create_if_missing: true,
            file_extension: "json".to_string(),
            format: json_hybrid_format(BinaryEncoding::JsonValue),
        };

        let storage = DirectoryStorage::new(config);
        storage.initialize().await.unwrap();

        let json_value = b"{\"name\":\"John\",\"age\":30}".to_vec();
        let parsed_json = serde_json::from_slice::<serde_json::Value>(&json_value);
        assert!(
            parsed_json.is_ok(),
            "JSON value should be valid: {:?}",
            parsed_json.err()
        );

        let message = KafkaMessage::new(
            Some(b"key".to_vec()),
            Some(json_value),
            "test-topic".to_string(),
            0,
            123,
        )
        .with_timestamp(1640995200000);

        let store_result = storage.store_message(message.clone()).await;
        assert!(
            store_result.is_ok(),
            "Failed to store message: {:?}",
            store_result.err()
        );

        let file_path = temp_dir
            .path()
            .join("test-topic")
            .join("partition-0")
            .join("123.json");
        assert!(file_path.exists(), "File should exist at {:?}", file_path);

        let content = std::fs::read_to_string(&file_path).unwrap();
        eprintln!("File content: {}", content);

        assert!(content.contains("\"name\""), "Content should contain 'name' field");
        assert!(content.contains("\"John\""), "Content should contain 'John' value");
        assert!(content.contains("\"age\""), "Content should contain 'age' field");
        assert!(content.contains("30"), "Content should contain '30' value");
        assert!(!content.contains("base64:"), "Content should not contain base64 encoding");
    }

    #[test]
    async fn test_directory_storage_default_format_is_json_hybrid() {
        let default = DirectoryStorageConfig::default();
        assert_eq!(default.format.format_name(), "json-hybrid");
    }
}

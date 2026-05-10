use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::core::errors::{StorageError, StorageResult};
use crate::core::format::MessageFormat;
use crate::core::models::{KafkaMessage, StorageStats};
use crate::formats::JsonHybridFormat;
use crate::storage::StorageBackend;

/// Configuration for the single-file (JSONL) storage backend.
///
/// Each message is serialized via the configured `MessageFormat` and
/// appended to `file_path`, optionally followed by a newline (default
/// behavior produces JSONL — one message per line).
#[derive(Clone)]
pub struct SingleFileStorageConfig {
    /// Path to the file where messages will be appended.
    pub file_path: PathBuf,
    /// Whether to create the file if it doesn't exist.
    pub create_if_missing: bool,
    /// Whether to append a newline after each message (recommended for JSONL).
    pub append_newline: bool,
    /// Serialization format (any `MessageFormat` impl).
    pub format: Arc<dyn MessageFormat + Send + Sync>,
}

impl fmt::Debug for SingleFileStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleFileStorageConfig")
            .field("file_path", &self.file_path)
            .field("create_if_missing", &self.create_if_missing)
            .field("append_newline", &self.append_newline)
            .field("format", &self.format.format_name())
            .finish()
    }
}

impl Default for SingleFileStorageConfig {
    fn default() -> Self {
        Self {
            file_path: PathBuf::from("./kafka-messages.jsonl"),
            create_if_missing: true,
            append_newline: true,
            format: Arc::new(JsonHybridFormat::default()),
        }
    }
}

/// Single-file storage backend (JSONL by default).
///
/// All messages land in one file with one serialized message per line —
/// pipe-friendly for tools like `jq`, `grep`, `wc -l`.
pub struct SingleFileStorage {
    config: SingleFileStorageConfig,
    stats: Arc<RwLock<StorageStats>>,
    topics: Arc<Mutex<HashSet<String>>>,
    partitions: Arc<Mutex<HashMap<String, HashSet<i32>>>>,
    file_mutex: Arc<AsyncMutex<()>>,
}

impl SingleFileStorage {
    pub fn new(config: SingleFileStorageConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(StorageStats::default())),
            topics: Arc::new(Mutex::new(HashSet::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            file_mutex: Arc::new(AsyncMutex::new(())),
        }
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
impl StorageBackend for SingleFileStorage {
    async fn store_message(&self, message: KafkaMessage) -> StorageResult<()> {
        let serialized = self
            .config
            .format
            .serialize(&message)
            .await
            .map_err(|e| StorageError::StoreFailed(format!("Failed to serialize message: {}", e)))?;

        let _guard = self.file_mutex.lock().await;

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

        file.write_all(&serialized).await.map_err(|e| {
            StorageError::StoreFailed(format!(
                "Failed to write to file {}: {}",
                self.config.file_path.display(),
                e
            ))
        })?;

        if self.config.append_newline {
            file.write_all(b"\n").await.map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to write newline to file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;
        }

        // Tokio's File buffer doesn't auto-flush on drop — be explicit so
        // tests that read the file back observe the writes deterministically.
        file.flush().await.map_err(|e| {
            StorageError::StoreFailed(format!(
                "Failed to flush file {}: {}",
                self.config.file_path.display(),
                e
            ))
        })?;

        let size = serialized.len() as u64 + if self.config.append_newline { 1 } else { 0 };
        self.update_stats(&message, size);
        Ok(())
    }

    async fn store_messages(&self, messages: &[KafkaMessage]) -> StorageResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut serialized: Vec<Vec<u8>> = Vec::with_capacity(messages.len());
        for message in messages {
            let bytes = self.config.format.serialize(message).await.map_err(|e| {
                StorageError::StoreFailed(format!("Failed to serialize message: {}", e))
            })?;
            serialized.push(bytes);
        }

        let _guard = self.file_mutex.lock().await;

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

        for (i, bytes) in serialized.iter().enumerate() {
            file.write_all(bytes).await.map_err(|e| {
                StorageError::StoreFailed(format!(
                    "Failed to write to file {}: {}",
                    self.config.file_path.display(),
                    e
                ))
            })?;

            if self.config.append_newline {
                file.write_all(b"\n").await.map_err(|e| {
                    StorageError::StoreFailed(format!(
                        "Failed to write newline to file {}: {}",
                        self.config.file_path.display(),
                        e
                    ))
                })?;
            }

            let size = bytes.len() as u64 + if self.config.append_newline { 1 } else { 0 };
            self.update_stats(&messages[i], size);
        }

        file.flush().await.map_err(|e| {
            StorageError::StoreFailed(format!(
                "Failed to flush file {}: {}",
                self.config.file_path.display(),
                e
            ))
        })?;

        Ok(())
    }

    async fn flush(&self) -> StorageResult<()> {
        let _guard = self.file_mutex.lock().await;
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

    fn get_stats(&self) -> StorageStats {
        self.stats.read().unwrap().clone()
    }

    async fn initialize(&self) -> StorageResult<()> {
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

        if self.config.create_if_missing && !self.config.file_path.exists() {
            fs::File::create(&self.config.file_path).await.map_err(|e| {
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

    async fn close(&self) -> StorageResult<()> {
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::JsonFormat;
    use std::fs;
    use tempfile::NamedTempFile;
    use tokio::test;

    fn json_format() -> Arc<dyn MessageFormat + Send + Sync> {
        Arc::new(JsonFormat::new())
    }

    #[test]
    async fn test_single_file_storage_initialization() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            format: json_format(),
        };
        let storage = SingleFileStorage::new(config);
        assert!(storage.initialize().await.is_ok());
    }

    #[test]
    async fn test_single_file_storage_store_message() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            format: json_format(),
        };

        let storage = SingleFileStorage::new(config);
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

        let content = fs::read_to_string(temp_file.path()).unwrap();
        assert!(!content.is_empty());
        // First line should be valid JSON of the original message.
        let first_line = content.lines().next().unwrap();
        let stored: KafkaMessage = serde_json::from_str(first_line).unwrap();
        assert_eq!(stored, message);

        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 1);
        assert!(stats.total_size > 0);
    }

    #[test]
    async fn test_single_file_storage_multiple_messages() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            format: json_format(),
        };

        let storage = SingleFileStorage::new(config);
        storage.initialize().await.unwrap();

        let messages = vec![
            KafkaMessage::new(
                Some(b"k1".to_vec()),
                Some(b"v1".to_vec()),
                "topic1".to_string(),
                0,
                1,
            ),
            KafkaMessage::new(
                Some(b"k2".to_vec()),
                Some(b"v2".to_vec()),
                "topic1".to_string(),
                1,
                2,
            ),
            KafkaMessage::new(
                Some(b"k3".to_vec()),
                Some(b"v3".to_vec()),
                "topic2".to_string(),
                0,
                3,
            ),
        ];

        for message in &messages {
            assert!(storage.store_message(message.clone()).await.is_ok());
        }

        let content = fs::read_to_string(temp_file.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 3);
        assert_eq!(stats.topic_count, 2);
    }

    #[test]
    async fn test_single_file_storage_batch_messages() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = SingleFileStorageConfig {
            file_path: temp_file.path().to_path_buf(),
            create_if_missing: true,
            append_newline: true,
            format: json_format(),
        };

        let storage = SingleFileStorage::new(config);
        storage.initialize().await.unwrap();

        let messages = vec![
            KafkaMessage::new(
                Some(b"a".to_vec()),
                Some(b"1".to_vec()),
                "t".to_string(),
                0,
                1,
            ),
            KafkaMessage::new(
                Some(b"b".to_vec()),
                Some(b"2".to_vec()),
                "t".to_string(),
                0,
                2,
            ),
        ];

        assert!(storage.store_messages(&messages).await.is_ok());

        let content = fs::read_to_string(temp_file.path()).unwrap();
        assert_eq!(content.lines().count(), 2);
    }

    #[test]
    async fn test_single_file_storage_default_format_is_json_hybrid() {
        let default = SingleFileStorageConfig::default();
        assert_eq!(default.format.format_name(), "json-hybrid");
    }
}

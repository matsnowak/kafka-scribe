#[cfg(feature = "database")]
pub mod database;
pub mod files;
pub mod transform;

use crate::core::{errors::StorageResult, models::KafkaMessage, models::StorageStats};
use async_trait::async_trait;

/// Trait for storage backends
///
/// This trait defines the interface for different storage backends that can be used
/// to store Kafka messages. Implementations of this trait can store messages in
/// various ways, such as in files, databases, or other storage systems.
///
/// # Examples
///
/// ```
/// # use async_trait::async_trait;
/// # use kafka_scribe::core::{models::KafkaMessage, models::StorageStats, errors::StorageResult};
/// # use kafka_scribe::storage::StorageBackend;
/// # use std::sync::Arc;
/// #
/// # struct FileStorage {
/// #     directory: String,
/// # }
/// #
/// # #[async_trait]
/// # impl StorageBackend for FileStorage {
/// #     async fn store_message(&self, message: KafkaMessage) -> StorageResult<()> {
/// #         // Store the message in a file
/// #         Ok(())
/// #     }
/// #
/// #     async fn flush(&self) -> StorageResult<()> {
/// #         // Ensure all pending writes are flushed
/// #         Ok(())
/// #     }
/// #
/// #     fn get_stats(&self) -> StorageStats {
/// #         // Return statistics about the storage
/// #         StorageStats::default()
/// #     }
/// #
/// #     async fn initialize(&self) -> StorageResult<()> {
/// #         // Initialize the storage backend
/// #         Ok(())
/// #     }
/// #
/// #     async fn close(&self) -> StorageResult<()> {
/// #         // Close the storage backend
/// #         Ok(())
/// #     }
/// # }
/// #
/// # async fn example() -> StorageResult<()> {
/// // Create a file storage backend
/// let storage = FileStorage {
///     directory: "/tmp/kafka-messages".to_string(),
/// };
///
/// // Initialize the storage
/// storage.initialize().await?;
///
/// // Store a single message
/// let message = KafkaMessage::new(
///     Some(b"key".to_vec()),
///     Some(b"value".to_vec()),
///     "topic".to_string(),
///     0,
///     100,
/// );
/// storage.store_message(message).await?;
///
/// // Store multiple messages in a batch
/// let messages = vec![
///     KafkaMessage::new(
///         Some(b"key1".to_vec()),
///         Some(b"value1".to_vec()),
///         "topic".to_string(),
///         0,
///         101,
///     ),
///     KafkaMessage::new(
///         Some(b"key2".to_vec()),
///         Some(b"value2".to_vec()),
///         "topic".to_string(),
///         0,
///         102,
///     ),
/// ];
/// storage.store_messages(&messages).await?;
///
/// // Flush any pending writes
/// storage.flush().await?;
///
/// // Get statistics about the storage
/// let stats = storage.get_stats();
/// println!("Total messages: {}", stats.message_count);
///
/// // Close the storage
/// storage.close().await?;
///
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store a message in the storage backend
    ///
    /// This method is called for each message that needs to be stored.
    /// Implementations should handle any necessary serialization and
    /// storage operations.
    ///
    /// # Arguments
    ///
    /// * `message` - The Kafka message to store
    ///
    /// # Returns
    ///
    /// A `StorageResult<()>` indicating success or failure
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the message cannot be stored
    async fn store_message(&self, message: KafkaMessage) -> StorageResult<()>;

    /// Store multiple messages in the storage backend
    ///
    /// This method is called to store multiple messages at once, which can be more
    /// efficient than calling store_message repeatedly. The default implementation
    /// calls store_message for each message, but implementations should override
    /// this method with a more efficient implementation when possible.
    ///
    /// # Arguments
    ///
    /// * `messages` - A slice of Kafka messages to store
    ///
    /// # Returns
    ///
    /// A `StorageResult<()>` indicating success or failure
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if any message cannot be stored
    async fn store_messages(&self, messages: &[KafkaMessage]) -> StorageResult<()> {
        for message in messages {
            self.store_message(message.clone()).await?;
        }
        Ok(())
    }

    /// Flush any pending writes to ensure durability
    ///
    /// This method is called to ensure that all previously stored messages
    /// have been durably persisted to the storage medium.
    ///
    /// # Returns
    ///
    /// A `StorageResult<()>` indicating success or failure
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the flush operation fails
    async fn flush(&self) -> StorageResult<()>;

    /// Get statistics about the storage
    ///
    /// This method returns information about the messages stored in this
    /// storage backend, such as count, size, and time range.
    ///
    /// # Returns
    ///
    /// A `StorageStats` struct containing statistics about the storage
    fn get_stats(&self) -> StorageStats;

    /// Initialize the storage backend
    ///
    /// This method is called before any messages are stored. Implementations
    /// should perform any necessary setup, such as creating directories or
    /// database tables.
    ///
    /// # Returns
    ///
    /// A `StorageResult<()>` indicating success or failure
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if initialization fails
    async fn initialize(&self) -> StorageResult<()>;

    /// Close the storage backend
    ///
    /// This method is called when the storage backend is no longer needed.
    /// Implementations should perform any necessary cleanup, such as closing
    /// file handles or database connections.
    ///
    /// # Returns
    ///
    /// A `StorageResult<()>` indicating success or failure
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the close operation fails
    async fn close(&self) -> StorageResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::errors::StorageError;
    use std::sync::{Arc, Mutex};

    // Mock implementation of StorageBackend for testing
    struct MockStorage {
        messages: Arc<Mutex<Vec<KafkaMessage>>>,
        initialized: Arc<Mutex<bool>>,
        flushed: Arc<Mutex<bool>>,
        closed: Arc<Mutex<bool>>,
        should_fail: bool,
    }

    impl MockStorage {
        fn new(should_fail: bool) -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
                initialized: Arc::new(Mutex::new(false)),
                flushed: Arc::new(Mutex::new(false)),
                closed: Arc::new(Mutex::new(false)),
                should_fail,
            }
        }
    }

    #[async_trait]
    impl StorageBackend for MockStorage {
        async fn store_message(&self, message: KafkaMessage) -> StorageResult<()> {
            if self.should_fail {
                return Err(StorageError::StoreFailed(
                    "Mock storage failure".to_string(),
                ));
            }

            let mut messages = self.messages.lock().unwrap();
            messages.push(message);
            Ok(())
        }

        async fn store_messages(&self, messages: &[KafkaMessage]) -> StorageResult<()> {
            if self.should_fail {
                return Err(StorageError::StoreFailed(
                    "Mock batch storage failure".to_string(),
                ));
            }

            let mut storage_messages = self.messages.lock().unwrap();
            storage_messages.extend_from_slice(messages);
            Ok(())
        }

        async fn flush(&self) -> StorageResult<()> {
            if self.should_fail {
                return Err(StorageError::FlushFailed("Mock flush failure".to_string()));
            }

            let mut flushed = self.flushed.lock().unwrap();
            *flushed = true;
            Ok(())
        }

        fn get_stats(&self) -> StorageStats {
            let messages = self.messages.lock().unwrap();
            let mut stats = StorageStats {
                message_count: messages.len() as u64,
                ..Default::default()
            };

            // Calculate total size (approximate)
            let total_size: u64 = messages
                .iter()
                .map(|m| {
                    let key_size = m.key.as_ref().map_or(0, |k| k.len() as u64);
                    let value_size = m.value.as_ref().map_or(0, |v| v.len() as u64);
                    key_size + value_size
                })
                .sum();
            stats.total_size = total_size;

            // Set timestamps if there are messages
            if !messages.is_empty() {
                let timestamps: Vec<i64> = messages.iter().filter_map(|m| m.timestamp).collect();

                if !timestamps.is_empty() {
                    stats.earliest_timestamp = timestamps.iter().min().cloned();
                    stats.latest_timestamp = timestamps.iter().max().cloned();
                }
            }

            // Count unique partitions and topics
            let mut partitions = std::collections::HashSet::new();
            let mut topics = std::collections::HashSet::new();

            for message in messages.iter() {
                partitions.insert(message.partition);
                topics.insert(&message.topic);
            }

            stats.partition_count = partitions.len() as u32;
            stats.topic_count = topics.len() as u32;

            stats
        }

        async fn initialize(&self) -> StorageResult<()> {
            if self.should_fail {
                return Err(StorageError::InitializationFailed(
                    "Mock initialization failure".to_string(),
                ));
            }

            let mut initialized = self.initialized.lock().unwrap();
            *initialized = true;
            Ok(())
        }

        async fn close(&self) -> StorageResult<()> {
            if self.should_fail {
                return Err(StorageError::StoreFailed("Mock close failure".to_string()));
            }

            let mut closed = self.closed.lock().unwrap();
            *closed = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_storage_backend_success() {
        let storage = MockStorage::new(false);

        // Test initialization
        assert!(storage.initialize().await.is_ok());
        assert!(*storage.initialized.lock().unwrap());

        // Test storing a message
        let message = KafkaMessage::new(
            Some(b"test-key".to_vec()),
            Some(b"test-value".to_vec()),
            "test-topic".to_string(),
            0,
            100,
        )
        .with_timestamp(1625482800000);

        assert!(storage.store_message(message.clone()).await.is_ok());

        // Test that the message was stored
        {
            let messages = storage.messages.lock().unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0], message);
        }

        // Test flushing
        assert!(storage.flush().await.is_ok());
        assert!(*storage.flushed.lock().unwrap());

        // Test getting stats
        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 1);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1625482800000));
        assert_eq!(stats.latest_timestamp, Some(1625482800000));
        assert_eq!(stats.partition_count, 1);
        assert_eq!(stats.topic_count, 1);

        // Test closing
        assert!(storage.close().await.is_ok());
        assert!(*storage.closed.lock().unwrap());
    }

    #[tokio::test]
    async fn test_storage_backend_failure() {
        let storage = MockStorage::new(true);

        // Test initialization failure
        assert!(storage.initialize().await.is_err());

        // Test storing a message failure
        let message = KafkaMessage::new(
            Some(b"test-key".to_vec()),
            Some(b"test-value".to_vec()),
            "test-topic".to_string(),
            0,
            100,
        );

        assert!(storage.store_message(message).await.is_err());

        // Test flushing failure
        assert!(storage.flush().await.is_err());

        // Test closing failure
        assert!(storage.close().await.is_err());
    }

    #[tokio::test]
    async fn test_store_messages_success() {
        let storage = MockStorage::new(false);

        // Initialize the storage
        assert!(storage.initialize().await.is_ok());

        // Create test messages
        let messages = vec![
            KafkaMessage::new(
                Some(b"key1".to_vec()),
                Some(b"value1".to_vec()),
                "test-topic".to_string(),
                0,
                100,
            )
            .with_timestamp(1625482800000),
            KafkaMessage::new(
                Some(b"key2".to_vec()),
                Some(b"value2".to_vec()),
                "test-topic".to_string(),
                0,
                101,
            )
            .with_timestamp(1625482800001),
            KafkaMessage::new(
                Some(b"key3".to_vec()),
                Some(b"value3".to_vec()),
                "test-topic".to_string(),
                1,
                102,
            )
            .with_timestamp(1625482800002),
        ];

        // Store messages in batch
        assert!(storage.store_messages(&messages).await.is_ok());

        // Verify all messages were stored
        let stored_messages = storage.messages.lock().unwrap();
        assert_eq!(stored_messages.len(), 3);
        assert_eq!(stored_messages[0], messages[0]);
        assert_eq!(stored_messages[1], messages[1]);
        assert_eq!(stored_messages[2], messages[2]);

        // Test stats after storing messages
        drop(stored_messages); // Release the lock
        let stats = storage.get_stats();
        assert_eq!(stats.message_count, 3);
        assert!(stats.total_size > 0);
        assert_eq!(stats.earliest_timestamp, Some(1625482800000));
        assert_eq!(stats.latest_timestamp, Some(1625482800002));
        assert_eq!(stats.partition_count, 2); // We have messages in partitions 0 and 1
        assert_eq!(stats.topic_count, 1);
    }

    #[tokio::test]
    async fn test_store_messages_failure() {
        let storage = MockStorage::new(true);

        // Create test messages
        let messages = vec![
            KafkaMessage::new(
                Some(b"key1".to_vec()),
                Some(b"value1".to_vec()),
                "test-topic".to_string(),
                0,
                100,
            ),
            KafkaMessage::new(
                Some(b"key2".to_vec()),
                Some(b"value2".to_vec()),
                "test-topic".to_string(),
                0,
                101,
            ),
        ];

        // Test storing messages failure
        assert!(storage.store_messages(&messages).await.is_err());

        // Verify no messages were stored
        let stored_messages = storage.messages.lock().unwrap();
        assert_eq!(stored_messages.len(), 0);
    }
}

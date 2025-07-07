pub mod files;
#[cfg(feature = "database")]
pub mod database;
pub mod transform;

use async_trait::async_trait;
use crate::core::{models::KafkaMessage, models::StorageStats, errors::StorageResult};

/// Trait for storage backends
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store a message
    async fn store_message(&self, message: KafkaMessage) -> StorageResult<()>;
    
    /// Flush any pending writes
    async fn flush(&self) -> StorageResult<()>;
    
    /// Get statistics about the storage
    fn get_stats(&self) -> StorageStats;
    
    /// Initialize the storage backend
    async fn initialize(&self) -> StorageResult<()>;
    
    /// Close the storage backend
    async fn close(&self) -> StorageResult<()>;
}
use thiserror::Error;

/// Main error type for kafka-scribe
#[derive(Error, Debug)]
pub enum ScribeError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    JsonSerialization(#[from] serde_json::Error),

    #[error("YAML serialization error: {0}")]
    YamlSerialization(#[from] serde_yaml::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Format error: {0}")]
    Format(#[from] FormatError),

    #[error("CLI error: {0}")]
    Cli(String),
}

/// Storage-specific errors
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Failed to initialize storage: {0}")]
    InitializationFailed(String),

    #[error("Failed to store message: {0}")]
    StoreFailed(String),

    #[error("Failed to retrieve message: {0}")]
    RetrieveFailed(String),

    #[error("Failed to flush storage: {0}")]
    FlushFailed(String),

    #[error("Storage not found: {0}")]
    NotFound(String),

    #[error("Storage is corrupted: {0}")]
    Corrupted(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Storage is full: {0}")]
    StorageFull(String),

    #[cfg(feature = "database")]
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Format-specific errors
#[derive(Error, Debug)]
pub enum FormatError {
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Decoding error: {0}")]
    Decoding(String),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, ScribeError>;
pub type StorageResult<T> = std::result::Result<T, StorageError>;
pub type FormatResult<T> = std::result::Result<T, FormatError>;
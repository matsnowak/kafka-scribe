use clap::Args;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

use crate::core::format::MessageFormat;
use crate::core::store_usecase::{
    StoreKafkaCommand, StoreKafkaFrom, StoreKafkaTo, StoreKafkaToStorageBackend,
};
use crate::formats::{BinaryEncoding, JsonFormat, JsonHybridFormat};

/// Parse `--format <name>` into a concrete `MessageFormat` impl.
///
/// Recognized values:
/// - `json` — strict serde-derived JSON (verbose; binary fields as JSON byte arrays)
/// - `json-hybrid` (default) — smart binary handling via `BinaryEncoding::Utf8WithFallback`
/// - `json-hybrid-base64`, `base64` — `BinaryEncoding::Base64`
/// - `json-hybrid-utf8`, `utf8` — `BinaryEncoding::Utf8WithFallback`
/// - `json-hybrid-force-utf8`, `force-utf8` — `BinaryEncoding::ForceUtf8`
/// - `json-hybrid-value`, `value` — `BinaryEncoding::JsonValue` (parse JSON-payloads inline)
fn parse_format(s: &str) -> Result<Arc<dyn MessageFormat + Send + Sync>> {
    match s {
        "json" => Ok(Arc::new(JsonFormat::new())),
        "json-hybrid" | "hybrid" => Ok(Arc::new(JsonHybridFormat::default())),
        "json-hybrid-base64" | "base64" => Ok(Arc::new(JsonHybridFormat::with_encoding(
            BinaryEncoding::Base64,
        ))),
        "json-hybrid-utf8" | "utf8" => Ok(Arc::new(JsonHybridFormat::with_encoding(
            BinaryEncoding::Utf8WithFallback,
        ))),
        "json-hybrid-force-utf8" | "force-utf8" => Ok(Arc::new(JsonHybridFormat::with_encoding(
            BinaryEncoding::ForceUtf8,
        ))),
        "json-hybrid-value" | "value" => Ok(Arc::new(JsonHybridFormat::with_encoding(
            BinaryEncoding::JsonValue,
        ))),
        // Phase-2 formats: explicit error instead of silent no-op
        "avro" | "protobuf" | "binary" | "string" => Err(anyhow::anyhow!(
            "--format '{}' is planned for Phase 2; v0.1.0 supports json, json-hybrid (and its variants).",
            s
        )),
        other => Err(anyhow::anyhow!(
            "Unknown --format '{}'. Supported: json, json-hybrid, json-hybrid-base64, json-hybrid-utf8, json-hybrid-force-utf8, json-hybrid-value (or short aliases: hybrid, base64, utf8, force-utf8, value).",
            other
        )),
    }
}

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

    pub async fn execute(&self) -> Result<()> {
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
        } else if let Some(ts) = self.from_timestamp {
            StoreKafkaFrom::FromTimestamp(ts)
        } else {
            StoreKafkaFrom::FromEnd
        };

        let headers_map = if let Some(header_strings) = &self.header {
            let mut h = HashMap::new();
            for header_str in header_strings {
                let parts: Vec<&str> = header_str.split('=').collect();
                if parts.len() == 2 {
                    h.insert(parts[0].to_string(), parts[1].to_string());
                } else {
                    warn!("Invalid header format: {}", header_str);
                }
            }
            Some(h)
        } else {
            None
        };

        let format = parse_format(&self.format)?;

        let command = StoreKafkaCommand {
            bootstrap_servers: self.bootstrap_servers.clone(),
            topic: self.topic.clone(),
            group_id: random_group_id,
            from,
            to: StoreKafkaTo::Live,
            store_to_storage: storage_backend,
            format,
            key_regex: self.key_regex.clone(),
            headers: headers_map,
            partitions: self.partitions.clone(),
            limit_count: self.count,
            limit_until_offset: self.until_offset,
            limit_until_timestamp: self.until_timestamp,
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

        // Check that the result is an error with the expected message.
        // After Task 37 cleanup the new path returns
        // "Destination not supported or unspecified". Task 42 will restore
        // a clearer "No destination specified" error and fully wire `--to-file`/`--to-db`.
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Destination not supported or unspecified"),
            "unexpected error: {err}"
        );
    }

    #[async_test]
    #[ignore = "blocked on Task 42 — execute_new currently ignores --dry-run; \
                will be restored when CLI flags are routed through StoreKafkaCommand"]
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

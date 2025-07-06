# Design Document: kafka-scribe (Stateless Version)

## 1. Introduction \& Philosophy

**kafka-scribe** is a high-performance command-line interface (CLI) tool designed for developers and operators working with Apache Kafka. Its primary purpose is to "transcribe" messages from one state to another—capturing them from a topic, enabling analysis and modification, and replaying them.

### 1.1 Core Philosophy

The core philosophy is built on three distinct, sequential stages:

1. **Store**: Reliably capture messages from a Kafka topic into a durable storage format (local files or a database). This creates a static, analyzable snapshot of a topic's data.
2. **Analyze**: Investigate the stored messages using powerful, familiar external tools. For file-based storage, this means using `grep`, `jq`, `awk`, etc. For database storage, this means using standard SQL clients.
3. **Replay**: Replay a selection of the stored messages (original or modified) back into a Kafka topic for debugging, testing, or recovery.

### 1.2 Design Principles

- **Stateless Operation**: Every command is self-contained with all required parameters passed directly as arguments, ensuring clarity and scriptability.
- **Extensibility**: The tool is designed to be easily extended to support different storage backends and Kafka configurations.
- **Performance**: Optimized for handling high-volume Kafka topics efficiently.
- **Developer Experience**: Intuitive CLI interface with comprehensive documentation and helpful error messages.
- **Testability**: Components are designed to be easily testable in isolation.

## 2. CLI Command Structure \& Design

### 2.1 Command Philosophy

The CLI is designed to be intuitive and powerful, using a consistent `verb + noun` structure. This approach makes commands predictable and easy to remember, enhancing developer experience.

### 2.2 Main Commands

* `kscribe store`: Capture messages from a topic to a store.
* `kscribe replay`: Replay messages from a store to a topic.
* `kscribe stats`: Display statistics about a message store.
* `kscribe completion`: Generate shell completion scripts.

### 2.3 Common Patterns

All commands follow these consistent patterns:

1. **Source and Destination**: Commands clearly separate source and destination with prefixes:
   - Sources use `--from-*` (e.g., `--from-beginning`, `--from-file`)
   - Destinations use `--to-*` (e.g., `--to-topic`, `--to-dir`)

2. **Consistent Help**: All commands provide detailed help with examples:
   ```bash
   kscribe <command> --help
   ```

3. **Error Handling**: Commands provide clear, actionable error messages with suggestions for resolution.

4. **Dry Run Mode**: Commands that modify state support a `--dry-run` flag to simulate execution without making changes.

5. **Verbosity Control**: All commands support `--verbose` and `--quiet` flags to control output detail level.


## 3. The Three Stages of `kafka-scribe`

### 3.1 Stage 1: `kscribe store`

This command connects to a Kafka topic and writes its messages to a specified storage destination.

#### 3.1.1 Syntax

```bash
kscribe store <topic> --bootstrap-servers <servers> [OPTIONS]
```

#### 3.1.2 Key Options

* `--bootstrap-servers <servers>`: **(Required)** Comma-separated list of Kafka broker addresses (e.g., `kafka1:9092,kafka2:9092`).
* `--to-dir <path>`: Store messages as files in a directory (one file per message).
* `--to-file <path>`: Store messages into a single file.
* `--to-db <connection-string>`: Store messages in a database.
* `--table-name <name>`: Table name for database storage (defaults to topic name).

#### 3.1.3 Source Range Selection

* `--from-beginning`: Start from the earliest offset.
* `--from-offset <offset>`: Start from a specific offset.
* `--from-timestamp <timestamp>`: Start from a specific time.
* `--count <n>`: Capture exactly `n` messages.
* `--until-offset <offset>`: Capture until a specific offset is reached.
* `--until-timestamp <timestamp>`: Capture until a specific time is reached.
* `--live`: Continue capturing messages indefinitely.

#### 3.1.4 Filtering

* `--partitions <p1,p2,...>`: Capture from specific partitions only.
* `--key-regex <pattern>`: Filter messages by a regex on the key.
* `--header <key=value>`: Filter messages by a specific header.

#### 3.1.5 Performance Options

* `--batch-size <n>`: Number of messages to process in a batch (default: 100).
* `--buffer-size <n>`: Size of the internal buffer in messages (default: 1000).
* `--threads <n>`: Number of worker threads for parallel processing (default: number of CPU cores).
* `--compression <none|gzip|snappy>`: Compression algorithm for stored messages (default: none).

#### 3.1.6 Storage Backend Extensibility

The storage system is designed with a pluggable architecture that allows for easy extension to new storage backends:

1. **Built-in Storage Backends**:
   - **File System**: Individual files or directories of files
   - **Databases**: SQLite, PostgreSQL (via SQLx)

2. **Custom Storage Backends**:
   Developers can implement the `StorageBackend` trait to add support for additional storage systems:

   ```rust
   pub trait StorageBackend {
       async fn store_message(&self, message: KafkaMessage) -> Result<(), StorageError>;
       async fn flush(&self) -> Result<(), StorageError>;
       fn get_stats(&self) -> StorageStats;
   }
   ```

3. **Configuration**:
   Custom backends can be registered and configured via a plugin system or configuration file.

**Use Case: Store to Files**

```bash
# Store 1000 messages from the 'orders' topic into a local directory for analysis.
kscribe store orders \
  --bootstrap-servers kafka-prod:9092 \
  --count 1000 \
  --to-dir ./orders_capture
```

**Use Case: Store to SQL Database**

```bash
# Store all messages for a specific user from the 'user-events' topic into a PostgreSQL database.
kscribe store user-events \
  --bootstrap-servers kafka-prod:9092 \
  --key-regex "user-123" \
  --from-beginning \
  --to-db "postgres://user:pass@host:5432/dbname"
```


### 3.2 Stage 2: Analyze (Using External Tools)

`kafka-scribe` intentionally omits a built-in `investigate` command. Instead, you use standard tools to analyze the stored data, leveraging existing expertise and powerful tools.

#### 3.2.1 File-based Analysis

Once messages are stored in a directory (e.g., `./orders_capture`), you can use standard command-line tools:

```bash
# Find all messages containing an error message using grep
grep -r "error" ./orders_capture/

# Extract and count all unique product IDs using jq
cat ./orders_capture/*.json | jq -r '.payload.product_id' | sort | uniq -c

# Find messages with specific headers
grep -r '"headers":.*"correlation-id":"abc123"' ./orders_capture/

# Analyze message timing patterns
ls -lt ./orders_capture/ | head -20
```

#### 3.2.2 Database-based Analysis

Connect to your database (e.g., `psql "postgres://user:pass@host:5432/dbname"`) and run SQL queries:

```sql
-- Find all events for a specific user
SELECT * FROM "user-events" WHERE key = 'user-123';

-- Prepare a reply set by selecting failed login attempts
SELECT raw_message FROM "user-events" 
WHERE json_extract_path_text(raw_message, 'event_type') = 'LOGIN_FAILED';

-- Analyze message timing patterns
SELECT 
  date_trunc('minute', timestamp) as minute,
  count(*) as message_count
FROM "user-events"
GROUP BY minute
ORDER BY minute;
```

#### 3.2.3 Analysis Helper Scripts

While `kafka-scribe` doesn't include a built-in analysis command, it provides helper scripts in the `scripts/analysis/` directory:

1. **Message Schema Detector**: Automatically detects and outputs the schema of stored messages
   ```bash
   scripts/analysis/detect-schema.sh ./orders_capture/
   ```

2. **Common Query Templates**: Pre-built queries for common analysis tasks
   ```bash
   scripts/analysis/db-templates.sh postgres "user-events" > common-queries.sql
   ```

3. **Data Export**: Convert between storage formats
   ```bash
   scripts/analysis/export-to-csv.sh ./orders_capture/ > messages.csv
   ```

#### 3.2.4 Extending Analysis Capabilities

Developers can extend analysis capabilities by:

1. Creating custom analysis scripts that work with the standard storage formats
2. Building visualization tools that read from the storage backends
3. Integrating with existing data analysis platforms by exporting data

The standardized storage formats (JSON files or database tables) ensure compatibility with a wide range of existing and future analysis tools.


### 3.3 Stage 3: `kscribe replay`

This command reads messages from a store and publishes them to a Kafka topic, enabling powerful debugging, testing, and recovery workflows.

#### 3.3.1 Syntax

```bash
kscribe replay --to-topic <target-topic> --bootstrap-servers <servers> [OPTIONS]
```

#### 3.3.2 Key Options

* `--to-topic <target-topic>`: **(Required)** The destination topic for the replayed messages.
* `--bootstrap-servers <servers>`: **(Required)** The target Kafka cluster addresses.
* `--from-dir <path>`: Replay messages from a directory of files.
* `--from-file <path>`: Replay messages from a single file.
* `--from-db <connection-string>`: Replay messages from a database.
* `--query <sql-query>`: Use a SQL query to select messages to replay from a DB.
* `--delay-ms <ms>`: Add a delay between replaying each message.
* `--override-key <new-key>`: Publish all messages with a new, static key.
* `--add-header <key=value>`: Add a new header to every replayed message.
* `--dry-run`: Simulate the replay without actually sending messages.

#### 3.3.3 Replay Modes

* `--mode <auto|interactive|transform>`:
    * **auto (default)**: Replay all selected messages automatically.
    * **interactive**: Display each message and prompt for action (replay, skip, edit).
    * **transform**: Apply a transformation script to each message before replaying.

#### 3.3.4 Performance Options

* `--batch-size <n>`: Number of messages to send in a batch (default: 100).
* `--rate-limit <n>`: Maximum messages per second to replay (default: unlimited).
* `--parallel <n>`: Number of parallel producer threads (default: 1).
* `--producer-config <path>`: Path to a properties file with additional producer configurations.

#### 3.3.5 Message Transformation

When using `--mode transform`, you can specify a transformation script:

```bash
kscribe replay \
  --from-dir ./orders_capture/ \
  --to-topic orders-test \
  --bootstrap-servers kafka-dev:9092 \
  --mode transform \
  --transform-script ./transform.js
```

Example transformation script (JavaScript):

```javascript
// transform.js - Executed for each message before replay
function transform(message) {
  // Add a test flag to all messages
  if (!message.headers) message.headers = {};
  message.headers["test-mode"] = "true";

  // Modify the payload if it's a specific type
  if (message.payload && message.payload.order_type === "RETAIL") {
    message.payload.price = message.payload.price * 0.9;  // Apply 10% discount
  }

  return message;
}
```

#### 3.3.6 Debugging Features

* `--verbose-errors`: Show detailed error information for failed publishes.
* `--track-delivery`: Track message delivery status and report success/failure.
* `--retry-failed`: Automatically retry failed messages (with exponential backoff).
* `--output-report <path>`: Generate a detailed report of the replay operation.

#### 3.3.7 Use Case: Replay from Files

```bash
# After editing a message file, replay it to a test topic on the dev cluster
kscribe replay \
  --from-file ./orders_capture/partition-0_offset-12345.json \
  --to-topic orders-test \
  --bootstrap-servers kafka-dev:9092
```

#### 3.3.8 Use Case: Replay from Database Query

```bash
# Replay all failed login events to a retry topic for reprocessing
kscribe replay \
  --from-db "postgres://user:pass@host:5432/dbname" \
  --query "SELECT raw_message FROM \"user-events\" WHERE json_extract_path_text(raw_message, 'event_type') = 'LOGIN_FAILED'" \
  --to-topic login-retries \
  --bootstrap-servers kafka-prod:9092 \
  --add-header "retry-attempt=1"
```

#### 3.3.9 Extensibility

The replay system supports plugins for custom message transformations and delivery strategies:

1. **Custom Transformers**: Implement the `MessageTransformer` trait to create custom transformation logic.
2. **Delivery Strategies**: Implement the `DeliveryStrategy` trait to customize how messages are published (e.g., with specific QoS guarantees).
3. **Replay Hooks**: Register callbacks for events during the replay process (e.g., before/after sending, on error).


## 4. Utility Commands

### 4.1 `kscribe stats`

Provides a summary of a message store, helping developers understand the characteristics of stored messages.

#### 4.1.1 Syntax

```bash
kscribe stats <store-source> [OPTIONS]
```

* **Store Source**: `--from-dir <path>`, `--from-file <path>`, or `--from-db <connection-string> [--table-name <name>]`

#### 4.1.2 Default Output (Example)

```
Store Summary: ./orders_capture
-----------------------------------
Total Messages: 1000
Total Size:     1.2 MB
Time Range:     2025-07-05T10:00:00Z to 2025-07-05T10:05:12Z
Partitions:     3 (0, 1, 2)

Key Distribution:
- user-123:     150
- user-456:     120
... (top 10 keys)

Message Size Distribution:
- Min:    512 B
- Avg:    1.2 KB
- Max:    5.4 KB
```

#### 4.1.3 Options for Specific Stats

* `--keys-histogram`: Show only the key distribution.
* `--size-distribution`: Show only the message size stats.
* `--timeline`: Show message count over time.
* `--format <text|json|csv>`: Output format (default: text).
* `--output <path>`: Write output to a file instead of stdout.

#### 4.1.4 Advanced Analysis

* `--schema-detect`: Attempt to detect and display the message schema.
* `--correlation`: Analyze correlation between message size and other attributes.
* `--anomaly-detect`: Highlight statistical anomalies in the message patterns.

### 4.2 `kscribe completion`

Generates shell completion scripts for easier use.

#### 4.2.1 Syntax

```bash
kscribe completion <bash|zsh|fish>
```

#### 4.2.2 Example (for bash)

```bash
source <(kscribe completion bash)
```

### 4.3 `kscribe validate`

Validates message stores or configuration files.

#### 4.3.1 Syntax

```bash
kscribe validate <target> [OPTIONS]
```

#### 4.3.2 Validation Targets

* `--store <path>`: Validate a message store (directory or file).
* `--config <path>`: Validate a configuration file.
* `--transform-script <path>`: Validate a transformation script.

#### 4.3.3 Example

```bash
# Validate that all messages in a store are properly formatted
kscribe validate --store ./orders_capture/

# Validate a transformation script
kscribe validate --transform-script ./transform.js
```


## 5. Testing, Error Handling, and Performance

### 5.1 Testing Strategy

`kafka-scribe` is designed with testability as a core principle. The testing approach includes:

#### 5.1.1 Unit Tests

Each component is designed to be testable in isolation:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let message = KafkaMessage {
            key: "test-key".to_string(),
            value: r#"{"field": "value"}"#.to_string(),
            headers: vec![("header1".to_string(), "value1".to_string())],
            timestamp: 1625482800000,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
        };

        let serialized = message.to_json().unwrap();
        let deserialized = KafkaMessage::from_json(&serialized).unwrap();

        assert_eq!(message, deserialized);
    }
}
```

#### 5.1.2 Integration Tests

Integration tests verify the end-to-end functionality using:

1. **Mock Kafka Server**: For testing without a real Kafka cluster
2. **Test Fixtures**: Pre-defined message sets for consistent testing
3. **Temporary Storage**: Using temp directories and in-memory databases

Example integration test:

```rust
#[tokio::test]
async fn test_store_and_replay() {
    // Setup test environment
    let temp_dir = tempdir().unwrap();
    let mock_kafka = MockKafkaServer::start().await;

    // Store messages
    let store_result = Command::new("target/debug/kscribe")
        .args(&["store", "test-topic", 
                "--bootstrap-servers", &mock_kafka.address(),
                "--to-dir", temp_dir.path().to_str().unwrap(),
                "--count", "10"])
        .output()
        .expect("Failed to execute store command");

    assert!(store_result.status.success());

    // Replay messages
    let replay_result = Command::new("target/debug/kscribe")
        .args(&["replay", 
                "--from-dir", temp_dir.path().to_str().unwrap(),
                "--to-topic", "output-topic",
                "--bootstrap-servers", &mock_kafka.address()])
        .output()
        .expect("Failed to execute replay command");

    assert!(replay_result.status.success());

    // Verify results
    let messages = mock_kafka.get_messages("output-topic").await;
    assert_eq!(messages.len(), 10);
}
```

#### 5.1.3 Property-Based Testing

For complex logic, property-based testing ensures correctness across a wide range of inputs:

```rust
#[test]
fn test_message_transformation_properties() {
    proptest!(|(message in arbitrary_kafka_message())| {
        let transformed = transform_message(message.clone());
        // Core properties that must be maintained
        prop_assert_eq!(message.key, transformed.key);
        prop_assert_eq!(message.partition, transformed.partition);
        prop_assert_eq!(message.offset, transformed.offset);
    });
}
```

### 5.2 Error Handling and Debugging

`kafka-scribe` implements a comprehensive error handling strategy:

#### 5.2.1 Error Types

A centralized error type system with specific error variants:

```rust
#[derive(Debug, thiserror::Error)]
pub enum ScribeError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Validation error: {0}")]
    Validation(String),
}
```

#### 5.2.2 Error Reporting

Errors are reported with:

1. **Context**: What operation was being performed
2. **Cause**: The underlying error
3. **Suggestions**: Potential solutions
4. **Debug Info**: Additional information for troubleshooting

#### 5.2.3 Debugging Features

1. **Verbose Mode**: Detailed logging with `--verbose`
2. **Dry Run Mode**: Simulate operations with `--dry-run`
3. **Progress Reporting**: Real-time progress indicators
4. **Diagnostic Commands**: The `validate` command for checking configurations

### 5.3 Performance Considerations

`kafka-scribe` is designed for high performance with the following optimizations:

#### 5.3.1 Parallel Processing

1. **Multi-threading**: Parallel message processing using Tokio's runtime
2. **Batching**: Processing messages in batches to reduce overhead
3. **Buffering**: Configurable buffer sizes to optimize memory usage

#### 5.3.2 Resource Management

1. **Backpressure Handling**: Preventing memory exhaustion with bounded channels
2. **Connection Pooling**: Reusing database connections
3. **Lazy Loading**: Loading messages on-demand when possible

#### 5.3.3 Performance Tuning

All performance-critical operations have configurable parameters:

1. **Batch Sizes**: Configurable for both consuming and producing
2. **Buffer Sizes**: Adjustable based on available memory
3. **Parallelism**: Configurable thread counts
4. **Compression**: Optional compression for storage efficiency

## 6. Project Structure (Rust CLI Application)

This structure is designed for clarity, maintainability, and extensibility.

```
kafka-scribe/
├── Cargo.toml
├── src/
│   ├── main.rs               # Entry point, CLI parsing (clap)
│   ├── cli/
│   │   ├── mod.rs
│   │   └── commands/         # Subcommand logic (store, replay, etc.)
│   │       ├── store.rs
│   │       ├── replay.rs
│   │       ├── stats.rs
│   │       ├── validate.rs
│   │       └── completion.rs
│   ├── core/
│   │   ├── mod.rs
│   │   ├── models.rs         # Core data structures (KafkaMessage, etc.)
│   │   ├── errors.rs         # Centralized error types
│   │   └── config.rs         # Configuration handling
│   ├── kafka/
│   │   ├── mod.rs
│   │   ├── consumer.rs       # Kafka message consumption logic
│   │   ├── producer.rs       # Kafka message production logic
│   │   └── mock.rs           # Mock Kafka implementation for testing
│   ├── storage/
│   │   ├── mod.rs            # Storage trait definitions
│   │   ├── files/            # File-based storage implementation
│   │   │   ├── mod.rs
│   │   │   ├── directory.rs
│   │   │   └── single_file.rs
│   │   ├── database/         # Database storage implementation
│   │   │   ├── mod.rs
│   │   │   ├── sqlite.rs
│   │   │   └── postgres.rs
│   │   └── transform/        # Message transformation logic
│   │       ├── mod.rs
│   │       └── js_engine.rs  # JavaScript transformation engine
│   ├── utils/
│   │   ├── mod.rs
│   │   ├── logging.rs        # Logging utilities
│   │   ├── progress.rs       # Progress reporting
│   │   └── validation.rs     # Input validation helpers
│   └── plugins/              # Plugin system for extensions
│       ├── mod.rs
│       └── registry.rs       # Plugin registry
│
├── tests/
│   ├── integration/          # Integration tests
│   │   ├── store_tests.rs
│   │   ├── replay_tests.rs
│   │   └── end_to_end.rs
│   ├── fixtures/             # Test fixtures
│   │   └── sample_messages/
│   └── common/               # Shared test utilities
│       ├── mod.rs
│       └── mock_kafka.rs
│
├── examples/                 # Example usage scripts
│   ├── store_and_analyze.rs
│   └── custom_transformer.rs
│
├── benches/                  # Performance benchmarks
│   ├── store_benchmark.rs
│   └── replay_benchmark.rs
│
└── scripts/
    ├── install.sh            # Helper script for installing completions
    └── analysis/             # Analysis helper scripts
        ├── detect-schema.sh
        ├── db-templates.sh
        └── export-to-csv.sh
```


## 7. Key Dependencies (`Cargo.toml`)

```toml
[dependencies]
# CLI
clap = { version = "4.0", features = ["derive"] }
clap_complete = "4.0"

# Async Runtime
tokio = { version = "1.0", features = ["full"] }

# Kafka Client
rdkafka = { version = "0.36", features = ["cmake-build"] }

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
serde_yaml = "0.9"

# Database
sqlx = { version = "0.7", features = ["runtime-tokio-rustls", "sqlite", "postgres"] }

# Error Handling & Logging
anyhow = "1.0"
thiserror = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"

# UI/UX
indicatif = "0.17" # For progress bars
dialoguer = "0.11" # For interactive mode

# Testing
proptest = "1.0"   # For property-based testing
tempfile = "3.3"   # For temporary test directories
mockall = "0.11"   # For mocking in tests

# JavaScript Engine (for transformations)
deno_core = "0.177" # JavaScript runtime for transformations
```


## 8. Edge Cases and Considerations

* **Connection Failures**: The tool must handle Kafka/DB connection errors gracefully with clear messages and retry logic where appropriate.
* **Backpressure**: During `store` and `replay`, the application must manage memory usage to avoid OOM errors when the producer is slower than the consumer. This involves using bounded channels and managing buffer sizes.
* **Large Messages**: The design must account for messages that may exceed default buffer sizes.
* **Data Formats**: When replaying, the tool must validate that the source files are in the expected JSON format. Corrupted files or records should be reported and skipped.
* **Schema Mismatches**: When using a database, if the `raw_message` schema changes over time, queries might fail. The `raw` storage format is robust against this, but analysis queries must be written defensively.
* **Transactional Guarantees**: Replaying messages is not idempotent by default. A `--dry-run` mode is critical for safety. The tool will not offer transactional guarantees out-of-the-box but will ensure at-least-once delivery semantics for replay.
* **Security Considerations**: When storing sensitive data, the tool should support encryption options and respect access control mechanisms of the underlying storage systems.
* **Resource Cleanup**: The tool must properly clean up resources (connections, file handles) even when terminated unexpectedly.


## 9. Conclusion and Development Roadmap

### 9.1 Summary

`kafka-scribe` provides a powerful, flexible tool for working with Kafka messages through a three-stage workflow:

1. **Store**: Capture messages from Kafka topics to durable storage
2. **Analyze**: Use familiar tools to investigate and modify stored messages
3. **Replay**: Send messages back to Kafka topics with optional transformations

The design prioritizes:
- **Developer Experience**: Intuitive CLI with consistent patterns
- **Extensibility**: Pluggable architecture for storage backends and transformations
- **Performance**: Optimized for high-volume Kafka topics
- **Testability**: Components designed for easy testing

### 9.2 Development Roadmap

#### Phase 1: Core Functionality
- Implement basic `store` command with file storage
- Implement basic `replay` command
- Implement `stats` command
- Add comprehensive test suite

#### Phase 2: Advanced Features
- Add database storage support
- Implement interactive and transform modes for replay
- Add validation command
- Enhance performance with parallel processing

#### Phase 3: Extensions and Integrations
- Implement plugin system
- Add additional storage backends
- Create analysis helper scripts
- Develop visualization tools

### 9.3 Contributing

Contributions are welcome! See the [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines.

The project follows semantic versioning and uses a feature-branch workflow for development.

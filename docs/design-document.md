# Design Document: kafka-scribe (Stateless Version)

## 1. Introduction \& Philosophy

**kafka-scribe** is a high-performance command-line interface (CLI) tool designed for developers and operators working with Apache Kafka. Its primary purpose is to "prescribe" messages from one state to another—capturing them from a topic, enabling analysis and modification, and replaying them.

The core philosophy is built on three distinct, sequential stages:

1. **Store**: Reliably capture messages from a Kafka topic into a durable storage format (local files or a database). This creates a static, analyzable snapshot of a topic's data.
2. **Analyze**: Investigate the stored messages using powerful, familiar external tools. For file-based storage, this means using `grep`, `jq`, `awk`, etc. For database storage, this means using standard SQL clients.
3. **Replay**: Replay a selection of the stored messages (original or modified) back into a Kafka topic for debugging, testing, or recovery.

This approach provides a simple yet powerful workflow. The tool operates statelessly, meaning every command is self-contained and all required parameters (like server addresses) are passed directly as arguments, ensuring clarity and scriptability.

## 2. CLI Command Structure \& Design

The CLI is designed to be intuitive and powerful, using a `verb + noun` structure.

**Main Commands:**

* `kscribe store`: Capture messages from a topic to a store.
* `kscribe replay`: Replay messages from a store to a topic.
* `kscribe stats`: Display statistics about a message store.
* `kscribe completion`: Generate shell completion scripts.


## 3. The Three Stages of `kafka-scribe`

### Stage 1: `kscribe store`

This command connects to a Kafka topic and writes its messages to a specified storage destination.

**Syntax:**

```bash
kscribe store <topic> --bootstrap-servers <servers> [OPTIONS]
```

**Key Options:**

* `--bootstrap-servers <servers>`: **(Required)** Comma-separated list of Kafka broker addresses (e.g., `kafka1:9092,kafka2:9092`).
* `--to-dir <path>`: Store messages as files in a directory (one file per message).
* `--to-file <path>`: Store messages into a single file.
* `--to-db <connection-string>`: Store messages in a database.
* `--table-name <name>`: Table name for database storage (defaults to topic name).

**Source Range Selection:**

* `--from-beginning`: Start from the earliest offset.
* `--from-offset <offset>`: Start from a specific offset.
* `--from-timestamp <timestamp>`: Start from a specific time.
* `--count <n>`: Capture exactly `n` messages.
* `--until-offset <offset>`: Capture until a specific offset is reached.
* `--until-timestamp <timestamp>`: Capture until a specific time is reached.
* `--live`: Continue capturing messages indefinitely.

**Filtering:**

* `--partitions <p1,p2,...>`: Capture from specific partitions only.
* `--key-regex <pattern>`: Filter messages by a regex on the key.
* `--header <key=value>`: Filter messages by a specific header.

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


### Stage 2: Analyze (Using External Tools)

`kafka-scribe` intentionally omits a built-in `investigate` command. Instead, you use standard tools to analyze the stored data.

**File-based Analysis:**

Once messages are stored in `./orders_capture`, you can use command-line tools:

```bash
# Find all messages containing an error message using grep
grep -r "error" ./orders_capture/

# Extract and count all unique product IDs using jq
cat ./orders_capture/*.json | jq -r '.payload.product_id' | sort | uniq -c
```

**Database-based Analysis:**

Connect to your database (e.g., `psql "postgres://user:pass@host:5432/dbname"`) and run SQL queries:

```sql
-- Find all events for a specific user
SELECT * FROM "user-events" WHERE key = 'user-123';

-- Prepare a reply set by selecting failed login attempts
SELECT raw_message FROM "user-events" WHERE json_extract_path_text(raw_message, 'event_type') = 'LOGIN_FAILED';
```


### Stage 3: `kscribe replay`

This command reads messages from a store and publishes them to a Kafka topic.

**Syntax:**

```bash
kscribe replay --to-topic <target-topic> --bootstrap-servers <servers> [OPTIONS]
```

**Key Options:**

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

**Replay Modes:**

* `--mode <auto|interactive>`:
    * **auto (default)**: Replay all selected messages automatically.
    * **interactive**: Display each message and prompt for action (replay, skip, edit).

**Use Case: Replay from Files**

```bash
# After editing a message file, replay it to a test topic on the dev cluster
kscribe replay \
  --from-file ./orders_capture/partition-0_offset-12345.json \
  --to-topic orders-test \
  --bootstrap-servers kafka-dev:9092
```

**Use Case: Replay from Database Query**

```bash
# Replay all failed login events to a retry topic for reprocessing
kscribe replay \
  --from-db "postgres://user:pass@host:5432/dbname" \
  --query "SELECT raw_message FROM \"user-events\" WHERE json_extract_path_text(raw_message, 'event_type') = 'LOGIN_FAILED'" \
  --to-topic login-retries \
  --bootstrap-servers kafka-prod:9092 \
  --add-header "retry-attempt=1"
```


## 4. Utility Commands

### `kscribe stats`

Provides a summary of a message store. Running it without arguments provides a full overview.

**Syntax:**

```bash
kscribe stats <store-source> [OPTIONS]
```

* **Store Source**: `--from-dir <path>`, `--from-file <path>`, or `--from-db <connection-string> [--table-name <name>]`

**Default Output (Example):**

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

**Options for Specific Stats:**

* `--keys-histogram`: Show only the key distribution.
* `--size-distribution`: Show only the message size stats.
* `--timeline`: Show message count over time.


### `kscribe completion`

Generates shell completion scripts for easier use.

**Syntax:**

```bash
kscribe completion <bash|zsh|fish>
```

**Example (for bash):**

```bash
source <(kscribe completion bash)
```


## 5. Project Structure (Rust CLI Application)

This structure is simplified for a CLI tool, promoting clarity and maintainability.

```
kafka-scribe/
├── Cargo.toml
├── src/
│   ├── main.rs               # Entry point, CLI parsing (clap)
│   ├── cli/
│   │   ├── mod.rs
│   │   └── commands/         # Subcommand logic (store, replay, etc.)
│   │       ├── store.rs
│   │       └── ...
│   ├── core/
│   │   ├── mod.rs
│   │   ├── models.rs         # Core data structures (KafkaMessage, etc.)
│   │   └── errors.rs         # Centralized error types
│   ├── kafka/
│   │   ├── mod.rs
│   │   ├── consumer.rs       # Kafka message consumption logic
│   │   └── producer.rs       # Kafka message production logic
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── files.rs          # File-based storage adapter
│   │   └── database.rs       # SQLx-based database adapter
│   └── utils.rs              # Shared helper functions
│
├── tests/
│   ├── integration_tests.rs  # End-to-end tests for CLI commands
│
└── scripts/
    └── install.sh            # Helper script for installing completions
```


## 6. Key Dependencies (`Cargo.toml`)

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
```


## 7. Edge Cases and Considerations

* **Connection Failures**: The tool must handle Kafka/DB connection errors gracefully with clear messages and retry logic where appropriate.
* **Backpressure**: During `store` and `replay`, the application must manage memory usage to avoid OOM errors when the producer is slower than the consumer. This involves using bounded channels and managing buffer sizes.
* **Large Messages**: The design must account for messages that may exceed default buffer sizes.
* **Data Formats**: When replaying, the tool must validate that the source files are in the expected JSON format. Corrupted files or records should be reported and skipped.
* **Schema Mismatches**: When using a database, if the `raw_message` schema changes over time, queries might fail. The `raw` storage format is robust against this, but analysis queries must be written defensively.
* **Transactional Guarantees**: Replaying messages is not idempotent by default. A `--dry-run` mode is critical for safety. The tool will not offer transactional guarantees out-of-the-box but will ensure at-least-once delivery semantics for replay.

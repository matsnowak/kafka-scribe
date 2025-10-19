# Kafka-Scribe Integration Test Suite

This directory contains integration tests for the kafka-scribe CLI tool. These tests execute the actual compiled binary rather than just testing individual modules, ensuring end-to-end functionality validation.

## Test Structure

The test suite is organized as follows:

```
tests/
├── integration/
│   ├── mod.rs                      # Integration test module definition
│   ├── store_command_tests.rs      # Tests for the `store` command
│   ├── replay_tests.rs             # Tests for the `replay` command
│   ├── stats_tests.rs              # Tests for the `stats` command
│   ├── e2e_tests.rs                # End-to-end workflow tests
│   ├── performance_tests.rs        # Performance and throughput tests
│   └── schema_validation_tests.rs  # Schema validation tests
├── common/
│   ├── mod.rs                      # Common utilities module definition
│   ├── kafka_setup.rs              # Utilities for setting up Kafka
│   ├── test_data.rs                # Utilities for generating test data
│   ├── cli_helpers.rs              # Utilities for executing CLI commands
│   └── dir_helpers.rs              # Utilities for directory operations
└── fixtures/
    ├── docker-compose.yml          # Docker Compose configuration for Kafka
    └── sample_messages/            # Sample messages for testing
```

## Test Coverage

The integration tests cover the following functionality:

### Store Command Tests

- **Basic Functionality**
  - Storing messages from a Kafka topic to a directory
  - Storing messages from a Kafka topic to a single file

- **Message Filtering**
  - Filtering messages by key regex
  - Filtering messages by header
  - Filtering messages by partition

- **Range Selection**
  - Limiting the number of messages stored
  - Filtering messages by timestamp
  - Filtering by offset ranges

- **Live Mode**
  - Continuous consumption with timeout
  - Capturing new messages as they arrive

- **Error Handling**
  - Invalid bootstrap server
  - Non-existent topic

- **Edge Cases**
  - Binary message data
  - Compressed output

### Replay Command Tests

- **Basic Functionality**
  - Replaying messages from files to a Kafka topic
  - Replaying from a directory of message files

- **Message Transformation**
  - Adding and modifying headers
  - Overriding message keys
  - Applying transformation scripts

- **Error Handling**
  - Invalid bootstrap server
  - Invalid message format

### Stats Command Tests

- **Basic Functionality**
  - Generating statistics for stored messages
  - Different output formats (text, JSON, CSV)

- **Analysis**
  - Message count by partition
  - Timestamp distribution
  - Key and header analysis

### End-to-End Workflow Tests

- **Complete Workflows**
  - Store-analyze-replay pipelines
  - Filtering and transformation workflows
  - Data integrity verification

### Performance Tests

- **Throughput Measurement**
  - Message processing rates
  - Different batch sizes
  - Parallel processing

### Schema Validation Tests

- **Format Validation**
  - JSON schema validation
  - Avro schema validation
  - Schema evolution scenarios

## Running the Tests

To run the integration tests, use the following command:

```bash
cargo test --test integration
```

To run a specific test, use:

```bash
cargo test --test integration -- test_basic_store_to_directory
```

## Test Environment Setup

### Requirements

The integration tests require:

1. **Docker**: Used to run Kafka and Zookeeper containers
2. **Rust Toolchain**: The tests are written in Rust and require a Rust toolchain
3. **Cargo**: For building the project and running tests
4. **Docker Compose**: For managing the Kafka and Zookeeper containers

### Environment Setup Steps

Follow these steps to set up your test environment:

1. **Install Docker and Docker Compose**:
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - Docker Compose is included with Docker Desktop for Windows and Mac
   - For Linux: `sudo apt-get install docker-compose` or equivalent

2. **Install Rust and Cargo**:
   - [Rust Installation Guide](https://www.rust-lang.org/tools/install)
   - Typically: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

3. **Build the Project**:
   ```bash
   cargo build
   ```

4. **Start Kafka**:
   ```bash
   cd tests/fixtures
   docker compose up -d
   # or if you're using the older docker-compose command
   docker-compose up -d
   ```

   The Docker Compose configuration is located at `tests/fixtures/docker-compose.yml`.

5. **Verify Kafka is Running**:
   ```bash
   docker ps
   ```
   You should see containers for Kafka and Zookeeper running.

### Stopping the Test Environment

To stop Kafka after running the tests:

```bash
cd tests/fixtures
docker compose down
# or if you're using the older docker-compose command
docker-compose down
```

### Automatic Kafka Setup

The test utilities include code to automatically start Kafka if it's not already running. This is handled by the `KafkaTestContext::ensure_kafka_is_running()` method in `tests/common/kafka_setup.rs`.

However, it's generally more reliable to start Kafka manually before running the tests, especially when running multiple test suites.

## Troubleshooting

### Common Issues

1. **Docker not running**:
   - Error: `Failed to connect to Docker daemon`
   - Solution: Start Docker with `systemctl start docker` or equivalent

2. **Docker Compose command not found**:
   - Error: `No such file or directory (os error 2)` when executing docker-compose
   - Solution: The code will automatically try both `docker compose` (new style) and `docker-compose` (old style) commands

3. **Port conflicts**:
   - Error: `Failed to bind to address`
   - Solution: Ensure no other services are using the required ports

4. **Timeouts**:
   - Error: `Timed out waiting for Kafka to be ready`
   - Solution: Increase the timeout in `kafka_setup.rs`

5. **Permission issues**:
   - Error: `Permission denied`
   - Solution: Ensure the user has permission to create files in the test directories

6. **Zookeeper container fails to start**:
   - Error: `dependency failed to start: container zookeeper exited (1)` or `dependency failed to start: container zookeeper is unhealthy`
   - Solution: The docker-compose.yml file has been updated with the following improvements:
     - Using Confluent Platform 6.2.1 instead of 7.3.0, which is more stable
     - Removed the obsolete `version` attribute
     - Added additional Zookeeper configuration parameters (SERVER_ID, MAX_CLIENT_CNXNS, 4LW_COMMANDS_WHITELIST)
     - Added memory limits (512MB for Zookeeper, 1GB for Kafka)
     - Increased healthcheck start periods to 30s to give containers more time to initialize
     - Added additional Kafka parameters for better testing (AUTO_CREATE_TOPICS_ENABLE, LOG_RETENTION_MS)

7. **Connection refused when connecting to Kafka**:
   - Error: `localhost:29092/bootstrap: Connect to ipv4#127.0.0.1:29092 failed: Connection refused`
   - Solution: The docker-compose.yml file has been updated with the following improvements:
     - Added explicit KAFKA_LISTENERS configuration to bind to all interfaces (0.0.0.0)
     - Added KAFKA_INTER_BROKER_LISTENER_NAME to specify which listener to use for broker communication
     - Added KAFKA_ALLOW_PLAINTEXT_LISTENER to explicitly allow plaintext listeners

### Debugging Tests

To enable debug logging during tests, set the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug cargo test --test integration
```

## Extending the Test Suite

To add new tests:

1. For new store command tests, add them to `tests/integration/store_command_tests.rs`
2. For tests of other commands, create new files in the `tests/integration/` directory
3. Update `tests/integration/mod.rs` to include any new test modules
4. Update `tests/integration.rs` to import and re-export the new test modules

### Common Test Patterns

Here are some common patterns used in the integration tests:

#### Basic Command Test Pattern

```rust
#[tokio::test]
async fn test_basic_command() -> Result<()> {
    // Initialize test environment
    let docker = init_test_environment();

    // Set up Kafka
    let kafka = KafkaTestContext::new(docker).await?;
    let topic = "test-topic-name";
    kafka.create_topic(topic, 1).await?;

    // Produce test messages
    let messages = generate_test_messages(10);
    kafka.produce_messages(topic, &messages, None).await?;

    // Create temporary directory for output
    let temp_dir = TestDirectory::new()?;

    // Run the command
    let args = vec![
        "command",
        "arg1",
        "arg2",
        "--option1",
        "value1",
    ];
    let output = run_command(args)?;

    // Validate command output
    validate_success(&output)?;

    // Validate results
    validate_results(&temp_dir, 10, |msg| {
        // Custom validation logic
        Ok(())
    })?;

    Ok(())
}
```

#### Testing with Filters

```rust
#[tokio::test]
async fn test_with_filters() -> Result<()> {
    // ... setup code ...

    // Run command with filters
    let args = vec![
        "command",
        "--filter-option",
        "filter-value",
    ];
    let output = run_command(args)?;

    // Validate filtered results
    validate_results(&temp_dir, expected_count, |msg| {
        // Verify filter was applied correctly
        if !msg.matches_filter_criteria() {
            anyhow::bail!("Message does not match filter criteria");
        }
        Ok(())
    })?;

    Ok(())
}
```

#### Testing Asynchronous Operations

```rust
#[tokio::test]
async fn test_async_operation() -> Result<()> {
    // ... setup code ...

    // Start a background task
    let background_handle = tokio::spawn(async move {
        // Background operations
        tokio::time::sleep(Duration::from_secs(2)).await;
        // More operations...
        Ok(())
    });

    // Run the main command
    let output = run_command(args)?;

    // Wait for background task to complete
    let background_result = background_handle.await??;

    // Validate results
    // ...

    Ok(())
}
```

#### Testing Error Conditions

```rust
#[tokio::test]
async fn test_error_condition() -> Result<()> {
    // ... setup code ...

    // Run command with invalid parameters
    let args = vec![
        "command",
        "--invalid-option",
        "value",
    ];
    let output = run_command(args)?;

    // Validate command failed as expected
    assert!(!output.status.success(), "Command should have failed");

    // Validate error message
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("expected error message"), 
            "Error message doesn't contain expected text: {}", stderr);

    Ok(())
}
```

## Performance Considerations

The integration tests connect to a running Kafka instance, which should be started manually before running the tests. To improve performance:

1. Use the `--test` flag to run only the integration tests you need
2. Consider running tests in parallel with `--parallel`
3. Keep the Kafka instance running between test runs to avoid startup overhead

## Test Fixtures and Utilities

The test suite includes several utilities and fixtures to simplify test creation and maintenance:

### Test Data Generation

The `tests/common/test_data.rs` module provides utilities for generating test data:

- `TestDataGenerator`: A class for generating deterministic test messages with a specific seed for reproducibility
- `generate_test_messages()`: Creates a set of standard test messages with JSON values
- `generate_binary_test_messages()`: Creates messages with binary data for testing binary handling
- `generate_key_filtered_test_messages()`: Creates messages with specific key patterns for testing key filtering
- `generate_header_filtered_test_messages()`: Creates messages with specific headers for testing header filtering
- `generate_timestamped_test_messages()`: Creates messages with different timestamps for testing timestamp filtering

The `TestDataGenerator` allows you to create different types of messages (orders, logs, binary data) with deterministic content, which is useful for creating reproducible tests.

### Kafka Test Utilities

The `tests/common/kafka_setup.rs` module provides utilities for setting up Kafka:

- `KafkaTestContext`: A wrapper around a Kafka connection for testing
- Methods for creating topics, producing messages, and consuming messages
- Automatic Kafka startup and connection management

The `KafkaTestContext` handles all the details of connecting to Kafka, creating topics, and producing/consuming messages. It also includes automatic startup of Kafka if it's not already running.

### CLI Execution Utilities

The `tests/common/cli_helpers.rs` module provides utilities for executing CLI commands:

- `CliExecutor`: A wrapper for executing kafka-scribe CLI commands with timeout handling
- `TestDirectory`: A wrapper around a temporary directory for testing
- Functions for validating command output and stored messages

The `CliExecutor` makes it easy to run kafka-scribe commands and validate their output. The `TestDirectory` class provides methods for working with temporary directories and validating stored messages.

### Directory Utilities

The `tests/common/dir_helpers.rs` module provides utilities for directory operations:

- `create_temp_dir()`: Creates a temporary directory for test data
- `compare_directories()`: Compares two directories to check if they contain the same files
- `load_json_files()`: Loads and parses JSON files from a directory
- `compare_json_values()`: Compares JSON values, ignoring specific fields

These utilities make it easy to work with directories and files in tests, including creating temporary directories, comparing directory contents, and loading/comparing JSON files.

## CI/CD Integration

These tests are designed to run in CI/CD environments. In your CI configuration:

1. Ensure Docker is available
2. Start Kafka before running the tests
3. Set appropriate timeouts for test execution
4. Consider caching Docker images to speed up test runs

Example GitHub Actions configuration:

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      # Ensure Docker is available
      docker:
        image: docker:dind
        options: --privileged
    steps:
      - uses: actions/checkout@v2
      - name: Set up Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Install docker-compose
        run: |
          sudo apt-get update
          sudo apt-get install -y docker-compose
      - name: Start Kafka
        run: |
          cd tests/fixtures
          docker-compose up -d
          # Wait for Kafka to be ready
          sleep 30
      - name: Run integration tests
        run: cargo test --test integration
        env:
          RUST_LOG: info
      - name: Stop Kafka
        run: |
          cd tests/fixtures
          docker-compose down
```

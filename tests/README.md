# Kafka-Scribe Integration Test Suite

This directory contains integration tests for the kafka-scribe CLI tool. These tests execute the actual compiled binary rather than just testing individual modules, ensuring end-to-end functionality validation.

## Test Structure

The test suite is organized as follows:

```
tests/
├── integration/
│   ├── mod.rs                  # Integration test module definition
│   └── store_command_tests.rs  # Tests for the `store` command
├── common/
│   ├── mod.rs                  # Common utilities module definition
│   ├── kafka_setup.rs          # Utilities for setting up Kafka
│   ├── test_data.rs            # Utilities for generating test data
│   └── cli_helpers.rs          # Utilities for executing CLI commands
└── fixtures/
    ├── docker-compose.yml      # Docker Compose configuration for Kafka
    └── sample_messages/        # Sample messages for testing
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

- **Error Handling**
  - Invalid bootstrap server
  - Non-existent topic

- **Edge Cases**
  - Binary message data

## Running the Tests

To run the integration tests, use the following command:

```bash
cargo test --test integration
```

To run a specific test, use:

```bash
cargo test --test integration -- test_basic_store_to_directory
```

## Test Environment Requirements

The integration tests require:

1. **Running Kafka Instance**: The tests assume that Kafka is already running and accessible at `localhost:29092`.
2. **Rust**: The tests are written in Rust and require a Rust toolchain.

### Starting Kafka Manually

Before running the tests, you need to start Kafka manually using the provided Docker Compose configuration:

```bash
cd tests/fixtures
docker compose up -d
# or if you're using the older docker-compose command
docker-compose up -d
```

The docker-compose configuration is located at `tests/fixtures/docker-compose.yml`.

To stop Kafka after running the tests:

```bash
cd tests/fixtures
docker compose down
# or if you're using the older docker-compose command
docker-compose down
```

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

## Performance Considerations

The integration tests connect to a running Kafka instance, which should be started manually before running the tests. To improve performance:

1. Use the `--test` flag to run only the integration tests you need
2. Consider running tests in parallel with `--parallel`
3. Keep the Kafka instance running between test runs to avoid startup overhead

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

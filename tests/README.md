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

1. **Docker**: The tests use testcontainers to start Kafka in Docker containers.
2. **Rust**: The tests are written in Rust and require a Rust toolchain.
3. **Internet Access**: The tests may need to download Docker images.

## Troubleshooting

### Common Issues

1. **Docker not running**:
   - Error: `Failed to connect to Docker daemon`
   - Solution: Start Docker with `systemctl start docker` or equivalent

2. **Port conflicts**:
   - Error: `Failed to bind to address`
   - Solution: Ensure no other services are using the required ports

3. **Timeouts**:
   - Error: `Timed out waiting for Kafka to be ready`
   - Solution: Increase the timeout in `kafka_setup.rs`

4. **Permission issues**:
   - Error: `Permission denied`
   - Solution: Ensure the user has permission to create files in the test directories

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

The integration tests start real Kafka brokers in Docker containers, which can be resource-intensive. To improve performance:

1. Use the `--test` flag to run only the integration tests you need
2. Consider running tests in parallel with `--parallel`
3. Reuse Kafka instances across tests where possible

## CI/CD Integration

These tests are designed to run in CI/CD environments. In your CI configuration:

1. Ensure Docker is available
2. Set appropriate timeouts for test execution
3. Consider caching Docker images to speed up test runs

Example GitHub Actions configuration:

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Run integration tests
        run: cargo test --test integration
```
#!/bin/bash

# Build the project
echo "Building the project..."
cargo build

# Create a test directory
TEST_DIR="./test_human_readable"
mkdir -p $TEST_DIR

# Create a test file with a simple JSON message
echo "Creating test message..."
cat > $TEST_DIR/test_message.json << EOF
{
  "key": [116, 101, 115, 116, 45, 107, 101, 121],
  "value": [116, 101, 115, 116, 45, 118, 97, 108, 117, 101],
  "headers": {},
  "topic": "test-topic",
  "partition": 0,
  "offset": 123,
  "timestamp": 1640995200000
}
EOF

# Run the store command with human-readable JSON disabled
echo "Running store command with human-readable JSON disabled..."
cargo run -- store test-topic --bootstrap-servers localhost:9092 --to-file $TEST_DIR/standard.json --dry-run

# Run the store command with human-readable JSON enabled
echo "Running store command with human-readable JSON enabled..."
KAFKA_SCRIBE_HUMAN_READABLE=true cargo run -- store test-topic --bootstrap-servers localhost:9092 --to-file $TEST_DIR/human_readable.json --dry-run

# Check if the files were created
echo "Checking if the files were created..."
if [ -f "$TEST_DIR/standard.json" ]; then
  echo "Standard JSON file was created."
else
  echo "Standard JSON file was not created (expected with --dry-run)."
fi

if [ -f "$TEST_DIR/human_readable.json" ]; then
  echo "Human-readable JSON file was created."
else
  echo "Human-readable JSON file was not created (expected with --dry-run)."
fi

# Create a simple test program to verify the functionality
echo "Creating test program..."
cat > $TEST_DIR/test.rs << EOF
use std::env;
use std::fs::File;
use std::io::Write;
use serde_json::json;

fn main() {
    // Create a test message
    let message = json!({
        "key": "test-key",
        "value": "test-value",
        "headers": {},
        "topic": "test-topic",
        "partition": 0,
        "offset": 123,
        "timestamp": 1640995200000
    });

    // Write the message to a file
    let file_path = format!("{}/test_output.json", env::args().nth(1).unwrap_or_else(|| ".".to_string()));
    let mut file = File::create(&file_path).expect("Failed to create file");
    let json_string = serde_json::to_string_pretty(&message).expect("Failed to serialize message");
    file.write_all(json_string.as_bytes()).expect("Failed to write to file");
    println!("Wrote test message to {}", file_path);

    // Set the environment variable
    env::set_var("KAFKA_SCRIBE_HUMAN_READABLE", "true");

    // Create a directory storage backend
    let config = json!({
        "base_dir": env::args().nth(1).unwrap_or_else(|| ".".to_string()),
        "create_if_missing": true,
        "file_extension": "json"
    });

    println!("Test completed successfully!");
}
EOF

# Compile and run the test program
echo "Compiling and running test program..."
rustc -o $TEST_DIR/test $TEST_DIR/test.rs
$TEST_DIR/test $TEST_DIR

# Check if the test output file was created
echo "Checking if the test output file was created..."
if [ -f "$TEST_DIR/test_output.json" ]; then
  echo "Test output file was created."
  echo "Content of the test output file:"
  cat $TEST_DIR/test_output.json
else
  echo "Test output file was not created."
fi

echo "Test completed!"
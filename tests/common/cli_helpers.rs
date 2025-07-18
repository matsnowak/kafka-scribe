//! Utilities for executing CLI commands and validating their output.
//!
//! This module provides functions for running the kafka-scribe binary
//! with different arguments and validating the output and exit code.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::{tempdir, TempDir};
use tracing::{debug, info};

use crate::common::test_data::JsonMessage;

/// A wrapper around a temporary directory for testing.
pub struct TestDirectory {
    /// The temporary directory.
    pub dir: TempDir,
}

impl TestDirectory {
    /// Creates a new temporary directory for testing.
    pub fn new() -> Result<Self> {
        let dir = tempdir().context("Failed to create temporary directory")?;
        info!("Created temporary directory: {}", dir.path().display());
        Ok(Self { dir })
    }

    /// Returns the path to the temporary directory.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// Creates a subdirectory in the temporary directory.
    pub fn create_subdir(&self, name: &str) -> Result<PathBuf> {
        let path = self.dir.path().join(name);
        fs::create_dir_all(&path).context("Failed to create subdirectory")?;
        Ok(path)
    }

    /// Counts the number of files in the temporary directory.
    pub fn count_files(&self) -> Result<usize> {
        let count = fs::read_dir(self.dir.path())
            .context("Failed to read directory")?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().map(|t| t.is_file()).unwrap_or(false))
            .count();
        Ok(count)
    }

    /// Reads all JSON messages from files in the temporary directory, recursively.
    pub fn read_json_messages(&self) -> Result<Vec<JsonMessage>> {
        let mut messages = Vec::new();
        self.read_json_messages_recursive(self.dir.path(), &mut messages)?;
        Ok(messages)
    }

    /// Helper method to recursively read JSON messages from files in a directory.
    fn read_json_messages_recursive(&self, dir: &Path, messages: &mut Vec<JsonMessage>) -> Result<()> {
        for entry in fs::read_dir(dir).context(format!("Failed to read directory: {}", dir.display()))? {
            let entry = entry.context("Failed to read directory entry")?;
            let path = entry.path();
            let file_type = entry.file_type().context("Failed to get file type")?;

            if file_type.is_dir() {
                // Recursively process subdirectories
                self.read_json_messages_recursive(&path, messages)?;
            } else if file_type.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                // Process JSON files
                let file = File::open(&path).context(format!("Failed to open file: {}", path.display()))?;
                let mut reader = BufReader::new(file);

                // Read the entire file content
                let mut content = String::new();
                reader.read_to_string(&mut content).context(format!("Failed to read file: {}", path.display()))?;

                // Parse the JSON content
                let json: serde_json::Value = 
                    serde_json::from_str(&content).context(format!("Failed to parse JSON from file: {}", path.display()))?;

                // Extract fields for JsonMessage
                let key = if json["key"].is_array() {
                    // Convert byte array to string
                    let bytes: Vec<u8> = json["key"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect();

                    if !bytes.is_empty() {
                        Some(String::from_utf8_lossy(&bytes).to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Extract value
                let value = if json["value"].is_array() {
                    // Try to parse the value bytes as JSON
                    let bytes: Vec<u8> = json["value"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect();

                    if !bytes.is_empty() {
                        match serde_json::from_slice::<serde_json::Value>(&bytes) {
                            Ok(parsed_json) => parsed_json,
                            Err(_) => serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string())
                        }
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    json["value"].clone()
                };

                // Extract headers
                let headers = if json["headers"].is_object() && !json["headers"].as_object().unwrap().is_empty() {
                    let mut map = HashMap::new();
                    for (k, v) in json["headers"].as_object().unwrap() {
                        if let Some(s) = v.as_str() {
                            map.insert(k.clone(), s.to_string());
                        }
                    }
                    if map.is_empty() { None } else { Some(map) }
                } else {
                    None
                };

                // Create JsonMessage
                let message = JsonMessage {
                    key,
                    value,
                    headers,
                    partition: json["partition"].as_i64().unwrap_or(0) as i32,
                    offset: json["offset"].as_i64().unwrap_or(0),
                    timestamp: json["timestamp"].as_i64().unwrap_or(0),
                };

                messages.push(message);
            }
        }

        Ok(())
    }

    /// Reads all JSON messages from a specific file.
    pub fn read_json_messages_from_file(&self, filename: &str) -> Result<Vec<JsonMessage>> {
        let path = self.dir.path().join(filename);
        let file = File::open(&path).context(format!("Failed to open file: {}", path.display()))?;
        let mut reader = BufReader::new(file);
        let mut messages = Vec::new();

        // Process each line in the file
        for line in reader.lines() {
            let line = line.context("Failed to read line")?;

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Parse the JSON content
            let json: serde_json::Value = 
                serde_json::from_str(&line).context(format!("Failed to parse JSON from line: {}", line))?;

            // Extract fields for JsonMessage
            let key = if json["key"].is_array() {
                // Convert byte array to string
                let bytes: Vec<u8> = json["key"]
                    .as_array()
                    .unwrap_or(&Vec::new())
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect();

                if !bytes.is_empty() {
                    Some(String::from_utf8_lossy(&bytes).to_string())
                } else {
                    None
                }
            } else {
                None
            };

            // Extract value
            let value = if json["value"].is_array() {
                // Try to parse the value bytes as JSON
                let bytes: Vec<u8> = json["value"]
                    .as_array()
                    .unwrap_or(&Vec::new())
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect();

                if !bytes.is_empty() {
                    match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(parsed_json) => parsed_json,
                        Err(_) => serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string())
                    }
                } else {
                    serde_json::Value::Null
                }
            } else {
                json["value"].clone()
            };

            // Extract headers
            let headers = if json["headers"].is_object() && !json["headers"].as_object().unwrap().is_empty() {
                let mut map = HashMap::new();
                for (k, v) in json["headers"].as_object().unwrap() {
                    if let Some(s) = v.as_str() {
                        map.insert(k.clone(), s.to_string());
                    }
                }
                if map.is_empty() { None } else { Some(map) }
            } else {
                None
            };

            // Create JsonMessage
            let message = JsonMessage {
                key,
                value,
                headers,
                partition: json["partition"].as_i64().unwrap_or(0) as i32,
                offset: json["offset"].as_i64().unwrap_or(0),
                timestamp: json["timestamp"].as_i64().unwrap_or(0),
            };

            messages.push(message);
        }

        Ok(messages)
    }
}

/// Runs the kafka-scribe binary with the given arguments.
pub fn run_kscribe<I, S>(args: I) -> Result<Output>
where
    I: IntoIterator<Item = S> + std::fmt::Debug,
    S: AsRef<OsStr>,
{
    debug!("Running command: {:?}", args);
    let output = Command::new("cargo")
        .args(["run", "--"])
        .args(args)
        .output()
        .context("Failed to execute command")?;

    debug!(
        "Command output: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(output)
}

/// Runs the kafka-scribe store command with the given arguments.
pub fn run_store_command<I, S>(args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    // Create a new vector with "store" as the first element
    let mut store_args = vec![String::from("store")];

    // Add all the arguments to the vector
    for arg in args {
        store_args.push(arg.as_ref().to_str().unwrap().to_string());
    }

    // Run the command with the new vector
    run_kscribe(store_args)
}

/// Validates that the command output indicates success.
pub fn validate_success(output: &Output) -> Result<()> {
    if !output.status.success() {
        anyhow::bail!(
            "Command failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

/// Validates that the command output indicates failure.
pub fn validate_failure(output: &Output) -> Result<()> {
    if output.status.success() {
        anyhow::bail!("Command succeeded when it was expected to fail");
    }
    Ok(())
}

/// Validates that the command output contains the expected text.
pub fn validate_output_contains(output: &Output, expected: &str) -> Result<()> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stdout.contains(expected) && !stderr.contains(expected) {
        anyhow::bail!(
            "Command output does not contain '{}': stdout={}, stderr={}",
            expected,
            stdout,
            stderr
        );
    }
    Ok(())
}

/// Validates that the command output does not contain the expected text.
pub fn validate_output_does_not_contain(output: &Output, unexpected: &str) -> Result<()> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if stdout.contains(unexpected) || stderr.contains(unexpected) {
        anyhow::bail!(
            "Command output contains '{}': stdout={}, stderr={}",
            unexpected,
            stdout,
            stderr
        );
    }
    Ok(())
}

/// Validates that the stored messages match the expected messages.
pub fn validate_stored_messages(
    dir: &TestDirectory,
    expected_count: usize,
    validate_fn: impl Fn(&JsonMessage) -> Result<()>,
) -> Result<()> {
    let messages = dir.read_json_messages()?;

    if messages.len() != expected_count {
        anyhow::bail!(
            "Expected {} messages, but found {}",
            expected_count,
            messages.len()
        );
    }

    for message in &messages {
        validate_fn(message)?;
    }

    Ok(())
}

/// Validates that the stored messages in a specific file match the expected messages.
pub fn validate_stored_messages_in_file(
    dir: &TestDirectory,
    filename: &str,
    expected_count: usize,
    validate_fn: impl Fn(&JsonMessage) -> Result<()>,
) -> Result<()> {
    let messages = dir.read_json_messages_from_file(filename)?;

    if messages.len() != expected_count {
        anyhow::bail!(
            "Expected {} messages in file {}, but found {}",
            expected_count,
            filename,
            messages.len()
        );
    }

    for message in &messages {
        validate_fn(message)?;
    }

    Ok(())
}

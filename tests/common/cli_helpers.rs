//! Utilities for executing CLI commands and validating their output.
//!
//! This module provides functions for running the kafka-scribe binary
//! with different arguments and validating the output and exit code.

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
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

    /// Reads all JSON messages from files in the temporary directory.
    pub fn read_json_messages(&self) -> Result<Vec<JsonMessage>> {
        let mut messages = Vec::new();

        for entry in fs::read_dir(self.dir.path()).context("Failed to read directory")? {
            let entry = entry.context("Failed to read directory entry")?;
            if !entry.file_type().context("Failed to get file type")?.is_file() {
                continue;
            }

            let file = File::open(entry.path()).context("Failed to open file")?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line.context("Failed to read line")?;
                let message: JsonMessage =
                    serde_json::from_str(&line).context("Failed to parse JSON message")?;
                messages.push(message);
            }
        }

        Ok(messages)
    }

    /// Reads all JSON messages from a specific file.
    pub fn read_json_messages_from_file(&self, filename: &str) -> Result<Vec<JsonMessage>> {
        let path = self.dir.path().join(filename);
        let file = File::open(path).context("Failed to open file")?;
        let reader = BufReader::new(file);
        let mut messages = Vec::new();

        for line in reader.lines() {
            let line = line.context("Failed to read line")?;
            let message: JsonMessage =
                serde_json::from_str(&line).context("Failed to parse JSON message")?;
            messages.push(message);
        }

        Ok(messages)
    }
}

/// Runs the kafka-scribe binary with the given arguments.
pub fn run_kscribe<I, S>(args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
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

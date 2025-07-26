//! Utilities for directory operations and comparisons in tests.
//!
//! This module provides functions for creating temporary directories,
//! comparing directories, and loading JSON files from directories.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use serde_json::{Value, from_str};
use tempfile::TempDir;
use tracing::debug;

/// Creates a temporary directory for test data
pub fn create_temp_dir(prefix: &str) -> Result<TempDir> {
    let temp_dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .context("Failed to create temporary directory")?;

    debug!("Created temporary directory: {:?}", temp_dir.path());
    Ok(temp_dir)
}

/// Compare directories to check if they contain the same files
pub fn compare_directories(dir1: &Path, dir2: &Path) -> Result<bool> {
    let files1 = get_directory_files(dir1)?;
    let files2 = get_directory_files(dir2)?;

    if files1.len() != files2.len() {
        debug!("Directory file count mismatch: {} vs {}", files1.len(), files2.len());
        return Ok(false);
    }

    for (path, content) in &files1 {
        if let Some(content2) = files2.get(path) {
            if content != content2 {
                debug!("Content mismatch for file: {}", path);
                return Ok(false);
            }
        } else {
            debug!("File {} exists in first directory but not in second", path);
            return Ok(false);
        }
    }

    Ok(true)
}

/// Get all files in a directory recursively
pub fn get_directory_files(dir: &Path) -> Result<HashMap<String, Vec<u8>>> {
    let mut files = HashMap::new();
    visit_directories(dir, dir, &mut files)?;
    Ok(files)
}

/// Visit directories recursively to collect files
fn visit_directories(
    base: &Path,
    dir: &Path,
    files: &mut HashMap<String, Vec<u8>>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            visit_directories(base, &path, files)?;
        } else {
            let relative = path.strip_prefix(base)?
                .to_str()
                .context("Invalid path encoding")?;

            let mut file = File::open(&path)?;
            let mut content = Vec::new();
            file.read_to_end(&mut content)?;

            files.insert(relative.to_string(), content);
        }
    }

    Ok(())
}

/// Load and parse JSON files from a directory
pub fn load_json_files(dir: &Path) -> Result<Vec<Value>> {
    let mut json_values = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
            let content = fs::read_to_string(&path)?;
            let value: Value = from_str(&content)?;
            json_values.push(value);
        }
    }

    Ok(json_values)
}

/// Compare JSON values, ignoring specific fields
pub fn compare_json_values(value1: &Value, value2: &Value, ignore_fields: &[&str]) -> bool {
    match (value1, value2) {
        (Value::Object(obj1), Value::Object(obj2)) => {
            // Check that all fields in obj1 exist in obj2 with the same values, except ignored fields
            for (key, value) in obj1 {
                if ignore_fields.contains(&key.as_str()) {
                    continue;
                }

                if let Some(value2) = obj2.get(key) {
                    if !compare_json_values(value, value2, ignore_fields) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            // Check that all fields in obj2 exist in obj1, except ignored fields
            for key in obj2.keys() {
                if ignore_fields.contains(&key.as_str()) {
                    continue;
                }

                if !obj1.contains_key(key) {
                    return false;
                }
            }

            true
        },
        (Value::Array(arr1), Value::Array(arr2)) => {
            if arr1.len() != arr2.len() {
                return false;
            }

            for (v1, v2) in arr1.iter().zip(arr2.iter()) {
                if !compare_json_values(v1, v2, ignore_fields) {
                    return false;
                }
            }

            true
        },
        _ => value1 == value2,
    }
}
//! Fixture management system for test data.
//!
//! This module provides a comprehensive system for managing test fixtures and test data.
//! It supports versioned test data, automated data generation, and cleanup utilities.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_string_pretty, from_str};
use tempfile::TempDir;
use tracing::{debug, info};
use uuid::Uuid;

use super::dir_helpers::{create_temp_dir, get_directory_files, load_json_files};
use super::test_data::{TestDataGenerator, TestMessage};

/// Version information for test fixtures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureVersion {
    /// Major version number
    pub major: u32,
    /// Minor version number
    pub minor: u32,
    /// Patch version number
    pub patch: u32,
    /// Creation timestamp
    pub created_at: String,
    /// Description of the fixture version
    pub description: String,
}

impl FixtureVersion {
    /// Create a new fixture version
    pub fn new(major: u32, minor: u32, patch: u32, description: &str) -> Self {
        use chrono::Utc;
        
        Self {
            major,
            minor,
            patch,
            created_at: Utc::now().to_rfc3339(),
            description: description.to_string(),
        }
    }
    
    /// Get the version string
    pub fn version_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Metadata for a test fixture
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureMetadata {
    /// Unique identifier for the fixture
    pub id: String,
    /// Name of the fixture
    pub name: String,
    /// Version information
    pub version: FixtureVersion,
    /// Tags for categorizing fixtures
    pub tags: Vec<String>,
    /// Additional properties
    pub properties: HashMap<String, Value>,
}

/// Scenario type for test fixtures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScenarioType {
    /// Basic scenario with simple messages
    Basic,
    /// Scenario with filtered messages
    Filtered,
    /// Scenario with error conditions
    Error,
    /// Scenario with large message volumes
    HighVolume,
    /// Custom scenario type
    Custom(String),
}

/// Test fixture manager
pub struct FixtureManager {
    /// Base directory for fixtures
    base_dir: PathBuf,
    /// Test data generator
    data_generator: TestDataGenerator,
    /// Temporary directories created by this manager
    temp_dirs: Vec<TempDir>,
}

impl FixtureManager {
    /// Create a new fixture manager
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        
        // Ensure the base directory exists
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir)
                .context("Failed to create base directory for fixtures")?;
        }
        
        Ok(Self {
            base_dir,
            data_generator: TestDataGenerator::new_random(),
            temp_dirs: Vec::new(),
        })
    }
    
    /// Create a new fixture manager with a specific seed
    pub fn with_seed(base_dir: impl AsRef<Path>, seed: u64) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        
        // Ensure the base directory exists
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir)
                .context("Failed to create base directory for fixtures")?;
        }
        
        Ok(Self {
            base_dir,
            data_generator: TestDataGenerator::new(seed),
            temp_dirs: Vec::new(),
        })
    }
    
    /// Get the path to a fixture directory
    pub fn fixture_path(&self, name: &str, version: Option<&str>) -> PathBuf {
        match version {
            Some(v) => self.base_dir.join(name).join(v),
            None => self.base_dir.join(name).join("latest"),
        }
    }
    
    /// Create a new fixture
    pub fn create_fixture(
        &mut self,
        name: &str,
        version: FixtureVersion,
        tags: Vec<String>,
        properties: HashMap<String, Value>,
    ) -> Result<PathBuf> {
        let fixture_id = Uuid::new_v4().to_string();
        let version_str = version.version_string();
        let fixture_dir = self.base_dir.join(name).join(&version_str);
        
        // Create the fixture directory
        fs::create_dir_all(&fixture_dir)
            .context("Failed to create fixture directory")?;
        
        // Create a symlink to the latest version
        let latest_link = self.base_dir.join(name).join("latest");
        if latest_link.exists() {
            fs::remove_file(&latest_link)
                .context("Failed to remove existing latest symlink")?;
        }
        
        #[cfg(unix)]
        std::os::unix::fs::symlink(&version_str, &latest_link)
            .context("Failed to create latest symlink")?;
        
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&version_str, &latest_link)
            .context("Failed to create latest symlink")?;
        
        // Create metadata file
        let metadata = FixtureMetadata {
            id: fixture_id,
            name: name.to_string(),
            version,
            tags,
            properties,
        };
        
        let metadata_json = to_string_pretty(&metadata)
            .context("Failed to serialize fixture metadata")?;
        
        let metadata_path = fixture_dir.join("metadata.json");
        fs::write(&metadata_path, metadata_json)
            .context("Failed to write fixture metadata")?;
        
        Ok(fixture_dir)
    }
    
    /// Load fixture metadata
    pub fn load_metadata(&self, name: &str, version: Option<&str>) -> Result<FixtureMetadata> {
        let fixture_dir = self.fixture_path(name, version);
        let metadata_path = fixture_dir.join("metadata.json");
        
        if !metadata_path.exists() {
            return Err(anyhow!("Fixture metadata not found: {}", metadata_path.display()));
        }
        
        let metadata_json = fs::read_to_string(&metadata_path)
            .context("Failed to read fixture metadata")?;
        
        let metadata: FixtureMetadata = from_str(&metadata_json)
            .context("Failed to parse fixture metadata")?;
        
        Ok(metadata)
    }
    
    /// List available fixtures
    pub fn list_fixtures(&self) -> Result<Vec<String>> {
        let mut fixtures = Vec::new();
        
        if !self.base_dir.exists() {
            return Ok(fixtures);
        }
        
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(name) = path.file_name() {
                    if let Some(name_str) = name.to_str() {
                        fixtures.push(name_str.to_string());
                    }
                }
            }
        }
        
        Ok(fixtures)
    }
    
    /// List available versions for a fixture
    pub fn list_versions(&self, name: &str) -> Result<Vec<String>> {
        let fixture_dir = self.base_dir.join(name);
        let mut versions = Vec::new();
        
        if !fixture_dir.exists() {
            return Ok(versions);
        }
        
        for entry in fs::read_dir(&fixture_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(version) = path.file_name() {
                    if let Some(version_str) = version.to_str() {
                        if version_str != "latest" {
                            versions.push(version_str.to_string());
                        }
                    }
                }
            }
        }
        
        Ok(versions)
    }
    
    /// Generate and save test messages for a fixture
    pub fn generate_messages(
        &mut self,
        fixture_dir: &Path,
        scenario: ScenarioType,
        count: usize,
    ) -> Result<Vec<TestMessage>> {
        let messages = match scenario {
            ScenarioType::Basic => self.data_generator.generate_message_batch(count),
            ScenarioType::Filtered => {
                // Generate messages with a specific key pattern for filtering tests
                self.data_generator.generate_filterable_messages("user-", count / 2)
                    .into_iter()
                    .chain(self.data_generator.generate_filterable_messages("order-", count / 2))
                    .collect()
            },
            ScenarioType::Error => {
                // Generate messages with potential error conditions
                let mut messages = self.data_generator.generate_message_batch(count);
                
                // Add some malformed messages
                if count > 5 {
                    // Make one message with an invalid key
                    if let Some(msg) = messages.get_mut(0) {
                        msg.key = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8
                    }
                    
                    // Make one message with a very large value
                    if let Some(msg) = messages.get_mut(1) {
                        msg.value = vec![b'X'; 1024 * 1024]; // 1MB of data
                    }
                }
                
                messages
            },
            ScenarioType::HighVolume => {
                // Generate a large number of messages
                self.data_generator.generate_message_batch(count.max(1000))
            },
            ScenarioType::Custom(pattern) => {
                // Generate messages with a custom key pattern
                self.data_generator.generate_filterable_messages(&pattern, count)
            },
        };
        
        // Save messages to the fixture directory
        self.save_messages(&messages, fixture_dir)?;
        
        Ok(messages)
    }
    
    /// Save test messages to a directory
    fn save_messages(&self, messages: &[TestMessage], dir: &Path) -> Result<()> {
        // Create the messages directory
        let messages_dir = dir.join("messages");
        fs::create_dir_all(&messages_dir)
            .context("Failed to create messages directory")?;
        
        // Save each message as a separate JSON file
        for (i, message) in messages.iter().enumerate() {
            let file_path = messages_dir.join(format!("message-{:05}.json", i));
            
            // Convert message to JSON
            let message_json = json!({
                "key": if message.key.is_empty() { 
                    Value::Null 
                } else {
                    match String::from_utf8(message.key.clone()) {
                        Ok(s) => Value::String(s),
                        Err(_) => Value::String(hex::encode(&message.key)),
                    }
                },
                "value": if message.value.is_empty() {
                    Value::Null
                } else {
                    match String::from_utf8(message.value.clone()) {
                        Ok(s) => match from_str::<Value>(&s) {
                            Ok(v) => v,
                            Err(_) => Value::String(s),
                        },
                        Err(_) => Value::String(hex::encode(&message.value)),
                    }
                },
                "headers": message.headers.clone().unwrap_or_default()
                    .iter()
                    .map(|(k, v)| {
                        let value = match String::from_utf8(v.clone()) {
                            Ok(s) => s,
                            Err(_) => hex::encode(v),
                        };
                        (k.clone(), value)
                    })
                    .collect::<HashMap<String, String>>(),
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
            });
            
            // Write JSON to file
            let json_string = to_string_pretty(&message_json)
                .context("Failed to serialize message to JSON")?;
            
            fs::write(&file_path, json_string)
                .context("Failed to write message to file")?;
        }
        
        // Create a combined file with all messages
        let all_messages_path = dir.join("all_messages.json");
        let all_messages: Vec<Value> = messages.iter()
            .map(|msg| {
                json!({
                    "key": if msg.key.is_empty() { 
                        Value::Null 
                    } else {
                        match String::from_utf8(msg.key.clone()) {
                            Ok(s) => Value::String(s),
                            Err(_) => Value::String(hex::encode(&msg.key)),
                        }
                    },
                    "value": if msg.value.is_empty() {
                        Value::Null
                    } else {
                        match String::from_utf8(msg.value.clone()) {
                            Ok(s) => match from_str::<Value>(&s) {
                                Ok(v) => v,
                                Err(_) => Value::String(s),
                            },
                            Err(_) => Value::String(hex::encode(&msg.value)),
                        }
                    },
                    "headers": msg.headers.clone().unwrap_or_default()
                        .iter()
                        .map(|(k, v)| {
                            let value = match String::from_utf8(v.clone()) {
                                Ok(s) => s,
                                Err(_) => hex::encode(v),
                            };
                            (k.clone(), value)
                        })
                        .collect::<HashMap<String, String>>(),
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "timestamp": msg.timestamp,
                })
            })
            .collect();
        
        let all_messages_json = to_string_pretty(&all_messages)
            .context("Failed to serialize all messages to JSON")?;
        
        fs::write(&all_messages_path, all_messages_json)
            .context("Failed to write all messages to file")?;
        
        Ok(())
    }
    
    /// Load test messages from a fixture
    pub fn load_messages(&self, name: &str, version: Option<&str>) -> Result<Vec<TestMessage>> {
        let fixture_dir = self.fixture_path(name, version);
        let all_messages_path = fixture_dir.join("all_messages.json");
        
        if !all_messages_path.exists() {
            return Err(anyhow!("Fixture messages not found: {}", all_messages_path.display()));
        }
        
        let json_str = fs::read_to_string(&all_messages_path)
            .context("Failed to read messages file")?;
        
        let json_values: Vec<Value> = from_str(&json_str)
            .context("Failed to parse messages JSON")?;
        
        let messages = json_values.into_iter()
            .map(|json| {
                // Extract key
                let key = match json.get("key") {
                    Some(Value::String(s)) => s.as_bytes().to_vec(),
                    Some(Value::Null) => Vec::new(),
                    _ => Vec::new(),
                };
                
                // Extract value
                let value = match json.get("value") {
                    Some(Value::String(s)) => s.as_bytes().to_vec(),
                    Some(Value::Object(_)) | Some(Value::Array(_)) => {
                        to_string_pretty(json.get("value").unwrap())
                            .unwrap_or_default()
                            .into_bytes()
                    },
                    Some(Value::Null) => Vec::new(),
                    _ => Vec::new(),
                };
                
                // Extract headers
                let headers = match json.get("headers") {
                    Some(Value::Object(obj)) => {
                        let mut headers_map = HashMap::new();
                        for (k, v) in obj {
                            if let Value::String(s) = v {
                                headers_map.insert(k.clone(), s.as_bytes().to_vec());
                            }
                        }
                        Some(headers_map)
                    },
                    _ => None,
                };
                
                // Extract partition, offset, and timestamp
                let partition = json.get("partition")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                
                let offset = json.get("offset")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                
                let timestamp = json.get("timestamp")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                
                TestMessage {
                    key,
                    value,
                    headers,
                    partition,
                    offset,
                    timestamp,
                }
            })
            .collect();
        
        Ok(messages)
    }
    
    /// Create a temporary directory for test data
    pub fn create_temp_dir(&mut self, prefix: &str) -> Result<PathBuf> {
        let temp_dir = create_temp_dir(prefix)?;
        let path = temp_dir.path().to_path_buf();
        self.temp_dirs.push(temp_dir);
        Ok(path)
    }
    
    /// Clean up all temporary directories created by this manager
    pub fn cleanup(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        
        // Take ownership of the temp_dirs vector
        let temp_dirs = std::mem::take(&mut self.temp_dirs);
        
        for temp_dir in temp_dirs {
            if let Err(e) = temp_dir.close() {
                errors.push(format!("Failed to clean up temporary directory: {}", e));
            }
        }
        
        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("Cleanup errors: {}", errors.join(", ")))
        }
    }
}

impl Drop for FixtureManager {
    fn drop(&mut self) {
        // Attempt to clean up temporary directories when the manager is dropped
        if let Err(e) = self.cleanup() {
            debug!("Error during fixture manager cleanup: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_fixture_version() {
        let version = FixtureVersion::new(1, 2, 3, "Test version");
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 2);
        assert_eq!(version.patch, 3);
        assert_eq!(version.description, "Test version");
        assert_eq!(version.version_string(), "1.2.3");
    }
    
    #[test]
    fn test_create_and_load_fixture() -> Result<()> {
        let temp_dir = tempdir()?;
        let mut manager = FixtureManager::new(temp_dir.path())?;
        
        // Create a fixture
        let version = FixtureVersion::new(1, 0, 0, "Initial version");
        let mut properties = HashMap::new();
        properties.insert("test".to_string(), json!("value"));
        
        let fixture_dir = manager.create_fixture(
            "test-fixture",
            version.clone(),
            vec!["test".to_string(), "example".to_string()],
            properties,
        )?;
        
        // Generate messages
        let messages = manager.generate_messages(
            &fixture_dir,
            ScenarioType::Basic,
            10,
        )?;
        
        assert_eq!(messages.len(), 10);
        
        // Load metadata
        let metadata = manager.load_metadata("test-fixture", None)?;
        assert_eq!(metadata.name, "test-fixture");
        assert_eq!(metadata.version.major, 1);
        assert_eq!(metadata.version.minor, 0);
        assert_eq!(metadata.version.patch, 0);
        assert_eq!(metadata.tags, vec!["test", "example"]);
        assert_eq!(metadata.properties.get("test"), Some(&json!("value")));
        
        // Load messages
        let loaded_messages = manager.load_messages("test-fixture", None)?;
        assert_eq!(loaded_messages.len(), 10);
        
        // List fixtures
        let fixtures = manager.list_fixtures()?;
        assert!(fixtures.contains(&"test-fixture".to_string()));
        
        // List versions
        let versions = manager.list_versions("test-fixture")?;
        assert!(versions.contains(&"1.0.0".to_string()));
        
        Ok(())
    }
}
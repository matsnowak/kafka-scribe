use std::collections::HashMap;
use anyhow::Result;
use serde_json::json;
use tempfile::tempdir;

use crate::common::fixture_manager::{FixtureManager, FixtureVersion, ScenarioType};

/// Test creating and loading a fixture
#[tokio::test]
async fn test_fixture_manager_create_and_load() -> Result<()> {
    // Create a temporary directory for the test
    let temp_dir = tempdir()?;
    let mut manager = FixtureManager::new(temp_dir.path())?;
    
    // Create a fixture version
    let version = FixtureVersion::new(1, 0, 0, "Test fixture");
    
    // Create properties for the fixture
    let mut properties = HashMap::new();
    properties.insert("description".to_string(), json!("Test fixture for fixture manager"));
    
    // Create the fixture
    let fixture_dir = manager.create_fixture(
        "test-fixture",
        version.clone(),
        vec!["test".to_string(), "example".to_string()],
        properties,
    )?;
    
    // Generate messages for the fixture
    let messages = manager.generate_messages(
        &fixture_dir,
        ScenarioType::Basic,
        10,
    )?;
    
    // Verify that 10 messages were generated
    assert_eq!(messages.len(), 10);
    
    // Load the fixture metadata
    let metadata = manager.load_metadata("test-fixture", None)?;
    
    // Verify the metadata
    assert_eq!(metadata.name, "test-fixture");
    assert_eq!(metadata.version.major, 1);
    assert_eq!(metadata.version.minor, 0);
    assert_eq!(metadata.version.patch, 0);
    assert_eq!(metadata.tags, vec!["test", "example"]);
    assert_eq!(
        metadata.properties.get("description"),
        Some(&json!("Test fixture for fixture manager"))
    );
    
    // Load the messages
    let loaded_messages = manager.load_messages("test-fixture", None)?;
    
    // Verify that the correct number of messages were loaded
    assert_eq!(loaded_messages.len(), 10);
    
    // List fixtures
    let fixtures = manager.list_fixtures()?;
    assert!(fixtures.contains(&"test-fixture".to_string()));
    
    // List versions
    let versions = manager.list_versions("test-fixture")?;
    assert!(versions.contains(&"1.0.0".to_string()));
    
    Ok(())
}

/// Test different scenario types
#[tokio::test]
async fn test_fixture_manager_scenarios() -> Result<()> {
    // Create a temporary directory for the test
    let temp_dir = tempdir()?;
    let mut manager = FixtureManager::with_seed(temp_dir.path(), 42)?;
    
    // Create a fixture
    let version = FixtureVersion::new(1, 0, 0, "Test fixture");
    let fixture_dir = manager.create_fixture(
        "scenario-test",
        version,
        vec!["test".to_string()],
        Default::default(),
    )?;
    
    // Test Basic scenario
    let basic_messages = manager.generate_messages(
        &fixture_dir,
        ScenarioType::Basic,
        10,
    )?;
    assert_eq!(basic_messages.len(), 10);
    
    // Create a new fixture for Filtered scenario
    let version = FixtureVersion::new(1, 0, 0, "Filtered scenario");
    let fixture_dir = manager.create_fixture(
        "filtered-test",
        version,
        vec!["test".to_string()],
        Default::default(),
    )?;
    
    // Test Filtered scenario
    let filtered_messages = manager.generate_messages(
        &fixture_dir,
        ScenarioType::Filtered,
        10,
    )?;
    assert_eq!(filtered_messages.len(), 10);
    
    // Verify that some messages have keys starting with "user-"
    let user_messages = filtered_messages.iter()
        .filter(|msg| {
            String::from_utf8_lossy(&msg.key).starts_with("user-")
        })
        .count();
    assert!(user_messages > 0);
    
    // Create a new fixture for Error scenario
    let version = FixtureVersion::new(1, 0, 0, "Error scenario");
    let fixture_dir = manager.create_fixture(
        "error-test",
        version,
        vec!["test".to_string()],
        Default::default(),
    )?;
    
    // Test Error scenario
    let error_messages = manager.generate_messages(
        &fixture_dir,
        ScenarioType::Error,
        10,
    )?;
    assert_eq!(error_messages.len(), 10);
    
    // Create a new fixture for HighVolume scenario
    let version = FixtureVersion::new(1, 0, 0, "HighVolume scenario");
    let fixture_dir = manager.create_fixture(
        "highvolume-test",
        version,
        vec!["test".to_string()],
        Default::default(),
    )?;
    
    // Test HighVolume scenario
    let highvolume_messages = manager.generate_messages(
        &fixture_dir,
        ScenarioType::HighVolume,
        10,
    )?;
    assert!(highvolume_messages.len() >= 1000);
    
    // Create a new fixture for Custom scenario
    let version = FixtureVersion::new(1, 0, 0, "Custom scenario");
    let fixture_dir = manager.create_fixture(
        "custom-test",
        version,
        vec!["test".to_string()],
        Default::default(),
    )?;
    
    // Test Custom scenario
    let custom_messages = manager.generate_messages(
        &fixture_dir,
        ScenarioType::Custom("product-".to_string()),
        10,
    )?;
    assert_eq!(custom_messages.len(), 10);
    
    // Verify that all messages have keys starting with "product-"
    let product_messages = custom_messages.iter()
        .filter(|msg| {
            String::from_utf8_lossy(&msg.key).starts_with("product-")
        })
        .count();
    assert_eq!(product_messages, 10);
    
    Ok(())
}

/// Test temporary directory management
#[tokio::test]
async fn test_fixture_manager_temp_dirs() -> Result<()> {
    // Create a fixture manager
    let temp_dir = tempdir()?;
    let mut manager = FixtureManager::new(temp_dir.path())?;
    
    // Create a temporary directory
    let dir1_path = manager.create_temp_dir("test1")?;
    assert!(dir1_path.exists());
    
    // Create another temporary directory
    let dir2_path = manager.create_temp_dir("test2")?;
    assert!(dir2_path.exists());
    
    // Clean up temporary directories
    manager.cleanup()?;
    
    // Verify that the directories no longer exist
    assert!(!dir1_path.exists());
    assert!(!dir2_path.exists());
    
    Ok(())
}
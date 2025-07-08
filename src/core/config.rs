use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Default Kafka bootstrap servers
    pub default_bootstrap_servers: Option<String>,
    /// Default storage directory
    pub default_storage_dir: Option<PathBuf>,
    /// Default batch size for operations
    pub default_batch_size: Option<usize>,
    /// Default buffer size
    pub default_buffer_size: Option<usize>,
    /// Default number of threads
    pub default_threads: Option<usize>,
    /// Custom settings
    pub custom: HashMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            default_bootstrap_servers: Some("localhost:9092".to_string()),
            default_storage_dir: Some(PathBuf::from("./kscribe-data")),
            default_batch_size: Some(100),
            default_buffer_size: Some(1000),
            default_threads: None, // Use system defaults
            custom: HashMap::new(),
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn load_from_file(path: &PathBuf) -> crate::core::errors::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn save_to_file(&self, path: &PathBuf) -> crate::core::errors::Result<()> {
        let contents = serde_yaml::to_string(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }

    /// Get default configuration file path
    pub fn default_config_path() -> PathBuf {
        dirs::home_dir()
            .map(|home| home.join(".kscribe.yaml"))
            .unwrap_or_else(|| PathBuf::from(".kscribe.yaml"))
    }

    /// Load configuration from default location or create default
    pub fn load_or_default() -> Self {
        let config_path = Self::default_config_path();
        if config_path.exists() {
            match Self::load_from_file(&config_path) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to load config from {}: {}",
                        config_path.display(),
                        e
                    );
                    Self::default()
                }
            }
        } else {
            Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: Config = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(
            config.default_bootstrap_servers,
            deserialized.default_bootstrap_servers
        );
        assert_eq!(config.default_batch_size, deserialized.default_batch_size);
    }

    #[test]
    fn test_config_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.yaml");

        let config = Config::default();
        config.save_to_file(&config_path).unwrap();

        let loaded_config = Config::load_from_file(&config_path).unwrap();
        assert_eq!(
            config.default_bootstrap_servers,
            loaded_config.default_bootstrap_servers
        );
    }
}

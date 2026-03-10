use crawfish_types::{EncounterPolicy, ExecutionContract};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub sqlite_path: PathBuf,
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ContractDefaultsConfig {
    #[serde(default)]
    pub org_defaults: ExecutionContract,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GovernanceConfig {
    #[serde(default)]
    pub system_defaults: EncounterPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FleetConfig {
    pub manifests_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeConfig {
    #[serde(default = "default_reconcile_interval_ms")]
    pub reconcile_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrawfishConfig {
    pub storage: StorageConfig,
    pub fleet: FleetConfig,
    #[serde(default)]
    pub contracts: ContractDefaultsConfig,
    #[serde(default)]
    pub governance: GovernanceConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            reconcile_interval_ms: default_reconcile_interval_ms(),
        }
    }
}

fn default_reconcile_interval_ms() -> u64 {
    5_000
}

impl CrawfishConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(path)?;
        Ok(toml::from_str(&contents)?)
    }

    pub fn manifest_dir(&self, root: &Path) -> PathBuf {
        root.join(&self.fleet.manifests_dir)
    }

    pub fn sqlite_path(&self, root: &Path) -> PathBuf {
        root.join(&self.storage.sqlite_path)
    }

    pub fn state_dir(&self, root: &Path) -> PathBuf {
        root.join(&self.storage.state_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_resolves_relative_paths() {
        let config = CrawfishConfig {
            storage: StorageConfig {
                sqlite_path: PathBuf::from(".crawfish/state/control.db"),
                state_dir: PathBuf::from(".crawfish/state"),
            },
            fleet: FleetConfig {
                manifests_dir: PathBuf::from("agents"),
            },
            contracts: ContractDefaultsConfig::default(),
            governance: GovernanceConfig::default(),
            runtime: RuntimeConfig::default(),
        };

        let root = PathBuf::from("/tmp/example");
        assert_eq!(
            config.manifest_dir(&root),
            PathBuf::from("/tmp/example/agents")
        );
        assert_eq!(
            config.sqlite_path(&root),
            PathBuf::from("/tmp/example/.crawfish/state/control.db")
        );
    }
}

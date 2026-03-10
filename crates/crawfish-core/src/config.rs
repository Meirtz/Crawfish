use crawfish_types::{
    CallerOwnerMapping, CapabilityVisibility, DataBoundaryPolicy, DefaultDisposition,
    EncounterPolicy, ExecutionContract, McpServerConfig, NetworkBoundaryPolicy, OwnerKind,
    ToolBoundaryPolicy, TrustDomain, WorkspaceBoundaryPolicy,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub sqlite_path: PathBuf,
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiConfig {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ContractDefaultsConfig {
    #[serde(default)]
    pub org_defaults: ExecutionContract,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: BTreeMap<String, McpServerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawAllowedCallerConfig {
    pub owner_kind: OwnerKind,
    pub owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_domain: Option<TrustDomain>,
    #[serde(default)]
    pub allowed_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawInboundConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub caller_owner_mapping: CallerOwnerMapping,
    #[serde(default = "default_openclaw_trust_domain")]
    pub default_trust_domain: TrustDomain,
    #[serde(default)]
    pub allowed_callers: BTreeMap<String, OpenClawAllowedCallerConfig>,
}

impl Default for OpenClawInboundConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            caller_owner_mapping: CallerOwnerMapping::Required,
            default_trust_domain: default_openclaw_trust_domain(),
            allowed_callers: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OpenClawConfig {
    #[serde(default)]
    pub inbound: OpenClawInboundConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovernanceConfig {
    #[serde(default = "default_system_encounter_policy")]
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
    pub api: ApiConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub openclaw: OpenClawConfig,
    #[serde(default)]
    pub contracts: ContractDefaultsConfig,
    #[serde(default)]
    pub governance: GovernanceConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            reconcile_interval_ms: default_reconcile_interval_ms(),
        }
    }
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            system_defaults: default_system_encounter_policy(),
        }
    }
}

fn default_reconcile_interval_ms() -> u64 {
    5_000
}

fn default_socket_path() -> PathBuf {
    PathBuf::from(".crawfish/run/crawfishd.sock")
}

fn default_openclaw_trust_domain() -> TrustDomain {
    TrustDomain::SameDeviceForeignOwner
}

fn default_system_encounter_policy() -> EncounterPolicy {
    EncounterPolicy {
        default_disposition: DefaultDisposition::AllowWithLease,
        capability_visibility: CapabilityVisibility::OwnerOnly,
        data_boundary: DataBoundaryPolicy::OwnerOnly,
        tool_boundary: ToolBoundaryPolicy::NoCrossOwnerMutation,
        workspace_boundary: WorkspaceBoundaryPolicy::Isolated,
        network_boundary: NetworkBoundaryPolicy::LocalOnly,
        human_approval_requirements: Vec::new(),
    }
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

    pub fn socket_path(&self, root: &Path) -> PathBuf {
        root.join(&self.api.socket_path)
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
            api: ApiConfig {
                socket_path: PathBuf::from(".crawfish/run/crawfishd.sock"),
            },
            mcp: McpConfig::default(),
            openclaw: OpenClawConfig::default(),
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
        assert_eq!(
            config.socket_path(&root),
            PathBuf::from("/tmp/example/.crawfish/run/crawfishd.sock")
        );
    }
}

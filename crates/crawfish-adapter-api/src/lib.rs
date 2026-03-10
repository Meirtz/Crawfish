use crawfish_types::{Action, ActionOutputs, CapabilityDescriptor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterRequest {
    DescribeCapabilities,
    ExecuteAction { action: Box<Action> },
    HealthCheck,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterResponse {
    Capabilities {
        capabilities: Vec<CapabilityDescriptor>,
    },
    ActionResult {
        outputs: ActionOutputs,
    },
    Health {
        healthy: bool,
    },
}

pub const ADAPTER_PROTOCOL_VERSION: &str = "crawfish-adapter-api/v0";

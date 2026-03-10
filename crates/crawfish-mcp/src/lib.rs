use async_trait::async_trait;
use crawfish_core::ExecutionSurface;
use crawfish_types::{
    Action, ActionOutputs, CapabilityDescriptor, CostClass, ExecutorClass, LatencyClass,
    McpToolBinding, Mutability, RiskClass,
};

#[derive(Debug, Clone)]
pub struct McpAdapter {
    server_name: String,
}

impl McpAdapter {
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
        }
    }

    pub fn describe_binding(&self, binding: &McpToolBinding) -> CapabilityDescriptor {
        CapabilityDescriptor {
            namespace: format!("mcp.{}", binding.capability),
            verbs: vec!["invoke".to_string()],
            executor_class: ExecutorClass::Agentic,
            mutability: Mutability::ReadOnly,
            risk_class: RiskClass::Medium,
            cost_class: CostClass::Standard,
            latency_class: LatencyClass::Interactive,
            approval_requirements: Vec::new(),
        }
    }
}

#[async_trait]
impl ExecutionSurface for McpAdapter {
    fn name(&self) -> &str {
        &self.server_name
    }

    fn supports(&self, capability: &CapabilityDescriptor) -> bool {
        capability.namespace.starts_with("mcp.")
    }

    async fn run(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let mut outputs = ActionOutputs {
            summary: Some(format!(
                "MCP execution stub for action {} on {}",
                action.id, self.server_name
            )),
            ..ActionOutputs::default()
        };
        outputs.metadata.insert(
            "execution_surface".to_string(),
            serde_json::json!(self.server_name),
        );
        Ok(outputs)
    }
}

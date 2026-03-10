use crawfish_types::{
    ContinuityModeName, DeadLetterPolicy, DeliveryContract, ExecutionContract, ExecutionPolicy,
    ExecutionStrategy, HumanHandoffPolicy, QualityPolicy, RecoveryPolicy, SafetyPolicy,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeliveryContractPatch {
    pub deadline_ms: Option<u64>,
    pub freshness_ttl_ms: Option<u64>,
    pub required_ack: Option<bool>,
    pub liveliness_window_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPolicyPatch {
    pub max_cost_usd: Option<f64>,
    pub max_tokens: Option<u64>,
    pub model_class: Option<String>,
    pub preferred_harnesses: Option<Vec<String>>,
    pub fallback_chain: Option<Vec<String>>,
    pub retry_budget: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SafetyPolicyPatch {
    pub tool_scope: Option<Vec<String>>,
    pub approval_policy: Option<crawfish_types::ApprovalPolicy>,
    pub mutation_mode: Option<crawfish_types::MutationMode>,
    pub data_zone: Option<String>,
    pub secret_policy: Option<crawfish_types::SecretPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QualityPolicyPatch {
    pub quality_class: Option<String>,
    pub evaluation_profile: Option<Option<String>>,
    pub evaluation_hook: Option<Option<String>>,
    pub minimum_confidence: Option<Option<f64>>,
    pub human_review_rule: Option<Option<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecoveryPolicyPatch {
    pub checkpoint_interval: Option<crawfish_types::CheckpointInterval>,
    pub resumability: Option<crawfish_types::Resumability>,
    pub fallback_behavior: Option<crawfish_types::FallbackBehavior>,
    pub continuity_preference: Option<Vec<ContinuityModeName>>,
    pub deterministic_fallbacks: Option<Vec<String>>,
    pub human_handoff_policy: Option<HumanHandoffPolicy>,
    pub dead_letter_policy: Option<DeadLetterPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionContractPatch {
    pub delivery: DeliveryContractPatch,
    pub execution: ExecutionPolicyPatch,
    pub safety: SafetyPolicyPatch,
    pub quality: QualityPolicyPatch,
    pub recovery: RecoveryPolicyPatch,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledExecutionPlan {
    pub contract: ExecutionContract,
    pub strategy: Option<ExecutionStrategy>,
}

pub fn compile_execution_plan(
    org_defaults: &ExecutionContract,
    agent_defaults: &ExecutionContract,
    action_overrides: &ExecutionContractPatch,
    strategy_defaults: &BTreeMap<String, ExecutionStrategy>,
    capability: &str,
    action_strategy: Option<ExecutionStrategy>,
) -> anyhow::Result<CompiledExecutionPlan> {
    let mut contract = merge_contracts(org_defaults, agent_defaults);
    apply_patch(&mut contract, action_overrides);
    validate_hard_policies(&contract)?;

    let mut strategy = strategy_defaults.get(capability).cloned();
    if let Some(explicit) = action_strategy {
        strategy = Some(explicit);
    }

    Ok(CompiledExecutionPlan { contract, strategy })
}

fn merge_contracts(base: &ExecutionContract, override_: &ExecutionContract) -> ExecutionContract {
    let mut merged = base.clone();
    merged.delivery = override_.delivery.clone();
    merged.execution = override_.execution.clone();
    merged.safety = override_.safety.clone();
    merged.quality = override_.quality.clone();
    merged.recovery = override_.recovery.clone();
    merged
}

fn apply_patch(contract: &mut ExecutionContract, patch: &ExecutionContractPatch) {
    apply_delivery_patch(&mut contract.delivery, &patch.delivery);
    apply_execution_patch(&mut contract.execution, &patch.execution);
    apply_safety_patch(&mut contract.safety, &patch.safety);
    apply_quality_patch(&mut contract.quality, &patch.quality);
    apply_recovery_patch(&mut contract.recovery, &patch.recovery);
}

fn apply_delivery_patch(contract: &mut DeliveryContract, patch: &DeliveryContractPatch) {
    if let Some(value) = patch.deadline_ms {
        contract.deadline_ms = Some(value);
    }
    if let Some(value) = patch.freshness_ttl_ms {
        contract.freshness_ttl_ms = Some(value);
    }
    if let Some(value) = patch.required_ack {
        contract.required_ack = value;
    }
    if let Some(value) = patch.liveliness_window_ms {
        contract.liveliness_window_ms = Some(value);
    }
}

fn apply_execution_patch(contract: &mut ExecutionPolicy, patch: &ExecutionPolicyPatch) {
    if let Some(value) = patch.max_cost_usd {
        contract.max_cost_usd = Some(value);
    }
    if let Some(value) = patch.max_tokens {
        contract.max_tokens = Some(value);
    }
    if let Some(value) = &patch.model_class {
        contract.model_class = Some(value.clone());
    }
    if let Some(value) = &patch.preferred_harnesses {
        contract.preferred_harnesses = value.clone();
    }
    if let Some(value) = &patch.fallback_chain {
        contract.fallback_chain = value.clone();
    }
    if let Some(value) = patch.retry_budget {
        contract.retry_budget = value;
    }
}

fn apply_safety_patch(contract: &mut SafetyPolicy, patch: &SafetyPolicyPatch) {
    if let Some(value) = &patch.tool_scope {
        contract.tool_scope = value.clone();
    }
    if let Some(value) = &patch.approval_policy {
        contract.approval_policy = value.clone();
    }
    if let Some(value) = &patch.mutation_mode {
        contract.mutation_mode = value.clone();
    }
    if let Some(value) = &patch.data_zone {
        contract.data_zone = value.clone();
    }
    if let Some(value) = &patch.secret_policy {
        contract.secret_policy = value.clone();
    }
}

fn apply_quality_patch(contract: &mut QualityPolicy, patch: &QualityPolicyPatch) {
    if let Some(value) = &patch.quality_class {
        contract.quality_class = value.clone();
    }
    if let Some(value) = &patch.evaluation_profile {
        contract.evaluation_profile = value.clone();
    }
    if let Some(value) = &patch.evaluation_hook {
        contract.evaluation_hook = value.clone();
    }
    if let Some(value) = patch.minimum_confidence {
        contract.minimum_confidence = value;
    }
    if let Some(value) = &patch.human_review_rule {
        contract.human_review_rule = value.clone();
    }
}

fn apply_recovery_patch(contract: &mut RecoveryPolicy, patch: &RecoveryPolicyPatch) {
    if let Some(value) = &patch.checkpoint_interval {
        contract.checkpoint_interval = value.clone();
    }
    if let Some(value) = &patch.resumability {
        contract.resumability = value.clone();
    }
    if let Some(value) = &patch.fallback_behavior {
        contract.fallback_behavior = value.clone();
    }
    if let Some(value) = &patch.continuity_preference {
        contract.continuity_preference = value.clone();
    }
    if let Some(value) = &patch.deterministic_fallbacks {
        contract.deterministic_fallbacks = value.clone();
    }
    if let Some(value) = &patch.human_handoff_policy {
        contract.human_handoff_policy = value.clone();
    }
    if let Some(value) = &patch.dead_letter_policy {
        contract.dead_letter_policy = value.clone();
    }
}

pub fn validate_hard_policies(contract: &ExecutionContract) -> anyhow::Result<()> {
    if matches!(
        contract.safety.mutation_mode,
        crawfish_types::MutationMode::Autonomous
    ) && matches!(
        contract.safety.approval_policy,
        crawfish_types::ApprovalPolicy::Always
    ) {
        anyhow::bail!("autonomous mutation cannot require always-on approval");
    }

    if let Some(max_cost) = contract.execution.max_cost_usd {
        if max_cost <= 0.0 {
            anyhow::bail!("max_cost_usd must be positive");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawfish_types::{
        ApprovalPolicy, ExecutionContract, ExecutionStrategyMode, FeedbackPolicy, MutationMode,
        VerificationSpec, VerifyLoopFailureMode,
    };

    #[test]
    fn contract_patch_overrides_agent_defaults() {
        let mut agent = ExecutionContract::default();
        agent.execution.max_cost_usd = Some(10.0);
        let mut patch = ExecutionContractPatch::default();
        patch.execution.max_cost_usd = Some(3.0);

        let compiled = compile_execution_plan(
            &ExecutionContract::default(),
            &agent,
            &patch,
            &BTreeMap::new(),
            "repo.review",
            None,
        )
        .unwrap();

        assert_eq!(compiled.contract.execution.max_cost_usd, Some(3.0));
    }

    #[test]
    fn explicit_strategy_wins() {
        let mut defaults = BTreeMap::new();
        defaults.insert(
            "task.plan".to_string(),
            ExecutionStrategy {
                mode: ExecutionStrategyMode::VerifyLoop,
                verification_spec: Some(VerificationSpec {
                    checks: Vec::new(),
                    require_all: true,
                    on_failure: VerifyLoopFailureMode::RetryWithFeedback,
                }),
                stop_budget: None,
                feedback_policy: FeedbackPolicy::InjectReason,
            },
        );

        let compiled = compile_execution_plan(
            &ExecutionContract::default(),
            &ExecutionContract::default(),
            &ExecutionContractPatch::default(),
            &defaults,
            "task.plan",
            Some(ExecutionStrategy {
                mode: ExecutionStrategyMode::SinglePass,
                verification_spec: None,
                stop_budget: None,
                feedback_policy: FeedbackPolicy::AppendReport,
            }),
        )
        .unwrap();

        assert_eq!(
            compiled.strategy.expect("strategy").mode,
            ExecutionStrategyMode::SinglePass
        );
    }

    #[test]
    fn invalid_autonomous_mutation_fails_validation() {
        let mut contract = ExecutionContract::default();
        contract.safety.mutation_mode = MutationMode::Autonomous;
        contract.safety.approval_policy = ApprovalPolicy::Always;

        let error = validate_hard_policies(&contract).unwrap_err();
        assert!(error
            .to_string()
            .contains("autonomous mutation cannot require always-on approval"));
    }
}

use super::*;

pub(crate) fn jurisdiction_class_for_action(
    action: &Action,
    encounter: Option<&EncounterRecord>,
) -> JurisdictionClass {
    if action
        .selected_executor
        .as_deref()
        .map(is_remote_harness_executor)
        .unwrap_or(false)
    {
        return JurisdictionClass::RemoteHarness;
    }

    match encounter.map(|encounter| &encounter.trust_domain) {
        Some(TrustDomain::SameOwnerLocal) => JurisdictionClass::SameOwnerLocal,
        Some(TrustDomain::SameDeviceForeignOwner) => JurisdictionClass::SameDeviceForeignOwner,
        Some(_) => JurisdictionClass::ExternalUnknown,
        None => match action
            .counterparty_refs
            .first()
            .map(|counterparty| &counterparty.trust_domain)
        {
            Some(TrustDomain::SameOwnerLocal) => JurisdictionClass::SameOwnerLocal,
            Some(TrustDomain::SameDeviceForeignOwner) => JurisdictionClass::SameDeviceForeignOwner,
            Some(_) => JurisdictionClass::ExternalUnknown,
            None => JurisdictionClass::ExternalUnknown,
        },
    }
}

pub(crate) fn interaction_model_for_action(
    action: &Action,
    encounter: Option<&EncounterRecord>,
) -> crawfish_types::InteractionModel {
    if action
        .selected_executor
        .as_deref()
        .map(is_remote_agent_executor)
        .unwrap_or(false)
    {
        return crawfish_types::InteractionModel::RemoteAgent;
    }

    if action
        .selected_executor
        .as_deref()
        .map(is_remote_harness_executor)
        .unwrap_or(false)
    {
        return crawfish_types::InteractionModel::RemoteHarness;
    }

    match encounter.map(|encounter| &encounter.trust_domain) {
        Some(TrustDomain::SameOwnerLocal) => crawfish_types::InteractionModel::SameOwnerSwarm,
        Some(TrustDomain::SameDeviceForeignOwner) => {
            crawfish_types::InteractionModel::SameDeviceMultiOwner
        }
        Some(_) => crawfish_types::InteractionModel::ExternalUnknown,
        None if matches!(
            action
                .counterparty_refs
                .first()
                .map(|counterparty| &counterparty.trust_domain),
            Some(TrustDomain::SameOwnerLocal)
        ) =>
        {
            crawfish_types::InteractionModel::SameOwnerSwarm
        }
        None if matches!(
            action
                .counterparty_refs
                .first()
                .map(|counterparty| &counterparty.trust_domain),
            Some(TrustDomain::SameDeviceForeignOwner)
        ) =>
        {
            crawfish_types::InteractionModel::SameDeviceMultiOwner
        }
        None if matches!(action.requester.kind, RequesterKind::Agent)
            && action.counterparty_refs.is_empty() =>
        {
            crawfish_types::InteractionModel::ContextSplit
        }
        None if action.initiator_owner.kind == OwnerKind::ServiceAccount
            && action.counterparty_refs.is_empty()
            && !action
                .external_refs
                .iter()
                .any(|reference| reference.kind.starts_with("openclaw.")) =>
        {
            crawfish_types::InteractionModel::ContextSplit
        }
        None => crawfish_types::InteractionModel::ExternalUnknown,
    }
}

pub(crate) fn interaction_model_is_frontier(
    interaction_model: &crawfish_types::InteractionModel,
) -> bool {
    !matches!(
        interaction_model,
        crawfish_types::InteractionModel::ContextSplit
    )
}

pub(crate) fn external_ref_value(external_refs: &[ExternalRef], kind: &str) -> Option<String> {
    external_refs
        .iter()
        .find(|reference| reference.kind == kind)
        .map(|reference| reference.value.clone())
}

pub(crate) fn default_doctrine_pack(
    action: &Action,
    interaction_model: &crawfish_types::InteractionModel,
    jurisdiction: JurisdictionClass,
) -> DoctrinePack {
    let mut rules = vec![crawfish_types::DoctrineRule {
        id: "results_need_evidence".to_string(),
        title: "Results need evidence".to_string(),
        summary: "Terminal outputs require traceable evidence and, when configured, evaluation."
            .to_string(),
        required_checkpoints: vec![crawfish_types::OversightCheckpoint::PostResult],
    }];

    if interaction_model_is_frontier(interaction_model) {
        rules.insert(
            0,
            crawfish_types::DoctrineRule {
                id: "explicit_jurisdiction".to_string(),
                title: "Explicit jurisdiction before action".to_string(),
                summary: "Authority must be classified before execution begins.".to_string(),
                required_checkpoints: vec![crawfish_types::OversightCheckpoint::Admission],
            },
        );
        rules.insert(
            1,
            crawfish_types::DoctrineRule {
                id: "dispatch_under_control".to_string(),
                title: "Dispatch under control".to_string(),
                summary:
                    "Execution surfaces are selected by the control plane, not by ambient trust."
                        .to_string(),
                required_checkpoints: vec![crawfish_types::OversightCheckpoint::PreDispatch],
            },
        );
        if matches!(
            interaction_model,
            crawfish_types::InteractionModel::RemoteAgent
        ) {
            rules.insert(
                2,
                crawfish_types::DoctrineRule {
                    id: "treaty_before_remote_delegation".to_string(),
                    title: "Remote delegation requires a treaty".to_string(),
                    summary: "Remote agent delegation must prove treaty scope and delegation evidence before dispatch and after results return.".to_string(),
                    required_checkpoints: vec![
                        crawfish_types::OversightCheckpoint::Admission,
                        crawfish_types::OversightCheckpoint::PreDispatch,
                        crawfish_types::OversightCheckpoint::PostResult,
                    ],
                },
            );
        }
    } else {
        rules.insert(
            0,
            crawfish_types::DoctrineRule {
                id: "context_split_coordination".to_string(),
                title: "Context split still needs evidence".to_string(),
                summary:
                    "Role-split or handoff-style sub-agents still need bounded dispatch and inspectable results."
                        .to_string(),
                required_checkpoints: vec![crawfish_types::OversightCheckpoint::PreDispatch],
            },
        );
    }

    if action.capability == "workspace.patch.apply" {
        rules.push(crawfish_types::DoctrineRule {
            id: "mutations_need_gate".to_string(),
            title: "Mutations need an enforceable gate".to_string(),
            summary: "Mutation must pass an explicit pre-mutation gate before write commit."
                .to_string(),
            required_checkpoints: vec![crawfish_types::OversightCheckpoint::PreMutation],
        });
    }

    let (id, title, summary) = if matches!(
        interaction_model,
        crawfish_types::InteractionModel::RemoteAgent
    ) {
        (
            "remote_agent_treaty_v1",
            "Remote agent treaty doctrine",
            "Remote agents are not just another harness; delegation requires treaty scope, checkpoint evidence, and inspectable lineage.",
        )
    } else if interaction_model_is_frontier(interaction_model) {
        (
            "swarm_frontier_v1",
            "Swarm frontier doctrine",
            "Constitutions do not enforce themselves; frontier encounters require runtime checkpoints and evidence.",
        )
    } else {
        (
            "context_split_coordination_v1",
            "Context-split coordination doctrine",
            "Role-split multi-agent patterns still need bounded dispatch and evidence, but they are not frontier governance by default.",
        )
    };

    DoctrinePack {
        id: id.to_string(),
        title: title.to_string(),
        summary: summary.to_string(),
        jurisdiction,
        rules,
    }
}

pub(crate) fn checkpoint_status_for_action(
    action: &Action,
    doctrine: &DoctrinePack,
    has_trace_bundle: bool,
    latest_evaluation: Option<&EvaluationRecord>,
    profile_resolved: bool,
) -> Vec<CheckpointStatus> {
    use crawfish_types::{CheckpointOutcome, OversightCheckpoint};
    let interaction_model = interaction_model_for_action(action, None);
    let has_remote_treaty = external_ref_value(&action.external_refs, "a2a.treaty_pack").is_some();
    let has_remote_receipt =
        external_ref_value(&action.external_refs, "a2a.delegation_receipt").is_some();
    let remote_outcome_disposition = remote_outcome_disposition_for_action(action);

    let requires = |checkpoint: OversightCheckpoint| {
        doctrine
            .rules
            .iter()
            .any(|rule| rule.required_checkpoints.contains(&checkpoint))
    };

    vec![
        CheckpointStatus {
            checkpoint: OversightCheckpoint::Admission,
            required: requires(OversightCheckpoint::Admission),
            outcome: CheckpointOutcome::Passed,
            reason: Some("action entered the control plane through admission".to_string()),
        },
        CheckpointStatus {
            checkpoint: OversightCheckpoint::PreDispatch,
            required: requires(OversightCheckpoint::PreDispatch),
            outcome: if action.selected_executor.is_some()
                && (!matches!(
                    interaction_model,
                    crawfish_types::InteractionModel::RemoteAgent
                ) || has_remote_treaty)
                || matches!(
                    action.phase,
                    ActionPhase::Completed | ActionPhase::Failed | ActionPhase::Blocked
                ) {
                CheckpointOutcome::Passed
            } else {
                CheckpointOutcome::Pending
            },
            reason: action
                .selected_executor
                .as_ref()
                .map(|executor| format!("executor selected: {executor}")),
        },
        CheckpointStatus {
            checkpoint: OversightCheckpoint::PreMutation,
            required: requires(OversightCheckpoint::PreMutation),
            outcome: if action.capability != "workspace.patch.apply" {
                CheckpointOutcome::Skipped
            } else if action.lock_detail.is_some() || action.phase == ActionPhase::Completed {
                CheckpointOutcome::Passed
            } else if action.phase == ActionPhase::AwaitingApproval {
                CheckpointOutcome::Pending
            } else {
                CheckpointOutcome::Failed
            },
            reason: if action.capability != "workspace.patch.apply" {
                Some("capability is proposal-only".to_string())
            } else {
                action.failure_reason.clone()
            },
        },
        CheckpointStatus {
            checkpoint: OversightCheckpoint::PostResult,
            required: requires(OversightCheckpoint::PostResult),
            outcome: if !matches!(
                action.phase,
                ActionPhase::Completed
                    | ActionPhase::Failed
                    | ActionPhase::Blocked
                    | ActionPhase::Expired
            ) {
                CheckpointOutcome::Pending
            } else if has_trace_bundle
                && (!evaluation_required_for_action(action)
                    || (profile_resolved && latest_evaluation.is_some()))
                && (!matches!(
                    interaction_model,
                    crawfish_types::InteractionModel::RemoteAgent
                ) || (has_remote_receipt
                    && matches!(
                        remote_outcome_disposition,
                        Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
                    )))
            {
                CheckpointOutcome::Passed
            } else {
                CheckpointOutcome::Failed
            },
            reason: if !has_trace_bundle {
                Some("trace bundle not available".to_string())
            } else if evaluation_required_for_action(action) && !profile_resolved {
                Some("evaluation profile required but unresolved".to_string())
            } else if evaluation_required_for_action(action) && latest_evaluation.is_none() {
                Some("evaluation required but missing".to_string())
            } else if matches!(
                interaction_model,
                crawfish_types::InteractionModel::RemoteAgent
            ) && !has_remote_receipt
            {
                Some("remote delegation receipt is missing".to_string())
            } else if matches!(
                interaction_model,
                crawfish_types::InteractionModel::RemoteAgent
            ) && !matches!(
                remote_outcome_disposition,
                Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
            ) {
                Some("remote outcome did not satisfy treaty post-result governance".to_string())
            } else {
                Some("terminal evidence present".to_string())
            },
        },
    ]
}

pub(crate) fn trust_domain_defaults(trust_domain: TrustDomain) -> crawfish_types::EncounterPolicy {
    let mut policy = crawfish_types::EncounterPolicy {
        default_disposition: crawfish_types::DefaultDisposition::AllowWithLease,
        capability_visibility: crawfish_types::CapabilityVisibility::OwnerOnly,
        data_boundary: crawfish_types::DataBoundaryPolicy::OwnerOnly,
        tool_boundary: crawfish_types::ToolBoundaryPolicy::NoCrossOwnerMutation,
        workspace_boundary: crawfish_types::WorkspaceBoundaryPolicy::Isolated,
        network_boundary: crawfish_types::NetworkBoundaryPolicy::LocalOnly,
        human_approval_requirements: Vec::new(),
    };

    match trust_domain {
        TrustDomain::SameOwnerLocal => {}
        TrustDomain::SameDeviceForeignOwner => {
            policy.default_disposition = crawfish_types::DefaultDisposition::RequireConsent;
            policy.workspace_boundary = crawfish_types::WorkspaceBoundaryPolicy::LeaseScoped;
            policy.data_boundary = crawfish_types::DataBoundaryPolicy::LeaseScoped;
            policy.network_boundary = crawfish_types::NetworkBoundaryPolicy::LeasedEgress;
        }
        TrustDomain::InternalOrg | TrustDomain::ExternalPartner => {
            policy.default_disposition = crawfish_types::DefaultDisposition::RequireConsent;
            policy.workspace_boundary = crawfish_types::WorkspaceBoundaryPolicy::LeaseScoped;
            policy.data_boundary = crawfish_types::DataBoundaryPolicy::LeaseScoped;
        }
        TrustDomain::PublicUnknown => {
            policy.default_disposition = crawfish_types::DefaultDisposition::Deny;
        }
    }

    policy
}

#[allow(dead_code)]
pub fn summarize_capabilities(manifest: &AgentManifest) -> Vec<CapabilityDescriptor> {
    manifest
        .capabilities
        .iter()
        .map(|capability| CapabilityDescriptor {
            namespace: capability.clone(),
            verbs: vec!["run".to_string()],
            executor_class: crawfish_types::ExecutorClass::Hybrid,
            mutability: if capability.contains("patch") || capability.contains("write") {
                Mutability::Mutating
            } else {
                Mutability::ReadOnly
            },
            risk_class: crawfish_types::RiskClass::Medium,
            cost_class: crawfish_types::CostClass::Standard,
            latency_class: crawfish_types::LatencyClass::Background,
            approval_requirements: Vec::new(),
        })
        .collect()
}

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

impl Supervisor {
    pub(crate) fn authorize(
        &self,
        manifest: &AgentManifest,
        request: &EncounterRequest,
    ) -> EncounterDecision {
        authorize_encounter(
            &GovernanceContext {
                system_defaults: self.config.governance.system_defaults.clone(),
                owner_policy: neutral_policy(),
                trust_domain_defaults: trust_domain_defaults(request.caller.trust_domain.clone()),
                manifest_policy: owner_policy_for_manifest(manifest),
            },
            request,
        )
    }

    pub(crate) async fn preflight_submission(
        &self,
        request: &SubmitActionRequest,
    ) -> anyhow::Result<(
        AgentManifest,
        CompiledExecutionPlan,
        EncounterRequest,
        EncounterDecision,
        bool,
    )> {
        let manifest = self
            .store
            .get_agent_manifest(&request.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", request.target_agent_id))?;
        self.validate_submit_action_request(&manifest, request)?;

        let compiled = compile_execution_plan(
            &self.config.contracts.org_defaults,
            &manifest.contract_defaults,
            &request.contract_overrides.clone().unwrap_or_default(),
            &manifest.strategy_defaults,
            &request.capability,
            request.execution_strategy.clone(),
        )
        .map_err(|error| anyhow::anyhow!("invalid action request: {error}"))?;

        let caller = request
            .counterparty_refs
            .first()
            .cloned()
            .unwrap_or_else(|| CounterpartyRef {
                agent_id: None,
                session_id: Some("local".to_string()),
                owner: request.initiator_owner.clone(),
                trust_domain: TrustDomain::SameOwnerLocal,
            });
        let encounter_request = EncounterRequest {
            caller,
            target_agent_id: request.target_agent_id.clone(),
            target_owner: manifest.owner.clone(),
            requested_capabilities: vec![request.capability.clone()],
            requests_workspace_write: request.workspace_write,
            requests_secret_access: request.secret_access,
            requests_mutating_capability: request.mutating,
        };
        let decision = self.authorize(&manifest, &encounter_request);
        let requires_approval = self.action_requires_approval(
            request,
            &manifest,
            &request.capability,
            &compiled.contract.safety.approval_policy,
        );

        Ok((
            manifest,
            compiled,
            encounter_request,
            decision,
            requires_approval,
        ))
    }

    pub(crate) fn action_requires_approval(
        &self,
        request: &SubmitActionRequest,
        manifest: &AgentManifest,
        capability: &str,
        approval_policy: &ApprovalPolicy,
    ) -> bool {
        if capability == "workspace.patch.apply" {
            return true;
        }

        if request.workspace_write || request.secret_access || request.mutating {
            return !matches!(approval_policy, ApprovalPolicy::None)
                || matches!(
                    manifest.workspace_policy.write_mode,
                    crawfish_types::WorkspaceWriteMode::ApprovalGated
                );
        }

        matches!(approval_policy, ApprovalPolicy::Always)
    }

    pub(crate) async fn create_encounter(
        &self,
        manifest: &AgentManifest,
        request: &EncounterRequest,
        _decision: &EncounterDecision,
        state: EncounterState,
    ) -> anyhow::Result<EncounterRecord> {
        let encounter = EncounterRecord {
            id: Uuid::new_v4().to_string(),
            initiator_ref: request.caller.clone(),
            target_agent_id: request.target_agent_id.clone(),
            target_owner: manifest.owner.clone(),
            trust_domain: request.caller.trust_domain.clone(),
            requested_capabilities: request.requested_capabilities.clone(),
            applied_policy_source: "system>owner>trust-domain>manifest".to_string(),
            state,
            grant_refs: Vec::new(),
            lease_ref: None,
            created_at: now_timestamp(),
        };
        self.store.insert_encounter(&encounter).await?;
        Ok(encounter)
    }

    pub(crate) async fn emit_audit_receipt(
        &self,
        encounter_ref: &str,
        grant_refs: Vec<String>,
        lease_ref: Option<String>,
        outcome: AuditOutcome,
        reason: String,
        approver_ref: Option<String>,
    ) -> anyhow::Result<AuditReceipt> {
        let receipt = AuditReceipt {
            id: Uuid::new_v4().to_string(),
            encounter_ref: encounter_ref.to_string(),
            grant_refs,
            lease_ref,
            outcome,
            reason,
            approver_ref,
            emitted_at: now_timestamp(),
        };
        self.store.insert_audit_receipt(&receipt).await?;
        Ok(receipt)
    }

    pub(crate) fn approval_expiry_for_action(&self, action: &Action) -> String {
        let base = action.created_at.parse::<u64>().unwrap_or_default();
        let deadline = action.contract.delivery.deadline_ms.unwrap_or(900_000);
        (base.saturating_add(deadline / 1000)).to_string()
    }

    pub(crate) async fn issue_grant_and_lease(
        &self,
        action: &Action,
        manifest: &AgentManifest,
        encounter: &mut EncounterRecord,
        approver_ref: Option<String>,
        reason: String,
    ) -> anyhow::Result<(ConsentGrant, CapabilityLease, AuditReceipt)> {
        let expires_at = self.approval_expiry_for_action(action);
        let grant = ConsentGrant {
            id: Uuid::new_v4().to_string(),
            grantor: manifest.owner.clone(),
            grantee: action.initiator_owner.clone(),
            purpose: action.goal.summary.clone(),
            scope: vec![action.capability.clone()],
            issued_at: now_timestamp(),
            expires_at: expires_at.clone(),
            revocable: true,
            approver_ref: approver_ref.clone(),
        };
        self.store.upsert_consent_grant(&grant).await?;

        let lease = CapabilityLease {
            id: Uuid::new_v4().to_string(),
            grant_ref: grant.id.clone(),
            lessor: manifest.owner.clone(),
            lessee: action.initiator_owner.clone(),
            capability_refs: vec![action.capability.clone()],
            scope: if action.contract.safety.tool_scope.is_empty() {
                vec![action.capability.clone()]
            } else {
                action.contract.safety.tool_scope.clone()
            },
            issued_at: now_timestamp(),
            expires_at,
            revocation_reason: None,
            audit_receipt_ref: String::new(),
        };
        self.store.upsert_capability_lease(&lease).await?;

        encounter.state = EncounterState::Leased;
        encounter.grant_refs = vec![grant.id.clone()];
        encounter.lease_ref = Some(lease.id.clone());
        self.store.insert_encounter(encounter).await?;

        let receipt = self
            .emit_audit_receipt(
                &encounter.id,
                vec![grant.id.clone()],
                Some(lease.id.clone()),
                AuditOutcome::Allowed,
                reason,
                approver_ref,
            )
            .await?;

        let mut persisted_lease = lease;
        persisted_lease.audit_receipt_ref = receipt.id.clone();
        self.store.upsert_capability_lease(&persisted_lease).await?;

        Ok((grant, persisted_lease, receipt))
    }

    pub(crate) async fn ensure_pre_execution_lease_valid(
        &self,
        action: &Action,
    ) -> anyhow::Result<()> {
        let Some(lease_ref) = &action.lease_ref else {
            if action.capability == "workspace.patch.apply" {
                anyhow::bail!("mutation action requires an active capability lease");
            }
            return Ok(());
        };
        let lease = self
            .store
            .get_capability_lease(lease_ref)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capability lease not found: {lease_ref}"))?;
        if lease.revocation_reason.is_some() {
            anyhow::bail!("capability lease {} has been revoked", lease.id);
        }
        let now = current_timestamp_seconds();
        let expires_at = lease.expires_at.parse::<u64>().unwrap_or_default();
        if expires_at > 0 && now >= expires_at {
            anyhow::bail!("capability lease {} has expired", lease.id);
        }
        Ok(())
    }
}

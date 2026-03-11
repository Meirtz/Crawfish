use super::*;

pub(crate) fn delegation_depth_for_action(action: &Action) -> Option<u32> {
    external_ref_value(&action.external_refs, "a2a.delegation_depth")
        .and_then(|value| value.parse::<u32>().ok())
        .or_else(|| {
            if matches!(
                interaction_model_for_action(action, None),
                crawfish_types::InteractionModel::RemoteAgent
            ) {
                Some(1)
            } else {
                None
            }
        })
}

pub(crate) fn federation_pack_id_for_action(action: &Action) -> Option<String> {
    external_ref_value(&action.external_refs, "a2a.federation_pack")
}

pub(crate) fn remote_followup_context(action: &Action) -> Option<&Value> {
    action.inputs.get("remote_followup")
}

pub(crate) fn active_remote_followup_ref_for_action(action: &Action) -> Option<String> {
    remote_followup_context(action)
        .and_then(Value::as_object)
        .and_then(|value| value.get("request_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub(crate) fn clear_remote_followup_context(action: &mut Action) {
    action.inputs.remove("remote_followup");
}

pub(crate) fn strategy_iteration_for_action(action: &Action) -> u32 {
    action
        .outputs
        .metadata
        .get("strategy_iteration")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

pub(crate) fn remote_evidence_status_for_action(action: &Action) -> Option<RemoteEvidenceStatus> {
    action
        .outputs
        .metadata
        .get("federation_remote_evidence_status")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn remote_state_disposition_for_action(
    action: &Action,
) -> Option<RemoteStateDisposition> {
    action
        .outputs
        .metadata
        .get("federation_remote_state_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn remote_review_disposition_for_action(
    action: &Action,
) -> Option<RemoteReviewDisposition> {
    action
        .outputs
        .metadata
        .get("remote_review_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn federation_decision_for_action(action: &Action) -> Option<FederationDecision> {
    action
        .outputs
        .metadata
        .get("federation_decision")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn remote_outcome_disposition_for_action(
    action: &Action,
) -> Option<crawfish_types::RemoteOutcomeDisposition> {
    action
        .outputs
        .metadata
        .get("treaty_remote_outcome_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn treaty_violations_for_action(
    action: &Action,
) -> Vec<crawfish_types::TreatyViolation> {
    action
        .outputs
        .metadata
        .get("treaty_violations")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

pub(crate) fn set_remote_review_disposition_metadata(
    outputs: &mut ActionOutputs,
    disposition: &RemoteReviewDisposition,
) {
    outputs.metadata.insert(
        "remote_review_disposition".to_string(),
        serde_json::to_value(disposition).unwrap_or(Value::Null),
    );
}

pub(crate) fn remote_review_reason_for_action(
    action: &Action,
    policy_incidents: &[PolicyIncident],
) -> Option<RemoteReviewReason> {
    if matches!(
        remote_evidence_status_for_action(action),
        Some(RemoteEvidenceStatus::ScopeViolation)
    ) || treaty_violations_for_action(action)
        .iter()
        .any(|violation| violation.code == "treaty_scope_violation")
    {
        return Some(RemoteReviewReason::ScopeViolation);
    }
    if matches!(
        remote_evidence_status_for_action(action),
        Some(RemoteEvidenceStatus::MissingRequiredEvidence)
    ) || policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_enforcement_gap")
    {
        return Some(RemoteReviewReason::EvidenceGap);
    }
    if matches!(
        remote_state_disposition_for_action(action),
        Some(RemoteStateDisposition::Blocked | RemoteStateDisposition::AwaitingApproval)
    ) || policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "remote_state_escalated")
    {
        return Some(RemoteReviewReason::RemoteStateEscalated);
    }
    if matches!(
        remote_outcome_disposition_for_action(action),
        Some(RemoteOutcomeDisposition::ReviewRequired)
    ) {
        return Some(RemoteReviewReason::ResultReviewRequired);
    }
    None
}

pub(crate) fn task_plan_delegated_data_scopes(action: &Action) -> Vec<String> {
    let mut scopes = Vec::new();
    for key in [
        "objective",
        "workspace_root",
        "context_files",
        "constraints",
        "desired_outputs",
        "background",
        "verification_feedback",
        "base_ref",
        "head_ref",
    ] {
        if action.inputs.contains_key(key) {
            scopes.push(key.to_string());
        }
    }
    scopes
}

pub(crate) fn set_treaty_result_metadata(
    outputs: &mut ActionOutputs,
    disposition: &crawfish_types::RemoteOutcomeDisposition,
    violations: &[crawfish_types::TreatyViolation],
) {
    outputs.metadata.insert(
        "treaty_remote_outcome_disposition".to_string(),
        serde_json::to_value(disposition).unwrap_or(Value::Null),
    );
    outputs.metadata.insert(
        "treaty_violations".to_string(),
        serde_json::to_value(violations).unwrap_or_else(|_| Value::Array(Vec::new())),
    );
}

pub(crate) fn set_federation_result_metadata(
    outputs: &mut ActionOutputs,
    decision: &FederationDecision,
    evidence_status: Option<&RemoteEvidenceStatus>,
    state_disposition: Option<&RemoteStateDisposition>,
    result_acceptance: Option<&RemoteResultAcceptance>,
) {
    outputs.metadata.insert(
        "federation_decision".to_string(),
        serde_json::to_value(decision).unwrap_or(Value::Null),
    );
    if let Some(evidence_status) = evidence_status {
        outputs.metadata.insert(
            "federation_remote_evidence_status".to_string(),
            serde_json::to_value(evidence_status).unwrap_or(Value::Null),
        );
    }
    if let Some(state_disposition) = state_disposition {
        outputs.metadata.insert(
            "federation_remote_state_disposition".to_string(),
            serde_json::to_value(state_disposition).unwrap_or(Value::Null),
        );
    }
    if let Some(result_acceptance) = result_acceptance {
        outputs.metadata.insert(
            "federation_remote_result_acceptance".to_string(),
            serde_json::to_value(result_acceptance).unwrap_or(Value::Null),
        );
    }
}

impl Supervisor {
    pub(crate) fn builtin_federation_pack_template(&self) -> FederationPack {
        FederationPack {
            id: "remote_task_plan_default".to_string(),
            title: "Remote task.plan federation pack".to_string(),
            summary: "Built-in remote governance pack for proposal-only task planning over a treaty-governed frontier.".to_string(),
            treaty_pack_id: "<binding treaty>".to_string(),
            review_defaults: crawfish_types::FederationReviewDefaults {
                enabled: true,
                priority: "high".to_string(),
            },
            alert_defaults: crawfish_types::FederationAlertDefaults {
                rules: vec!["frontier_gap_detected".to_string()],
            },
            required_remote_evidence: vec![
                crawfish_types::TreatyEvidenceRequirement::DelegationReceiptPresent,
                crawfish_types::TreatyEvidenceRequirement::RemoteTaskRefPresent,
                crawfish_types::TreatyEvidenceRequirement::TerminalStateVerified,
                crawfish_types::TreatyEvidenceRequirement::ArtifactClassesAllowed,
                crawfish_types::TreatyEvidenceRequirement::DataScopesAllowed,
            ],
            result_acceptance_policy: RemoteResultAcceptance::Accepted,
            scope_violation_policy: RemoteResultAcceptance::Rejected,
            evidence_gap_policy: RemoteResultAcceptance::ReviewRequired,
            blocked_remote_policy: RemoteStateDisposition::Blocked,
            auth_required_policy: RemoteStateDisposition::AwaitingApproval,
            remote_failure_policy: RemoteStateDisposition::Failed,
            required_checkpoints: vec![
                OversightCheckpoint::Admission,
                OversightCheckpoint::PreDispatch,
                OversightCheckpoint::PostResult,
            ],
            followup_allowed: true,
            max_followup_attempts: 2,
            followup_review_priority: "high".to_string(),
            max_delegation_depth: 1,
        }
    }

    pub(crate) fn resolve_federation_pack_by_id(
        &self,
        pack_id: &str,
        treaty_pack: Option<&crawfish_types::TreatyPack>,
    ) -> Option<FederationPack> {
        if let Some(pack) = self.config.federation.packs.get(pack_id).cloned() {
            return Some(pack);
        }
        if pack_id == "remote_task_plan_default" {
            return Some(match treaty_pack {
                Some(treaty_pack) => self.builtin_federation_pack(treaty_pack),
                None => self.builtin_federation_pack_template(),
            });
        }
        None
    }

    pub(crate) fn remote_followup_reasons_for_bundle(
        &self,
        bundle: &RemoteEvidenceBundle,
    ) -> Vec<RemoteFollowupReason> {
        let mut reasons = Vec::new();
        if matches!(
            bundle.remote_evidence_status,
            Some(RemoteEvidenceStatus::MissingRequiredEvidence)
        ) {
            reasons.push(RemoteFollowupReason::MissingTreatyEvidence);
        }
        if bundle.evidence_items.iter().any(|item| {
            !item.satisfied && matches!(item.checkpoint, Some(OversightCheckpoint::PostResult))
        }) {
            reasons.push(RemoteFollowupReason::PostResultCheckpointGap);
        }
        if matches!(
            bundle.remote_evidence_status,
            Some(RemoteEvidenceStatus::ScopeViolation)
        ) {
            reasons.push(RemoteFollowupReason::ScopeDataAmbiguity);
        }
        if bundle
            .evidence_items
            .iter()
            .any(|item| item.id == "artifact_classes_allowed" && !item.satisfied)
        {
            reasons.push(RemoteFollowupReason::ArtifactAdmissibilityAmbiguity);
        }
        if reasons.is_empty() {
            reasons.push(RemoteFollowupReason::OperatorRequestedClarification);
        }
        let mut deduped = Vec::new();
        for reason in reasons {
            if !deduped.contains(&reason) {
                deduped.push(reason);
            }
        }
        deduped
    }

    pub(crate) fn requested_evidence_for_bundle(
        &self,
        bundle: &RemoteEvidenceBundle,
    ) -> Vec<String> {
        let mut requested = bundle
            .evidence_items
            .iter()
            .filter(|item| !item.satisfied)
            .map(|item| item.summary.clone())
            .collect::<Vec<_>>();
        requested.extend(
            bundle
                .policy_incidents
                .iter()
                .filter(|incident| {
                    matches!(
                        incident.reason_code.as_str(),
                        "frontier_enforcement_gap" | "treaty_scope_violation"
                    )
                })
                .map(|incident| incident.summary.clone()),
        );
        requested.sort();
        requested.dedup();
        if requested.is_empty() {
            requested.push("provide admissibility evidence for the remote result".to_string());
        }
        requested
    }

    pub(crate) async fn create_remote_followup_request(
        &self,
        action: &Action,
        bundle: &RemoteEvidenceBundle,
        reason_code: String,
        operator_note: Option<String>,
    ) -> anyhow::Result<RemoteFollowupRequest> {
        let treaty_pack_id = bundle
            .treaty_pack_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("remote follow-up requires treaty pack lineage"))?;
        let federation_pack_id = bundle
            .federation_pack_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("remote follow-up requires federation pack lineage"))?;
        let remote_principal = bundle
            .remote_principal
            .clone()
            .ok_or_else(|| anyhow::anyhow!("remote follow-up requires remote principal lineage"))?;
        let timestamp = now_timestamp();
        let request = RemoteFollowupRequest {
            id: Uuid::new_v4().to_string(),
            action_id: action.id.clone(),
            remote_evidence_ref: bundle.id.clone(),
            treaty_pack_id,
            federation_pack_id,
            remote_principal,
            remote_task_ref: bundle.remote_task_ref.clone(),
            reason_code,
            reasons: self.remote_followup_reasons_for_bundle(bundle),
            requested_evidence: self.requested_evidence_for_bundle(bundle),
            operator_note,
            status: RemoteFollowupStatus::Open,
            dispatched_at: None,
            dispatched_by: None,
            closed_at: None,
            superseded_by: None,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        self.store.upsert_remote_followup_request(&request).await?;
        Ok(request)
    }

    pub(crate) async fn close_active_remote_followup_request(
        &self,
        action: &Action,
    ) -> anyhow::Result<Option<RemoteFollowupRequest>> {
        let Some(followup_id) = active_remote_followup_ref_for_action(action) else {
            return Ok(None);
        };
        let Some(mut followup) = self.store.get_remote_followup_request(&followup_id).await? else {
            return Ok(None);
        };
        if matches!(
            followup.status,
            RemoteFollowupStatus::Open | RemoteFollowupStatus::Dispatched
        ) {
            followup.status = RemoteFollowupStatus::Closed;
            followup.closed_at = Some(now_timestamp());
            followup.updated_at = now_timestamp();
            self.store.upsert_remote_followup_request(&followup).await?;
        }
        Ok(Some(followup))
    }

    pub(crate) async fn list_federation_packs(&self) -> anyhow::Result<FederationPackListResponse> {
        let mut packs = self
            .config
            .federation
            .packs
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if !packs
            .iter()
            .any(|pack| pack.id == "remote_task_plan_default")
        {
            packs.push(self.builtin_federation_pack_template());
        }
        Ok(FederationPackListResponse { packs })
    }

    pub(crate) async fn get_federation_pack(
        &self,
        pack_id: &str,
    ) -> anyhow::Result<Option<FederationPackDetailResponse>> {
        Ok(self
            .resolve_federation_pack_by_id(pack_id, None)
            .map(|pack| FederationPackDetailResponse { pack }))
    }
}

impl Supervisor {
    pub(crate) fn resolve_mcp_adapter(
        &self,
        manifest: &AgentManifest,
        action: &Action,
    ) -> anyhow::Result<Option<(McpAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::Mcp(binding) => Some(binding.clone()),
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let server = self
            .config
            .mcp
            .servers
            .get(&binding.server)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("mcp server {} is not configured", binding.server))?;
        let adapter =
            McpAdapter::configured(binding.server.clone(), server.clone(), binding.clone())?;
        let mut external_refs = vec![
            ExternalRef {
                kind: "mcp_server".to_string(),
                value: binding.server.clone(),
                endpoint: Some(server.url),
            },
            ExternalRef {
                kind: "mcp_tool".to_string(),
                value: binding.tool,
                endpoint: None,
            },
        ];
        if let Some(resource_ref) = action
            .inputs
            .get("mcp_resource_ref")
            .and_then(Value::as_str)
        {
            external_refs.push(ExternalRef {
                kind: "mcp_resource".to_string(),
                value: resource_ref.to_string(),
                endpoint: None,
            });
        }
        Ok(Some((adapter, external_refs)))
    }

    pub(crate) fn resolve_openclaw_adapter(
        &self,
        manifest: &AgentManifest,
    ) -> anyhow::Result<Option<(OpenClawAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::Openclaw(binding) => Some(binding.clone()),
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let external_refs = vec![
            ExternalRef {
                kind: "openclaw.gateway_url".to_string(),
                value: binding.gateway_url.clone(),
                endpoint: Some(binding.gateway_url.clone()),
            },
            ExternalRef {
                kind: "openclaw.target_agent".to_string(),
                value: binding.target_agent.clone(),
                endpoint: None,
            },
        ];
        Ok(Some((
            OpenClawAdapter::new(binding, self.state_dir()),
            external_refs,
        )))
    }

    pub(crate) fn resolve_local_harness_adapter(
        &self,
        manifest: &AgentManifest,
        harness: LocalHarnessKind,
    ) -> anyhow::Result<Option<(LocalHarnessAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::LocalHarness(binding)
                if binding.capability == "task.plan" && binding.harness == harness =>
            {
                Some(binding.clone())
            }
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let harness_name = match binding.harness {
            LocalHarnessKind::ClaudeCode => "claude_code",
            LocalHarnessKind::Codex => "codex",
        };
        let external_refs = vec![
            ExternalRef {
                kind: "local_harness.harness".to_string(),
                value: harness_name.to_string(),
                endpoint: None,
            },
            ExternalRef {
                kind: "local_harness.command".to_string(),
                value: binding.command.clone(),
                endpoint: None,
            },
        ];
        Ok(Some((
            LocalHarnessAdapter::new(binding, self.state_dir()),
            external_refs,
        )))
    }

    pub(crate) fn resolve_a2a_adapter(
        &self,
        manifest: &AgentManifest,
    ) -> anyhow::Result<Option<(A2aAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::A2a(binding) if binding.capability == "task.plan" => {
                Some(binding.clone())
            }
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let treaty_pack = self
            .config
            .treaties
            .packs
            .get(&binding.treaty_pack)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "treaty pack {} is not configured for remote delegation",
                    binding.treaty_pack
                )
            })?;
        let external_refs = vec![
            ExternalRef {
                kind: "a2a.agent_card_url".to_string(),
                value: binding.agent_card_url.clone(),
                endpoint: Some(binding.agent_card_url.clone()),
            },
            ExternalRef {
                kind: "a2a.remote_principal".to_string(),
                value: treaty_pack.remote_principal.id.clone(),
                endpoint: None,
            },
        ];
        Ok(Some((
            A2aAdapter::new(binding, treaty_pack, self.state_dir()),
            external_refs,
        )))
    }

    pub(crate) fn builtin_federation_pack(
        &self,
        treaty_pack: &crawfish_types::TreatyPack,
    ) -> FederationPack {
        FederationPack {
            id: "remote_task_plan_default".to_string(),
            title: "Remote task.plan federation pack".to_string(),
            summary: "Default remote-agent governance pack for proposal-only task planning over a treaty-governed frontier.".to_string(),
            treaty_pack_id: treaty_pack.id.clone(),
            review_defaults: crawfish_types::FederationReviewDefaults {
                enabled: treaty_pack.review_queue,
                priority: "high".to_string(),
            },
            alert_defaults: crawfish_types::FederationAlertDefaults {
                rules: treaty_pack.alert_rules.clone(),
            },
            required_remote_evidence: treaty_pack.required_result_evidence.clone(),
            result_acceptance_policy: RemoteResultAcceptance::Accepted,
            scope_violation_policy: match treaty_pack.on_scope_violation {
                crawfish_types::TreatyEscalationMode::Deny => RemoteResultAcceptance::Rejected,
                crawfish_types::TreatyEscalationMode::ReviewRequired => {
                    RemoteResultAcceptance::ReviewRequired
                }
            },
            evidence_gap_policy: match treaty_pack.on_evidence_gap {
                crawfish_types::TreatyEscalationMode::Deny => RemoteResultAcceptance::Rejected,
                crawfish_types::TreatyEscalationMode::ReviewRequired => {
                    RemoteResultAcceptance::ReviewRequired
                }
            },
            blocked_remote_policy: RemoteStateDisposition::Blocked,
            auth_required_policy: RemoteStateDisposition::AwaitingApproval,
            remote_failure_policy: RemoteStateDisposition::Failed,
            required_checkpoints: treaty_pack.required_checkpoints.clone(),
            followup_allowed: true,
            max_followup_attempts: 2,
            followup_review_priority: "high".to_string(),
            max_delegation_depth: treaty_pack.max_delegation_depth,
        }
    }

    pub(crate) fn resolve_federation_pack(
        &self,
        binding: &crawfish_types::A2ARemoteAgentBinding,
        treaty_pack: &crawfish_types::TreatyPack,
    ) -> anyhow::Result<FederationPack> {
        match binding.federation_pack.as_deref() {
            Some(pack_id) => self
                .config
                .federation
                .packs
                .get(pack_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("federation pack {pack_id} is not configured")),
            None => Ok(self.builtin_federation_pack(treaty_pack)),
        }
    }

    pub(crate) fn compile_treaty_decision(
        &self,
        action: &Action,
        binding: &crawfish_types::A2ARemoteAgentBinding,
        treaty_pack: &crawfish_types::TreatyPack,
    ) -> anyhow::Result<crawfish_types::TreatyDecision> {
        if action.initiator_owner.kind != treaty_pack.local_owner.kind
            || action.initiator_owner.id != treaty_pack.local_owner.id
        {
            anyhow::bail!(
                "treaty {} requires local owner {}:{}, got {}:{}",
                treaty_pack.id,
                runtime_enum_to_snake(&treaty_pack.local_owner.kind),
                treaty_pack.local_owner.id,
                runtime_enum_to_snake(&action.initiator_owner.kind),
                action.initiator_owner.id
            );
        }
        if !treaty_pack
            .allowed_capabilities
            .iter()
            .any(|capability| capability == &binding.capability)
        {
            anyhow::bail!(
                "treaty {} does not allow capability {}",
                treaty_pack.id,
                binding.capability
            );
        }
        if treaty_pack.max_delegation_depth != 1 {
            anyhow::bail!(
                "treaty {} exceeds supported delegation depth {}",
                treaty_pack.id,
                treaty_pack.max_delegation_depth
            );
        }

        let delegated_data_scopes = task_plan_delegated_data_scopes(action);
        let out_of_scope = delegated_data_scopes
            .iter()
            .filter(|scope| {
                !treaty_pack
                    .allowed_data_scopes
                    .iter()
                    .any(|allowed| allowed == *scope)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !out_of_scope.is_empty() {
            anyhow::bail!(
                "treaty {} does not allow delegated data scopes: {}",
                treaty_pack.id,
                out_of_scope.join(", ")
            );
        }

        Ok(crawfish_types::TreatyDecision {
            treaty_pack_id: treaty_pack.id.clone(),
            remote_principal: treaty_pack.remote_principal.clone(),
            capability: binding.capability.clone(),
            requested_scopes: binding.required_scopes.clone(),
            delegated_data_scopes,
            required_checkpoints: treaty_pack.required_checkpoints.clone(),
            required_result_evidence: treaty_pack.required_result_evidence.clone(),
            delegation_depth: 1,
            on_scope_violation: treaty_pack.on_scope_violation.clone(),
            on_evidence_gap: treaty_pack.on_evidence_gap.clone(),
            review_queue: treaty_pack.review_queue,
            alert_rules: treaty_pack.alert_rules.clone(),
        })
    }

    pub(crate) fn compile_federation_decision(
        &self,
        action: &Action,
        treaty_decision: &crawfish_types::TreatyDecision,
        federation_pack: &FederationPack,
    ) -> anyhow::Result<FederationDecision> {
        if federation_pack.treaty_pack_id != treaty_decision.treaty_pack_id {
            anyhow::bail!(
                "federation pack {} is bound to treaty {}, not {}",
                federation_pack.id,
                federation_pack.treaty_pack_id,
                treaty_decision.treaty_pack_id
            );
        }
        if federation_pack.max_delegation_depth < treaty_decision.delegation_depth {
            anyhow::bail!(
                "federation pack {} does not allow delegation depth {}",
                federation_pack.id,
                treaty_decision.delegation_depth
            );
        }
        Ok(FederationDecision {
            federation_pack_id: federation_pack.id.clone(),
            treaty_pack_id: treaty_decision.treaty_pack_id.clone(),
            remote_principal: treaty_decision.remote_principal.clone(),
            capability: action.capability.clone(),
            required_checkpoints: federation_pack.required_checkpoints.clone(),
            required_remote_evidence: federation_pack.required_remote_evidence.clone(),
            delegation_depth: treaty_decision.delegation_depth,
            escalation: crawfish_types::RemoteEscalationPolicy {
                result_acceptance_policy: federation_pack.result_acceptance_policy.clone(),
                scope_violation_policy: federation_pack.scope_violation_policy.clone(),
                evidence_gap_policy: federation_pack.evidence_gap_policy.clone(),
                blocked_remote_policy: federation_pack.blocked_remote_policy.clone(),
                auth_required_policy: federation_pack.auth_required_policy.clone(),
                remote_failure_policy: federation_pack.remote_failure_policy.clone(),
            },
            review_defaults: federation_pack.review_defaults.clone(),
            alert_defaults: federation_pack.alert_defaults.clone(),
            remote_state_disposition: None,
            remote_evidence_status: None,
            remote_result_acceptance: None,
            summary: format!(
                "{} interprets remote task.plan results for treaty {} at delegation depth {}",
                federation_pack.id,
                treaty_decision.treaty_pack_id,
                treaty_decision.delegation_depth
            ),
        })
    }

    pub(crate) fn evaluate_federation_post_result(
        &self,
        outputs: &ActionOutputs,
        receipt: &crawfish_types::DelegationReceipt,
        _decision: &crawfish_types::TreatyDecision,
        treaty_pack: &crawfish_types::TreatyPack,
        federation_pack: &FederationPack,
    ) -> (
        crawfish_types::RemoteOutcomeDisposition,
        RemoteEvidenceStatus,
        Vec<crawfish_types::TreatyViolation>,
    ) {
        let mut violations = Vec::new();
        for requirement in &federation_pack.required_remote_evidence {
            match requirement {
                crawfish_types::TreatyEvidenceRequirement::DelegationReceiptPresent => {
                    if receipt.id.is_empty() {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "frontier_enforcement_gap".to_string(),
                            summary: "delegation receipt is missing".to_string(),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some("delegation_receipt".to_string()),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::RemoteTaskRefPresent => {
                    if receipt
                        .remote_task_ref
                        .as_deref()
                        .unwrap_or_default()
                        .is_empty()
                    {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "frontier_enforcement_gap".to_string(),
                            summary: "remote task reference is missing".to_string(),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some("remote_task_ref".to_string()),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::TerminalStateVerified => {
                    let verified = outputs
                        .metadata
                        .get("a2a_remote_state")
                        .and_then(Value::as_str)
                        .map(|state| matches!(state, "completed" | "failed"))
                        .unwrap_or(false)
                        && outputs.metadata.contains_key("a2a_result");
                    if !verified {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "frontier_enforcement_gap".to_string(),
                            summary:
                                "remote terminal state could not be proven from returned evidence"
                                    .to_string(),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some("terminal_state".to_string()),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::ArtifactClassesAllowed => {
                    let disallowed = outputs
                        .artifacts
                        .iter()
                        .map(artifact_basename)
                        .filter(|artifact| {
                            !treaty_pack
                                .allowed_artifact_classes
                                .iter()
                                .any(|allowed| allowed == artifact)
                        })
                        .collect::<Vec<_>>();
                    for artifact in disallowed {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "treaty_scope_violation".to_string(),
                            summary: format!(
                                "remote result returned disallowed artifact class {artifact}"
                            ),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some(artifact),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::DataScopesAllowed => {
                    let disallowed = receipt
                        .delegated_data_scopes
                        .iter()
                        .filter(|scope| {
                            !treaty_pack
                                .allowed_data_scopes
                                .iter()
                                .any(|allowed| allowed == *scope)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    for scope in disallowed {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "treaty_scope_violation".to_string(),
                            summary: format!(
                                "remote delegation used a data scope outside treaty allowance: {scope}"
                            ),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some(scope),
                        });
                    }
                }
            }
        }

        let evidence_status = if violations
            .iter()
            .any(|violation| violation.code == "treaty_scope_violation")
        {
            RemoteEvidenceStatus::ScopeViolation
        } else if violations
            .iter()
            .any(|violation| violation.code == "frontier_enforcement_gap")
        {
            RemoteEvidenceStatus::MissingRequiredEvidence
        } else {
            RemoteEvidenceStatus::Satisfied
        };

        let disposition = match evidence_status {
            RemoteEvidenceStatus::ScopeViolation => match federation_pack.scope_violation_policy {
                RemoteResultAcceptance::Accepted => {
                    crawfish_types::RemoteOutcomeDisposition::Accepted
                }
                RemoteResultAcceptance::ReviewRequired => {
                    crawfish_types::RemoteOutcomeDisposition::ReviewRequired
                }
                RemoteResultAcceptance::Rejected => {
                    crawfish_types::RemoteOutcomeDisposition::Rejected
                }
            },
            RemoteEvidenceStatus::MissingRequiredEvidence => {
                match federation_pack.evidence_gap_policy {
                    RemoteResultAcceptance::Accepted => {
                        crawfish_types::RemoteOutcomeDisposition::Accepted
                    }
                    RemoteResultAcceptance::ReviewRequired => {
                        crawfish_types::RemoteOutcomeDisposition::ReviewRequired
                    }
                    RemoteResultAcceptance::Rejected => {
                        crawfish_types::RemoteOutcomeDisposition::Rejected
                    }
                }
            }
            _ => match federation_pack.result_acceptance_policy {
                RemoteResultAcceptance::Accepted => {
                    crawfish_types::RemoteOutcomeDisposition::Accepted
                }
                RemoteResultAcceptance::ReviewRequired => {
                    crawfish_types::RemoteOutcomeDisposition::ReviewRequired
                }
                RemoteResultAcceptance::Rejected => {
                    crawfish_types::RemoteOutcomeDisposition::Rejected
                }
            },
        };

        (disposition, evidence_status, violations)
    }

    pub(crate) fn resolve_openclaw_caller(
        &self,
        target_owner: &OwnerRef,
        caller: &OpenClawCallerContext,
    ) -> Result<OpenClawResolvedCaller, RuntimeError> {
        let inbound = &self.config.openclaw.inbound;
        if !inbound.enabled {
            return Err(RuntimeError::Forbidden(
                "openclaw inbound is disabled".to_string(),
            ));
        }

        let allowed = inbound.allowed_callers.get(&caller.caller_id);
        let (owner, trust_domain, allowed_scopes) = match (allowed, &inbound.caller_owner_mapping) {
            (Some(configured), _) => {
                let owner = OwnerRef {
                    kind: configured.owner_kind.clone(),
                    id: configured.owner_id.clone(),
                    display_name: configured.display_name.clone(),
                };
                let trust_domain = configured.trust_domain.clone().unwrap_or_else(|| {
                    if owner == *target_owner {
                        TrustDomain::SameOwnerLocal
                    } else {
                        inbound.default_trust_domain.clone()
                    }
                });
                (owner, trust_domain, configured.allowed_scopes.clone())
            }
            (None, CallerOwnerMapping::Required) => {
                return Err(RuntimeError::Forbidden(format!(
                    "openclaw caller is not mapped: {}",
                    caller.caller_id
                )));
            }
            (None, CallerOwnerMapping::BestEffort) => (
                OwnerRef {
                    kind: OwnerKind::ServiceAccount,
                    id: caller.caller_id.clone(),
                    display_name: caller.display_name.clone(),
                },
                inbound.default_trust_domain.clone(),
                Vec::new(),
            ),
        };

        if !allowed_scopes.is_empty()
            && caller
                .scopes
                .iter()
                .any(|scope| !allowed_scopes.iter().any(|allowed| allowed == scope))
        {
            return Err(RuntimeError::Forbidden(format!(
                "openclaw caller requested scopes outside its allowlist: {}",
                caller.caller_id
            )));
        }

        Ok(OpenClawResolvedCaller {
            caller_id: caller.caller_id.clone(),
            counterparty: CounterpartyRef {
                agent_id: None,
                session_id: Some(caller.session_id.clone()),
                owner,
                trust_domain,
            },
            requester_id: caller.session_id.clone(),
            effective_scopes: caller.scopes.clone(),
        })
    }

    pub(crate) fn openclaw_external_refs(
        &self,
        caller: &OpenClawCallerContext,
        effective_scopes: &[String],
    ) -> Vec<ExternalRef> {
        let mut refs = vec![
            ExternalRef {
                kind: "openclaw.caller_id".to_string(),
                value: caller.caller_id.clone(),
                endpoint: None,
            },
            ExternalRef {
                kind: "openclaw.session_id".to_string(),
                value: caller.session_id.clone(),
                endpoint: None,
            },
            ExternalRef {
                kind: "openclaw.channel_id".to_string(),
                value: caller.channel_id.clone(),
                endpoint: None,
            },
        ];

        if let Some(workspace_root) = &caller.workspace_root {
            refs.push(ExternalRef {
                kind: "openclaw.workspace_root".to_string(),
                value: workspace_root.clone(),
                endpoint: None,
            });
        }

        refs.extend(effective_scopes.iter().cloned().map(|scope| ExternalRef {
            kind: "openclaw.scope".to_string(),
            value: scope,
            endpoint: None,
        }));

        refs.extend(caller.trace_ids.iter().map(|(key, value)| ExternalRef {
            kind: format!("openclaw.trace.{key}"),
            value: value.to_string(),
            endpoint: None,
        }));

        refs
    }

    pub(crate) fn action_visible_to_openclaw(
        &self,
        caller: &OpenClawResolvedCaller,
        action: &Action,
    ) -> bool {
        if action.initiator_owner == caller.counterparty.owner {
            return true;
        }

        if action.requester.id == caller.requester_id {
            return true;
        }

        if action.counterparty_refs.iter().any(|counterparty| {
            counterparty.owner == caller.counterparty.owner
                && counterparty.session_id.as_deref() == caller.counterparty.session_id.as_deref()
        }) {
            return true;
        }

        action.external_refs.iter().any(|reference| {
            (reference.kind == "openclaw.caller_id" && reference.value == caller.caller_id)
                || (reference.kind == "openclaw.session_id"
                    && Some(reference.value.as_str()) == caller.counterparty.session_id.as_deref())
        })
    }

    pub(crate) fn agent_visible_to_openclaw(
        &self,
        caller: &OpenClawResolvedCaller,
        manifest: &AgentManifest,
    ) -> bool {
        if manifest.owner == caller.counterparty.owner {
            return true;
        }

        !matches!(
            owner_policy_for_manifest(manifest).capability_visibility,
            CapabilityVisibility::Private | CapabilityVisibility::OwnerOnly
        )
    }

    pub(crate) async fn submit_openclaw_action(
        &self,
        request: OpenClawInboundActionRequest,
    ) -> Result<OpenClawInboundActionResponse, RuntimeError> {
        let manifest = self
            .store
            .get_agent_manifest(&request.target_agent_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("agent not found: {}", request.target_agent_id))
            })?;
        let caller = self.resolve_openclaw_caller(&manifest.owner, &request.caller)?;

        let mut inputs = request.inputs;
        if let Some(workspace_root) = &request.caller.workspace_root {
            inputs
                .entry("workspace_root".to_string())
                .or_insert_with(|| Value::String(workspace_root.clone()));
        }

        let submit_request = normalize_submit_request(SubmitActionRequest {
            target_agent_id: request.target_agent_id,
            requester: crawfish_types::RequesterRef {
                kind: crawfish_types::RequesterKind::Session,
                id: caller.requester_id.clone(),
            },
            initiator_owner: caller.counterparty.owner.clone(),
            capability: request.capability,
            goal: request.goal,
            inputs,
            contract_overrides: request.contract_overrides,
            execution_strategy: request.execution_strategy,
            schedule: request.schedule,
            counterparty_refs: vec![caller.counterparty.clone()],
            data_boundary: request.data_boundary,
            workspace_write: request.workspace_write,
            secret_access: request.secret_access,
            mutating: request.mutating,
        });

        let (_, _, _, decision, _) = self
            .preflight_submission(&submit_request)
            .await
            .map_err(map_submit_error)?;
        if matches!(decision.disposition, EncounterDisposition::Deny) {
            return Err(RuntimeError::Forbidden(decision.reason));
        }

        let submitted = self
            .submit_action(submit_request)
            .await
            .map_err(map_submit_error)?;
        let mut action = self
            .store
            .get_action(&submitted.action_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("action not found: {}", submitted.action_id))
            })?;
        let trace_refs = self.openclaw_external_refs(&request.caller, &caller.effective_scopes);
        action.external_refs.extend(trace_refs.clone());
        self.store
            .upsert_action(&action)
            .await
            .map_err(RuntimeError::Internal)?;
        self.store
            .append_action_event(
                &action.id,
                "openclaw_inbound",
                serde_json::json!({
                    "caller_id": request.caller.caller_id,
                    "session_id": request.caller.session_id,
                    "channel_id": request.caller.channel_id,
                    "scopes": caller.effective_scopes,
                    "trace_ids": request.caller.trace_ids,
                }),
            )
            .await
            .map_err(RuntimeError::Internal)?;

        Ok(OpenClawInboundActionResponse {
            action_id: action.id,
            phase: submitted.phase,
            requester_id: caller.requester_id,
            trace_refs,
        })
    }

    pub(crate) async fn inspect_openclaw_action(
        &self,
        action_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<ActionDetail, RuntimeError> {
        let action = self
            .store
            .get_action(action_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {action_id}")))?;
        let caller = self.resolve_openclaw_caller(&action.initiator_owner, &context.caller)?;
        if !self.action_visible_to_openclaw(&caller, &action) {
            return Err(RuntimeError::Forbidden(format!(
                "openclaw caller cannot inspect action: {action_id}"
            )));
        }

        self.inspect_action(action_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {action_id}")))
    }

    pub(crate) async fn list_openclaw_action_events(
        &self,
        action_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<ActionEventsResponse, RuntimeError> {
        self.inspect_openclaw_action(action_id, context).await?;
        self.list_action_events(action_id)
            .await
            .map_err(RuntimeError::Internal)
    }

    pub(crate) async fn inspect_openclaw_agent_status(
        &self,
        agent_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<OpenClawAgentStatusResponse, RuntimeError> {
        let manifest = self
            .store
            .get_agent_manifest(agent_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("agent not found: {agent_id}")))?;
        let caller = self.resolve_openclaw_caller(&manifest.owner, &context.caller)?;
        if !self.agent_visible_to_openclaw(&caller, &manifest) {
            return Err(RuntimeError::Forbidden(format!(
                "openclaw caller cannot inspect agent: {agent_id}"
            )));
        }
        let detail = self
            .inspect_agent(agent_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("agent not found: {agent_id}")))?;
        Ok(OpenClawAgentStatusResponse {
            agent_id: detail.lifecycle.agent_id,
            desired_state: agent_state_name(&detail.lifecycle.desired_state).to_string(),
            observed_state: agent_state_name(&detail.lifecycle.observed_state).to_string(),
            health: health_status_name(&detail.lifecycle.health).to_string(),
            transition_reason: detail.lifecycle.transition_reason,
            last_transition_at: detail.lifecycle.last_transition_at,
            degradation_profile: detail
                .lifecycle
                .degradation_profile
                .as_ref()
                .map(degraded_profile_name)
                .map(str::to_string),
            continuity_mode: detail
                .lifecycle
                .continuity_mode
                .as_ref()
                .map(continuity_mode_name)
                .map(str::to_string),
        })
    }
}

impl Supervisor {
    pub(crate) async fn sync_remote_review_state(
        &self,
        action: &Action,
        remote_evidence_ref: Option<&str>,
        disposition: &RemoteReviewDisposition,
    ) -> anyhow::Result<()> {
        if let Some(bundle_id) = remote_evidence_ref {
            if let Some(mut bundle) = self.store.get_remote_evidence_bundle(bundle_id).await? {
                bundle.remote_review_disposition = Some(disposition.clone());
                self.store.insert_remote_evidence_bundle(&bundle).await?;
            }
        }

        if let Some(mut trace) = self.store.get_trace_bundle(&action.id).await? {
            trace.remote_review_disposition = Some(disposition.clone());
            self.store.put_trace_bundle(&trace).await?;
        }

        Ok(())
    }
}

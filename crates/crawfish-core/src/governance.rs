use crawfish_types::{
    AgentManifest, ApprovalRequirement, CapabilityVisibility, CounterpartyRef, DataBoundaryPolicy,
    DefaultDisposition, EncounterPolicy, NetworkBoundaryPolicy, OwnerRef, ToolBoundaryPolicy,
    TrustDomain, WorkspaceBoundaryPolicy,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncounterRequest {
    pub caller: CounterpartyRef,
    pub target_agent_id: String,
    pub target_owner: OwnerRef,
    pub requested_capabilities: Vec<String>,
    pub requests_workspace_write: bool,
    pub requests_secret_access: bool,
    pub requests_mutating_capability: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernanceContext {
    pub system_defaults: EncounterPolicy,
    pub owner_policy: EncounterPolicy,
    pub trust_domain_defaults: EncounterPolicy,
    pub manifest_policy: EncounterPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncounterDisposition {
    Deny,
    AwaitConsent,
    IssueLease,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncounterDecision {
    pub disposition: EncounterDisposition,
    pub effective_policy: EncounterPolicy,
    pub required_grant: bool,
    pub lease_required: bool,
    pub reason: String,
}

pub fn authorize_encounter(
    context: &GovernanceContext,
    request: &EncounterRequest,
) -> EncounterDecision {
    let effective = narrow_policy(
        &context.system_defaults,
        &context.owner_policy,
        &context.trust_domain_defaults,
        &context.manifest_policy,
    );

    if request.caller.trust_domain == TrustDomain::SameDeviceForeignOwner
        && (request.requests_workspace_write
            || request.requests_secret_access
            || request.requests_mutating_capability)
        && effective.tool_boundary == ToolBoundaryPolicy::NoCrossOwnerMutation
    {
        return EncounterDecision {
            disposition: EncounterDisposition::Deny,
            effective_policy: effective,
            required_grant: false,
            lease_required: false,
            reason: "same-device foreign-owner mutation or secret access is denied by default"
                .to_string(),
        };
    }

    let approvals_required = !effective.human_approval_requirements.is_empty();
    let (disposition, required_grant, lease_required) = match effective.default_disposition {
        DefaultDisposition::Deny => (EncounterDisposition::Deny, false, false),
        DefaultDisposition::RequireConsent => (EncounterDisposition::AwaitConsent, true, false),
        DefaultDisposition::AllowWithLease => {
            if approvals_required {
                (EncounterDisposition::AwaitConsent, true, true)
            } else {
                (EncounterDisposition::IssueLease, true, true)
            }
        }
    };

    let reason = explain_decision(
        &disposition,
        &request.caller.trust_domain,
        approvals_required,
    );

    EncounterDecision {
        disposition,
        effective_policy: effective,
        required_grant,
        lease_required,
        reason,
    }
}

pub fn owner_policy_for_manifest(manifest: &AgentManifest) -> EncounterPolicy {
    if manifest.encounter_policy == EncounterPolicy::default() {
        neutral_policy()
    } else {
        manifest.encounter_policy.clone()
    }
}

pub fn neutral_policy() -> EncounterPolicy {
    EncounterPolicy {
        default_disposition: DefaultDisposition::AllowWithLease,
        capability_visibility: CapabilityVisibility::Discoverable,
        data_boundary: DataBoundaryPolicy::Redacted,
        tool_boundary: ToolBoundaryPolicy::LeaseScoped,
        workspace_boundary: WorkspaceBoundaryPolicy::ReadShared,
        network_boundary: NetworkBoundaryPolicy::Allowlisted,
        human_approval_requirements: Vec::new(),
    }
}

fn explain_decision(
    disposition: &EncounterDisposition,
    trust_domain: &TrustDomain,
    approvals_required: bool,
) -> String {
    if matches!(disposition, EncounterDisposition::Deny) {
        "encounter denied by the effective policy".to_string()
    } else if matches!(trust_domain, TrustDomain::SameDeviceForeignOwner) && approvals_required {
        "foreign-owner encounter requires explicit consent before any lease can be issued"
            .to_string()
    } else if approvals_required {
        "encounter requires approval under the effective policy".to_string()
    } else {
        "encounter is leasable under the effective policy".to_string()
    }
}

fn narrow_policy(
    system_defaults: &EncounterPolicy,
    owner_policy: &EncounterPolicy,
    trust_domain_defaults: &EncounterPolicy,
    manifest_policy: &EncounterPolicy,
) -> EncounterPolicy {
    let first = narrow_pair(system_defaults, owner_policy);
    let second = narrow_pair(&first, trust_domain_defaults);
    narrow_pair(&second, manifest_policy)
}

fn narrow_pair(left: &EncounterPolicy, right: &EncounterPolicy) -> EncounterPolicy {
    EncounterPolicy {
        default_disposition: narrow_disposition(
            &left.default_disposition,
            &right.default_disposition,
        ),
        capability_visibility: narrow_capability_visibility(
            &left.capability_visibility,
            &right.capability_visibility,
        ),
        data_boundary: narrow_data_boundary(&left.data_boundary, &right.data_boundary),
        tool_boundary: narrow_tool_boundary(&left.tool_boundary, &right.tool_boundary),
        workspace_boundary: narrow_workspace_boundary(
            &left.workspace_boundary,
            &right.workspace_boundary,
        ),
        network_boundary: narrow_network_boundary(&left.network_boundary, &right.network_boundary),
        human_approval_requirements: union_approval_requirements(
            &left.human_approval_requirements,
            &right.human_approval_requirements,
        ),
    }
}

fn narrow_disposition(left: &DefaultDisposition, right: &DefaultDisposition) -> DefaultDisposition {
    if matches!(left, DefaultDisposition::Deny) || matches!(right, DefaultDisposition::Deny) {
        DefaultDisposition::Deny
    } else if matches!(left, DefaultDisposition::RequireConsent)
        || matches!(right, DefaultDisposition::RequireConsent)
    {
        DefaultDisposition::RequireConsent
    } else {
        DefaultDisposition::AllowWithLease
    }
}

fn narrow_capability_visibility(
    left: &CapabilityVisibility,
    right: &CapabilityVisibility,
) -> CapabilityVisibility {
    use CapabilityVisibility::*;
    match (left, right) {
        (Private, _) | (_, Private) => Private,
        (OwnerOnly, _) | (_, OwnerOnly) => OwnerOnly,
        (EncounterScoped, _) | (_, EncounterScoped) => EncounterScoped,
        _ => Discoverable,
    }
}

fn narrow_data_boundary(
    left: &DataBoundaryPolicy,
    right: &DataBoundaryPolicy,
) -> DataBoundaryPolicy {
    use DataBoundaryPolicy::*;
    match (left, right) {
        (OwnerOnly, _) | (_, OwnerOnly) => OwnerOnly,
        (LeaseScoped, _) | (_, LeaseScoped) => LeaseScoped,
        _ => Redacted,
    }
}

fn narrow_tool_boundary(
    left: &ToolBoundaryPolicy,
    right: &ToolBoundaryPolicy,
) -> ToolBoundaryPolicy {
    use ToolBoundaryPolicy::*;
    match (left, right) {
        (NoCrossOwnerMutation, _) | (_, NoCrossOwnerMutation) => NoCrossOwnerMutation,
        (ApprovalRequired, _) | (_, ApprovalRequired) => ApprovalRequired,
        _ => LeaseScoped,
    }
}

fn narrow_workspace_boundary(
    left: &WorkspaceBoundaryPolicy,
    right: &WorkspaceBoundaryPolicy,
) -> WorkspaceBoundaryPolicy {
    use WorkspaceBoundaryPolicy::*;
    match (left, right) {
        (Isolated, _) | (_, Isolated) => Isolated,
        (LeaseScoped, _) | (_, LeaseScoped) => LeaseScoped,
        _ => ReadShared,
    }
}

fn narrow_network_boundary(
    left: &NetworkBoundaryPolicy,
    right: &NetworkBoundaryPolicy,
) -> NetworkBoundaryPolicy {
    use NetworkBoundaryPolicy::*;
    match (left, right) {
        (LocalOnly, _) | (_, LocalOnly) => LocalOnly,
        (LeasedEgress, _) | (_, LeasedEgress) => LeasedEgress,
        _ => Allowlisted,
    }
}

fn union_approval_requirements(
    left: &[ApprovalRequirement],
    right: &[ApprovalRequirement],
) -> Vec<ApprovalRequirement> {
    let mut requirements = left.to_vec();
    for requirement in right {
        if !requirements.contains(requirement) {
            requirements.push(requirement.clone());
        }
    }
    requirements
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawfish_types::{
        CapabilityVisibility, DataBoundaryPolicy, DefaultDisposition, EncounterPolicy, OwnerKind,
    };

    fn owner(id: &str) -> OwnerRef {
        OwnerRef {
            kind: OwnerKind::Human,
            id: id.to_string(),
            display_name: None,
        }
    }

    fn caller(owner_id: &str, trust_domain: TrustDomain) -> CounterpartyRef {
        CounterpartyRef {
            agent_id: None,
            session_id: Some("session-1".to_string()),
            owner: owner(owner_id),
            trust_domain,
        }
    }

    #[test]
    fn foreign_owner_mutation_is_denied() {
        let context = GovernanceContext {
            system_defaults: EncounterPolicy::default(),
            owner_policy: EncounterPolicy::default(),
            trust_domain_defaults: EncounterPolicy::default(),
            manifest_policy: EncounterPolicy::default(),
        };
        let request = EncounterRequest {
            caller: caller("alice", TrustDomain::SameDeviceForeignOwner),
            target_agent_id: "repo_reviewer".to_string(),
            target_owner: owner("bob"),
            requested_capabilities: vec!["repo.review".to_string()],
            requests_workspace_write: true,
            requests_secret_access: false,
            requests_mutating_capability: true,
        };

        let decision = authorize_encounter(&context, &request);
        assert_eq!(decision.disposition, EncounterDisposition::Deny);
    }

    #[test]
    fn same_owner_local_encounter_can_be_leased_under_permissive_defaults() {
        let context = GovernanceContext {
            system_defaults: EncounterPolicy {
                default_disposition: DefaultDisposition::AllowWithLease,
                ..EncounterPolicy::default()
            },
            owner_policy: neutral_policy(),
            trust_domain_defaults: EncounterPolicy {
                default_disposition: DefaultDisposition::AllowWithLease,
                ..EncounterPolicy::default()
            },
            manifest_policy: neutral_policy(),
        };
        let request = EncounterRequest {
            caller: caller("alice", TrustDomain::SameOwnerLocal),
            target_agent_id: "repo_reviewer".to_string(),
            target_owner: owner("alice"),
            requested_capabilities: vec!["repo.review".to_string()],
            requests_workspace_write: false,
            requests_secret_access: false,
            requests_mutating_capability: false,
        };

        let decision = authorize_encounter(&context, &request);
        assert_eq!(decision.disposition, EncounterDisposition::IssueLease);
    }

    #[test]
    fn narrowing_prefers_more_restrictive_visibility_and_consent() {
        let system_defaults = EncounterPolicy {
            default_disposition: DefaultDisposition::AllowWithLease,
            capability_visibility: CapabilityVisibility::Discoverable,
            data_boundary: DataBoundaryPolicy::LeaseScoped,
            ..EncounterPolicy::default()
        };
        let owner_policy = EncounterPolicy {
            default_disposition: DefaultDisposition::RequireConsent,
            capability_visibility: CapabilityVisibility::OwnerOnly,
            ..EncounterPolicy::default()
        };

        let effective = super::narrow_policy(
            &system_defaults,
            &owner_policy,
            &EncounterPolicy::default(),
            &EncounterPolicy::default(),
        );

        assert_eq!(effective.default_disposition, DefaultDisposition::Deny);
        assert_eq!(
            effective.capability_visibility,
            CapabilityVisibility::OwnerOnly
        );
    }
}

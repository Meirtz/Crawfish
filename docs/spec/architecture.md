# Crawfish Architecture Spec

Canonical terminology is defined in [`glossary.md`](glossary.md).
Forward-looking design principles are defined in [`philosophy.md`](philosophy.md).

## Scope

This document defines the runtime architecture for Crawfish v0.1 and the stable conceptual interfaces the implementation should preserve.

## Design Constraints

- Rust-first, not Rust-only, with Rust 1.88+ compatibility for the runtime spine.
- External protocol adapters may be implemented out of process in other languages, but the core runtime, CLI, state machine logic, and persistence layer are Rust-owned.
- Single-host mode must be easy to run locally.
- Same-device foreign-owner encounters must be a first-class design case, not deferred as a future federation-only concern.
- Cluster-oriented control-plane concepts must be designed in from day one even if full multi-host support is P1.
- P0 protocol requirement is MCP.
- P1 interop priority is bidirectional OpenClaw integration through plugins, Gateway RPC, and Gateway agent execution.
- ACP-compatible harness adapters and A2A remote-agent integration are P1.
- The architecture must distinguish tool plane, harness plane, and agent plane rather than collapsing everything into one subprocess abstraction.

## Architectural Philosophy

The architecture follows seven product-level commitments:

- **control is a feature**: runtime supervision, policy compilation, and inspection are core behavior, not operational afterthoughts
- **specialization is a strength**: the runtime should coordinate specialized harnesses when they are better than a monolithic local loop
- **harnesses are plentiful; operability is scarce**: the runtime wins by governing many execution surfaces coherently, not by replacing them with one more surface
- **reasoning is volatile; verification must survive model churn**: contracts and deterministic checks should outlive any single provider or harness release cycle
- **governance is not optional**: agent interaction must be governed through explicit encounter, consent, lease, and audit semantics
- **bounded autonomy is healthier than unconstrained autonomy**: agents act through contracts and capabilities, not by implicit permission
- **safe contraction beats silent drift**: when the system is under pressure, it should enter a declared degraded profile rather than behave unpredictably
- **continuity must outlive any single reasoning provider**: the control plane should remain useful even when every external model or harness is unreachable

## Runtime Model

### Component Topology

| Component | Responsibilities | P0 status |
| --- | --- | --- |
| `crawfishd` supervisor | desired-state reconciliation, scheduling, lifecycle transitions, contract compilation, inspection | required |
| agent worker runtime | executes agent turns, tool sessions, checkpoints, and action callbacks | required |
| action store | persists action metadata, phases, feedback events, result references, and failure reasons | required |
| checkpoint store | persists restart-safe execution snapshots | required |
| policy engine | compiles organization rules, agent defaults, encounter policy, and action overrides into executable decisions | required |
| governance engine | classifies encounters, issues grants and leases, records revocation and receipts | required |
| telemetry pipeline | structured logs, metrics, traces, audit events, and governance receipts | required |
| protocol adapter mesh | normalized access to tools, specialized harnesses, and remote agents | MCP required, others phased |

### Control Plane vs Execution Plane

| Plane | Responsibilities | Non-responsibilities |
| --- | --- | --- |
| control plane | desired state, agent placement, lease management, admission control, policy compilation, inspection, recovery decisions | does not generate task content |
| execution plane | model calls, tool calls, artifact generation, checkpoints, feedback, local state mutation | does not define global policy or topology |

The implementation must keep this split visible in code structure and APIs. Model reasoning belongs in workers. Operational decisions belong in the supervisor.

## Public Primitives

The following primitives are the stable conceptual surface for Crawfish. Field names are intentionally explicit because other documents reference them directly.

```rust
pub struct OwnerRef {
  pub kind: OwnerKind,
  pub id: String,
  pub display_name: Option<String>,
}

pub struct AgentManifest {
  pub id: String,
  pub owner: OwnerRef,
  pub trust_domain: TrustDomain,
  pub role: String,
  pub capabilities: Vec<String>,
  pub exposed_capabilities: Vec<String>,
  pub dependencies: Vec<String>,
  pub runtime: RuntimeProfile,
  pub lifecycle: LifecyclePolicy,
  pub encounter_policy: EncounterPolicy,
  pub contract_defaults: ExecutionContract,
  pub adapters: Vec<AdapterBinding>,
  pub workspace_policy: WorkspacePolicy,
  pub default_data_boundaries: Vec<String>,
  pub strategy_defaults: BTreeMap<String, ExecutionStrategy>,
}

pub struct Action {
  pub id: String,
  pub requester: RequesterRef,
  pub initiator_owner: OwnerRef,
  pub counterparty_refs: Vec<CounterpartyRef>,
  pub goal: GoalSpec,
  pub capability: String,
  pub inputs: Metadata,
  pub contract: ExecutionContract,
  pub execution_strategy: Option<ExecutionStrategy>,
  pub grant_refs: Vec<String>,
  pub lease_ref: Option<String>,
  pub data_boundary: String,
  pub schedule: ScheduleSpec,
  pub phase: ActionPhase,
  pub checkpoint_ref: Option<String>,
  pub outputs: ActionOutputs,
}

pub struct ExecutionContract {
  pub delivery: DeliveryContract,
  pub execution: ExecutionPolicy,
  pub safety: SafetyPolicy,
  pub quality: QualityPolicy,
  pub recovery: RecoveryPolicy,
}

pub struct CapabilityDescriptor {
  pub namespace: String,
  pub verbs: Vec<String>,
  pub executor_class: ExecutorClass,
  pub mutability: Mutability,
  pub risk_class: RiskClass,
  pub cost_class: CostClass,
  pub latency_class: LatencyClass,
  pub approval_requirements: Vec<ApprovalRequirement>,
}

pub struct LifecycleRecord {
  pub agent_id: String,
  pub desired_state: AgentState,
  pub observed_state: AgentState,
  pub health: HealthStatus,
  pub transition_reason: Option<String>,
  pub last_transition_at: String,
  pub degradation_profile: Option<DegradedProfileName>,
  pub continuity_mode: Option<ContinuityModeName>,
  pub failure_count: u32,
}

pub struct EncounterRecord {
  pub id: String,
  pub initiator_ref: CounterpartyRef,
  pub target_agent_id: String,
  pub target_owner: OwnerRef,
  pub trust_domain: TrustDomain,
  pub requested_capabilities: Vec<String>,
  pub applied_policy_source: String,
  pub state: EncounterState,
  pub grant_refs: Vec<String>,
  pub lease_ref: Option<String>,
  pub created_at: String,
}

pub struct ConsentGrant {
  pub id: String,
  pub grantor: OwnerRef,
  pub grantee: OwnerRef,
  pub purpose: String,
  pub scope: Vec<String>,
  pub issued_at: String,
  pub expires_at: String,
  pub revocable: bool,
  pub approver_ref: Option<String>,
}

pub struct CapabilityLease {
  pub id: String,
  pub grant_ref: String,
  pub lessor: OwnerRef,
  pub lessee: OwnerRef,
  pub capability_refs: Vec<String>,
  pub scope: Vec<String>,
  pub issued_at: String,
  pub expires_at: String,
  pub revocation_reason: Option<String>,
  pub audit_receipt_ref: String,
}

pub struct AuditReceipt {
  pub id: String,
  pub encounter_ref: String,
  pub grant_refs: Vec<String>,
  pub lease_ref: Option<String>,
  pub outcome: AuditOutcome,
  pub reason: String,
  pub approver_ref: Option<String>,
  pub emitted_at: String,
}

pub struct TreatyDecision {
  pub treaty_pack_id: String,
  pub remote_principal: RemotePrincipalRef,
  pub allowed_capabilities: Vec<String>,
  pub allowed_data_scopes: Vec<String>,
  pub allowed_artifact_classes: Vec<String>,
  pub required_checkpoints: Vec<OversightCheckpoint>,
  pub required_result_evidence: Vec<TreatyEvidenceRequirement>,
  pub on_scope_violation: TreatyEscalationMode,
  pub on_evidence_gap: TreatyEscalationMode,
  pub review_queue: bool,
  pub alert_rules: Vec<String>,
  pub delegation_depth: u32,
}

pub enum TreatyEvidenceRequirement {
  DelegationReceipt,
  RemoteTaskRef,
  TerminalStateVerified,
  AllowedArtifactClasses,
  AllowedDataScopes,
}

pub struct TreatyViolation {
  pub reason_code: String,
  pub summary: String,
}

pub struct FederationPack {
  pub id: String,
  pub treaty_pack_id: String,
  pub review_defaults: FederationReviewDefaults,
  pub alert_defaults: FederationAlertDefaults,
  pub required_remote_evidence: Vec<TreatyEvidenceRequirement>,
  pub result_acceptance_policy: RemoteResultAcceptance,
  pub scope_violation_policy: RemoteResultAcceptance,
  pub evidence_gap_policy: RemoteResultAcceptance,
  pub blocked_remote_policy: RemoteStateDisposition,
  pub auth_required_policy: RemoteStateDisposition,
  pub remote_failure_policy: RemoteStateDisposition,
  pub max_delegation_depth: u32,
}

pub struct FederationDecision {
  pub federation_pack_id: String,
  pub treaty_pack_id: String,
  pub remote_principal: RemotePrincipalRef,
  pub required_checkpoints: Vec<OversightCheckpoint>,
  pub required_remote_evidence: Vec<TreatyEvidenceRequirement>,
  pub escalation_policy: RemoteEscalationPolicy,
}

pub struct RemoteEscalationPolicy {
  pub result_acceptance_policy: RemoteResultAcceptance,
  pub scope_violation_policy: RemoteResultAcceptance,
  pub evidence_gap_policy: RemoteResultAcceptance,
  pub blocked_remote_policy: RemoteStateDisposition,
  pub auth_required_policy: RemoteStateDisposition,
  pub remote_failure_policy: RemoteStateDisposition,
}

pub enum RemoteStateDisposition {
  Running,
  Blocked,
  AwaitingApproval,
  Failed,
}

pub enum RemoteEvidenceStatus {
  Complete,
  MissingRequiredEvidence,
  ScopeViolation,
  Unknown,
}

pub enum RemoteResultAcceptance {
  Accepted,
  ReviewRequired,
  Rejected,
}

pub enum RemoteOutcomeDisposition {
  Accepted,
  ReviewRequired,
  Rejected,
}
```

### Governance Doctrine And Evaluation Primitives

The runtime now carries a second layer above encounter policy: doctrine and evaluation. This is the difference between having rules and being able to prove they were enforced.

```rust
pub struct DoctrinePack {
  pub id: String,
  pub name: String,
  pub jurisdiction: JurisdictionClass,
  pub rules: Vec<DoctrineRule>,
}

pub enum JurisdictionClass {
  SameOwnerLocal,
  SameDeviceForeignOwner,
  RemoteHarness,
  ExternalUnknown,
}

pub enum InteractionModel {
  ContextSplit,
  SameOwnerSwarm,
  SameDeviceMultiOwner,
  RemoteHarness,
  ExternalUnknown,
}

pub enum OversightCheckpoint {
  Admission,
  PreDispatch,
  PreMutation,
  PostResult,
}

pub struct EnforcementRecord {
  pub id: String,
  pub action_id: String,
  pub checkpoint: OversightCheckpoint,
  pub outcome: CheckpointOutcome,
  pub reason: String,
  pub created_at: String,
}

pub struct PolicyIncident {
  pub id: String,
  pub action_id: String,
  pub doctrine_pack_id: String,
  pub jurisdiction: JurisdictionClass,
  pub reason_code: String,
  pub summary: String,
  pub severity: PolicyIncidentSeverity,
  pub checkpoint: Option<OversightCheckpoint>,
  pub created_at: String,
}

pub struct TraceBundle {
  pub id: String,
  pub action_id: String,
  pub capability: String,
  pub interaction_model: InteractionModel,
  pub selected_executor: Option<String>,
  pub artifact_refs: Vec<ArtifactRef>,
  pub external_refs: Vec<ExternalRef>,
  pub events: Vec<BTreeMap<String, serde_json::Value>>,
  pub enforcement_records: Vec<EnforcementRecord>,
  pub policy_incidents: Vec<PolicyIncident>,
}

pub struct EvaluationRecord {
  pub id: String,
  pub action_id: String,
  pub evaluator: String,
  pub status: EvaluationStatus,
  pub score: Option<f64>,
  pub summary: String,
  pub findings: Vec<String>,
  pub feedback_note_id: Option<String>,
  pub created_at: String,
}

pub struct ReviewQueueItem {
  pub id: String,
  pub action_id: String,
  pub source: String,
  pub status: ReviewQueueStatus,
  pub summary: String,
  pub evaluation_ref: Option<String>,
}
```

### Supporting Shapes

```rust
pub struct RuntimeProfile {
  pub concurrency_group: ConcurrencyGroup,
  pub max_parallel_actions: u32,
  pub memory_scope: MemoryScope,
}

pub enum TrustDomain {
  SameOwnerLocal,
  SameDeviceForeignOwner,
  InternalOrg,
  ExternalPartner,
  PublicUnknown,
}

type CounterpartyRef = {
  agent_id?: string;
  session_id?: string;
  owner: OwnerRef;
  trust_domain: TrustDomain;
};

type LifecyclePolicy = {
  heartbeat_seconds: number;
  activate_timeout_seconds: number;
  degrade_after_failures: number;
  drain_timeout_seconds: number;
  allowed_degraded_profiles: DegradedProfileName[];
  health_probe: HealthProbe;
};

type AdapterBinding =
  | McpToolBinding
  | LocalHarnessBinding
  | OpenClawBinding
  | AcpHarnessBinding
  | A2ARemoteAgentBinding;

type McpToolBinding = {
  adapter: "mcp";
  capability: string;
  default_scope: string[];
};

type LocalHarnessBinding = {
  adapter: "local_harness";
  capability: string;
  harness: "claude_code" | "codex";
  command: string;
  args: string[];
  required_scopes: string[];
  lease_required: boolean;
  workspace_policy: "inherit" | "crawfish_managed";
  env_allowlist: string[];
  timeout_seconds: number;
};

type OpenClawBinding = {
  adapter: "openclaw";
  gateway_url: string;
  auth_ref: string;
  target_agent: string;
  session_mode: "ephemeral" | "sticky";
  caller_owner_mapping: "required" | "best_effort";
  default_trust_domain: TrustDomain;
  required_scopes: string[];
  lease_required: boolean;
  workspace_policy: "inherit" | "openclaw_managed" | "crawfish_managed";
};

type AcpHarnessBinding = {
  adapter: "acp";
  harness: string;
  capabilities: string[];
  session_mode: "ephemeral" | "persistent";
  default_scope: string[];
};

type A2ARemoteAgentBinding = {
  adapter: "a2a";
  capability: string;
  agent_card_url: string;
  auth_ref?: string;
  treaty_pack: string;
  federation_pack?: string;
  required_scopes: string[];
  streaming_mode: "prefer_streaming" | "poll_only";
  allow_in_task_auth: boolean;
};

type TreatyPack = {
  id: string;
  local_owner_kind: "human" | "team" | "org" | "service_account";
  local_owner_id: string;
  remote_principal_kind: "agent" | "service" | "org";
  remote_principal_id: string;
  allowed_capabilities: string[];
  allowed_data_scopes: string[];
  allowed_artifact_classes: string[];
  auth_forwarding: "none";
  required_checkpoints: ("admission" | "pre_dispatch" | "post_result")[];
  required_result_evidence: (
    | "delegation_receipt"
    | "remote_task_ref"
    | "terminal_state_verified"
    | "allowed_artifact_classes"
    | "allowed_data_scopes"
  )[];
  max_delegation_depth: number;
  on_scope_violation: "deny" | "review_required";
  on_evidence_gap: "review_required" | "deny";
  review_queue: boolean;
  alert_rules: string[];
};

type FederationPack = {
  id: string;
  treaty_pack_id: string;
  review_defaults: {
    enabled: boolean;
    priority: string;
  };
  alert_defaults: {
    enabled: boolean;
    rules: string[];
  };
  required_remote_evidence: (
    | "delegation_receipt"
    | "remote_task_ref"
    | "terminal_state_verified"
    | "allowed_artifact_classes"
    | "allowed_data_scopes"
  )[];
  result_acceptance_policy: "accepted" | "review_required" | "rejected";
  scope_violation_policy: "review_required" | "rejected";
  evidence_gap_policy: "review_required" | "rejected";
  blocked_remote_policy: "blocked" | "failed";
  auth_required_policy: "awaiting_approval" | "failed";
  remote_failure_policy: "failed" | "blocked";
  max_delegation_depth: number;
};

type WorkspacePolicy = {
  isolation: "none" | "per_agent" | "per_action";
  lock_mode: "none" | "file" | "branch";
  write_mode: "read_only" | "approval_gated" | "autonomous";
};

type EncounterPolicy = {
  default_disposition: "deny" | "require_consent" | "allow_with_lease";
  capability_visibility: "private" | "owner_only" | "encounter_scoped" | "discoverable";
  data_boundary: "owner_only" | "redacted" | "lease_scoped";
  tool_boundary: "no_cross_owner_mutation" | "lease_scoped" | "approval_required";
  workspace_boundary: "isolated" | "read_shared" | "lease_scoped";
  network_boundary: "local_only" | "allowlisted" | "leased_egress";
  human_approval_requirements: ApprovalRequirement[];
};

type EncounterDecision = {
  disposition: "deny" | "await_consent" | "issue_lease";
  effective_policy: EncounterPolicy;
  required_grant: boolean;
  lease_required: boolean;
  reason: string;
};

type ExecutionStrategy = {
  mode: "single_pass" | "verify_loop";
  verification_spec: VerificationSpec | null;
  stop_budget: StopBudget | null;
  feedback_policy: "inject_reason" | "append_report" | "handoff";
};

type VerificationSpec = {
  checks: VerificationCheck[];
  require_all: boolean;
  on_failure: "retry_with_feedback" | "human_handoff" | "fail";
};

type StopBudget = {
  max_iterations: number;
  max_cost_usd: number | null;
  max_elapsed_ms: number | null;
};

type VerificationCheck =
  | { kind: "command_exit"; command: string[]; cwd?: string }
  | { kind: "file_exists"; path: string }
  | { kind: "file_pattern"; path: string; pattern: string }
  | { kind: "test_suite"; command: string[]; cwd?: string }
  | { kind: "lint"; command: string[]; cwd?: string }
  | { kind: "schema_validate"; schema_ref: string; target_ref: string }
  | { kind: "approval_gate"; policy_ref: string };

type DegradedProfileName =
  | "read_only"
  | "dependency_isolation"
  | "budget_guard"
  | "provider_failover";

type EncounterState =
  | "discovered"
  | "classified"
  | "policy_checked"
  | "awaiting_consent"
  | "granted"
  | "leased"
  | "active"
  | "denied"
  | "revoked"
  | "expired";

type ContinuityModeName =
  | "deterministic_only"
  | "store_and_forward"
  | "human_handoff"
  | "suspended";
```

## Agent Lifecycle State Machine

### States

| State | Meaning | Typical triggers in | Guard conditions | Typical triggers out |
| --- | --- | --- | --- | --- |
| `Unconfigured` | manifest exists but runtime resources are not prepared | manifest load, discovery, restart bootstrap | manifest is valid | `Configuring` |
| `Configuring` | model adapters, tool sessions, dependencies, and secrets are being prepared | supervisor reconcile step | config inputs available | `Inactive`, `Failed` |
| `Inactive` | ready but not accepting new actions | successful configuration, drained worker | dependencies known | `Activating`, `Finalized` |
| `Activating` | warm-up and health check phase | operator start, reconcile to desired active state | health probe can run | `Active`, `Degraded`, `Failed` |
| `Active` | accepting new actions under normal policy | healthy worker and healthy dependencies | hard policy satisfied | `Degraded`, `Draining`, `Failed` |
| `Degraded` | still serving with reduced capability or stricter restrictions | dependency impairment, provider instability, budget pressure, repeated soft failures | hard safety policies still satisfiable | `Active`, `Draining`, `Failed` |
| `Draining` | no new actions accepted; in-flight work finishes or checkpoints | rollout, shutdown, dependency maintenance, operator drain | worker still able to checkpoint or finish | `Inactive`, `Finalized`, `Failed` |
| `Failed` | runtime cannot safely continue | hard policy violation, repeated crash loop, unrecoverable dependency loss | none | `Configuring`, `Finalized` |
| `Finalized` | resources released and lifecycle closed | delete, permanent shutdown | none | none |

### Failure Transitions

| From | Trigger | Result |
| --- | --- | --- |
| `Configuring` | dependency unavailable but not fatal | remain reconcilable and retry, or move to `Inactive` if startup policy allows |
| `Activating` | health check soft-fails but fallback profile exists | `Degraded` |
| `Active` | dependency misses liveliness window but degraded profile exists | `Degraded` |
| `Active` | hard safety policy becomes unsatisfied | `Failed` |
| `Draining` | checkpoint write fails and worker cannot continue safely | `Failed` |
| `Failed` | operator retry or automatic backoff retry | `Configuring` |

### Recovery Rules

- `Degraded` must preserve safety before preserving throughput.
- degraded profiles are deterministic policy outputs, not ad hoc worker decisions
- a return from `Degraded` to `Active` requires a fresh health probe and dependency check
- `Failed` requires explicit reconfiguration or an allowed automatic retry path

### Degraded Profiles

`Degraded` only becomes meaningful when it has named profiles with explicit contraction semantics.

| Profile | Typical trigger | Contraction behavior | Recovery condition |
| --- | --- | --- | --- |
| `read_only` | mutation policy tightened, approval system unavailable, unsafe write conditions | all mutating capabilities become proposal-only or blocked | write approvals and safety checks are available again |
| `dependency_isolation` | upstream agent or required adapter is impaired | actions depending on that capability are blocked or downgraded; independent actions continue | dependency health and liveliness recover |
| `budget_guard` | action or swarm approaches spend threshold | expensive model or harness routes are disabled; cheaper routes are preferred; non-critical work may be deferred | budget pressure clears or policy window resets |
| `provider_failover` | provider, model endpoint, or harness instability | route to fallback model or harness while preserving hard policy | primary execution route becomes healthy again |

### Degraded Implementation Rules

- degraded profiles are chosen by the control plane, not improvised by workers
- a profile must state what remains allowed, what is prohibited, and what fallback route applies
- `inspect` must expose the active profile, trigger reason, and recovery requirement
- a degraded system may be less capable, but it must never be less safe

### Continuity Modes

`Degraded` handles soft contraction while agentic execution is still available. Continuity modes cover more extreme conditions, including full model outage, harness outage, or network loss.

| Continuity mode | Typical trigger | What still works | Exit condition |
| --- | --- | --- | --- |
| `deterministic_only` | every model or harness route for an action class is unavailable | rule engines, local analyzers, cached reads, static transforms, policy checks | at least one approved agentic route is healthy again |
| `store_and_forward` | external network, SaaS, or remote dependency outage | local evidence collection, checkpointing, queue persistence, partial local preprocessing | connectivity and dependency checks recover |
| `human_handoff` | policy requires manual control or no safe automated route remains | structured evidence package, suggested next step, explicit escalation | operator resolves or reroutes the action |
| `suspended` | no safe work can continue even deterministically | control-plane visibility, alerting, and recovery attempts | repair succeeds or operator intervenes |

### Continuity Escalation Rules

- continuity is chosen by the control plane after contract evaluation, not by ad hoc worker heuristics
- continuity must never widen permissions or mutate safety boundaries
- the runtime prefers `degraded` before `deterministic_only`, `deterministic_only` before `store_and_forward`, and `store_and_forward` before `human_handoff`, unless policy says otherwise
- `suspended` is the last resort when the runtime can preserve visibility but not useful work
- `inspect` must distinguish `degradation_profile` from `continuity_mode`

## Encounter Lifecycle

An encounter is the governed moment when an external session, another agent, or another owner-context attempts to access an agent or capability.

### Encounter States

| State | Meaning | Exit conditions |
| --- | --- | --- |
| `discovered` | a caller or counterparty has been identified | owner and trust domain are classified |
| `classified` | owner, trust domain, and requested capability set are known | governance policy is evaluated |
| `policy_checked` | effective encounter policy has been computed | `awaiting_consent`, `denied`, or `leased` |
| `awaiting_consent` | human or owner approval is required before crossing the boundary | `granted`, `denied`, or `expired` |
| `granted` | explicit consent exists but runtime lease has not been issued yet | `leased` or `revoked` |
| `leased` | short-lived capability lease exists and may be exercised | `active`, `expired`, or `revoked` |
| `active` | the encounter is currently being exercised by running work | `expired`, `revoked`, or terminal action completion |
| `denied` | policy or approval prevented the encounter | none |
| `revoked` | a previously valid grant or lease has been withdrawn | none |
| `expired` | the encounter timed out or the lease window ended | none |

### Encounter Rules

- every cross-owner or cross-domain interaction must create an `EncounterRecord`
- same-device foreign-owner interactions are encounters even when no network is involved
- a valid `ConsentGrant` is necessary but not sufficient; actual runtime use requires a `CapabilityLease`
- grants may authorize intent, while leases authorize a bounded execution window
- revocation must terminate future work immediately and surface through inspection

## Action State Machine

### Phases

| Phase | Meaning | Entry conditions | Exit conditions |
| --- | --- | --- | --- |
| `accepted` | action stored and admitted but not yet assigned | contract validated and capacity reserved | `running`, `expired` |
| `running` | worker currently executing the action | worker lease held and inputs resolved | `blocked`, `awaiting_approval`, `cancelling`, `completed`, `failed` |
| `blocked` | action cannot proceed until dependency, external data, or scheduler condition clears | missing dependency, unavailable adapter, deferred due to backpressure | `running`, `expired`, `failed` |
| `awaiting_approval` | action is paused pending a human or policy gate | mutating step requires approval | `running`, `expired`, `failed` |
| `cancelling` | cancellation signal accepted and being propagated | user or supervisor requested cancel | `completed`, `failed` |
| `completed` | action finished and emitted terminal outputs | successful execution or policy-approved partial completion | none |
| `failed` | action terminated without a successful result | unrecoverable error or hard policy violation | none |
| `expired` | action exceeded its deadline or approval window | deadline or waiting window exceeded | none |

### Action Guards

- an action may enter `running` only if it has a compiled contract, an assigned worker, and all hard policies are satisfiable
- an action may enter `awaiting_approval` only from `running`
- an action may return from `blocked` only after dependency or resource checks pass
- an action in `cancelling` must still emit a terminal record even if tool cancellation is best-effort underneath

### Required Failure Taxonomy

Every terminal failure must classify into one of these buckets:

- `policy_denied`
- `dependency_unavailable`
- `tool_failure`
- `provider_failure`
- `connectivity_loss`
- `checkpoint_failure`
- `deadline_exceeded`
- `budget_exceeded`
- `operator_cancelled`
- `repair_exhausted`
- `internal_runtime_error`

## Contract Resolution And Execution Plan

### Precedence

Contracts are compiled in this order:

1. organization defaults
2. agent manifest defaults
3. action-level overrides

The merge is field-wise, not document-wise. Absent values fall through. Present values override unless doing so would violate a higher-level hard policy.

### Hard vs Soft Policy

| Policy type | Behavior |
| --- | --- |
| hard policy | must never be violated; if unsatisfied, reject, block, or fail |
| soft policy | preferred; runtime may degrade or reroute before declaring failure |

### Compilation Algorithm

```ts
function compileExecutionPlan(
  orgDefaults: ExecutionContract,
  agentDefaults: ExecutionContract,
  actionOverrides: Partial<ExecutionContract>,
  strategyDefaults: Record<string, ExecutionStrategy>,
  actionStrategy: ExecutionStrategy | null
): CompiledExecutionPlan {
  const merged = mergeFieldwise(orgDefaults, agentDefaults, actionOverrides);
  validateHardPolicies(merged);
  const fallbackPlan = deriveFallbackPlan(merged);
  const routingPlan = deriveRoutingPlan(merged);
  const strategyPlan = resolveExecutionStrategy(strategyDefaults, actionStrategy);
  return attachOperationalMetadata(merged, fallbackPlan, routingPlan, strategyPlan);
}
```

### Execution Strategy Resolution

Execution strategy resolves after contract merge but before worker assignment.

Resolution order:

1. manifest `strategy_defaults` for the selected capability class
2. explicit `action.execution_strategy`
3. runtime safety adjustments, which may narrow but never widen verification requirements

Rules:

- `verify_loop` is mandatory for the documented verification-sensitive capability classes unless a higher-level hard policy blocks that action entirely
- deterministic checks named in `verification_spec` are part of completion semantics, not optional telemetry
- a degraded or continuity transition may replace the selected adapter, but it must not silently downgrade `verify_loop` into unverifiable completion

### Contract Layers

| Layer | Required fields | Purpose |
| --- | --- | --- |
| `delivery` | `deadline_ms`, `freshness_ttl_ms`, `required_ack`, `liveliness_window_ms` | time and delivery semantics |
| `execution` | `max_cost_usd`, `max_tokens`, `model_class`, `preferred_harnesses`, `fallback_chain`, `retry_budget` | model, harness, and compute constraints |
| `safety` | `tool_scope`, `approval_policy`, `mutation_mode`, `data_zone`, `secret_policy` | what the action may touch |
| `quality` | `quality_class`, `evaluation_profile`, `evaluation_hook`, `minimum_confidence`, `human_review_rule` | doctrine-aware evaluation, escalation, and review behavior |
| `recovery` | `checkpoint_interval`, `resumability`, `fallback_behavior`, `continuity_preference`, `deterministic_fallbacks`, `human_handoff_policy`, `dead_letter_policy` | restart, outage continuity, and terminal behavior |

### Contract Contraction Under Degradation

Degraded mode works by contracting the effective execution contract.

Examples:

- `read_only` narrows `safety.mutation_mode` and tool or harness scopes
- `budget_guard` tightens `execution.max_cost_usd` and prunes expensive fallback targets
- `provider_failover` rewrites the active target inside `execution.fallback_chain`
- `dependency_isolation` blocks capabilities whose dependencies are unhealthy without changing unrelated actions
- `deterministic_only` replaces agentic targets with approved `recovery.deterministic_fallbacks`
- `store_and_forward` keeps local collection and checkpointing active while deferring remote side effects until the contract can be satisfied again

## Governance Resolution And Encounter Policy

Governance resolution is separate from execution contract compilation. It decides whether two parties may interact at all, under what boundaries, and through what lease.

### Governance Doctrine Layer

Encounter policy answers whether an interaction is allowed. Doctrine answers whether the runtime can prove that the interaction is being governed correctly.

This is also where Crawfish separates older context-split multi-agent patterns from real swarm encounters. LangChain's multi-agent framing is explicitly about [context engineering](https://docs.langchain.com/oss/python/langchain/multi-agent), OpenAI's Agents SDK centers [handoffs](https://openai.github.io/openai-agents-python/handoffs/) and shared [context](https://openai.github.io/openai-agents-python/context/), and AutoGen Swarm describes agents that [share the same message context](https://microsoft.github.io/autogen/0.7.3/user-guide/agentchat-user-guide/swarm.html). Crawfish still supports that `context_split` case as a derived interaction model, but the doctrine compiler treats everything outside that narrow class as frontier governance territory.

This is the architecture-level answer to the frontier problem:

- a constitution or policy may say what should happen
- the runtime still needs jurisdiction
- the runtime still needs checkpoints
- the runtime still needs evidence that the checkpoints ran
- the runtime still needs escalation when enforcement is incomplete

Each action should therefore compile:

- `InteractionModel`
- `JurisdictionClass`
- `DoctrinePack`
- `CheckpointStatus`
- `EnforcementRecord`
- `PolicyIncident`

If a required checkpoint cannot be satisfied, the action must not silently continue. The only legal exits are deny, store-and-forward, or human handoff.

In practice the doctrine compiler interprets the interaction classes this way:

- `context_split`: multiple workers may coordinate inside one bounded application with no meaningful owner or harness boundary
- `same_owner_swarm`: the control plane is already operating over multiple bounded workers under one owner
- `same_device_multi_owner`: frontier territory on the same machine
- `remote_harness`: frontier territory because execution crosses into a harness surface
- `external_unknown`: frontier territory with insufficient authority or trust classification

Everything except `context_split` must prove doctrine evidence at the relevant checkpoints. Missing evidence produces a `PolicyIncident` with `reason_code = "frontier_enforcement_gap"`.

### Governance Precedence

Governance is resolved in this order:

1. system governance defaults
2. owner policy
3. trust-domain defaults
4. agent manifest `encounter_policy`
5. action or request overrides

Lower layers may narrow permissions, shorten leases, or add approval, but they must never widen a stricter upstream boundary.

### Resolution Algorithm

```ts
function authorizeEncounter(
  systemDefaults: EncounterPolicy,
  ownerPolicy: EncounterPolicy,
  trustDomainDefaults: EncounterPolicy,
  manifestPolicy: EncounterPolicy,
  requestOverrides: Partial<EncounterPolicy>
): EncounterDecision {
  const merged = mergeNarrowingOnly(
    systemDefaults,
    ownerPolicy,
    trustDomainDefaults,
    manifestPolicy,
    requestOverrides
  );
  return decideEncounter(merged);
}
```

### Same-Device Governance Rules

- `same_device_foreign_owner` is a foreign trust domain even without network traffic
- no shared workspace, memory scope, secret resolution, or mutating capability is allowed by default across owners
- read access may still require redaction or approval depending on the effective `EncounterPolicy`
- resource arbitration for shared local targets must be deterministic and owner-aware rather than first-writer-wins

### Oversight Checkpoints

The minimum doctrine checkpoints are:

| Checkpoint | Meaning |
| --- | --- |
| `admission` | the action was admitted under a valid jurisdiction, contract, and encounter decision |
| `pre_dispatch` | execution is still allowed at dispatch time under current lease, route, and policy state |
| `pre_mutation` | any mutating work still has valid approval, lease, lock, and workspace authority |
| `post_result` | results are accompanied by enough evidence, evaluation, or escalation before the action is treated as safely complete |

This is where “constitution is not self-enforcing” becomes runtime behavior rather than philosophy alone.

## Capability Normalization Model

Every execution surface must produce or map to a `CapabilityDescriptor`. Crawfish schedules against capabilities, not against raw protocol objects.

| Source | Native source object | Normalized namespace example | Notes |
| --- | --- | --- | --- |
| local deterministic executor | local function, rule set, classifier, or analyzer | `local.repo.ownership.index` | continuity-critical execution that does not require an LLM, remote harness, or external network |
| MCP tool | tool name plus server metadata | `mcp.github.pull_request.review` | primary P0 tool plane |
| OpenClaw gateway agent | OpenClaw `agent` run on a Gateway session | `openclaw.repo.patch.plan` | first-priority P1 harness interop; supports bidirectional integration between Crawfish and OpenClaw |
| ACP-compatible harness | ACP session plus harness metadata | `acp.codex.repo.edit` | P1, used for specialized coding and harness-driven execution |
| A2A remote agent | Agent Card capability | `a2a.partner.incident.triage` | P1, used for remote delegation |

### Normalization Rules

- `verbs` describe what the capability does, not which protocol exposed it
- `mutability` is derived from the effective action, not only the transport
- `risk_class` is assigned by policy and adapter metadata together
- `approval_requirements` are explicit and carried into the compiled execution plan
- `latency_class` and `cost_class` support scheduling and fallback decisions
- `executor_class` indicates whether the capability can survive major reasoning outages
- exposed capability does not mean ambient accessibility; encounter policy still governs discoverability and leaseability
- OpenClaw gateway agents are treated as harness execution surfaces rather than as control-plane peers
- local CLI-backed wrappers such as Claude Code and Codex are also treated as harness execution surfaces rather than privileged runtime components
- ACP-compatible harnesses are normalized as execution surfaces, not as generic tools; they may expose session semantics, permission prompts, and richer cancellation behavior than MCP tools

## Protocol Planes

| Plane | Primary protocol | Role |
| --- | --- | --- |
| tool plane | MCP | connect tools and services into the runtime |
| harness plane | local CLI wrappers, OpenClaw Gateway, and ACP-compatible adapters | invoke specialized general-purpose harnesses for planning, research, operations, investigation, or coding |
| agent plane | A2A | delegate work to remote agent systems |

`P1e` adds a native local harness sub-surface inside the harness plane. Claude Code and Codex are wrapped as per-action ephemeral subprocesses under Rust control, with allowlisted environment propagation, workspace inheritance rules, and normalized event and failure reporting.

The harness plane matters because a specialized agent harness is neither just a tool nor just a remote agent. It has richer session semantics, permission prompts, workspace expectations, and cancellation behavior. Crawfish should treat that difference explicitly. See [OpenClaw's Agent Loop](https://docs.openclaw.ai/concepts/agent-loop), [OpenClaw Gateway protocol](https://docs.openclaw.ai/gateway/protocol), [ACP at Zed](https://zed.dev/acp), and the [Agent Client Protocol specification](https://github.com/agentclientprotocol/agent-client-protocol) for the protocol layers Crawfish should sit above.

ACPX-like tools are useful evidence of this layer, but they should be treated as implementations of the harness plane rather than as the protocol definition itself.

## OpenClaw Interoperability Model

OpenClaw is the first-priority harness ecosystem integration because it spans both ingress and execution.

### Inbound: OpenClaw -> Crawfish

`P1a` is Gateway-RPC-first. The implemented inbound surface is a thin bridge package under `integrations/openclaw-inbound/` that forwards OpenClaw RPC calls to `crawfishd` over the local Unix socket. It does not own policy, state, or retries.

Current minimum RPC surface:

- `crawfish.action.submit`
- `crawfish.action.inspect`
- `crawfish.action.events`
- `crawfish.agent.status`

Inbound rules:

- every inbound call must compile into a normal Crawfish action with a stable action id
- OpenClaw session metadata must map to `OwnerRef`, `TrustDomain`, and effective session scope before action admission
- inbound calls must pass encounter policy before an action is created
- OpenClaw session metadata is treated as requester and trace context, not as lifecycle authority
- OpenClaw does not own Crawfish lifecycle state; it is an external caller
- `P1a` keeps `cancel` and `resume` out of the OpenClaw surface until Crawfish exposes stable action-level operator semantics for them

### Outbound: Crawfish -> OpenClaw

When an action requires a gateway-native or harness-specialized execution surface, Crawfish may select an `openclaw` binding and call the [Gateway protocol](https://docs.openclaw.ai/gateway/protocol) over WebSocket.

Outbound rules:

- Crawfish invokes OpenClaw `agent` and `agent.wait` entrypoints through the Gateway adapter
- OpenClaw `runId` maps to a single Crawfish action attempt
- lifecycle, assistant, and tool stream events are converted into action feedback records
- outbound OpenClaw execution must be bound to a `CapabilityLease`
- OpenClaw session or workspace scopes may narrow permissions, but never widen Crawfish hard policy
- the current `P1b` implementation supports `session_mode = ephemeral` only
- the current `P1b` implementation supports `workspace_policy = inherit | crawfish_managed`; `openclaw_managed` is parsed but rejected at runtime
- `auth_ref` currently resolves to an environment variable name containing the Gateway bearer token
- `task.plan` is the first outbound capability, remains proposal-only, and now defaults to `verify_loop` under deterministic verification

### Shared Governance Kernel

OpenClaw inbound and outbound paths must reuse the same governance model as A2A and other remote-agent systems:

- classify owner and trust domain
- resolve encounter policy
- obtain consent if required
- issue short-lived lease
- emit auditable receipt

## A2A Remote-Agent Delegation

A2A is the first implemented remote-agent plane, using the [A2A project](https://github.com/a2aproject/A2A) and Google's ["A2A: A New Era of Agent Interoperability"](https://developers.googleblog.com/a2a-a-new-era-of-agent-interoperability/) as the reference shape.

The distinction from the harness plane is intentional:

- harness crossing changes execution surface semantics
- remote-agent delegation crosses an authority boundary
- remote delegation therefore requires treaty scope, delegation receipts, and frontier checkpoint evidence

Current `P1i` rules:

- outbound only
- `task.plan` only
- one-hop delegation only
- treaty required before dispatch
- no in-task auth forwarding
- no mutation over A2A
- prefer streaming when available, otherwise submit plus poll

Current `P1k` treaty-escalation rules:

- remote delegation is not lawful until a `TreatyDecision` is compiled
- treaty admission validates local owner, remote principal, capability, allowed data scopes, and delegation depth before dispatch
- no remote task may be created if treaty admission fails
- post-result governance is mandatory, not optional
- remote outcomes are classified as `accepted`, `review_required`, or `rejected`
- returned artifact classes and data scopes are checked against the treaty before the result is accepted
- missing required result evidence creates an explicit frontier incident instead of silently trusting the result

Current `P1m` federation-pack rules:

- remote delegation now compiles a `FederationDecision` in addition to a `TreatyDecision`
- federation packs are static remote-governance bundles; they do not negotiate treaties, they interpret remote states and results
- treaty decides whether delegation is lawful
- federation pack decides how `input-required`, `auth-required`, evidence gaps, scope violations, and remote terminal results are escalated
- a missing configured federation pack falls back only to the built-in `remote_task_plan_default` behavior when the binding allows the built-in path
- remote result acceptance is now driven by `RemoteResultAcceptance` plus `RemoteEvidenceStatus`, not by a single implicit success branch
- remote state handling is now driven by `RemoteStateDisposition`, so operators can see why the control plane blocked, awaited approval, or failed the action

Current `P1n` remote-evidence rules:

- every `remote_agent` action attempt records a `RemoteEvidenceBundle`
- evidence bundles carry treaty pack id, federation pack id, remote principal, remote task ref, checkpoint evidence, artifact manifest, scope/data evidence, policy incidents, and treaty violations
- `review_required` remote outcomes now create `remote_result_review` items rather than blending into generic action review
- remote review resolution is explicit:
  - `accept_result` -> action may complete
  - `reject_result` -> action fails
  - `needs_followup` -> action remains blocked and a follow-up review item is opened
- inspect, trace, dataset capture, evaluation records, alerts, and pairwise comparisons now inherit remote evidence metadata

Treaty escalation semantics are intentionally narrow in alpha:

- `on_scope_violation = deny | review_required`
- `on_evidence_gap = review_required | deny`
- `max_delegation_depth = 1`
- no secret forwarding
- no treaty negotiation protocol

Remote state mapping is normalized into the normal Crawfish action model:

- `submitted` or `working` -> `running`
- `input-required` -> `blocked`
- `auth-required` -> `awaiting_approval`
- `completed` -> `completed`
- `failed`, `rejected`, or `canceled` -> `failed`

Remote lineage remains inspectable through:

- remote principal
- remote task id
- treaty pack
- federation pack
- delegation receipt
- doctrine checkpoint status
- remote outcome disposition
- treaty violations
- delegation depth

### Remote Result Governance

Remote result governance is the point where doctrine and treaty meet.

At `post_result`, the runtime must check:

- whether the remote terminal state is provable
- whether the returned artifact classes are allowed by the treaty
- whether the returned data scope stayed inside treaty limits
- whether all treaty-required result evidence is present

Result handling rules:

- if evidence is complete and scope is valid, the remote outcome is `accepted`
- if evidence is incomplete and the treaty escalation mode allows review, the remote outcome becomes `review_required` and the action is blocked for operator attention
- if the result violates treaty scope, or the treaty escalation mode requires denial, the remote outcome is `rejected` and the action fails

Federation packs make this reusable by fixing the remote escalation contract ahead of time:

- which remote state transitions should become `blocked`, `awaiting_approval`, or `failed`
- whether missing result evidence becomes `review_required` or `rejected`
- whether scope violations are escalated for review or rejected outright
- which review defaults and alert defaults apply when the frontier evidence chain is incomplete

This is where the architecture treats remote agents as different from harnesses. A harness can be an execution surface. A remote agent is another authority boundary, which means the result itself must remain governable after the call returns.

## Execution Strategies

Execution strategy is orthogonal to adapter choice. A task might run through OpenClaw, an ACP-compatible harness, or a local executor and still choose either a single pass or a verify loop.

| Strategy | Use when | Completion rule |
| --- | --- | --- |
| `single_pass` | straightforward analysis, enrichment, classification, or low-risk proposal work | normal terminal success and contract satisfaction |
| `verify_loop` | proposal, planning, investigation, migration, or mutation-sensitive work that must be proven rather than asserted | deterministic verification passes, handoff occurs, or stop budget is exhausted |

Ralph-style loops are modeled as `verify_loop`, not as a separate runtime category. See the [Ralph prototype](https://github.com/iannuttall/ralph) and [ralph-loop-agent](https://github.com/vercel-labs/ralph-loop-agent) for the inspiration this strategy absorbs. In Crawfish, coding is only one early use case for verified execution, not the category anchor.

### Verify-Loop Capability Classes

Only the following capability classes should default to `verify_loop`:

- `task.plan`
- `workspace.patch.apply`
- `migration.refactor`
- `spec.implement`

Other actions remain `single_pass` unless a manifest explicitly opts in.

### Verify-Loop Rules

- verify loops must use deterministic checks, not self-reported model confidence
- each iteration should start from fresh runtime context plus explicit feedback from the previous verification round
- verification failure feeds structured evidence back into the next attempt
- if `stop_budget` is exhausted without a passing verification result, the action moves to `human_handoff`, `store_and_forward`, or `failed` according to the compiled contract and current strategy behavior

### Example

```ts
const reviewCapability: CapabilityDescriptor = {
  namespace: "mcp.github.pull_request.review",
  verbs: ["inspect", "annotate", "summarize"],
  executor_class: "agentic",
  mutability: "proposal_only",
  risk_class: "medium",
  cost_class: "standard",
  latency_class: "interactive",
  approval_requirements: []
};
```

## Deterministic Continuity

Deterministic continuity is a first-class runtime concern, not a consolation prize.

When every model or harness route is down, Crawfish should still preserve the safe subset of useful work through deterministic executors. These executors are traditional software components, not miniature prompt loops.

For the hero swarm, the minimum deterministic continuity set should look like this:

| Agent | Deterministic continuity examples |
| --- | --- |
| `repo_indexer` | filesystem walk, parser pipeline, CODEOWNERS extraction, ownership graph refresh |
| `repo_reviewer` | static lint, changed-file policy rules, diff heuristics, risk scoring, TODO or secret scanners |
| `ci_triage` | exit-code classifiers, regex or signature-based failure grouping, retry advisories |
| `incident_enricher` | local log collection, dependency graph lookup, known-service blast radius computation |

The runtime should schedule these deterministic capabilities through the same action model so they remain observable, cancellable, checkpointable, and policy-bound.

## Self-Repair Loop

Crawfish should borrow from [Kubernetes self-healing](https://kubernetes.io/docs/concepts/architecture/self-healing/), [Temporal's durable execution model](https://docs.temporal.io/), and IBM's [autonomic computing](https://research.ibm.com/publications/autonomic-computing-architectural-approach-and-prototype) tradition: monitor, diagnose, repair, verify.

| Phase | Runtime behavior |
| --- | --- |
| monitor | heartbeats, adapter health, budget pressure, checkpoint failures, provider errors, connectivity probes |
| diagnose | classify failure taxonomy, identify affected capabilities, decide whether degradation or continuity is possible |
| repair | reconnect adapters, rebuild sessions, replay checkpoints, rebuild local caches, quarantine dependencies, rotate to deterministic continuity |
| verify | re-run probes, confirm contract satisfaction, either return to normal service or escalate further |

Self-repair is intentionally conservative. It restores declared service inside the current policy envelope. It does not invent new permissions or mutate the product's goals.

### Repair Rules

- repair actions must themselves be auditable
- repair may tighten execution or safety, but must never relax hard policy
- repeated failed repair attempts increment failure history and may force `human_handoff` or `suspended`
- repair is allowed to change route, checkpoint strategy, or queue state, but not business intent

## Controlled Self-Evolution

Crawfish should support learning, but only in controlled forms.

Allowed evolution targets include:

- routing weights for harness or provider selection
- degradation and continuity thresholds
- candidate deterministic rules derived from repeated incidents
- repair playbook ranking based on historical success

Disallowed by default:

- unrestricted runtime code self-modification
- silent expansion of tool scope or mutation rights
- automatic promotion of unreviewed policies into production

The preferred workflow is shadow mode, evaluation, approval, then rollout. Evolution should improve future control-plane decisions, not turn the runtime into an opaque self-writing system.

## Scheduling, Concurrency, And Backpressure

The scheduler is deadline-aware and policy-aware.

### Required Behavior

- control-plane traffic has the highest priority band
- user-facing actions outrank background maintenance work
- within a band, earlier deadlines outrank later deadlines
- actions sharing mutable workspace or memory run in an exclusive group
- read-only or stateless actions may run in a reentrant group
- admission control rejects or defers actions when budget, token rate, worker slots, or adapter capacity are exhausted
- each running action holds a worker lease; lease loss triggers recovery rather than duplicate execution

## State, Checkpointing, And Recovery

### Required Checkpoint Boundaries

- after each model turn
- after each mutating tool call
- at explicit yield points during long-running tool execution
- before entering `awaiting_approval`

### Minimum Checkpoint Contents

- action id and phase
- assigned agent and worker
- input references and relevant context references
- pending approvals
- tool outputs needed for replay safety
- artifact references
- trace correlation ids

### Recovery Semantics

- on supervisor restart, resumable actions re-enter scheduling from the last checkpoint
- actions without valid checkpoints follow their recovery policy: replay, dead-letter, or fail
- checkpoint write failure during a mutating step is a terminal runtime error unless the adapter guarantees idempotent replay
- if all approved agentic routes are unavailable, the recovery policy may move the action into `deterministic_only`, `store_and_forward`, or `human_handoff`
- store-and-forward must preserve enough local evidence for a later agentic pass or operator handoff

## Security And Governance

### Baseline Requirements

- secrets are referenced, not embedded
- mutating tools require explicit policy and may also require approval
- ACP permission or write prompts must map into the same `awaiting_approval` control-plane state as local policy gates
- OpenClaw gateway tokens and session scopes are referenced and audited like any other external auth material
- workspace isolation is per-agent or per-action for coding workloads
- owner, trust-domain, and data-boundary constraints propagate through actions, checkpoints, and adapter calls
- all write decisions are auditable with policy reason and approver identity
- continuity and repair actions may narrow privileges, but they must never expand them

### Governance Baseline Requirements

- every agent must declare an `OwnerRef` and `TrustDomain`
- every cross-owner or cross-domain interaction must evaluate `EncounterPolicy` before execution admission
- grants are explicit, purpose-bound, scope-bound, and time-bound
- leases are short-lived runtime authorizations derived from grants, not permanent capability ownership
- revocation must invalidate future use immediately and leave an `AuditReceipt`
- same-device foreign-owner interactions must default to deny or require consent for write, secret, and mutable workspace access

### Same-Device Resource Arbitration

- ownership and trust domain participate in local workspace and capability arbitration
- same-device contention must not resolve by arrival order alone
- a lower-trust or foreign-owner agent may be blocked, downgraded, or forced into approval even if the local resource exists

## Observability

Observability is not only logs and traces. In Crawfish it now includes the evaluation spine that turns runtime evidence into reviewable quality state.

### Required Trace Spans

- action lifecycle
- encounter lifecycle
- agent turn
- tool call
- approval wait
- scheduler decision
- lifecycle transition

### Required Metrics

- deadline hit rate
- budget overrun rate
- degraded agent count
- recovery success rate
- continuity mode activations
- repair success rate
- encounter count by trust domain
- grant issuance count
- lease revocation latency
- audit receipt coverage
- verify-loop iteration count
- verify-loop completion rate
- OpenClaw outbound run failure rate
- checkpoint replay count
- approval wait time
- token and dollar burn per successful action

## Evaluation Spine

The evaluation spine is a LangSmith-like backend substrate for the swarm control plane. It is not a hosted product UI yet; it is the runtime layer that later UI and hosted control planes can build on.

The current post-execution chain is:

1. execute action
2. assemble `TraceBundle`
3. resolve `EvaluationProfile`
4. execute `ScorecardSpec`
5. persist `EvaluationRecord`
6. create `ReviewQueueItem` when policy or evaluator requires human attention
7. emit `AlertEvent` when doctrine or evaluation requires escalation
8. capture `DatasetCase` when the profile enables dataset freezing
9. persist `FeedbackNote` and review resolution without rewriting action history

### Evaluation Profiles, Scorecards, And Datasets

`evaluation_profile` is now the primary config selector. `evaluation_hook` remains an alpha-era compatibility alias and should only normalize into built-in profiles.

The built-in defaults are:

| Capability | Profile | Scorecard | Dataset |
| --- | --- | --- | --- |
| `task.plan` | `task_plan_default` | `task_plan_scorecard` | `task_plan_dataset` |
| `task.plan` over `remote_agent` | `task_plan_remote_default` | `task_plan_remote_scorecard` | `task_plan_dataset` |
| `repo.review` | `repo_review_default` | `repo_review_scorecard` | `repo_review_dataset` |
| `incident.enrich` | `incident_enrich_default` | `incident_enrich_scorecard` | `incident_enrich_dataset` |

Each profile defines:

- one scorecard
- whether a review queue item should be created
- which alert rules apply
- whether dataset capture is enabled
- whether post-result evaluation is required

The config surface for this layer is:

- `[evaluation.profiles.<name>]`
- `[evaluation.scorecards.<name>]`
- `[evaluation.datasets.<name>]`
- `[evaluation.alerts.<name>]`

The first deterministic criterion kinds are:

- `artifact_present`
- `artifact_absent`
- `json_field_nonempty`
- `json_schema_valid`
- `list_min_len`
- `regex_match`
- `numeric_threshold`
- `field_equals`
- `token_coverage`
- `checkpoint_passed`
- `incident_absent`
- `external_ref_present`
- `interaction_model_is`
- `remote_outcome_disposition_is`
- `treaty_violation_absent`

The initial capability set remains:

- `task.plan`
- `repo.review`
- `incident.enrich`

`verify_loop` is part of the same spine. Verification failures are recorded as evaluations, not only as action events.

The remote-agent plane now has a distinct evaluator path as well. When `task.plan` crosses into the A2A plane, the runtime switches to `task_plan_remote_default`, which scores:

- remote interaction model classification
- federation pack and federation decision lineage
- delegation receipt and remote task lineage
- accepted vs escalated remote outcome disposition
- treaty-violation absence
- the same plan-quality artifacts and schema checks used for local planning

LangSmith's [observability concepts](https://docs.langchain.com/langsmith/observability-concepts), [pairwise evaluation](https://docs.langchain.com/langsmith/evaluate-pairwise), [annotation queues](https://docs.langchain.com/langsmith/annotation-queues), [automation rules](https://docs.langchain.com/langsmith/set-up-automation-rules), and [experiment comparison](https://docs.langchain.com/langsmith/compare-experiment-results) are useful reference shapes here. Anthropic's [Claude's Constitution](https://www.anthropic.com/constitution) and [Constitutional AI](https://www.anthropic.com/research/constitutional-ai-harmlessness-from-ai-feedback/) are useful reference shapes for rule-guided behavior. Crawfish lifts both ideas into runtime governance: checkpoints, evidence, incidents, review, escalation, datasets, replay experiments, and side-by-side executor comparison.

### Dataset Capture And Replay

The evaluation spine now extends beyond post-result scoring into reusable quality operations.

- completed traces may be frozen as `DatasetCase`s
- datasets are grouped by capability and carry doctrine, jurisdiction, checkpoint, and incident metadata
- replay runs execute those cases against one explicit executor surface
- replay results persist under `ExperimentRun` and `ExperimentCaseResult`
- replay does not create normal production review queue noise or alert spam by default

This is how Crawfish turns trace history into institutional memory rather than an archive of unread logs.

### Pairwise Profiles And Comparative Review

Comparative evaluation is now a first-class runtime substrate, not an ad hoc external script.

The pairwise types are:

- `PairwiseProfile`
- `PairwiseExperimentRun`
- `PairwiseCaseResult`
- `PairwiseOutcome`

The config surface for this layer is:

- `[evaluation.pairwise_profiles.<name>]`

The built-in default is:

| Capability | Pairwise profile |
| --- | --- |
| `task.plan` | `task_plan_pairwise_default` |

Current pairwise comparison is executor-first, not prompt-first and not strategy-first:

- one dataset
- one left executor surface
- one right executor surface
- two isolated replay runs
- one deterministic winner decision plus optional human review

The winner selection order is fixed:

1. fewer treaty or federation-governance violations wins
2. fewer doctrine or policy incidents wins
3. successful terminal status beats failed status
4. higher normalized evaluation score wins when the delta exceeds the configured margin
5. otherwise the result is `needs_review`

Review queue items can now be opened in two kinds:

- `action_eval`
- `pairwise_eval`

Pairwise review items carry:

- `pairwise_run_ref`
- `pairwise_case_ref`
- `left_case_result_ref`
- `right_case_result_ref`
- `priority`
- `reason_code`

Resolving a pairwise review creates a `FeedbackNote` tied to the pairwise lineage. It does not rewrite the source actions or experiment history.

Alert policy is also pairwise-aware:

- `comparison_regression`
- `comparison_attention_required`

Pairwise runs remain isolated from production operator noise:

- no production review queue items
- no production alerts
- no production action mutation

## Reference Stack For v0.1

| Area | v0.1 decision |
| --- | --- |
| runtime language | Rust |
| runtime compatibility | Rust 1.88+ |
| local mode state | SQLite |
| local mode queue and events | embedded queue or event bus inside supervisor process |
| team and production state | Postgres control state |
| durable bus upgrade path | external durable bus, introduced after P0 |
| required protocol | MCP |
| protocol status | MCP in P0, OpenClaw inbound/outbound and A2A outbound in P1, ACP-compatible harness adapters phased after |
| external adapter boundary | JSON-RPC 2.0 over stdio |
| telemetry | structured logs plus OpenTelemetry-compatible traces and metrics |

## CLI Surface

The P0 CLI surface is:

- `crawfish init`
- `crawfish run`
- `crawfish status`
- `crawfish inspect`
- `crawfish action events`
- `crawfish action trace`
- `crawfish action evals`
- `crawfish treaty list`
- `crawfish treaty show`
- `crawfish federation list`
- `crawfish federation show`
- `crawfish review list`
- `crawfish review resolve`
- `crawfish eval dataset list`
- `crawfish eval dataset show`
- `crawfish eval run`
- `crawfish eval run-status`
- `crawfish alert list`
- `crawfish alert ack`
- `crawfish drain`
- `crawfish resume`
- `crawfish policy validate`

### Required CLI Behaviors

- `status` shows lifecycle state, owner, trust domain, degradation profile, and continuity mode for each agent
- `inspect` accepts agent id or action id and surfaces compiled contract, execution strategy, selected adapter or harness, external run ids such as OpenClaw `runId`, encounter state, grant refs, lease refs, treaty and federation summaries for remote-agent work, dependency health, recent transitions, degradation profile, continuity mode, remote evidence status, and failure reasons
- `action trace` returns the trace bundle for one action, including event lineage, artifact refs, external refs, enforcement records, treaty and federation metadata, and policy incidents
- `action evals` returns deterministic evaluations and verification-linked scorecards for one action
- `treaty list` and `treaty show` expose recognized remote principals, allowed capabilities, scopes, artifact classes, and checkpoint requirements
- `federation list` and `federation show` expose the remote escalation contract: treaty binding, required evidence, result acceptance rules, scope-violation policy, and blocked/auth-required behavior
- `review list` returns operator review queue items opened by doctrine or evaluation
- `review resolve` records operator resolution and feedback without rewriting the action history
- `eval dataset list` and `eval dataset show` expose captured evaluation datasets and frozen cases
- `eval run` and `eval run-status` expose isolated replay experiments per executor surface
- `eval compare` and `eval compare-status` expose executor-first pairwise comparison runs
- `alert list` and `alert ack` expose doctrine and evaluation escalation signals
- `review list --kind pairwise` filters operator review items down to side-by-side comparisons
- `review resolve` supports pairwise outcomes such as `prefer_left`, `prefer_right`, `tie`, and `needs_followup`
- `drain` prevents new work assignment and reports progress until agents are inactive or finalized
- `resume` re-enables drained agents or reschedules resumable actions
- `policy validate` reports whether a manifest, encounter policy, or action override violates hard policy before runtime execution

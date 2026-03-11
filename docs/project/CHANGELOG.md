# Changelog

This project follows a simple alpha changelog discipline: user-visible changes must be recorded here before merge.

## Unreleased

### Added

- Rust-first Hero P0 slice with deterministic `repo.index`, deterministic-first `repo.review`, and `ci.triage`
- deterministic `incident.enrich` with typed enrichment and summary artifacts
- SSE MCP client support for remote tool-backed inputs
- restart recovery with deterministic checkpoint metadata
- local operator inspection for artifact refs, checkpoint refs, recovery stage, encounter metadata, and external refs
- public repository governance files, templates, and contribution policy
- approval-gated `workspace.patch.apply` via the new `workspace_editor` agent
- operator control commands for `action list`, `action approve`, `action reject`, and `lease revoke`
- runtime persistence and inspection for consent grants, capability leases, and audit receipts
- `action events` over the local UDS API for operator timeline inspection
- reference hero demo assets plus `examples/hero-swarm/demo.sh`
- `P1a` OpenClaw inbound Gateway RPC bridge under `integrations/openclaw-inbound/`
- OpenClaw inbound caller mapping, scoped action inspection, and scoped agent status over the local UDS API
- `task.plan` and the `task_planner` hero agent as the first OpenClaw outbound capability
- native Rust `crawfish-openclaw` Gateway adapter with streamed lifecycle, assistant, and tool event mapping
- deterministic fallback planning for `task.plan` when the preferred OpenClaw route is unavailable
- deprecated compatibility aliases for `[fleet]`, `coding.patch.plan`, and legacy task-planning input keys during `0.1.x alpha`
- explicit abandoned-run lineage for restarted OpenClaw-backed actions
- `verify_loop` as a real runtime execution strategy for `task.plan`
- deterministic verification feedback, iteration lineage, and strategy-aware checkpoint recovery
- a new `docs/spec/philosophy.md` manifesto and a rebuilt public README with repo-tracked hero art
- native Rust local harness adapters for Claude Code and Codex under `crawfish-harness-local`
- local-first `task.plan` routing across Claude Code, Codex, OpenClaw, and deterministic fallback
- normalized local harness failure taxonomy and process event lineage for `task.plan`
- doctrine-layer runtime types, checkpoint status, enforcement records, and policy incidents
- evaluation spine primitives for trace bundles, evaluations, review queue items, feedback notes, and alert-oriented event lineage
- operator commands and UDS endpoints for `action trace`, `action evals`, `review list`, and `review resolve`
- named evaluation profiles, scorecards, datasets, experiment runs, experiment case results, and alert events
- operator commands and UDS endpoints for `eval dataset list`, `eval dataset show`, `eval run`, `eval run-status`, `alert list`, and `alert ack`
- automatic dataset capture and isolated replay runs for `task.plan`, `repo.review`, and `incident.enrich`
- derived `interaction_model` metadata across action inspection, trace bundles, and dataset cases
- native Rust `crawfish-a2a` outbound adapter with treaty-governed remote delegation for `task.plan`
- read-only treaty operator surfaces via `crawfish treaty list`, `crawfish treaty show`, and `/v1/treaties`
- delegation receipts, remote principal lineage, and remote task refs in action inspection and trace metadata
- richer deterministic evaluation criteria, including JSON schema, regex, numeric-threshold, equality, and artifact-absence checks
- executor-first pairwise comparison runs, pairwise case results, and built-in `task_plan_pairwise_default`
- operator commands and UDS endpoints for `eval compare`, `eval compare-status`, and pairwise review filtering
- criterion-level evidence persisted inside `EvaluationRecord`
- pairwise-aware review queue items, feedback-note lineage, and comparison-oriented alert rules
- decision-complete treaty packs with result-evidence and escalation semantics for A2A outbound delegation
- remote outcome dispositions for treaty-governed delegation: `accepted`, `review_required`, and `rejected`
- treaty-aware review queue and alert integration for remote-agent results
- treaty violation reporting and post-result frontier-gap visibility in action inspection and trace metadata
- remote-aware evaluation criteria for treaty-governed `task.plan`, including external-ref presence, interaction-model matching, remote outcome disposition, and treaty-violation absence
- automatic `task_plan_remote_default` profile selection for `task.plan` when execution crosses into the `remote_agent` interaction model
- remote-outcome metadata on evaluation records and experiment case results, including remote disposition and treaty-violation counts
- federation packs, federation decisions, and remote escalation metadata for A2A outbound delegation
- read-only operator surfaces for `federation list`, `federation show`, and `/v1/federation/packs`
- `RemoteEvidenceBundle`, `RemoteEvidenceItem`, and remote review dispositions for every `remote_agent` action attempt
- `remote_result_review` queue items plus operator read surface for `action remote-evidence` and `/v1/actions/{id}/remote-evidence`
- explicit remote review resolutions: `accept_result`, `reject_result`, and `needs_followup`
- `RemoteFollowupRequest` and `RemoteAttemptRecord` for same-action remote admissibility continuation
- operator surfaces for `action remote-followups`, `action remote-followup-dispatch`, and `/v1/actions/{id}/remote-followups`

### Changed

- `Cargo.lock` is now tracked because Crawfish publishes binaries
- the runnable example under `examples/hero-swarm/` is now the reference public example, not a loose demo
- public terminology now uses `agent swarm` as the primary term, with `fleet` retained only as a temporary alpha migration alias
- `policy validate` is now a strict dry-run with no runtime persistence side effects
- action phase persistence now uses canonical snake_case values such as `awaiting_approval`
- workspace mutation now enforces workspace-scoped file locking and stable failure codes for lock, lease, approval, route, executor, and restart states
- denied action admission now fails before encounter persistence when governance rejects the request at preflight
- `ExecutionSurface` now returns structured outputs plus external refs and surface event batches, so harness adapters can attach run lineage without bypassing runtime inspection
- the public project language now explicitly treats OpenClaw, Codex, Claude Code, Gemini CLI, and future adapters as specialized general-purpose harnesses rather than coding-only surfaces
- `task_planner` now defaults `task.plan` to `verify_loop` with deterministic proof rather than plain single-pass completion
- `task_planner` now prefers local harnesses before OpenClaw, and the public example/demo reflects that local-first route order
- `task.plan` now normalizes `context_files` as the primary contextual file input while still accepting the alpha-era `files_of_interest` alias
- the public implementation boundary is now documented as Rust-first, not Rust-only
- the public philosophy and architecture now explicitly distinguish constitutions from runtime enforcement, and position evaluation as a swarm control-plane substrate rather than a future UI-only concern
- `quality.evaluation_profile` is now the primary evaluation config surface; `evaluation_hook` remains alpha-compatible but deprecated
- the public README and spec now explicitly distinguish context-split multi-agent coordination from real swarm encounters across owners, harnesses, and trust domains, with inline source citations on first mention
- `PolicyIncident.reason_code` is now the primary runtime field, with legacy `code` preserved as an alpha-compatibility alias
- `task.plan` routing now treats A2A as the first real remote-agent plane, distinct from both harness crossings and local context-split coordination
- the public README and spec now explicitly describe A2A as treaty-governed remote delegation rather than a deferred protocol placeholder
- the evaluation spine now includes pairwise comparison and human side-by-side review so routing choices can improve without adding an opaque LLM judge
- treaty-governed remote delegation now checks remote results after dispatch, not only whether delegation was allowed before dispatch
- treaty configuration now treats result evidence, artifact class scope, data scope, and escalation behavior as first-class runtime inputs
- executor-first pairwise comparison now prefers fewer treaty-governance violations before broader doctrine and policy incident counts
- remote-agent `task.plan` outcomes are now evaluated as frontier-evidence events, not only as returned artifact quality
- treaty-governed remote delegation now compiles a federation pack so remote state and result escalation are interpreted by a reusable control-plane policy rather than adapter-local rules
- inspect and trace now surface federation pack id, federation decision, remote evidence status, and remote state disposition for remote-agent actions
- remote-agent inspect, trace, dataset capture, alerting, and experiment results now inherit remote evidence refs and remote review disposition metadata
- `needs_followup` now creates a structured remote follow-up request rather than a generic lingering review state
- same-action remote re-delegation now preserves treaty, federation, principal, evidence-bundle, and attempt lineage instead of spawning a disconnected second action
- federation packs now control remote follow-up allowance and attempt limits through `followup_allowed`, `max_followup_attempts`, and `followup_review_priority`

### Migration Notes

- prefer `quality.evaluation_profile` for new configs and manifests
- prefer `[evaluation.pairwise_profiles.<name>]` when customizing comparison behavior beyond the built-in `task_plan_pairwise_default`
- prefer the expanded `[treaties.packs.<name>]` fields for new remote-agent integrations, especially `required_result_evidence`, `on_scope_violation`, `on_evidence_gap`, and `alert_rules`
- prefer `[federation.packs.<name>]` for remote-agent escalation defaults such as `result_acceptance_policy`, `scope_violation_policy`, `evidence_gap_policy`, and blocked/auth-required handling
- prefer `[federation.packs.<name>]` follow-up controls when remote `review_required` outcomes need admissibility continuation under the same action
- expect `task.plan` to resolve to `task_plan_remote_default` automatically when the selected executor crosses into the `remote_agent` plane
- `quality.evaluation_hook` still parses during `0.1.x alpha`, but only legacy built-ins are normalized automatically
- prefer `PolicyIncident.reason_code` in new integrations and tooling; the older `code` name remains accepted as a compatibility alias during alpha

### Security

- same-device foreign-owner mutation remains denied by default in the current governance baseline
- local workspace mutation now requires approval and an active lease before commit

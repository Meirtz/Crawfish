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

### Migration Notes

- prefer `quality.evaluation_profile` for new configs and manifests
- prefer `[evaluation.pairwise_profiles.<name>]` when customizing comparison behavior beyond the built-in `task_plan_pairwise_default`
- `quality.evaluation_hook` still parses during `0.1.x alpha`, but only legacy built-ins are normalized automatically
- prefer `PolicyIncident.reason_code` in new integrations and tooling; the older `code` name remains accepted as a compatibility alias during alpha

### Security

- same-device foreign-owner mutation remains denied by default in the current governance baseline
- local workspace mutation now requires approval and an active lease before commit

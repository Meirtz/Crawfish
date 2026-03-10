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
- reference hero demo assets plus `examples/hero-fleet/demo.sh`
- `P1a` OpenClaw inbound Gateway RPC bridge under `integrations/openclaw-inbound/`
- OpenClaw inbound caller mapping, scoped action inspection, and scoped agent status over the local UDS API
- `coding.patch.plan` and the `coding_planner` hero agent as the first OpenClaw outbound capability
- native Rust `crawfish-openclaw` Gateway adapter with streamed lifecycle, assistant, and tool event mapping
- deterministic fallback planning for `coding.patch.plan` when the preferred OpenClaw route is unavailable
- explicit abandoned-run lineage for restarted OpenClaw-backed actions

### Changed

- `Cargo.lock` is now tracked because Crawfish publishes binaries
- the runnable example under `examples/hero-fleet/` is now the reference public example, not a loose demo
- `policy validate` is now a strict dry-run with no runtime persistence side effects
- action phase persistence now uses canonical snake_case values such as `awaiting_approval`
- workspace mutation now enforces workspace-scoped file locking and stable failure codes for lock, lease, approval, route, executor, and restart states
- denied action admission now fails before encounter persistence when governance rejects the request at preflight
- `ExecutionSurface` now returns structured outputs plus external refs and surface event batches, so harness adapters can attach run lineage without bypassing runtime inspection

### Security

- same-device foreign-owner mutation remains denied by default in the current governance baseline
- local workspace mutation now requires approval and an active lease before commit

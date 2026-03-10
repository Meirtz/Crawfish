# Changelog

This project follows a simple alpha changelog discipline: user-visible changes must be recorded here before merge.

## Unreleased

### Added

- Rust-first Hero P0 slice with deterministic `repo.index`, deterministic-first `repo.review`, and `ci.triage`
- SSE MCP client support for remote tool-backed inputs
- restart recovery with deterministic checkpoint metadata
- local operator inspection for artifact refs, checkpoint refs, recovery stage, encounter metadata, and external refs
- public repository governance files, templates, and contribution policy
- approval-gated `workspace.patch.apply` via the new `workspace_editor` agent
- operator control commands for `action list`, `action approve`, `action reject`, and `lease revoke`
- runtime persistence and inspection for consent grants, capability leases, and audit receipts

### Changed

- `Cargo.lock` is now tracked because Crawfish publishes binaries
- the runnable example under `examples/hero-fleet/` is now the reference public example, not a loose demo
- `policy validate` is now a strict dry-run with no runtime persistence side effects
- action phase persistence now uses canonical snake_case values such as `awaiting_approval`

### Security

- same-device foreign-owner mutation remains denied by default in the current governance baseline
- local workspace mutation now requires approval and an active lease before commit

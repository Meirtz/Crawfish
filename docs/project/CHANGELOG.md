# Changelog

This project follows a simple alpha changelog discipline: user-visible changes must be recorded here before merge.

## Unreleased

### Added

- Rust-first Hero P0 slice with deterministic `repo.index`, deterministic-first `repo.review`, and `ci.triage`
- SSE MCP client support for remote tool-backed inputs
- restart recovery with deterministic checkpoint metadata
- local operator inspection for artifact refs, checkpoint refs, recovery stage, encounter metadata, and external refs
- public repository governance files, templates, and contribution policy

### Changed

- `Cargo.lock` is now tracked because Crawfish publishes binaries
- the runnable example under `examples/hero-fleet/` is now the reference public example, not a loose demo

### Security

- same-device foreign-owner mutation remains denied by default in the current governance baseline

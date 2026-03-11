# Documentation

This repository keeps product and architecture documentation under `docs/`.

## Layout

- `docs/spec/`
  - Canonical Markdown specifications and product documents.
- `docs/exports/`
  - Generated exports derived from the Markdown spec set.
- `docs/archive/`
  - Historical or legacy materials retained for reference.
- `docs/experimental/`
  - Notes and entry points for retained experimental alpha surfaces.

## Source Of Truth

The source of truth for product definition and architecture is the Markdown set in `docs/spec/`.

Current spec entry points:

- [`spec/philosophy.md`](spec/philosophy.md)
- [`spec/vision.md`](spec/vision.md)
- [`spec/architecture.md`](spec/architecture.md)
- [`spec/v0.1-plan.md`](spec/v0.1-plan.md)
- [`spec/glossary.md`](spec/glossary.md)
- [`experimental/README.md`](experimental/README.md)

The implementation boundary is **Rust-first, not Rust-only**:

- the runtime, control plane, storage, and native outbound adapters live in the Cargo workspace
- isolated edge bridges may live under [`../integrations/`](../integrations/)
- the current example is the thin OpenClaw inbound bridge at [`../integrations/openclaw-inbound/`](../integrations/openclaw-inbound/)

## Project Governance

Public maintenance and contribution policy lives in:

- [`../.github/CONTRIBUTING.md`](../.github/CONTRIBUTING.md)
- [`../.github/CODE_OF_CONDUCT.md`](../.github/CODE_OF_CONDUCT.md)
- [`../.github/SECURITY.md`](../.github/SECURITY.md)
- [`../.github/SUPPORT.md`](../.github/SUPPORT.md)
- [`project/CHANGELOG.md`](project/CHANGELOG.md)

## Runnable Example

The public happy path is the **mainline alpha** example under `examples/hero-swarm/`.

The live alpha example swarm is tracked under:

- [`../examples/hero-swarm/Crawfish.toml`](../examples/hero-swarm/Crawfish.toml)
- [`../examples/hero-swarm/agents/repo_indexer.toml`](../examples/hero-swarm/agents/repo_indexer.toml)
- [`../examples/hero-swarm/agents/repo_reviewer.toml`](../examples/hero-swarm/agents/repo_reviewer.toml)
- [`../examples/hero-swarm/agents/ci_triage.toml`](../examples/hero-swarm/agents/ci_triage.toml)
- [`../examples/hero-swarm/agents/task_planner.toml`](../examples/hero-swarm/agents/task_planner.toml)
- [`../examples/hero-swarm/agents/workspace_editor.toml`](../examples/hero-swarm/agents/workspace_editor.toml)

That example is the current implementation reference for local swarm control, deterministic and local-harness execution, approval-gated local mutation, and inspectable operator state.

Experimental alpha surfaces are documented separately in:

- [`experimental/README.md`](experimental/README.md)

## Export Policy

The latest consolidated DOCX export is tracked at:

- [`exports/Crawfish-PRD.docx`](exports/Crawfish-PRD.docx)

To regenerate it, run:

```bash
python3 scripts/export_docset.py
```

## Archive Policy

Historical materials are retained under `docs/archive/` for provenance and comparison, but they are not editable specs.

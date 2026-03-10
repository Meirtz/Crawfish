# Crawfish

> Lifecycle-managed agent runtime and control plane for always-on production systems.

Crawfish is a general-purpose agent runtime for teams that need agents to run under explicit operational constraints: lifecycle, deadlines, budgets, approvals, tool scopes, checkpointing, recovery, and continuity during serious outages. It sits between lightweight agent SDKs and durable workflow engines.

Philosophically, Crawfish treats agents as bounded software workers, not mystical autonomous entities. The runtime's job is not to maximize autonomy at all costs. Its job is to maximize useful agency under explicit control.

In a world where specialized harnesses and agent gateways such as [OpenClaw](https://docs.openclaw.ai/concepts/agent-loop), [Codex](https://openai.com/codex/), [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview), [Gemini CLI](https://github.com/google-gemini/gemini-cli), and ACP-compatible clients keep multiplying, Crawfish is not trying to be one more harness. It is the continuity and governance layer above many harnesses.

When agents roam across owners, laptops, and networks, governance is not optional. The same machine is already a frontier: different agents may belong to different humans, teams, or contexts and should not silently share workspace, memory, secrets, or mutating authority just because they happen to run side by side.

This repository is now **Rust-first for implementation** and **Markdown-first for product and architecture specs**. The Markdown files in [`docs/spec/`](docs/spec/) remain the design source of truth. The Cargo workspace under [`crates/`](crates/) is the implementation source of truth. Generated exports live in [`docs/exports/`](docs/exports/), and historical materials live in [`docs/archive/`](docs/archive/).

To regenerate the consolidated DOCX export from Markdown, run `python3 scripts/export_docset.py`.

To compile and test the Rust workspace, run `cargo test --workspace`.
To compile and test the OpenClaw inbound bridge, run `npm test --prefix integrations/openclaw-inbound`.

## Project Status

Crawfish is public and maintained seriously, but it is still an **explicit alpha** project.

- Current maturity: `0.x` / `alpha`
- Supported implementation baseline: Rust-first
- Supported runtime environments: macOS and Linux
- Supported MCP transport: SSE only

Alpha means the project is real, tested, and maintained, but not yet compatibility-stable.

## Public Surfaces (Alpha)

These surfaces are public today, but still unstable during alpha:

- `crawfish` CLI
- `Crawfish.toml` and agent manifest format
- local UDS HTTP API
- Rust crates in the workspace

Breaking alpha changes are allowed, but they must ship with:

- a changelog entry in [`docs/project/CHANGELOG.md`](docs/project/CHANGELOG.md)
- README or spec updates for the changed behavior
- a migration note when the break is user-visible

Project maintenance docs live in:

- [`docs/project/CHANGELOG.md`](docs/project/CHANGELOG.md)
- [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md)
- [`.github/SECURITY.md`](.github/SECURITY.md)
- [`.github/SUPPORT.md`](.github/SUPPORT.md)

## Why Crawfish

- **Harnesses are plentiful; operability is scarce.** Crawfish exists to make many harnesses, gateways, and agent runtimes behave like one governable system.
- **Governance is not optional.** When agents from different owners or trust domains meet, Crawfish treats that as a governed encounter, not an implicit collaboration.
- **Managed lifecycle instead of ad hoc loops.** Agents are supervised resources with desired state, observed state, health checks, drain behavior, and degraded modes.
- **Action-based durable execution instead of fire-and-forget calls.** Long-running work is modeled as a first-class action with feedback, cancellation, checkpointing, and resumability.
- **Execution contracts instead of policy hidden in prompts.** Delivery, cost, safety, quality, and recovery rules are compiled and enforced by the runtime.
- **Three-plane interoperability instead of one transport assumption.** Crawfish uses MCP for tools, OpenClaw Gateway and ACP-compatible integrations for specialized harnesses, and A2A for remote agent delegation.
- **Continuity above the model layer instead of provider lock-in.** When models, harnesses, or networks are impaired, Crawfish contracts safely into deterministic fallbacks, queueing, cached reads, or human handoff.

Compared with [LangGraph](https://docs.langchain.com/oss/python/langgraph/overview), [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/), and [Temporal Workflows](https://docs.temporal.io/workflows), Crawfish is centered on operating agent fleets under policy and failure pressure, not only composing agent logic or durable control flow. Compared with [OpenClaw](https://docs.openclaw.ai/concepts/agent-loop), Crawfish is the continuity control plane, not the interactive agent loop or gateway surface.

## System

```mermaid
flowchart LR
    subgraph CP["Control plane"]
        SD["crawfishd supervisor"]
        PE["Policy engine"]
        SCH["Scheduler and admission control"]
        LC["Lifecycle manager"]
    end

    subgraph EP["Execution plane"]
        W1["Agent worker: repo_indexer"]
        W2["Agent worker: repo_reviewer"]
        W3["Agent worker: ci_triage"]
        W4["Agent worker: workspace_editor"]
        W5["Agent worker: incident_enricher"]
    end

    subgraph PM["Protocol mesh"]
        MCP["Tool plane: MCP"]
        OCI["Ingress: OpenClaw plugin and RPC (P1)"]
        OCO["Harness plane: OpenClaw Gateway adapter (P1)"]
        ACP["Harness plane: ACP-compatible adapters (P1)"]
        A2A["Agent plane: A2A (P1)"]
    end

    subgraph SS["State stores"]
        ACT["Action store"]
        CFG["Manifest, policy, and governance store"]
        GOV["Encounter, grant, and lease store"]
        CKPT["Checkpoint store"]
        OBS["Telemetry, audit, and receipt sink"]
    end

    SD --> LC
    SD --> PE
    SD --> SCH
    PE --> SCH
    SCH --> W1
    SCH --> W2
    SCH --> W3
    SCH --> W4
    SCH --> W5
    W1 --> MCP
    W2 --> MCP
    W3 --> MCP
    W5 --> MCP
    SD -. future .-> OCI
    W2 -. future .-> OCO
    W2 -. future .-> ACP
    W3 -. future .-> A2A
    SD --> ACT
    SD --> CFG
    SD --> GOV
    W1 --> CKPT
    W2 --> CKPT
    W3 --> CKPT
    W4 --> CKPT
    W5 --> CKPT
    SD --> OBS
    W1 --> OBS
    W2 --> OBS
    W3 --> OBS
    W4 --> OBS
    W5 --> OBS
```

## Lifecycle

```mermaid
flowchart LR
    subgraph AG["Agent lifecycle"]
        AU["Unconfigured"] --> AC["Configuring"]
        AC --> AI["Inactive"]
        AC --> AF["Failed"]
        AI --> AV["Activating"]
        AV --> AA["Active"]
        AV --> AD["Degraded"]
        AA --> AD
        AA --> AR["Draining"]
        AD --> AA
        AD --> AR
        AR --> AI
        AR --> AZ["Finalized"]
        AF --> AC
        AF --> AZ
    end

    subgraph AX["Action lifecycle"]
        X1["Accepted"] --> X2["Running"]
        X1 --> X8["Expired"]
        X2 --> X3["Blocked"]
        X2 --> X4["AwaitingApproval"]
        X2 --> X5["Cancelling"]
        X2 --> X6["Completed"]
        X2 --> X7["Failed"]
        X3 --> X2
        X4 --> X2
        X4 --> X8
        X5 --> X6
        X5 --> X7
    end
```

## Hero Demo

The first public story is a small engineering and operations fleet running under one supervisor:

- `repo_indexer` keeps repository structure and ownership context warm.
- `repo_reviewer` reviews pull requests and produces structured findings.
- `ci_triage` classifies failed CI runs and suggests next actions.
- `workspace_editor` performs approval-gated local workspace mutation through a deterministic path.
- `incident_enricher` gathers logs, traces, and likely blast radius for production alerts.

The demo shows five things at once:

1. Dependency-aware activation and drain order.
2. Degraded behavior when `repo_indexer` or an MCP dependency is impaired.
3. Approval-gated local workspace mutation with explicit grant, lease, and revoke control.
4. Restart recovery from the last durable checkpoint.
5. Continuity behavior when every external model route is unavailable.

The same demo should also prove a harder claim: if every external model route is unavailable, the fleet does not simply disappear. It keeps the control plane alive, continues deterministic work where possible, queues or hands off the rest, and makes the contraction explicit.

Future versions of the same demo should also show bidirectional OpenClaw interop and Ralph-style verified coding actions: OpenClaw can submit work into Crawfish as an external control surface, while selected coding actions such as `coding.patch.plan` route out through the OpenClaw Gateway agent loop and complete only after deterministic verification passes.

That same future demo should also show a same-device foreign-owner encounter: a roaming external agent can request access to a local capability, but it must pass encounter policy, receive explicit consent, and execute only through a revocable capability lease.

## v0.1 Scope

The alpha scope is intentionally narrow:

- `crawfishd` supervisor daemon
- lifecycle manager
- action execution
- execution contract v1
- MCP integration
- checkpoint and resume
- observability baseline
- security baseline

Continuity baseline is delivered through these same P0 systems. It is not a separate product surface so much as the discipline with which lifecycle, recovery, and inspection behave under serious outage.

OpenClaw interoperability, `ACP`, and `A2A` remain important parts of the product story, but they are intentionally deferred until after the lifecycle, contract, and recovery model is proven in P0.

## Current Alpha Slice

The current Rust alpha now covers one runnable Hero P0 slice plus the first `P1a` OpenClaw inbound surface:

- `repo.index` scans a local workspace and emits `repo_index.json`.
- `repo.review` runs deterministic review checks and reuses or bootstraps the latest repo index.
- `ci.triage` classifies CI failures from direct logs or from an SSE MCP tool route.
- `incident.enrich` turns local logs plus service topology into deterministic blast-radius and next-step artifacts.
- `workspace.patch.apply` performs deterministic local file edits under explicit approval, grant, and lease control.
- `P1a` adds a thin OpenClaw inbound Gateway RPC bridge under [`integrations/openclaw-inbound/`](integrations/openclaw-inbound/) with `crawfish.action.submit`, `crawfish.action.inspect`, `crawfish.action.events`, and `crawfish.agent.status`.
- OpenClaw inbound caller mapping is configured under `[openclaw.inbound]` in [`examples/hero-fleet/Crawfish.toml`](examples/hero-fleet/Crawfish.toml), and governance remains enforced in `crawfishd`, not in the bridge.
- `inspect` surfaces artifact refs, checkpoint refs, recovery stage, continuity mode, encounter metadata, and external refs.
- `action list`, `action events`, `action approve`, `action reject`, and `lease revoke` expose the operator control path over the local UDS API.
- restart recovery requeues `running` actions and resumes deterministic work from checkpoint metadata.
- same-owner local read-only actions can be leased automatically, while same-device foreign-owner mutation remains denied by default.
- `workspace_editor` now enforces workspace-scoped file locks and surfaces lock conflict and lease-expiry failures as stable operator-visible metadata.

The first external tool transport implemented in code is `MCP over SSE`. `repo_reviewer` remains deterministic-first, while `ci_triage` can fetch remote log material through MCP and then complete the actual classification locally.

With this `P1a` inbound slice in place, the next planned milestone is `P1b OpenClaw outbound`.

## Quickstart

Start with the design docs:

1. [`docs/spec/vision.md`](docs/spec/vision.md) for category, positioning, and competitive wedge.
2. [`docs/spec/architecture.md`](docs/spec/architecture.md) for public primitives, state machines, and runtime model.
3. [`docs/spec/v0.1-plan.md`](docs/spec/v0.1-plan.md) for alpha scope, milestones, and acceptance criteria.
4. [`docs/spec/glossary.md`](docs/spec/glossary.md) for canonical terminology.
5. [`docs/README.md`](docs/README.md) for repository documentation policy and export locations.

Then use the Rust workspace:

```bash
cargo test --workspace
cargo run -p crawfish-cli --bin crawfish -- init ./sandbox
cd sandbox
cargo run -p crawfish-cli --bin crawfish -- run &
CRAWFISH_PID=$!
sleep 1
cargo run -p crawfish-cli --bin crawfish -- status --json
cargo run -p crawfish-cli --bin crawfish -- action submit \
  --target-agent repo_reviewer \
  --capability repo.review \
  --goal "review pull request" \
  --caller-owner local-dev \
  --inputs-json "{\"workspace_root\":\"$(pwd)\",\"changed_files\":[\"src/lib.rs\"]}" \
  --json
cargo run -p crawfish-cli --bin crawfish -- action submit \
  --target-agent ci_triage \
  --capability ci.triage \
  --goal "triage local logs" \
  --caller-owner local-dev \
  --inputs-json "{\"log_text\":\"error: test failed, to rerun pass \`cargo test\`\"}" \
  --json
cargo run -p crawfish-cli --bin crawfish -- action submit \
  --target-agent incident_enricher \
  --capability incident.enrich \
  --goal "enrich local incident" \
  --caller-owner local-dev \
  --inputs-json "{\"service_name\":\"api\",\"log_text\":\"ERROR api timeout contacting db\\nWARN web received 503 service unavailable from api\\n\"}" \
  --json
MUTATION_ID=$(cargo run -p crawfish-cli --bin crawfish -- action submit \
  --target-agent workspace_editor \
  --capability workspace.patch.apply \
  --goal "apply local patch" \
  --caller-owner local-dev \
  --workspace-write \
  --mutating \
  --inputs-json "{\"workspace_root\":\"$(pwd)\",\"edits\":[{\"path\":\"notes.txt\",\"op\":\"create\",\"contents\":\"hello from crawfish\\n\"}]}" \
  --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["action_id"])')
cargo run -p crawfish-cli --bin crawfish -- action list --phase awaiting_approval --json
cargo run -p crawfish-cli --bin crawfish -- action approve "$MUTATION_ID" --approver local-dev --json
cargo run -p crawfish-cli --bin crawfish -- inspect "$MUTATION_ID" --json
cargo run -p crawfish-cli --bin crawfish -- action events "$MUTATION_ID" --json
kill $CRAWFISH_PID
```

For a full sample configuration, start from [`examples/hero-fleet/Crawfish.toml`](examples/hero-fleet/Crawfish.toml) and the agent manifests under [`examples/hero-fleet/agents/`](examples/hero-fleet/agents/).
For a repeatable local demo that exercises review, incident enrichment, approval-gated mutation, and operator event inspection, run [`examples/hero-fleet/demo.sh`](examples/hero-fleet/demo.sh).

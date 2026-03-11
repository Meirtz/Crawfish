# Crawfish Glossary

This glossary is shared across the repository. Other documents should reuse these terms exactly rather than redefining them.

## Core Terms

| Term | Definition |
| --- | --- |
| **runtime** | The full Crawfish system that accepts manifests and actions, supervises workers, enforces contracts, and manages lifecycle and recovery. |
| **control plane** | The part of the runtime responsible for desired state, scheduling, lifecycle transitions, policy compilation, admission control, inspection, and recovery decisions. |
| **execution plane** | The part of the runtime that performs agent turns, tool calls, checkpoints, and result generation. |
| **swarm** | A governed set of agents and harness-backed workers operating under one Crawfish control plane. A swarm does not imply shared trust, shared memory, or automatic context sharing. |
| **agent** | A bounded software actor defined by an `AgentManifest`. An agent has identity, capabilities, policy defaults, dependencies, and lifecycle behavior. In Crawfish, an agent is neither a chatbot persona nor an unconstrained autonomous entity. |
| **owner** | The controlling principal for an agent or session, such as a human, team, org, or service account. Governance decisions must always be attributable to owners. |
| **worker** | A concrete execution instance that runs one agent's turns and tool sessions under supervision. One agent may map to one or more workers depending on deployment mode. |
| **harness** | A specialized agent runtime or execution surface optimized for a class of work such as planning, research, investigation, operations, or coding. Examples include OpenClaw, ACP-compatible agents, and harness-specific CLIs. |
| **action** | The durable unit of work in Crawfish. An action has a goal, compiled contract, schedule, phase, checkpoints, and outputs. |
| **deterministic executor** | A traditional programmatic executor that does not rely on an LLM or external agent harness. Examples include policy evaluators, static analyzers, rule engines, AST transforms, classifiers, and queueing logic. |
| **contract** | The runtime-enforced set of delivery, execution, safety, quality, and recovery constraints applied to an action. |
| **execution strategy** | The per-action execution pattern used by Crawfish, such as `single_pass` or `verify_loop`, independent of which adapter or harness is selected. |
| **interaction model** | A derived runtime classification describing what kind of boundary the action is actually crossing, such as `context_split`, `same_owner_swarm`, `same_device_multi_owner`, `remote_harness`, or `remote_agent`. It is not user-supplied configuration. |
| **treaty** | The runtime law that decides whether remote delegation across a principal boundary is allowed at all, including which capabilities, scopes, artifact classes, and checkpoints are authorized. |
| **federation pack** | A static remote-governance bundle that interprets remote-agent states, evidence gaps, scope violations, and remote results after delegation has been allowed by a treaty. |
| **verification spec** | The deterministic check set attached to a `verify_loop`, such as tests, lint, schema validation, file checks, or approval gates. |
| **policy** | A rule set that influences or constrains execution. Policies may be hard or soft and may come from organization defaults, agent defaults, or action overrides. |
| **trust domain** | The relationship class between the caller and the target runtime surface, such as `same_owner_local`, `same_device_foreign_owner`, or `external_partner`. It is an input to governance, not merely a label. |
| **data boundary** | The rule that determines whether context or artifacts remain owner-local, must be redacted, or may be shared only through an encounter-scoped lease. |
| **capability** | A normalized description of something an agent or adapter can do, including verbs, mutability, risk, latency, cost, and approval expectations. |
| **degraded** | A managed operating mode in which an agent stays available with reduced capability or stricter restrictions rather than failing hard. |
| **degraded profile** | A named policy-driven contraction of capability such as `read_only`, `dependency_isolation`, or `budget_guard`. A degraded profile defines what is restricted, what is still allowed, and how recovery occurs. |
| **continuity mode** | A runtime condition in which Crawfish preserves the safest useful subset of behavior during major outages, including deterministic execution, queueing, cached reads, or human handoff. |
| **adapter** | A protocol bridge that exposes external tools, harnesses, or agents to Crawfish through a normalized capability surface. Examples include MCP clients, ACP harness connectors, and A2A connectors. |
| **encounter** | A governed interaction attempt between a caller and an agent or capability. Encounters are classified, evaluated by policy, and may be denied, approved, leased, or revoked. |
| **encounter policy** | The rule set that governs what may happen when two parties meet, including capability visibility, data boundary, workspace boundary, network boundary, and approval requirements. |
| **consent grant** | An explicit, purpose-bound, scope-bound, and time-bound authorization to cross a boundary. A grant authorizes intent; it does not by itself authorize indefinite execution. |
| **capability lease** | A short-lived runtime authorization derived from a grant. Leases are revocable, auditable, and bound to specific capabilities and scopes. |
| **audit receipt** | Formal governance evidence that records the encounter, decision, grant or lease references, outcome, and approver or revocation details. It is not just another log line. |

## Supporting Terms

| Term | Definition |
| --- | --- |
| **supervisor** | The `crawfishd` daemon that owns reconciliation, scheduling, inspection, and lifecycle orchestration. |
| **tool plane** | The interoperability plane for tools. In Crawfish this is primarily MCP. |
| **harness plane** | The interoperability plane for specialized agent harnesses. In Crawfish this includes OpenClaw Gateway and ACP-compatible harness integration. |
| **agent plane** | The interoperability plane for remote or external agents. In Crawfish this is primarily A2A. |
| **remote evidence bundle** | The control-plane evidence package attached to a `remote_agent` action attempt. It captures treaty and federation lineage, remote task refs, checkpoint evidence, artifact and scope evidence, and any incidents or violations tied to the attempt. |
| **remote attempt** | One concrete remote delegation attempt attached to a local action. A single action may accumulate multiple remote attempts when follow-up is explicitly re-dispatched under the same treaty and federation context. |
| **remote evidence status** | The runtime classification of whether required remote result evidence is complete, missing, invalid, or outside allowed scope. |
| **remote review disposition** | The operator-facing state that explains whether a remote result is pending review, accepted, rejected, or requires follow-up before the action can move forward. |
| **remote outcome disposition** | The control-plane judgment applied to a remote result after treaty and federation checks, currently `accepted`, `review_required`, or `rejected`. |
| **remote follow-up request** | A structured admissibility continuation created from a `review_required` remote outcome. It names the missing evidence, pins the treaty and federation context, and can be explicitly re-dispatched on the same action. |
| **OpenClaw binding** | A first-class harness binding that lets Crawfish call OpenClaw through its Gateway protocol or be called by OpenClaw through plugins and RPC. |
| **manifest** | The declarative specification of an agent, including capabilities, dependencies, runtime profile, lifecycle policy, and contract defaults. |
| **checkpoint** | A durable recovery record written at safe execution boundaries such as model turns, mutating tool calls, or explicit yield points. |
| **context split** | A narrow multi-agent pattern in which sub-agents or bounded workers coordinate inside one application authority without a meaningful owner, trust-domain, or external-harness boundary. Context split is coordination, not full swarm governance. |
| **action store** | The persistent storage layer for action metadata, feedback events, terminal state, and artifact references. |
| **hard policy** | A rule that must never be violated. If it cannot be satisfied, the runtime rejects, blocks, or fails the action. |
| **soft policy** | A preferred rule that may trigger rerouting or degradation before the runtime declares failure. |
| **sovereignty-first** | The governance stance that agents should be treated as belonging to explicit owners and trust domains before any coordination assumptions are made. |
| **repair loop** | The control-plane cycle that monitors failure signals, diagnoses the fault, applies a bounded repair action, and verifies whether service can be restored safely. |
| **self-repair** | Runtime actions that restore service without changing the product's core goals or policy envelope, such as reconnecting adapters, replaying checkpoints, rebuilding caches, or isolating failed dependencies. |
| **controlled self-evolution** | Deliberate adaptation of policy, routing, or thresholds based on evidence, performed offline, in shadow mode, or within tightly bounded envelopes. It is not unrestricted runtime self-modification. |
| **human handoff** | An explicit continuity outcome in which Crawfish packages context, evidence, and next-step guidance for an operator when no safe automated route remains. |
| **verify loop** | A verified execution pattern, inspired by Ralph-style loops, in which a verification-sensitive action iterates through work, deterministic verification, feedback injection, and retry until success, handoff, or stop budget exhaustion. |
| **hero demo** | The canonical public example for Crawfish: an engineering and operations swarm made up of `repo_indexer`, `repo_reviewer`, `ci_triage`, `incident_enricher`, `workspace_editor`, and `task_planner`. |
| **mainline alpha** | The supported public happy path for the current release line: local swarm control, local harness routing, deterministic fallback, approval-gated local mutation, and inspectable runtime state. |
| **experimental alpha** | Implemented and tested surfaces that remain outside the current public happy path, such as OpenClaw, A2A, and federation or treaty-driven remote governance. |
| **support center** | The part of the product that current onboarding, default templates, reference examples, and public guarantees are optimized around. In the current release line, that is the mainline alpha local swarm path. |

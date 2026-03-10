# Crawfish Philosophy

Canonical terminology is defined in [`glossary.md`](glossary.md).

## Why This Document Exists

Crawfish is not a project that should be designed by looking backward at yesterday's application stack and then asking how to squeeze agents into it.

The emerging environment is different:

- harnesses will proliferate faster than standards stabilize
- reasoning quality will improve, regress, split, and recombine across providers
- governance and institutions will lag behind capability growth
- same-device and multi-owner agent encounters will become normal before the ecosystem feels ready for them

This document defines the forward-looking philosophy that the rest of the spec set should inherit.

It also adopts one hard lesson from frontier history: a constitution can define a lawful order without actually producing one. When capability expands faster than institutions, the gap between stated rules and enforceable order becomes a systems problem. Crawfish is designed for that gap.

## Principle 1: Build For Swarm-Age Governance, Not Single-Agent Demos

The future category is not the single assistant with a convenient shell. It is the governed swarm: many bounded workers, many execution surfaces, many owners, many risk envelopes, and many possible encounter paths.

That means Crawfish should optimize for:

- lifecycle over novelty
- supervision over vibes
- contracts over prompt folklore
- explicit authority over ambient trust

If a design choice only makes a one-agent demo look cleaner but weakens swarm governance, it is the wrong trade.

## Principle 2: Harnesses Are Replaceable, Control Planes Are Strategic

Harnesses are important, but they are not the strategic center of the system.

OpenClaw, Codex, Claude Code, Gemini CLI, ACP-compatible adapters, and future agentic surfaces will come and go, improve, fragment, or be superseded. The enduring system problem is not how to build one more harness. It is how to govern many harnesses as one swarm.

This is why Crawfish treats harnesses as execution surfaces:

- selectable
- inspectable
- fallible
- replaceable

The control plane is where long-term strategic value accumulates.

## Principle 3: Reasoning Is Volatile; Contracts And Verification Must Survive Model Churn

Model quality is real, but it is unstable.

Provider availability changes. Tool behavior changes. Routing assumptions age badly. Benchmark wins decay. A runtime that treats reasoning quality as fixed infrastructure will inherit that volatility.

Crawfish should therefore assume:

- reasoning surfaces are changeable
- self-reported success is weak evidence
- deterministic verification is a first-class runtime behavior
- continuity must preserve useful work even when the best reasoning route disappears

This is why `verify_loop` matters. It is not a coding gimmick. It is a way to make correctness less dependent on one model's current mood.

## Principle 4: Institutions Lag Capability Growth; Runtime Guardrails Cannot

The social, legal, and organizational rules around agents will arrive late.

That lag is predictable. Systems should not wait for every policy regime, enterprise standard, or ecosystem treaty to be perfect before they encode guardrails. If the software can already act, the runtime already needs:

- owner attribution
- trust-domain classification
- encounter policy
- approval and lease semantics
- revocation
- audit receipts

Crawfish should be law-like earlier than the market feels comfortable with, because the capability curve will move first.

## Principle 5: Constitutions Do Not Enforce Themselves

High-level rules are necessary. They are still incomplete.

Anthropic's [Constitutional AI](https://www.anthropic.com/research/constitutional-ai-harmlessness-from-ai-feedback/) and [Claude's Constitution](https://www.anthropic.com/constitution) are important reference points for rule-guided model behavior. They show how principles can shape a model. They do not by themselves solve the runtime problem of roaming agents, mixed owners, mutable workspaces, or external harnesses.

A runtime has to answer harder questions:

- which jurisdiction applies here
- which checkpoint must run before execution continues
- what evidence proves the checkpoint happened
- what escalation path exists when the check cannot be enforced

Without checkpoints, evidence, and escalation, a constitution is advisory text. Crawfish therefore treats doctrine as executable runtime structure: doctrine packs, jurisdiction classes, oversight checkpoints, enforcement records, and policy incidents.

## Principle 6: Frontier Enforcement Gap Is A Runtime Problem, Not Just A Policy Problem

The most dangerous failures in swarm systems will not be only “bad rules.” They will be cases where good rules exist but the runtime cannot prove enforcement.

That frontier gap appears when:

- policy says a review is required but no post-result checkpoint exists
- a capability is proposal-only in theory but a harness can still mutate by accident
- a trust boundary is named but not compiled into admission and dispatch behavior
- a doctrine exists but no operator signal appears when the doctrine is bypassed or underspecified

Crawfish should surface this gap explicitly. It should not silently treat missing enforcement as success.

That is why the runtime needs:

- policy incidents
- checkpoint status
- doctrine summaries on inspected actions
- review queues and alerts when enforcement is incomplete

## Principle 7: Evaluation Is How A Swarm Learns Without Becoming Opaque

The swarm will not become reliable by accumulating more raw traces alone.

Observability is the rear-view mirror. Evaluation is the learning loop.

LangSmith provides a useful reference shape for this through its [observability concepts](https://docs.langchain.com/langsmith/observability-concepts), [annotation queues](https://docs.langchain.com/langsmith/annotation-queues), and [automation rules](https://docs.langchain.com/langsmith/set-up-automation-rules): traces, evaluation, review, and operator automation belong to one system. Crawfish should absorb that lesson at the runtime layer.

This means:

- every significant action should become a trace bundle
- deterministic scorecards should produce durable evaluation records
- review queues should capture “needs human eyes” cases without erasing history
- feedback should be structured and reusable, not trapped in ad-hoc operator memory
- dataset cases should turn production traces into replayable institutional memory
- replay experiments should let the swarm learn from prior work without contaminating production operator queues

Evaluation is not just reporting. It is how the swarm improves while staying inspectable.

## Principle 8: Institutions Lag Capability Growth

Capability growth outruns process. That is not a temporary bug in the market; it is the normal order of technological change.

The relevant system question is therefore not whether constitutions, enterprise processes, and norms matter. They do. The question is whether the runtime can keep order while those institutions are still catching up.

Crawfish should assume:

- model behavior standards will remain partial and uneven
- organizational review capacity will be limited
- multi-owner swarm encounters will arrive before ecosystem-wide treaties do
- evaluation and doctrine will need to serve as provisional institutional memory

This is why review queues, alerts, dataset capture, replay, and checkpoint evidence belong in the runtime itself rather than in post hoc dashboards alone.

## Principle 9: Design For Future Multi-Owner Encounters, Not Yesterday's App Sandbox

The same laptop is already a frontier.

A local swarm can contain a personal agent, a company-bound agent, a tool-driven ops agent, and a roaming harness-backed session, each with different owners, context, and rights. The runtime should therefore assume that future federation problems are already visible locally in smaller form.

That means:

- same-device foreign-owner encounters are first-class
- workspace, memory, secrets, and network access are boundary objects
- cooperation is leased, not implied
- inspection and audit are product behavior, not incident response afterthoughts

If the local model is coherent, it can extend outward. If it is not coherent locally, no federation story will save it later.

## Design Consequences

These principles imply concrete architectural choices:

- Crawfish is **Rust-first, not Rust-only**
  - the runtime spine lives in Rust
  - isolated edge bridges may use other languages when that lowers integration friction
- execution strategy is separate from harness selection
  - `single_pass` and `verify_loop` are runtime patterns
  - OpenClaw, deterministic executors, and future adapters are execution surfaces
- continuity and governance are product surfaces
  - not hidden in internal scripts
  - not left to application glue
- doctrine and evaluation are runtime layers
  - not just documentation language
  - not only future UI ideas
- public terminology should stay future-correct
  - `swarm`, not `fleet`
  - `general-purpose harness`, not `coding-only harness`

## What This Means For The Roadmap

The roadmap should bias toward:

- stronger verification
- stronger governance
- stronger doctrine enforcement and visible frontier-gap reporting
- richer evaluation spines and operator review flows
- richer harness interoperability without category confusion
- better operator visibility

It should avoid:

- overfitting the product narrative to coding-only use cases
- treating one harness as the product center
- shipping more autonomy before guardrails, continuity, and inspection are ready

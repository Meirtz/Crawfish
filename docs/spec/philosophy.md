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

## Principle 5: Design For Future Multi-Owner Encounters, Not Yesterday's App Sandbox

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
- public terminology should stay future-correct
  - `swarm`, not `fleet`
  - `general-purpose harness`, not `coding-only harness`

## What This Means For The Roadmap

The roadmap should bias toward:

- stronger verification
- stronger governance
- richer harness interoperability without category confusion
- better operator visibility

It should avoid:

- overfitting the product narrative to coding-only use cases
- treating one harness as the product center
- shipping more autonomy before guardrails, continuity, and inspection are ready

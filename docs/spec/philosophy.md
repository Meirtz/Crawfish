# Crawfish Philosophy

Canonical terminology is defined in [`glossary.md`](glossary.md).

## Why This Document Exists

Crawfish is not a project that should be designed by looking backward at yesterday's application stack and then asking how to squeeze agents into it.

The emerging environment is different:

- harnesses will proliferate faster than standards stabilize
- reasoning quality will improve, regress, split, and recombine across providers
- governance and institutions will lag behind capability growth, which is also the broader institutional-lag frame behind Notion's ["Steam, Steel, and Infinite Minds"](https://www.notion.com/blog/steam-steel-and-infinite-minds-ai)
- same-device and multi-owner agent encounters will become normal before the ecosystem feels ready for them

This document defines the forward-looking philosophy that the rest of the spec set should inherit.

It also adopts one hard lesson from frontier history: a constitution can define a lawful order without actually producing one. When capability expands faster than institutions, the gap between stated rules and enforceable order becomes a systems problem. Crawfish is designed for that gap.

## Principle 1: Build For Swarm-Age Governance, Not Single-Agent Demos

The future category is not the single assistant with a convenient shell. It is the governed swarm: many bounded workers, many execution surfaces, many owners, many risk envelopes, and many possible encounter paths.

This is also where Crawfish breaks with much earlier "multi-agent" framing. A large share of earlier multi-agent systems focused on context-managed sub-agents inside one application: LangChain explicitly frames the problem around [context engineering](https://docs.langchain.com/oss/python/langchain/multi-agent), OpenAI's Agents SDK centers [handoffs](https://openai.github.io/openai-agents-python/handoffs/) and shared run [context](https://openai.github.io/openai-agents-python/context/), and AutoGen Swarm describes agents that [share the same message context](https://microsoft.github.io/autogen/0.7.3/user-guide/agentchat-user-guide/swarm.html). Those patterns solve coordination inside one app. Crawfish targets the next problem: real encounters across owners, trust domains, and harness surfaces.

That means Crawfish should optimize for:

- lifecycle over novelty
- supervision over vibes
- contracts over prompt folklore
- explicit authority over ambient trust

If a design choice only makes a one-agent demo look cleaner but weakens swarm governance, it is the wrong trade.

## Principle 2: Harnesses Are Replaceable, Control Planes Are Strategic

Harnesses are important, but they are not the strategic center of the system.

OpenClaw, Codex, Claude Code, Gemini CLI, ACP-compatible adapters, and future agentic surfaces will come and go, improve, fragment, or be superseded. The enduring system problem is not how to build one more harness. It is how to govern many harnesses as one swarm.

That also means distinguishing harnesses from remote agent planes. A2A's [Agent Card](https://github.com/a2aproject/A2A) model and Google's ["A2A: A New Era of Agent Interoperability"](https://developers.googleblog.com/a2a-a-new-era-of-agent-interoperability/) launch framing matter here: remote delegation is not just another wrapper. It is a separate authority boundary that requires treaty scope and inspectable delegation evidence.

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

LangSmith provides a useful reference shape for this through its [observability concepts](https://docs.langchain.com/langsmith/observability-concepts), [pairwise evaluation](https://docs.langchain.com/langsmith/evaluate-pairwise), [annotation queues](https://docs.langchain.com/langsmith/annotation-queues), [automation rules](https://docs.langchain.com/langsmith/set-up-automation-rules), and [experiment comparison](https://docs.langchain.com/langsmith/compare-experiment-results): traces, evaluation, comparison, review, and operator automation belong to one system. Crawfish should absorb that lesson at the runtime layer.

This means:

- every significant action should become a trace bundle
- deterministic scorecards should produce durable evaluation records
- pairwise comparison should teach routing choices without hiding disagreement behind one aggregate score
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

The reason is not just operational hygiene. It is institutional memory. If swarms are going to operate in frontier conditions before broader governance catches up, then traces, evaluations, review queues, and replay datasets become the runtime's way to remember, compare, and correct behavior without pretending the constitution enforced itself.

## Principle 9: Design For Future Multi-Owner Encounters, Not Yesterday's App Sandbox

The same laptop is already a frontier.

A local swarm can contain a personal agent, a company-bound agent, a tool-driven ops agent, and a roaming harness-backed session, each with different owners, context, and rights. The runtime should therefore assume that future federation problems are already visible locally in smaller form.

That means:

- same-device foreign-owner encounters are first-class
- workspace, memory, secrets, and network access are boundary objects
- cooperation is leased, not implied
- inspection and audit are product behavior, not incident response afterthoughts

If the local model is coherent, it can extend outward. If it is not coherent locally, no federation story will save it later.

The next frontier after same-device governance is treaty-governed remote delegation. The [A2A project](https://github.com/a2aproject/A2A) is useful because it provides a task-oriented remote-agent plane, but the runtime still has to decide when delegation is lawful, which treaty pack applies, and which evidence proves the delegation stayed inside scope.

## Principle 10: Treaties Precede Marketplaces

Remote-agent delegation should not begin with a marketplace mindset. It should begin with treaty semantics.

Before swarms can safely rely on reputation systems, federation packs, or delegated service catalogs, they need a narrower legal core:

- which remote principal is recognized
- which capabilities are allowed to cross the boundary
- which data scopes may leave the local control plane
- which artifact classes may come back
- which checkpoints must run before the result is trusted
- what happens when evidence is missing

This is why Crawfish treats remote delegation as treaty-governed rather than merely discoverable. The [A2A project](https://github.com/a2aproject/A2A) provides the remote task shape; Crawfish adds the requirement that delegation must be justified before dispatch and governable after result.

The correct order is:

1. treaty
2. federation interpretation
3. evidence
4. escalation
5. only later, broader federation economics

If that order is reversed, the swarm scales contact before it scales law.

## Principle 10.5: Treaties Are Necessary, But Not Sufficient

A treaty decides whether remote delegation is lawful. It does not, by itself, decide how the local control plane should interpret every remote state and remote result that follows.

That is why Crawfish now adds federation packs on top of treaties:

- a treaty says whether delegation is allowed
- a federation pack says how `input-required`, `auth-required`, evidence gaps, scope violations, and returned results should be interpreted
- a control plane therefore stays consistent across remote attempts instead of scattering escalation logic across adapters

This matters because frontier governance often fails after the remote side replies, not before it starts. A system that can say “yes, you may delegate” but cannot say “this remote result must be reviewed” is still under-governed.

## Principle 11: Remote Results Must Be Governed, Not Just Remote Calls

Delegation risk does not end when the remote task starts. It becomes more subtle when the result comes back.

A remote result may still be unacceptable if:

- the runtime cannot prove the remote terminal state
- the returned artifact class was outside treaty scope
- the returned data scope exceeded what the treaty allowed
- the treaty required evidence that never arrived

That is why post-result governance is part of the control plane rather than an afterthought in application code. Remote outcomes need a disposition:

- accepted
- review required
- rejected

Without that distinction, remote delegation turns every successful HTTP response into an ambient trust event.

The same lesson applies to evaluation. A remote result should not be judged only by whether it produced plausible content. It also needs to be judged by whether the frontier evidence chain held together. In practical terms, that means remote outcomes need scorecards that can inspect treaty violations, federation decisions, remote outcome disposition, delegation receipts, and checkpoint evidence rather than only the returned artifact text.

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
- treaties are runtime law, not partner metadata
  - remote delegation requires scope, evidence, and escalation
  - post-result governance is as important as dispatch-time governance
- federation packs are runtime interpretation, not marketplace metadata
  - they make remote escalation rules inspectable and reusable
  - they keep remote-state and remote-result handling consistent across delegations
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

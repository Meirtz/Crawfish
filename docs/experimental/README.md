# Experimental Alpha Surfaces

Crawfish keeps a narrow public happy path and a broader set of retained experimental surfaces.

## Mainline vs Experimental

- `mainline alpha` is the supported getting-started path for this repository:
  - local swarm control
  - local harness routing
  - deterministic fallback
  - approval-gated local mutation
  - inspectable events, traces, evaluations, and restart recovery
- `experimental alpha` keeps more advanced protocol and federation work compiled and tested:
  - OpenClaw inbound and outbound
  - A2A outbound remote delegation
  - treaty / federation / remote evidence / remote follow-up governance

These experimental surfaces are retained because they are strategically important, but they are not the default onboarding path and they are not the homepage promise.

## What Experimental Means Here

Experimental does **not** mean throwaway. It means:

- compiled and regression-tested
- retained in the repository
- still part of the long-range architecture
- not the recommended getting-started path
- not what `crawfish init` generates by default

The supported support center is still the local mainline swarm path.

## Remote Governance, Compressed

The remote line is intentionally reduced to four ideas:

- **treaty**: can this remote delegation happen at all
- **federation pack**: how should the control plane interpret remote states and remote results
- **evidence bundle**: what proof is required to admit the remote outcome
- **follow-up**: how the same action continues when the evidence is incomplete

That keeps the conceptual model visible without making the remote/federation stack the public onboarding center.
## Experimental Example

The current remote/protocol example lives under [`examples/experimental/remote-swarm/`](/Users/meirtz/Documents/Tencent/CrawFish/examples/experimental/remote-swarm).

It demonstrates:

- remote-aware `task.plan`
- OpenClaw outbound routing
- A2A outbound delegation
- treaty and federation metadata
- remote evidence and follow-up lineage

Use [`examples/hero-swarm/`](/Users/meirtz/Documents/Tencent/CrawFish/examples/hero-swarm) for the mainline local path.

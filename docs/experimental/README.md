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

## Experimental Example

The current remote/protocol example lives under [`examples/experimental/remote-swarm/`](/Users/meirtz/Documents/Tencent/CrawFish/examples/experimental/remote-swarm).

It demonstrates:

- remote-aware `task.plan`
- OpenClaw outbound routing
- A2A outbound delegation
- treaty and federation metadata
- remote evidence and follow-up lineage

Use [`examples/hero-swarm/`](/Users/meirtz/Documents/Tencent/CrawFish/examples/hero-swarm) for the mainline local path.

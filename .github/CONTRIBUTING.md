# Contributing to Crawfish

Thanks for contributing.

Crawfish is a public Rust-first alpha project. We want serious maintenance discipline now, even though the public surfaces are still unstable.

## Before You Open A PR

- Read [`../README.md`](/Users/meirtz/Documents/Tencent/CrawFish/README.md) and the specs under [`../docs/spec/`](/Users/meirtz/Documents/Tencent/CrawFish/docs/spec/).
- Prefer small, reviewable changes.
- Open an issue first for major behavior changes, public API changes, or architecture shifts.

## Local Checks

Run the required local gates before pushing:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Branch And Merge Policy

- Maintainer and automation branches use the `codex/*` prefix.
- External contributors may use any branch name, but PRs are squash-merged.
- Release tags are cut from `main` only.

## Documentation And Changelog Discipline

- If a change is user-visible, update [`../docs/project/CHANGELOG.md`](/Users/meirtz/Documents/Tencent/CrawFish/docs/project/CHANGELOG.md).
- If a change affects behavior in the runnable example, update these in the same PR:
  - [`../README.md`](/Users/meirtz/Documents/Tencent/CrawFish/README.md)
  - [`../docs/spec/v0.1-plan.md`](/Users/meirtz/Documents/Tencent/CrawFish/docs/spec/v0.1-plan.md)
  - [`../examples/hero-fleet/Crawfish.toml`](/Users/meirtz/Documents/Tencent/CrawFish/examples/hero-fleet/Crawfish.toml)
- If a breaking alpha change affects users, include a short migration note in the changelog and docs.

## Public Surface Expectations

The following surfaces are public but still unstable in alpha:

- `crawfish` CLI
- `Crawfish.toml` and agent manifest format
- local UDS HTTP API
- Rust workspace crates

Breaking changes are allowed during alpha, but they must be explicit and documented.

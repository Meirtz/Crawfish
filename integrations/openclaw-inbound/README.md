# OpenClaw Inbound Bridge

This package is the `P1a` OpenClaw inbound bridge for Crawfish.

It is intentionally thin:

- stateless Gateway RPC translation only
- no policy decisions
- no owner or trust-domain configuration
- no retries beyond a single daemon request

Supported RPC methods:

- `crawfish.action.submit`
- `crawfish.action.inspect`
- `crawfish.action.events`
- `crawfish.agent.status`

The bridge forwards every call to the local Crawfish daemon over the Unix domain socket exposed by `crawfishd`.

By default it uses:

- `CRAWFISH_SOCKET_PATH=.crawfish/run/crawfishd.sock`

Build and test it with:

```bash
npm test --prefix integrations/openclaw-inbound
```

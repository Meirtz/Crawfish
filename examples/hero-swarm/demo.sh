#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKDIR="${1:-$(mktemp -d)}"

cleanup() {
  if [[ -n "${CRAWFISH_PID:-}" ]]; then
    kill "${CRAWFISH_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

pushd "${REPO_ROOT}" >/dev/null
cargo run -p crawfish-cli --bin crawfish -- init "${WORKDIR}"
cp "${REPO_ROOT}/examples/hero-swarm/Crawfish.toml" "${WORKDIR}/Crawfish.toml"
cp "${REPO_ROOT}"/examples/hero-swarm/agents/*.toml "${WORKDIR}/agents/"
mkdir -p "${WORKDIR}/src" "${WORKDIR}/tests" "${WORKDIR}/incident"
cat > "${WORKDIR}/src/lib.rs" <<'EOF'
pub fn value() -> u32 { 42 } // TODO tighten checks
EOF
cat > "${WORKDIR}/tests/lib_test.rs" <<'EOF'
#[test]
fn smoke() {
    assert_eq!(crate::value(), 42);
}
EOF
cp "${REPO_ROOT}/examples/hero-swarm/data/sample-incident.log" "${WORKDIR}/incident/sample-incident.log"
cp "${REPO_ROOT}/examples/hero-swarm/data/service-manifest.toml" "${WORKDIR}/incident/service-manifest.toml"

cargo run -p crawfish-cli --bin crawfish -- run --config "${WORKDIR}/Crawfish.toml" &
CRAWFISH_PID=$!
sleep 1

echo "== Swarm status =="
cargo run -p crawfish-cli --bin crawfish -- status --config "${WORKDIR}/Crawfish.toml" --json

echo "== Submit review action =="
REVIEW_ID="$(cargo run -p crawfish-cli --bin crawfish -- action submit \
  --config "${WORKDIR}/Crawfish.toml" \
  --target-agent repo_reviewer \
  --capability repo.review \
  --goal "review pull request" \
  --caller-owner local-dev \
  --inputs-json "{\"workspace_root\":\"${WORKDIR}\",\"changed_files\":[\"src/lib.rs\"]}" \
  --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["action_id"])')"
cargo run -p crawfish-cli --bin crawfish -- inspect "${REVIEW_ID}" --config "${WORKDIR}/Crawfish.toml" --json
cargo run -p crawfish-cli --bin crawfish -- action events "${REVIEW_ID}" --config "${WORKDIR}/Crawfish.toml" --json

echo "== Submit incident enrichment action =="
INCIDENT_ID="$(cargo run -p crawfish-cli --bin crawfish -- action submit \
  --config "${WORKDIR}/Crawfish.toml" \
  --target-agent incident_enricher \
  --capability incident.enrich \
  --goal "enrich local incident" \
  --caller-owner local-dev \
  --inputs-json "{\"service_name\":\"api\",\"log_file\":\"${WORKDIR}/incident/sample-incident.log\",\"service_manifest_file\":\"${WORKDIR}/incident/service-manifest.toml\"}" \
  --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["action_id"])')"
cargo run -p crawfish-cli --bin crawfish -- inspect "${INCIDENT_ID}" --config "${WORKDIR}/Crawfish.toml" --json

if [[ -n "${OPENCLAW_GATEWAY_URL:-}" ]] && [[ -n "${OPENCLAW_GATEWAY_TOKEN:-}" ]]; then
  python3 - "${WORKDIR}/agents/task_planner.toml" "${OPENCLAW_GATEWAY_URL}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
gateway_url = sys.argv[2]
contents = path.read_text()
path.write_text(contents.replace("ws://127.0.0.1:9988/gateway", gateway_url))
PY

  echo "== Submit OpenClaw outbound planning action =="
  PLAN_ID="$(cargo run -p crawfish-cli --bin crawfish -- action submit \
    --config "${WORKDIR}/Crawfish.toml" \
    --target-agent task_planner \
    --capability task.plan \
    --goal "plan a safe task" \
    --caller-owner local-dev \
    --inputs-json "{\"workspace_root\":\"${WORKDIR}\",\"objective\":\"Add validation checks around the repo indexing path\",\"files_of_interest\":[\"src/lib.rs\"],\"desired_outputs\":[\"rollout checklist\"]}" \
    --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["action_id"])')"
  cargo run -p crawfish-cli --bin crawfish -- inspect "${PLAN_ID}" --config "${WORKDIR}/Crawfish.toml" --json
  cargo run -p crawfish-cli --bin crawfish -- action events "${PLAN_ID}" --config "${WORKDIR}/Crawfish.toml" --json
else
  echo "== Skip OpenClaw outbound demo (set OPENCLAW_GATEWAY_URL and OPENCLAW_GATEWAY_TOKEN to enable) =="
fi

echo "== Submit approval-gated mutation action =="
MUTATION_ID="$(cargo run -p crawfish-cli --bin crawfish -- action submit \
  --config "${WORKDIR}/Crawfish.toml" \
  --target-agent workspace_editor \
  --capability workspace.patch.apply \
  --goal "apply local patch" \
  --caller-owner local-dev \
  --workspace-write \
  --mutating \
  --inputs-json "{\"workspace_root\":\"${WORKDIR}\",\"edits\":[{\"path\":\"notes.txt\",\"op\":\"create\",\"contents\":\"hello from crawfish\\n\"}]}" \
  --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["action_id"])')"
cargo run -p crawfish-cli --bin crawfish -- action list --config "${WORKDIR}/Crawfish.toml" --phase awaiting_approval --json
cargo run -p crawfish-cli --bin crawfish -- action approve "${MUTATION_ID}" --config "${WORKDIR}/Crawfish.toml" --approver local-dev --json
cargo run -p crawfish-cli --bin crawfish -- inspect "${MUTATION_ID}" --config "${WORKDIR}/Crawfish.toml" --json
cargo run -p crawfish-cli --bin crawfish -- action events "${MUTATION_ID}" --config "${WORKDIR}/Crawfish.toml" --json

popd >/dev/null

echo "Demo workspace: ${WORKDIR}"

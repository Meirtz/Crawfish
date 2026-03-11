use super::*;

pub(super) async fn build_supervisor_with_config(
    dir: &Path,
    config_contents: String,
) -> anyhow::Result<Arc<Supervisor>> {
    build_supervisor_with_config_and_openclaw_gateway(dir, config_contents, None).await
}

pub(super) async fn build_supervisor_with_config_and_openclaw_gateway(
    dir: &Path,
    config_contents: String,
    openclaw_gateway_url: Option<&str>,
) -> anyhow::Result<Arc<Supervisor>> {
    tokio::fs::create_dir_all(dir.join("agents")).await?;
    tokio::fs::create_dir_all(dir.join(".crawfish/state")).await?;
    tokio::fs::create_dir_all(dir.join(".crawfish/run")).await?;
    tokio::fs::create_dir_all(dir.join("src")).await?;
    tokio::fs::create_dir_all(dir.join("tests")).await?;
    tokio::fs::write(dir.join("Crawfish.toml"), config_contents).await?;
    tokio::fs::write(
        dir.join("src/lib.rs"),
        "pub fn value() -> u32 { 42 } // TODO follow up\n",
    )
    .await?;
    tokio::fs::write(dir.join("tests/lib_test.rs"), "#[test] fn smoke() {}\n").await?;
    for agent in [
        "repo_indexer",
        "repo_reviewer",
        "ci_triage",
        "incident_enricher",
        "task_planner",
        "workspace_editor",
    ] {
        let mut manifest = std::fs::read_to_string(format!(
            "{}/../../examples/hero-swarm/agents/{agent}.toml",
            env!("CARGO_MANIFEST_DIR")
        ))?;
        if agent == "task_planner" {
            manifest = manifest.replace(
                "command = \"claude\"",
                "command = \"__test_missing_claude__\"",
            );
            manifest = manifest.replace(
                "command = \"codex\"",
                "command = \"__test_missing_codex__\"",
            );
            if let Some(gateway_url) = openclaw_gateway_url {
                manifest = manifest.replace("ws://127.0.0.1:9988/gateway", gateway_url);
            }
        }
        tokio::fs::write(dir.join(format!("agents/{agent}.toml")), manifest).await?;
    }
    let supervisor = Arc::new(Supervisor::from_config_path(&dir.join("Crawfish.toml")).await?);
    supervisor.run_once().await?;
    Ok(supervisor)
}

pub(super) async fn build_supervisor_with_task_planner_manifest(
    dir: &Path,
    task_planner_manifest: String,
    openclaw_gateway_url: Option<&str>,
) -> anyhow::Result<Arc<Supervisor>> {
    let config =
        include_str!("../../../../examples/experimental/remote-swarm/Crawfish.toml").to_string();
    build_supervisor_with_task_planner_manifest_and_config(
        dir,
        task_planner_manifest,
        config,
        openclaw_gateway_url,
    )
    .await
}

pub(super) async fn build_supervisor_with_task_planner_manifest_and_config(
    dir: &Path,
    task_planner_manifest: String,
    config_contents: String,
    openclaw_gateway_url: Option<&str>,
) -> anyhow::Result<Arc<Supervisor>> {
    tokio::fs::create_dir_all(dir.join("agents")).await?;
    tokio::fs::create_dir_all(dir.join(".crawfish/state")).await?;
    tokio::fs::create_dir_all(dir.join(".crawfish/run")).await?;
    tokio::fs::create_dir_all(dir.join("src")).await?;
    tokio::fs::create_dir_all(dir.join("tests")).await?;
    tokio::fs::write(dir.join("Crawfish.toml"), config_contents).await?;
    tokio::fs::write(
        dir.join("src/lib.rs"),
        "pub fn value() -> u32 { 42 } // TODO follow up\n",
    )
    .await?;
    tokio::fs::write(dir.join("tests/lib_test.rs"), "#[test] fn smoke() {}\n").await?;
    for agent in [
        "repo_indexer",
        "repo_reviewer",
        "ci_triage",
        "incident_enricher",
        "workspace_editor",
    ] {
        let manifest = std::fs::read_to_string(format!(
            "{}/../../examples/hero-swarm/agents/{agent}.toml",
            env!("CARGO_MANIFEST_DIR")
        ))?;
        tokio::fs::write(dir.join(format!("agents/{agent}.toml")), manifest).await?;
    }
    let manifest = if let Some(gateway_url) = openclaw_gateway_url {
        task_planner_manifest.replace("ws://127.0.0.1:9988/gateway", gateway_url)
    } else {
        task_planner_manifest
    };
    tokio::fs::write(dir.join("agents/task_planner.toml"), manifest).await?;
    let supervisor = Arc::new(Supervisor::from_config_path(&dir.join("Crawfish.toml")).await?);
    supervisor.run_once().await?;
    Ok(supervisor)
}

pub(super) async fn build_supervisor(dir: &Path) -> anyhow::Result<Arc<Supervisor>> {
    build_supervisor_with_config(
        dir,
        include_str!("../../../../examples/hero-swarm/Crawfish.toml").to_string(),
    )
    .await
}

pub(super) async fn build_supervisor_with_mcp(
    dir: &Path,
    mcp_url: &str,
) -> anyhow::Result<Arc<Supervisor>> {
    let config = include_str!("../../../../examples/hero-swarm/Crawfish.toml")
        .replace("http://127.0.0.1:8877/sse", mcp_url);
    build_supervisor_with_config(dir, config).await
}

pub(super) async fn build_supervisor_with_openclaw(dir: &Path) -> anyhow::Result<Arc<Supervisor>> {
    let manifest = local_task_planner_manifest(
        "__test_missing_claude__",
        "__test_missing_codex__",
        "ws://127.0.0.1:9988/gateway",
    );
    build_supervisor_with_task_planner_manifest_and_config(
        dir,
        manifest,
        include_str!("../../../../examples/experimental/remote-swarm/Crawfish.toml").to_string(),
        None,
    )
    .await
}

pub(super) async fn build_supervisor_with_openclaw_gateway(
    dir: &Path,
    gateway_url: &str,
) -> anyhow::Result<Arc<Supervisor>> {
    let manifest = local_task_planner_manifest(
        "__test_missing_claude__",
        "__test_missing_codex__",
        gateway_url,
    );
    build_supervisor_with_task_planner_manifest_and_config(
        dir,
        manifest,
        include_str!("../../../../examples/experimental/remote-swarm/Crawfish.toml").to_string(),
        Some(gateway_url),
    )
    .await
}

pub(super) fn local_owner(id: &str) -> crawfish_types::OwnerRef {
    crawfish_types::OwnerRef {
        kind: crawfish_types::OwnerKind::Human,
        id: id.to_string(),
        display_name: None,
    }
}

pub(super) fn workspace_patch_request(
    dir: &Path,
    edits: Value,
    deadline_ms: Option<u64>,
) -> SubmitActionRequest {
    SubmitActionRequest {
        target_agent_id: "workspace_editor".to_string(),
        requester: RequesterRef {
            kind: RequesterKind::User,
            id: "operator".to_string(),
        },
        initiator_owner: local_owner("local-dev"),
        capability: "workspace.patch.apply".to_string(),
        goal: crawfish_types::GoalSpec {
            summary: "apply local patch".to_string(),
            details: None,
        },
        inputs: std::collections::BTreeMap::from([
            (
                "workspace_root".to_string(),
                serde_json::json!(dir.display().to_string()),
            ),
            ("edits".to_string(), edits),
        ]),
        contract_overrides: deadline_ms.map(|deadline_ms| ExecutionContractPatch {
            delivery: crawfish_core::DeliveryContractPatch {
                deadline_ms: Some(deadline_ms),
                freshness_ttl_ms: None,
                required_ack: None,
                liveliness_window_ms: None,
            },
            ..ExecutionContractPatch::default()
        }),
        execution_strategy: None,
        schedule: None,
        counterparty_refs: Vec::new(),
        data_boundary: None,
        workspace_write: true,
        secret_access: false,
        mutating: true,
    }
}

pub(super) fn openclaw_caller(caller_id: &str) -> OpenClawCallerContext {
    OpenClawCallerContext {
        caller_id: caller_id.to_string(),
        session_id: format!("{caller_id}-session"),
        channel_id: "gateway".to_string(),
        workspace_root: None,
        scopes: vec!["crawfish.read".to_string(), "crawfish.submit".to_string()],
        display_name: None,
        trace_ids: crawfish_types::Metadata::default(),
    }
}

pub(super) fn task_plan_request(dir: &Path, objective: &str) -> SubmitActionRequest {
    SubmitActionRequest {
        target_agent_id: "task_planner".to_string(),
        requester: RequesterRef {
            kind: RequesterKind::User,
            id: "operator".to_string(),
        },
        initiator_owner: local_owner("local-dev"),
        capability: "task.plan".to_string(),
        goal: crawfish_types::GoalSpec {
            summary: objective.to_string(),
            details: None,
        },
        inputs: std::collections::BTreeMap::from([
            ("objective".to_string(), serde_json::json!(objective)),
            (
                "workspace_root".to_string(),
                serde_json::json!(dir.display().to_string()),
            ),
            (
                "desired_outputs".to_string(),
                serde_json::json!(["plan", "risks"]),
            ),
        ]),
        contract_overrides: None,
        execution_strategy: None,
        schedule: None,
        counterparty_refs: Vec::new(),
        data_boundary: None,
        workspace_write: false,
        secret_access: false,
        mutating: false,
    }
}

pub(super) async fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    tokio::fs::write(&path, body).await.unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

pub(super) fn local_task_planner_manifest(
    claude_command: &str,
    codex_command: &str,
    openclaw_gateway_url: &str,
) -> String {
    format!(
        r#"id = "task_planner"
role = "task_planner"
trust_domain = "same_owner_local"
capabilities = ["task.plan"]
exposed_capabilities = ["task.plan"]
default_data_boundaries = ["owner_local"]

[owner]
kind = "human"
id = "local-dev"
display_name = "Local Developer"

[contract_defaults.execution]
preferred_harnesses = ["claude_code", "codex", "a2a", "openclaw"]
fallback_chain = ["deterministic"]

[contract_defaults.safety]
approval_policy = "on_mutation"
mutation_mode = "proposal_only"

[strategy_defaults."task.plan"]
mode = "verify_loop"
feedback_policy = "inject_reason"

[strategy_defaults."task.plan".verification_spec]
require_all = true
on_failure = "retry_with_feedback"
checks = []

[strategy_defaults."task.plan".stop_budget]
max_iterations = 3

[[adapters]]
adapter = "local_harness"
capability = "task.plan"
harness = "claude_code"
command = "{claude_command}"
args = []
required_scopes = ["planning:read", "planning:propose"]
lease_required = false
workspace_policy = "crawfish_managed"
env_allowlist = ["PATH", "HOME", "CODEX_HOME", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"]
timeout_seconds = 5

[[adapters]]
adapter = "local_harness"
capability = "task.plan"
harness = "codex"
command = "{codex_command}"
args = ["exec", "--skip-git-repo-check"]
required_scopes = ["planning:read", "planning:propose"]
lease_required = false
workspace_policy = "crawfish_managed"
env_allowlist = ["PATH", "HOME", "CODEX_HOME", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"]
timeout_seconds = 5

[[adapters]]
adapter = "openclaw"
gateway_url = "{openclaw_gateway_url}"
auth_ref = "OPENCLAW_GATEWAY_TOKEN"
target_agent = "task-planner"
session_mode = "ephemeral"
caller_owner_mapping = "required"
default_trust_domain = "same_device_foreign_owner"
required_scopes = ["planning:read", "planning:propose"]
lease_required = false
workspace_policy = "crawfish_managed"

[[adapters]]
adapter = "a2a"
capability = "task.plan"
agent_card_url = "http://127.0.0.1:7788/agent-card.json"
auth_ref = "A2A_REMOTE_TOKEN"
treaty_pack = "remote_task_planning"
required_scopes = ["planning:read", "planning:propose"]
streaming_mode = "prefer_streaming"
allow_in_task_auth = false
"#
    )
}

pub(super) async fn spawn_api_server(
    supervisor: Arc<Supervisor>,
) -> (tokio::task::JoinHandle<()>, PathBuf) {
    let socket_path = supervisor.config().socket_path(supervisor.root());
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await.unwrap();
    }
    let listener = UnixListener::bind(&socket_path).unwrap();
    let app = api_router(Arc::clone(&supervisor));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (handle, socket_path)
}

pub(super) async fn post_uds_json<T: serde::Serialize>(
    socket_path: &Path,
    endpoint: &str,
    payload: &T,
) -> (StatusCode, Value) {
    let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
    let uri: Uri = hyperlocal::Uri::new(socket_path, endpoint).into();
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(payload).unwrap())))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json)
}

pub(super) async fn get_uds_json(socket_path: &Path, endpoint: &str) -> (StatusCode, Value) {
    let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
    let uri: Uri = hyperlocal::Uri::new(socket_path, endpoint).into();
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json)
}

#[derive(Clone)]
struct RuntimeMcpState {
    sessions: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<String>>>>,
    next_session: Arc<AtomicUsize>,
    log_text: String,
}

#[derive(serde::Deserialize)]
struct SessionQuery {
    session: String,
}

pub(super) async fn spawn_runtime_mcp_server(log_text: &str) -> String {
    let state = RuntimeMcpState {
        sessions: Arc::new(Mutex::new(HashMap::new())),
        next_session: Arc::new(AtomicUsize::new(1)),
        log_text: log_text.to_string(),
    };

    let app = Router::new()
        .route("/sse", get(runtime_mock_sse))
        .route("/messages", post(runtime_mock_messages))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{address}/sse")
}

async fn runtime_mock_sse(
    AxumState(state): AxumState<RuntimeMcpState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let session_id = format!(
        "session-{}",
        state.next_session.fetch_add(1, Ordering::SeqCst)
    );
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    state.sessions.lock().await.insert(session_id.clone(), tx);

    let initial = stream::once(async move {
        Ok(Event::default()
            .event("endpoint")
            .data(format!("/messages?session={session_id}")))
    });
    let rest = stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|payload| (Ok(Event::default().event("message").data(payload)), rx))
    });
    Sse::new(initial.chain(rest))
}

async fn runtime_mock_messages(
    AxumState(state): AxumState<RuntimeMcpState>,
    Query(query): Query<SessionQuery>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let sender = state.sessions.lock().await.get(&query.session).cloned();
    if let Some(sender) = sender {
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let log_text = state.log_text.clone();
        tokio::spawn(async move {
            let response = match method.as_str() {
                "initialize" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"protocolVersion": "2024-11-05"}
                }),
                "tools/list" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"tools": [{"name": "ci_runs_inspect"}]}
                }),
                "tools/call" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [{"type": "text", "text": "remote CI logs fetched"}],
                        "structuredContent": {
                            "provider": "github_actions",
                            "log_text": log_text
                        }
                    }
                }),
                _ => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"message": "unknown method"}
                }),
            };
            let _ = sender.send(response.to_string());
        });
    }

    Json(serde_json::json!({"accepted": true}))
}

pub(super) async fn spawn_mock_openclaw_gateway() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let run_counter = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let run_counter = Arc::clone(&run_counter);
            tokio::spawn(async move {
                let ws = accept_hdr_async(stream, |_request: &WsRequest, response: WsResponse| {
                    Ok(response)
                })
                .await
                .unwrap();
                let (mut sink, mut source) = ws.split();
                let mut active_run_id: Option<String> = None;
                let mut last_prompt = String::new();
                while let Some(message) = source.next().await {
                    let WsMessage::Text(text) = message.unwrap() else {
                        continue;
                    };
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    let method = frame.get("method").and_then(Value::as_str).unwrap();
                    let id = frame.get("id").and_then(Value::as_str).unwrap();
                    match method {
                        "connect" => {
                            sink.send(WsMessage::Text(
                                serde_json::json!({
                                    "type":"res",
                                    "id": id,
                                    "ok": true,
                                    "result": {"sessionKey":"gateway-session"}
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        "agent" => {
                            let attempt = run_counter.fetch_add(1, Ordering::SeqCst) + 1;
                            let run_id = format!("run-{attempt}");
                            last_prompt = frame
                                .pointer("/params/message")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            active_run_id = Some(run_id.clone());
                            sink.send(WsMessage::Text(
                                serde_json::json!({
                                    "type":"event",
                                    "event":"assistant",
                                    "runId":run_id,
                                    "payload":{
                                        "stream":"assistant",
                                        "text": format!("OpenClaw planning attempt {attempt}")
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                            sink.send(WsMessage::Text(
                                serde_json::json!({
                                    "type":"res",
                                    "id": id,
                                    "ok": true,
                                    "result": {"runId": active_run_id}
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        }
                        "agent.wait" => {
                            let run_id = active_run_id
                                .clone()
                                .expect("agent.wait should follow agent");
                            let has_feedback =
                                last_prompt.contains("Verification feedback to address:");
                            sink.send(WsMessage::Text(
                                serde_json::json!({
                                    "type":"event",
                                    "event":"tool",
                                    "runId":run_id,
                                    "payload":{
                                        "stream":"tool",
                                        "message":"read target files and shape proposal"
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                            let result = if has_feedback {
                                serde_json::json!({
                                    "status":"completed",
                                    "confidence":"High confidence once the checklist and rollout notes are included.",
                                    "text":"# Task Plan\n1. Inspect `src/lib.rs` and the repo indexing boundary.\n2. Add validation checks around the indexing path and capture a rollout checklist.\n3. Update tests and document the rollout checklist.\nRisks: config drift can hide indexing regressions.\nAssumptions: the rollout checklist can stay proposal-only for this task.\nTest: cargo test --workspace\nChecklist: include rollout checklist in the final proposal."
                                })
                            } else {
                                serde_json::json!({
                                    "status":"completed",
                                    "text":"# Task Plan\n1. Inspect `src/lib.rs`.\n2. Add validation checks around the repo indexing path.\nRisks: config drift.\nTest: cargo test --workspace"
                                })
                            };
                            sink.send(WsMessage::Text(
                                serde_json::json!({
                                    "type":"res",
                                    "id": id,
                                    "ok": true,
                                    "result": result
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                            break;
                        }
                        other => panic!("unexpected gateway method: {other}"),
                    }
                }
            });
        }
    });
    format!("ws://{address}")
}

#[derive(Clone, Copy)]
pub(super) enum RuntimeA2aMode {
    StreamingCompleted,
    InputRequired,
    AuthRequired,
    StreamingMissingTaskRef,
}

#[derive(Clone)]
struct RuntimeA2aState {
    mode: RuntimeA2aMode,
}

pub(super) async fn spawn_runtime_a2a_server(mode: RuntimeA2aMode) -> String {
    let app = Router::new()
        .route("/agent-card.json", get(runtime_a2a_agent_card))
        .route("/rpc", post(runtime_a2a_rpc))
        .with_state(RuntimeA2aState { mode });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{address}/agent-card.json")
}

async fn runtime_a2a_agent_card(
    AxumState(_state): AxumState<RuntimeA2aState>,
    request: Request<axum::body::Body>,
) -> Json<Value> {
    let host = request
        .headers()
        .get("host")
        .and_then(|header| header.to_str().ok())
        .unwrap_or("127.0.0.1:0");
    Json(serde_json::json!({
        "id": "remote-task-planner",
        "name": "remote-task-planner",
        "url": format!("http://{host}/rpc"),
        "capabilities": ["task.plan"],
        "skills": [{"id": "task.plan", "name": "task.plan", "tags": ["task.plan"]}]
    }))
}

async fn runtime_a2a_rpc(
    AxumState(state): AxumState<RuntimeA2aState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let id = payload.get("id").cloned().unwrap_or(Value::Null);
    let response = match (state.mode, method) {
        (RuntimeA2aMode::StreamingCompleted, "message/stream") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "task": {
                    "id": "remote-task-1",
                    "status": { "state": "completed" },
                    "result": {
                        "text": "# Task Plan\n1. Inspect the objective and the context files.\n2. Produce a proposal-only plan with ordered steps and rollout notes.\nRisks: remote coordination can drift from local assumptions.\nAssumptions: the plan remains proposal-only.\nTest: verify the proposal covers desired outputs."
                    }
                },
                "events": [
                    { "kind": "lifecycle", "state": "working" },
                    { "kind": "assistant", "text": "Remote planner is preparing a task plan." }
                ]
            }
        }),
        (RuntimeA2aMode::StreamingMissingTaskRef, "message/stream") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "task": {
                    "id": "",
                    "status": { "state": "completed" },
                    "result": {
                        "text": "# Task Plan\n1. Inspect the objective and the context files.\n2. Produce a proposal-only plan with ordered steps and rollout notes.\nRisks: remote coordination can drift from local assumptions.\nAssumptions: the plan remains proposal-only.\nTest: verify the proposal covers desired outputs."
                    }
                },
                "events": [
                    { "kind": "lifecycle", "state": "working" },
                    { "kind": "assistant", "text": "Remote planner completed without a durable task id." }
                ]
            }
        }),
        (RuntimeA2aMode::InputRequired | RuntimeA2aMode::AuthRequired, "message/stream") => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "message": "stream unsupported" }
            })
        }
        (_, "message/send") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "task": {
                    "id": "remote-task-1",
                    "status": { "state": "submitted" }
                }
            }
        }),
        (RuntimeA2aMode::InputRequired, "tasks/get") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "task": {
                    "id": "remote-task-1",
                    "status": {
                        "state": "input-required",
                        "message": "Remote planner needs more input."
                    }
                }
            }
        }),
        (RuntimeA2aMode::AuthRequired, "tasks/get") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "task": {
                    "id": "remote-task-1",
                    "status": {
                        "state": "auth-required",
                        "message": "Remote planner requires authorization."
                    }
                }
            }
        }),
        (_, "tasks/get") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "task": {
                    "id": "remote-task-1",
                    "status": {
                        "state": "completed"
                    },
                    "result": {
                        "text": "# Task Plan\n1. Inspect the objective and the context files.\n2. Produce a proposal-only plan with ordered steps and rollout notes.\nRisks: remote coordination can drift from local assumptions.\nAssumptions: the plan remains proposal-only.\nTest: verify the proposal covers desired outputs."
                    }
                }
            }
        }),
        (_, other) => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "message": format!("unexpected method: {other}") }
        }),
    };
    Json(response)
}

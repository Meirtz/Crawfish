use super::support::*;
use super::*;

#[tokio::test]
async fn submit_action_completes_deterministically() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "review pull request".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "changed_files".to_string(),
                    serde_json::json!(["src/lib.rs"]),
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
        })
        .await
        .unwrap();

    supervisor.process_action_queue_once().await.unwrap();
    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert!(detail
        .artifact_refs
        .iter()
        .any(|artifact| artifact.kind == "repo_index"));
    assert!(detail
        .artifact_refs
        .iter()
        .any(|artifact| artifact.kind == "review_summary"));
    assert!(detail.action.checkpoint_ref.is_some());
}

#[tokio::test]
async fn submit_action_rejects_invalid_inputs() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let error = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "review pull request".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([(
                "workspace_root".to_string(),
                serde_json::json!(dir.path().display().to_string()),
            )]),
            contract_overrides: None,
            execution_strategy: None,
            schedule: None,
            counterparty_refs: Vec::new(),
            data_boundary: None,
            workspace_write: false,
            secret_access: false,
            mutating: false,
        })
        .await
        .expect_err("request should be rejected");

    assert!(error
        .to_string()
        .starts_with("invalid action request: repo.review requires"));
}

#[tokio::test]
async fn health_endpoint_works_over_uds() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();
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

    let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
    let uri: Uri = hyperlocal::Uri::new(&socket_path, "/v1/health").into();
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: HealthResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.status, "ok");

    handle.abort();
}

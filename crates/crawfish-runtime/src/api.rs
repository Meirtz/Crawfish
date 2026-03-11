use super::*;

pub(crate) fn api_router(supervisor: Arc<Supervisor>) -> Router {
    Router::new()
        .route("/v1/health", get(health_handler))
        .route("/v1/agents", get(list_agents_handler))
        .route("/v1/agents/{id}", get(agent_detail_handler))
        .route(
            "/v1/actions",
            get(list_actions_handler).post(submit_action_handler),
        )
        .route("/v1/actions/{id}", get(action_detail_handler))
        .route("/v1/actions/{id}/events", get(action_events_handler))
        .route(
            "/v1/actions/{id}/remote-evidence",
            get(action_remote_evidence_handler),
        )
        .route(
            "/v1/actions/{id}/remote-followups",
            get(action_remote_followups_handler),
        )
        .route(
            "/v1/actions/{id}/remote-followups/{followup_id}/dispatch",
            post(action_remote_followup_dispatch_handler),
        )
        .route("/v1/actions/{id}/trace", get(action_trace_handler))
        .route(
            "/v1/actions/{id}/evaluations",
            get(action_evaluations_handler),
        )
        .route("/v1/actions/{id}/approve", post(approve_action_handler))
        .route("/v1/actions/{id}/reject", post(reject_action_handler))
        .route("/v1/leases/{id}/revoke", post(revoke_lease_handler))
        .route("/v1/review-queue", get(review_queue_handler))
        .route(
            "/v1/review-queue/{id}/resolve",
            post(resolve_review_queue_item_handler),
        )
        .route("/v1/evaluation/datasets", get(evaluation_datasets_handler))
        .route(
            "/v1/evaluation/datasets/{name}",
            get(evaluation_dataset_detail_handler),
        )
        .route("/v1/evaluation/runs", post(start_evaluation_run_handler))
        .route(
            "/v1/evaluation/runs/{id}",
            get(evaluation_run_detail_handler),
        )
        .route(
            "/v1/evaluation/compare",
            post(start_pairwise_evaluation_run_handler),
        )
        .route(
            "/v1/evaluation/compare/{id}",
            get(pairwise_evaluation_run_detail_handler),
        )
        .route("/v1/alerts", get(alert_list_handler))
        .route("/v1/alerts/{id}/ack", post(alert_ack_handler))
        .route("/v1/treaties", get(treaty_list_handler))
        .route("/v1/treaties/{id}", get(treaty_detail_handler))
        .route("/v1/federation/packs", get(federation_list_handler))
        .route("/v1/federation/packs/{id}", get(federation_detail_handler))
        .route(
            "/v1/inbound/openclaw/actions",
            post(openclaw_submit_action_handler),
        )
        .route(
            "/v1/inbound/openclaw/actions/{id}/inspect",
            post(openclaw_action_detail_handler),
        )
        .route(
            "/v1/inbound/openclaw/actions/{id}/events",
            post(openclaw_action_events_handler),
        )
        .route(
            "/v1/inbound/openclaw/agents/{id}/status",
            post(openclaw_agent_status_handler),
        )
        .route("/v1/admin/drain", post(drain_handler))
        .route("/v1/admin/resume", post(resume_handler))
        .route("/v1/policy/validate", post(policy_validate_handler))
        .with_state(supervisor)
}

pub(crate) async fn health_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<HealthResponse>, RuntimeError> {
    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        socket_path: supervisor
            .config()
            .socket_path(supervisor.root())
            .display()
            .to_string(),
    }))
}

pub(crate) async fn list_agents_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<SwarmStatusResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_status()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn agent_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<AgentDetail>, RuntimeError> {
    supervisor
        .inspect_agent(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("agent not found: {id}")))
}

pub(crate) async fn action_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionDetail>, RuntimeError> {
    supervisor
        .inspect_action(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

pub(crate) async fn action_events_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionEventsResponse>, RuntimeError> {
    if supervisor
        .store()
        .get_action(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .is_none()
    {
        return Err(RuntimeError::NotFound(format!("action not found: {id}")));
    }

    Ok(Json(
        supervisor
            .list_action_events(&id)
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn action_remote_evidence_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionRemoteEvidenceResponse>, RuntimeError> {
    supervisor
        .get_action_remote_evidence(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

pub(crate) async fn action_remote_followups_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionRemoteFollowupsResponse>, RuntimeError> {
    supervisor
        .get_action_remote_followups(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

pub(crate) async fn action_remote_followup_dispatch_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath((id, followup_id)): AxumPath<(String, String)>,
    Json(request): Json<DispatchRemoteFollowupRequest>,
) -> Result<Json<DispatchRemoteFollowupResponse>, RuntimeError> {
    match supervisor
        .dispatch_remote_followup(&id, &followup_id, request)
        .await
    {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.contains("not found") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn action_trace_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionTraceResponse>, RuntimeError> {
    supervisor
        .get_action_trace(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

pub(crate) async fn action_evaluations_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionEvaluationsResponse>, RuntimeError> {
    if supervisor
        .store()
        .get_action(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .is_none()
    {
        return Err(RuntimeError::NotFound(format!("action not found: {id}")));
    }

    Ok(Json(
        supervisor
            .list_action_evaluations(&id)
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn list_actions_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Query(query): Query<ActionListQuery>,
) -> Result<Json<ActionListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_actions(query.phase.as_deref())
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn submit_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<SubmitActionRequest>,
) -> Result<Json<SubmittedAction>, RuntimeError> {
    match supervisor.submit_action(request).await {
        Ok(submitted) => Ok(Json(submitted)),
        Err(error) => Err(map_submit_error(error)),
    }
}

pub(crate) async fn openclaw_submit_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<OpenClawInboundActionRequest>,
) -> Result<Json<OpenClawInboundActionResponse>, RuntimeError> {
    supervisor.submit_openclaw_action(request).await.map(Json)
}

pub(crate) async fn openclaw_action_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(context): Json<OpenClawInspectionContext>,
) -> Result<Json<ActionDetail>, RuntimeError> {
    supervisor
        .inspect_openclaw_action(&id, context)
        .await
        .map(Json)
}

pub(crate) async fn openclaw_action_events_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(context): Json<OpenClawInspectionContext>,
) -> Result<Json<ActionEventsResponse>, RuntimeError> {
    supervisor
        .list_openclaw_action_events(&id, context)
        .await
        .map(Json)
}

pub(crate) async fn openclaw_agent_status_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(context): Json<OpenClawInspectionContext>,
) -> Result<Json<OpenClawAgentStatusResponse>, RuntimeError> {
    supervisor
        .inspect_openclaw_agent_status(&id, context)
        .await
        .map(Json)
}

pub(crate) async fn approve_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ApproveActionRequest>,
) -> Result<Json<SubmittedAction>, RuntimeError> {
    match supervisor.approve_action(&id, request).await {
        Ok(submitted) => Ok(Json(submitted)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("action not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn reject_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<RejectActionRequest>,
) -> Result<Json<SubmittedAction>, RuntimeError> {
    match supervisor.reject_action(&id, request).await {
        Ok(submitted) => Ok(Json(submitted)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("action not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn revoke_lease_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<RevokeLeaseRequest>,
) -> Result<Json<AdminActionResponse>, RuntimeError> {
    match supervisor.revoke_lease(&id, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("capability lease not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn review_queue_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Query(query): Query<ReviewQueueQuery>,
) -> Result<Json<ReviewQueueResponse>, RuntimeError> {
    let mut response = supervisor
        .list_review_queue()
        .await
        .map_err(RuntimeError::Internal)?;
    if let Some(kind) = query.kind.as_deref() {
        response.items.retain(|item| match kind {
            "action" | "action_eval" => item.kind == ReviewQueueKind::ActionEval,
            "pairwise" | "pairwise_eval" => item.kind == ReviewQueueKind::PairwiseEval,
            "remote" | "remote_result_review" => item.kind == ReviewQueueKind::RemoteResultReview,
            _ => true,
        });
    }
    Ok(Json(response))
}

pub(crate) async fn resolve_review_queue_item_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ResolveReviewQueueItemRequest>,
) -> Result<Json<ResolveReviewQueueItemResponse>, RuntimeError> {
    match supervisor.resolve_review_queue_item(&id, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("review queue item not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn evaluation_datasets_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<EvaluationDatasetsResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_evaluation_datasets()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn evaluation_dataset_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<EvaluationDatasetDetailResponse>, RuntimeError> {
    supervisor
        .get_evaluation_dataset(&name)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("dataset not found: {name}")))
}

pub(crate) async fn start_evaluation_run_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<StartEvaluationRunRequest>,
) -> Result<Json<StartEvaluationRunResponse>, RuntimeError> {
    match supervisor.start_evaluation_run(request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("dataset not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn evaluation_run_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ExperimentRunDetailResponse>, RuntimeError> {
    supervisor
        .get_evaluation_run(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("evaluation run not found: {id}")))
}

pub(crate) async fn start_pairwise_evaluation_run_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<StartPairwiseEvaluationRunRequest>,
) -> Result<Json<StartPairwiseEvaluationRunResponse>, RuntimeError> {
    match supervisor.start_pairwise_evaluation_run(request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("dataset not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn pairwise_evaluation_run_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PairwiseExperimentRunDetailResponse>, RuntimeError> {
    supervisor
        .get_pairwise_evaluation_run(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("pairwise evaluation run not found: {id}")))
}

pub(crate) async fn alert_list_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<AlertListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_alerts()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn alert_ack_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<AcknowledgeAlertRequest>,
) -> Result<Json<AcknowledgeAlertResponse>, RuntimeError> {
    match supervisor.acknowledge_alert(&id, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("alert not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

pub(crate) async fn treaty_list_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<TreatyListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_treaties()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn treaty_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<TreatyDetailResponse>, RuntimeError> {
    supervisor
        .get_treaty(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("treaty not found: {id}")))
}

pub(crate) async fn federation_list_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<FederationPackListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_federation_packs()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) async fn federation_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<FederationPackDetailResponse>, RuntimeError> {
    supervisor
        .get_federation_pack(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("federation pack not found: {id}")))
}

pub(crate) async fn drain_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<AdminActionResponse>, RuntimeError> {
    supervisor.drain().await.map_err(RuntimeError::Internal)?;
    Ok(Json(AdminActionResponse {
        status: "draining".to_string(),
    }))
}

pub(crate) async fn resume_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<AdminActionResponse>, RuntimeError> {
    supervisor.resume().await.map_err(RuntimeError::Internal)?;
    Ok(Json(AdminActionResponse {
        status: "active".to_string(),
    }))
}

pub(crate) async fn policy_validate_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<PolicyValidationRequest>,
) -> Result<Json<PolicyValidationResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .validate_policy_request(request)
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

pub(crate) fn error_body(message: String) -> serde_json::Value {
    serde_json::json!({ "error": message })
}

pub(crate) fn map_submit_error(error: anyhow::Error) -> RuntimeError {
    let message = error.to_string();
    if message.starts_with("agent not found:") {
        RuntimeError::NotFound(message)
    } else if message.starts_with("invalid action request:") {
        RuntimeError::BadRequest(message)
    } else if message.contains("denied") || message.contains("consent") {
        RuntimeError::Forbidden(message)
    } else {
        RuntimeError::Internal(error)
    }
}

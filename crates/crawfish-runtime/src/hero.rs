use anyhow::{anyhow, Context};
use async_trait::async_trait;
use crawfish_core::DeterministicExecutor;
use crawfish_types::{
    Action, ActionOutputs, ArtifactRef, CiFailureFamily, CiTriageArtifact,
    IncidentEnrichmentArtifact, RepoIndexArtifact, ReviewFinding, ReviewFindingsArtifact,
    ReviewRiskLevel, TaskPlanArtifact, TaskPlanStep, WorkspaceApplyResult, WorkspaceEdit,
    WorkspaceEditOp, WorkspaceRejectedEdit,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use walkdir::WalkDir;

pub struct RepoIndexerDeterministicExecutor {
    state_dir: PathBuf,
}

pub struct RepoReviewerDeterministicExecutor {
    state_dir: PathBuf,
    repo_index: RepoIndexArtifact,
    repo_index_ref: Option<ArtifactRef>,
}

pub struct CiTriageDeterministicExecutor {
    state_dir: PathBuf,
}

pub struct IncidentEnricherDeterministicExecutor {
    state_dir: PathBuf,
}

pub struct TaskPlannerDeterministicExecutor {
    state_dir: PathBuf,
}

pub struct WorkspacePatchApplyDeterministicExecutor {
    state_dir: PathBuf,
}

impl RepoIndexerDeterministicExecutor {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }
}

impl RepoReviewerDeterministicExecutor {
    pub fn new(
        state_dir: PathBuf,
        repo_index: RepoIndexArtifact,
        repo_index_ref: Option<ArtifactRef>,
    ) -> Self {
        Self {
            state_dir,
            repo_index,
            repo_index_ref,
        }
    }
}

impl CiTriageDeterministicExecutor {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }
}

impl IncidentEnricherDeterministicExecutor {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }
}

impl TaskPlannerDeterministicExecutor {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }
}

impl WorkspacePatchApplyDeterministicExecutor {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }
}

#[async_trait]
impl DeterministicExecutor for RepoIndexerDeterministicExecutor {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let workspace_root = required_input_string(action, "workspace_root")?;
        let workspace_path = PathBuf::from(&workspace_root);
        if !workspace_path.is_dir() {
            return Err(anyhow!(
                "workspace_root must point to an existing directory: {workspace_root}"
            ));
        }

        let files = collect_repo_files(&workspace_path);
        let languages = language_counts(&files);
        let test_files = detect_test_files(&files);
        let test_file_map = map_files_to_tests(&files, &test_files);
        let (owners, ownership_source) = resolve_owners(&workspace_path, &files).await?;

        let artifact = RepoIndexArtifact {
            files,
            languages,
            test_files,
            test_file_map,
            owners,
            ownership_source,
        };

        let artifact_ref =
            write_json_artifact(&self.state_dir, &action.id, "repo_index.json", &artifact).await?;

        Ok(ActionOutputs {
            summary: Some(format!(
                "Indexed {} files, {} test files, {} ownership entries",
                artifact.files.len(),
                artifact.test_files.len(),
                artifact.owners.len()
            )),
            artifacts: vec![artifact_ref],
            metadata: BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(workspace_root),
                ),
                (
                    "executor_class".to_string(),
                    serde_json::json!("deterministic"),
                ),
                (
                    "indexed_file_count".to_string(),
                    serde_json::json!(artifact.files.len()),
                ),
            ]),
        })
    }
}

#[async_trait]
impl DeterministicExecutor for RepoReviewerDeterministicExecutor {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let workspace_root = required_input_string(action, "workspace_root")?;
        let changed_files = changed_files_from_action(action).await?;
        if changed_files.is_empty() {
            return Err(anyhow!(
                "repo.review requires diff_text, diff_file, or changed_files"
            ));
        }

        let diff_text = diff_text_from_action(action).await.unwrap_or_default();
        let todo_files =
            scan_changed_files_for_markers(&workspace_root, &changed_files, &["TODO", "FIXME"]);
        let risky_files = changed_files
            .iter()
            .filter(|file| is_risky_path(file))
            .cloned()
            .collect::<Vec<_>>();
        let secret_hits = detect_secret_patterns(&diff_text, &workspace_root, &changed_files);
        let missing_test_files = changed_files
            .iter()
            .filter(|file| !is_test_file(file))
            .filter(|file| !self.repo_index.test_file_map.contains_key(*file))
            .cloned()
            .collect::<Vec<_>>();

        let mut findings = Vec::new();
        if !missing_test_files.is_empty() {
            findings.push(ReviewFinding {
                title: "Missing test coverage".to_string(),
                detail: format!(
                    "Changed files do not have mapped tests in the repository index: {}",
                    missing_test_files.join(", ")
                ),
                severity: "medium".to_string(),
                files: missing_test_files.clone(),
            });
        }
        if !todo_files.is_empty() {
            findings.push(ReviewFinding {
                title: "TODO or FIXME in changed files".to_string(),
                detail: format!(
                    "Changed files still contain TODO/FIXME markers: {}",
                    todo_files.join(", ")
                ),
                severity: "medium".to_string(),
                files: todo_files.clone(),
            });
        }
        if !risky_files.is_empty() {
            findings.push(ReviewFinding {
                title: "Risky paths changed".to_string(),
                detail: format!(
                    "The diff touches high-sensitivity paths: {}",
                    risky_files.join(", ")
                ),
                severity: "high".to_string(),
                files: risky_files.clone(),
            });
        }
        if !secret_hits.is_empty() {
            findings.push(ReviewFinding {
                title: "Potential secret material detected".to_string(),
                detail: format!(
                    "The diff or changed files contain credential-like patterns: {}",
                    secret_hits.join(", ")
                ),
                severity: "high".to_string(),
                files: changed_files.clone(),
            });
        }
        if diff_text.lines().count() > 300 || changed_files.len() > 20 {
            findings.push(ReviewFinding {
                title: "Large review surface".to_string(),
                detail: "The change set is large enough that manual follow-up is recommended."
                    .to_string(),
                severity: "medium".to_string(),
                files: changed_files.clone(),
            });
        }
        if findings.is_empty() {
            findings.push(ReviewFinding {
                title: "No deterministic findings".to_string(),
                detail: "The deterministic review checks did not surface actionable issues."
                    .to_string(),
                severity: "low".to_string(),
                files: changed_files.clone(),
            });
        }

        let artifact = ReviewFindingsArtifact {
            risk_level: calculate_review_risk(&findings),
            changed_files: changed_files.clone(),
            findings: findings.clone(),
        };
        let json_ref = write_json_artifact(
            &self.state_dir,
            &action.id,
            "review_findings.json",
            &artifact,
        )
        .await?;
        let markdown = build_review_summary_markdown(&artifact, action);
        let markdown_ref =
            write_text_artifact(&self.state_dir, &action.id, "review_summary.md", &markdown)
                .await?;

        let mut artifacts = Vec::new();
        if let Some(repo_index_ref) = &self.repo_index_ref {
            artifacts.push(repo_index_ref.clone());
        }
        artifacts.push(json_ref);
        artifacts.push(markdown_ref);

        Ok(ActionOutputs {
            summary: Some(format!(
                "Deterministic review produced {} findings for {} changed files",
                artifact.findings.len(),
                artifact.changed_files.len()
            )),
            artifacts,
            metadata: BTreeMap::from([
                (
                    "executor_class".to_string(),
                    serde_json::json!("deterministic"),
                ),
                (
                    "risk_level".to_string(),
                    serde_json::json!(format!("{:?}", artifact.risk_level).to_lowercase()),
                ),
            ]),
        })
    }
}

#[async_trait]
impl DeterministicExecutor for CiTriageDeterministicExecutor {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let log_text = triage_log_text_from_action(action).await?;
        let artifact = classify_ci_failure(&log_text);
        let json_ref =
            write_json_artifact(&self.state_dir, &action.id, "ci_triage.json", &artifact).await?;
        let markdown_ref = write_text_artifact(
            &self.state_dir,
            &action.id,
            "ci_triage_summary.md",
            &build_ci_triage_summary_markdown(&artifact, action),
        )
        .await?;

        Ok(ActionOutputs {
            summary: Some(artifact.summary.clone()),
            artifacts: vec![json_ref, markdown_ref],
            metadata: BTreeMap::from([
                (
                    "executor_class".to_string(),
                    serde_json::json!("deterministic"),
                ),
                (
                    "failure_family".to_string(),
                    serde_json::json!(format!("{:?}", artifact.family).to_lowercase()),
                ),
            ]),
        })
    }
}

#[async_trait]
impl DeterministicExecutor for IncidentEnricherDeterministicExecutor {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let log_text = incident_log_text_from_action(action).await?;
        let service_manifest = incident_service_manifest_from_action(action).await?;
        let service_name = optional_input_string(action, "service_name");
        let alert_name = optional_input_string(action, "alert_name");
        let run_url = optional_input_string(action, "run_url");

        let probable_blast_radius = probable_blast_radius(
            service_name.clone(),
            alert_name.clone(),
            &log_text,
            service_manifest.as_ref(),
        );
        let error_signatures = extract_error_signatures(&log_text);
        let repeated_symptoms = extract_repeated_symptoms(&log_text);
        let next_steps = incident_next_steps(
            &probable_blast_radius,
            &error_signatures,
            &repeated_symptoms,
        );

        let artifact = IncidentEnrichmentArtifact {
            service_name,
            alert_name,
            run_url,
            probable_blast_radius,
            error_signatures,
            repeated_symptoms,
            next_steps,
        };

        let json_ref = write_json_artifact(
            &self.state_dir,
            &action.id,
            "incident_enrichment.json",
            &artifact,
        )
        .await?;
        let markdown_ref = write_text_artifact(
            &self.state_dir,
            &action.id,
            "incident_summary.md",
            &build_incident_summary_markdown(&artifact, action),
        )
        .await?;

        Ok(ActionOutputs {
            summary: Some(format!(
                "Incident enrichment found {} likely impacted services and {} error signatures",
                artifact.probable_blast_radius.len(),
                artifact.error_signatures.len()
            )),
            artifacts: vec![json_ref, markdown_ref],
            metadata: BTreeMap::from([
                (
                    "executor_class".to_string(),
                    serde_json::json!("deterministic"),
                ),
                (
                    "blast_radius_count".to_string(),
                    serde_json::json!(artifact.probable_blast_radius.len()),
                ),
            ]),
        })
    }
}

#[async_trait]
impl DeterministicExecutor for TaskPlannerDeterministicExecutor {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let workspace_root = optional_input_string(action, "workspace_root");
        let repo_files = if let Some(workspace_root) = &workspace_root {
            let workspace_path = PathBuf::from(workspace_root);
            if !workspace_path.is_dir() {
                return Err(anyhow!(
                    "workspace_root must point to an existing directory: {workspace_root}"
                ));
            }
            collect_repo_files(&workspace_path)
        } else {
            Vec::new()
        };

        let objective = task_plan_objective_from_action(action)?;
        let files_of_interest = input_string_array(action, "files_of_interest");
        let constraints = input_string_array(action, "constraints");
        let target_files = select_task_plan_target_files(
            &repo_files,
            &objective,
            &files_of_interest,
            &constraints,
        );
        let risks = task_plan_risks(&target_files, &constraints);
        let assumptions = task_plan_assumptions(action, &target_files);
        let test_suggestions = task_plan_test_suggestions(&target_files);

        let artifact = TaskPlanArtifact {
            target_files: target_files.clone(),
            ordered_steps: task_plan_steps(&objective, &target_files, &constraints),
            risks,
            assumptions,
            test_suggestions,
            confidence_summary: task_plan_confidence_summary(&target_files, &constraints),
        };

        let json_ref =
            write_json_artifact(&self.state_dir, &action.id, "task_plan.json", &artifact).await?;
        let markdown = build_task_plan_markdown(&artifact, action, &objective);
        let markdown_ref =
            write_text_artifact(&self.state_dir, &action.id, "task_plan.md", &markdown).await?;

        Ok(ActionOutputs {
            summary: Some(format!(
                "Generated a task plan for {} target files",
                artifact.target_files.len()
            )),
            artifacts: vec![json_ref, markdown_ref],
            metadata: BTreeMap::from([
                (
                    "executor_class".to_string(),
                    serde_json::json!("deterministic"),
                ),
                (
                    "target_file_count".to_string(),
                    serde_json::json!(artifact.target_files.len()),
                ),
                (
                    "workspace_bound".to_string(),
                    serde_json::json!(workspace_root.is_some()),
                ),
            ]),
        })
    }
}

#[async_trait]
impl DeterministicExecutor for WorkspacePatchApplyDeterministicExecutor {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        let workspace_root = required_input_string(action, "workspace_root")?;
        let workspace_path = PathBuf::from(&workspace_root);
        if !workspace_path.is_dir() {
            return Err(anyhow!(
                "workspace_root must point to an existing directory: {workspace_root}"
            ));
        }

        let edits = workspace_edits_from_action(action)?;
        if edits.is_empty() {
            return Err(anyhow!("workspace.patch.apply requires at least one edit"));
        }

        let mut result = WorkspaceApplyResult {
            applied: Vec::new(),
            rejected: Vec::new(),
        };

        for edit in edits {
            match apply_workspace_edit(&workspace_path, &edit).await {
                Ok(()) => result.applied.push(edit.path.clone()),
                Err(error) => result.rejected.push(WorkspaceRejectedEdit {
                    path: edit.path.clone(),
                    reason: error.to_string(),
                }),
            }
        }

        let artifact_ref = write_json_artifact(
            &self.state_dir,
            &action.id,
            "workspace_apply_result.json",
            &result,
        )
        .await?;

        Ok(ActionOutputs {
            summary: Some(format!(
                "Applied {} edits, rejected {} edits",
                result.applied.len(),
                result.rejected.len()
            )),
            artifacts: vec![artifact_ref],
            metadata: BTreeMap::from([
                (
                    "executor_class".to_string(),
                    serde_json::json!("deterministic"),
                ),
                (
                    "applied_count".to_string(),
                    serde_json::json!(result.applied.len()),
                ),
                (
                    "rejected_count".to_string(),
                    serde_json::json!(result.rejected.len()),
                ),
            ]),
        })
    }
}

pub async fn write_json_artifact<T: serde::Serialize>(
    state_dir: &Path,
    action_id: &str,
    file_name: &str,
    value: &T,
) -> anyhow::Result<ArtifactRef> {
    let artifacts_dir = state_dir.join("artifacts").join(action_id);
    fs::create_dir_all(&artifacts_dir).await?;
    let path = artifacts_dir.join(file_name);
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&path, bytes).await?;
    Ok(ArtifactRef {
        kind: infer_artifact_kind(file_name),
        path: path.display().to_string(),
    })
}

pub async fn write_text_artifact(
    state_dir: &Path,
    action_id: &str,
    file_name: &str,
    contents: &str,
) -> anyhow::Result<ArtifactRef> {
    let artifacts_dir = state_dir.join("artifacts").join(action_id);
    fs::create_dir_all(&artifacts_dir).await?;
    let path = artifacts_dir.join(file_name);
    fs::write(&path, contents).await?;
    Ok(ArtifactRef {
        kind: infer_artifact_kind(file_name),
        path: path.display().to_string(),
    })
}

pub fn required_input_string(action: &Action, key: &str) -> anyhow::Result<String> {
    action
        .inputs
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("missing required string input: {key}"))
}

fn infer_artifact_kind(file_name: &str) -> String {
    file_name
        .strip_suffix(".json")
        .or_else(|| file_name.strip_suffix(".md"))
        .unwrap_or(file_name)
        .to_string()
}

pub async fn load_json_artifact<T: serde::de::DeserializeOwned>(
    artifact_ref: &ArtifactRef,
) -> anyhow::Result<T> {
    let contents = fs::read_to_string(&artifact_ref.path).await?;
    Ok(serde_json::from_str(&contents)?)
}

fn workspace_edits_from_action(action: &Action) -> anyhow::Result<Vec<WorkspaceEdit>> {
    let value = action
        .inputs
        .get("edits")
        .cloned()
        .ok_or_else(|| anyhow!("workspace.patch.apply requires edits"))?;
    Ok(serde_json::from_value(value)?)
}

async fn apply_workspace_edit(workspace_root: &Path, edit: &WorkspaceEdit) -> anyhow::Result<()> {
    let relative = Path::new(&edit.path);
    if relative.is_absolute() {
        return Err(anyhow!("path must be relative to workspace_root"));
    }
    if relative
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(anyhow!(
            "path traversal outside workspace_root is not allowed"
        ));
    }

    let target_path = workspace_root.join(relative);
    let parent = target_path
        .parent()
        .ok_or_else(|| anyhow!("edit path must have a parent"))?;
    if !fs::try_exists(parent).await? {
        fs::create_dir_all(parent).await?;
    }
    let canonical_parent = fs::canonicalize(parent).await?;
    let canonical_root = fs::canonicalize(workspace_root).await?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(anyhow!("path escapes workspace_root"));
    }
    if let Ok(metadata) = fs::symlink_metadata(&target_path).await {
        if metadata.file_type().is_symlink() {
            return Err(anyhow!("symlink targets are not allowed"));
        }
    }

    match edit.op {
        WorkspaceEditOp::Create => {
            let contents = edit
                .contents
                .as_ref()
                .ok_or_else(|| anyhow!("create edits require contents"))?;
            if fs::try_exists(&target_path).await? {
                return Err(anyhow!("create edit target already exists"));
            }
            fs::write(&target_path, contents).await?;
        }
        WorkspaceEditOp::Replace => {
            let contents = edit
                .contents
                .as_ref()
                .ok_or_else(|| anyhow!("replace edits require contents"))?;
            let expected_sha = edit
                .expected_sha256
                .as_ref()
                .ok_or_else(|| anyhow!("replace edits require expected_sha256"))?;
            let existing = fs::read(&target_path)
                .await
                .with_context(|| format!("replace target missing: {}", edit.path))?;
            if sha256_hex(&existing) != *expected_sha {
                return Err(anyhow!("replace edit expected_sha256 mismatch"));
            }
            fs::write(&target_path, contents).await?;
        }
        WorkspaceEditOp::Delete => {
            let expected_sha = edit
                .expected_sha256
                .as_ref()
                .ok_or_else(|| anyhow!("delete edits require expected_sha256"))?;
            let existing = fs::read(&target_path)
                .await
                .with_context(|| format!("delete target missing: {}", edit.path))?;
            if sha256_hex(&existing) != *expected_sha {
                return Err(anyhow!("delete edit expected_sha256 mismatch"));
            }
            fs::remove_file(&target_path).await?;
        }
    }

    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn collect_repo_files(workspace_root: &Path) -> Vec<String> {
    let mut files = WalkDir::new(workspace_root)
        .into_iter()
        .filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            !matches!(
                name.as_ref(),
                ".git" | ".crawfish" | "target" | "node_modules"
            )
        })
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| {
            entry
                .path()
                .strip_prefix(workspace_root)
                .ok()
                .map(normalize_path)
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn normalize_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn language_counts(files: &[String]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for file in files {
        let extension = Path::new(file)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("no_ext");
        *counts.entry(extension.to_string()).or_insert(0) += 1;
    }
    counts
}

fn detect_test_files(files: &[String]) -> Vec<String> {
    files
        .iter()
        .filter(|file| is_test_file(file))
        .cloned()
        .collect()
}

fn is_test_file(path: &str) -> bool {
    let file_name = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path);
    path.contains("/tests/")
        || path.starts_with("tests/")
        || file_name.ends_with("_test.rs")
        || file_name.ends_with(".test.ts")
        || file_name.ends_with(".test.js")
        || file_name.ends_with(".spec.ts")
        || file_name.ends_with(".spec.js")
        || file_name.starts_with("test_")
}

fn map_files_to_tests(files: &[String], test_files: &[String]) -> BTreeMap<String, Vec<String>> {
    let mut mapping = BTreeMap::new();
    for file in files.iter().filter(|file| !is_test_file(file)) {
        let stem = Path::new(file)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(file)
            .to_string();
        let related = test_files
            .iter()
            .filter(|test| test.contains(&stem))
            .cloned()
            .collect::<Vec<_>>();
        if !related.is_empty() {
            mapping.insert(file.clone(), related);
        }
    }
    mapping
}

async fn changed_files_from_action(action: &Action) -> anyhow::Result<Vec<String>> {
    if let Some(files) = action
        .inputs
        .get("changed_files")
        .and_then(|value| value.as_array())
    {
        let changed_files = files
            .iter()
            .filter_map(|value| value.as_str().map(ToString::to_string))
            .collect::<Vec<_>>();
        return Ok(changed_files);
    }

    let diff = diff_text_from_action(action).await?;
    Ok(parse_changed_files_from_diff(&diff))
}

async fn diff_text_from_action(action: &Action) -> anyhow::Result<String> {
    if let Some(diff_text) = action
        .inputs
        .get("diff_text")
        .and_then(|value| value.as_str())
    {
        return Ok(diff_text.to_string());
    }

    if let Some(diff_file) = action
        .inputs
        .get("diff_file")
        .and_then(|value| value.as_str())
    {
        return Ok(fs::read_to_string(diff_file).await?);
    }

    Err(anyhow!(
        "repo.review requires diff_text, diff_file, or changed_files"
    ))
}

async fn triage_log_text_from_action(action: &Action) -> anyhow::Result<String> {
    if let Some(log_text) = action
        .inputs
        .get("log_text")
        .and_then(|value| value.as_str())
    {
        return Ok(log_text.to_string());
    }

    if let Some(log_file) = action
        .inputs
        .get("log_file")
        .and_then(|value| value.as_str())
    {
        return Ok(fs::read_to_string(log_file).await?);
    }

    if action.inputs.contains_key("mcp_resource_ref") {
        return Err(anyhow!(
            "ci.triage with mcp_resource_ref requires MCP transport support"
        ));
    }

    Err(anyhow!(
        "ci.triage requires log_text, log_file, or mcp_resource_ref"
    ))
}

async fn incident_log_text_from_action(action: &Action) -> anyhow::Result<String> {
    if let Some(log_text) = action
        .inputs
        .get("log_text")
        .and_then(|value| value.as_str())
    {
        return Ok(log_text.to_string());
    }

    if let Some(log_file) = action
        .inputs
        .get("log_file")
        .and_then(|value| value.as_str())
    {
        return Ok(fs::read_to_string(log_file).await?);
    }

    Ok(String::new())
}

async fn incident_service_manifest_from_action(
    action: &Action,
) -> anyhow::Result<Option<IncidentServiceManifest>> {
    let Some(path) = action
        .inputs
        .get("service_manifest_file")
        .and_then(|value| value.as_str())
    else {
        return Ok(None);
    };

    let contents = fs::read_to_string(path).await?;
    if path.ends_with(".toml") {
        return Ok(Some(toml::from_str(&contents)?));
    }

    if let Ok(doc) = serde_json::from_str(&contents) {
        return Ok(Some(doc));
    }

    Ok(Some(toml::from_str(&contents)?))
}

fn optional_input_string(action: &Action, key: &str) -> Option<String> {
    action
        .inputs
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

fn input_string_array(action: &Action, key: &str) -> Vec<String> {
    action
        .inputs
        .get(key)
        .and_then(|value| value.as_array())
        .into_iter()
        .flat_map(|values| values.iter())
        .filter_map(|value| value.as_str())
        .map(ToString::to_string)
        .collect()
}

fn task_plan_objective_from_action(action: &Action) -> anyhow::Result<String> {
    [
        optional_input_string(action, "objective"),
        optional_input_string(action, "task"),
        optional_input_string(action, "spec_text"),
        optional_input_string(action, "problem_statement"),
    ]
    .into_iter()
    .flatten()
    .find(|value| !value.trim().is_empty())
    .ok_or_else(|| anyhow!("task.plan requires objective, task, spec_text, or problem_statement"))
}

fn select_task_plan_target_files(
    repo_files: &[String],
    objective: &str,
    files_of_interest: &[String],
    constraints: &[String],
) -> Vec<String> {
    if !files_of_interest.is_empty() {
        return files_of_interest.to_vec();
    }

    let lowered = format!("{objective} {}", constraints.join(" ")).to_lowercase();
    let tokens = lowered
        .split(|char: char| !char.is_ascii_alphanumeric())
        .filter(|token| token.len() >= 3)
        .collect::<Vec<_>>();

    let mut scored = repo_files
        .iter()
        .map(|file| {
            let lower = file.to_lowercase();
            let mut score = 0usize;
            for token in &tokens {
                if lower.contains(token) {
                    score += 2;
                }
            }
            if lower.contains("test") {
                score += 1;
            }
            if lower.ends_with("readme.md") {
                score += 1;
            }
            (file.clone(), score)
        })
        .filter(|(_, score)| *score > 0)
        .collect::<Vec<_>>();

    scored.sort_by(|lhs, rhs| rhs.1.cmp(&lhs.1).then_with(|| lhs.0.cmp(&rhs.0)));
    let mut selected = scored
        .into_iter()
        .take(6)
        .map(|(file, _)| file)
        .collect::<Vec<_>>();

    if selected.is_empty() {
        selected = repo_files
            .iter()
            .filter(|file| file.starts_with("src/") || file.starts_with("tests/"))
            .take(4)
            .cloned()
            .collect::<Vec<_>>();
    }

    if selected.is_empty() {
        selected = repo_files.iter().take(4).cloned().collect::<Vec<_>>();
    }

    selected
}

fn task_plan_steps(
    objective: &str,
    target_files: &[String],
    constraints: &[String],
) -> Vec<TaskPlanStep> {
    let target_files_summary = if target_files.is_empty() {
        "the current workspace".to_string()
    } else {
        target_files.join(", ")
    };
    let constraint_summary = if constraints.is_empty() {
        "No explicit constraints were supplied.".to_string()
    } else {
        format!("Honor these constraints: {}.", constraints.join("; "))
    };

    vec![
        TaskPlanStep {
            title: "Confirm scope".to_string(),
            detail: format!(
                "Review the request and anchor on this objective: {objective}. {constraint_summary}"
            ),
        },
        TaskPlanStep {
            title: "Inspect likely touch points".to_string(),
            detail: format!(
                "Start with these files or modules: {target_files_summary}."
            ),
        },
        TaskPlanStep {
            title: "Draft the proposal".to_string(),
            detail: "Describe the intended changes, why they are needed, and what should stay unchanged.".to_string(),
        },
        TaskPlanStep {
            title: "Plan validation".to_string(),
            detail: "List deterministic checks, tests, and edge cases that should confirm the patch is safe before any mutation path is used.".to_string(),
        },
    ]
}

fn task_plan_risks(target_files: &[String], constraints: &[String]) -> Vec<String> {
    let mut risks = Vec::new();
    if target_files.is_empty() {
        risks.push("Target file selection is heuristic because the request did not include files_of_interest.".to_string());
    }
    if target_files.iter().any(|file| is_risky_path(file)) {
        risks.push(
            "The proposed patch touches sensitive configuration, auth, or policy code paths."
                .to_string(),
        );
    }
    if constraints.is_empty() {
        risks.push("No explicit constraints were provided, so the plan may need operator narrowing before implementation.".to_string());
    }
    if risks.is_empty() {
        risks.push("No deterministic blockers were found, but the plan still requires human review before any mutation path.".to_string());
    }
    risks
}

fn task_plan_assumptions(action: &Action, target_files: &[String]) -> Vec<String> {
    let mut assumptions = Vec::new();
    if target_files.is_empty() {
        assumptions.push(
            "The request can be satisfied without prior knowledge of the exact target files."
                .to_string(),
        );
    }
    if optional_input_string(action, "base_ref").is_none()
        || optional_input_string(action, "head_ref").is_none()
    {
        assumptions.push("The plan is based on the current workspace state because base/head refs were not both supplied.".to_string());
    }
    assumptions.push(
        "This capability produces a proposal only and does not mutate the workspace.".to_string(),
    );
    assumptions
}

fn task_plan_test_suggestions(target_files: &[String]) -> Vec<String> {
    let mut suggestions = vec![
        "Run the narrowest existing test target that covers the changed modules.".to_string(),
        "Add or update deterministic tests for the intended behavior delta.".to_string(),
    ];
    if target_files.iter().any(|file| file.ends_with(".rs")) {
        suggestions.push(
            "Run `cargo test --workspace` and targeted Rust checks for the touched crate."
                .to_string(),
        );
    }
    if target_files
        .iter()
        .any(|file| file.ends_with(".ts") || file.ends_with(".js"))
    {
        suggestions.push("Run the project’s TypeScript or JavaScript test/lint targets for the affected package.".to_string());
    }
    suggestions
}

fn task_plan_confidence_summary(target_files: &[String], constraints: &[String]) -> String {
    match (target_files.is_empty(), constraints.is_empty()) {
        (false, false) => "medium confidence: likely target files and constraints are both available".to_string(),
        (false, true) => "medium-low confidence: target files are available, but the request lacks explicit constraints".to_string(),
        (true, false) => "medium-low confidence: constraints exist, but file targeting is heuristic".to_string(),
        (true, true) => "low confidence: both file targeting and constraints are heuristic".to_string(),
    }
}

fn build_task_plan_markdown(
    artifact: &TaskPlanArtifact,
    action: &Action,
    objective: &str,
) -> String {
    let mut markdown = vec![
        "# Task Plan".to_string(),
        String::new(),
        format!("Request: {}", action.goal.summary),
        format!("Objective: {objective}"),
        String::new(),
        "## Target Files".to_string(),
    ];

    if artifact.target_files.is_empty() {
        markdown.push("- No concrete file set was identified.".to_string());
    } else {
        markdown.extend(artifact.target_files.iter().map(|file| format!("- {file}")));
    }

    markdown.push(String::new());
    markdown.push("## Ordered Steps".to_string());
    markdown.extend(
        artifact
            .ordered_steps
            .iter()
            .enumerate()
            .map(|(index, step)| format!("{}. **{}**: {}", index + 1, step.title, step.detail)),
    );

    markdown.push(String::new());
    markdown.push("## Risks".to_string());
    markdown.extend(artifact.risks.iter().map(|risk| format!("- {risk}")));

    markdown.push(String::new());
    markdown.push("## Assumptions".to_string());
    markdown.extend(
        artifact
            .assumptions
            .iter()
            .map(|assumption| format!("- {assumption}")),
    );

    markdown.push(String::new());
    markdown.push("## Suggested Validation".to_string());
    markdown.extend(
        artifact
            .test_suggestions
            .iter()
            .map(|suggestion| format!("- {suggestion}")),
    );

    markdown.push(String::new());
    markdown.push(format!("Confidence: {}", artifact.confidence_summary));

    markdown.join("\n")
}

fn parse_changed_files_from_diff(diff: &str) -> Vec<String> {
    let mut files = Vec::new();
    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ b/") {
            files.push(path.to_string());
        } else if let Some(path) = line.strip_prefix("diff --git a/") {
            if let Some((_, rhs)) = path.split_once(" b/") {
                files.push(rhs.to_string());
            }
        }
    }
    files.sort();
    files.dedup();
    files
}

fn scan_changed_files_for_markers(
    workspace_root: &str,
    changed_files: &[String],
    markers: &[&str],
) -> Vec<String> {
    changed_files
        .iter()
        .filter_map(|file| {
            let path = Path::new(workspace_root).join(file);
            let contents = std::fs::read_to_string(path).ok()?;
            if markers.iter().any(|marker| contents.contains(marker)) {
                Some(file.clone())
            } else {
                None
            }
        })
        .collect()
}

fn is_risky_path(path: &str) -> bool {
    let lower = path.to_lowercase();
    [
        "auth",
        "secret",
        "config",
        "policy",
        "migration",
        "credential",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn detect_secret_patterns(
    diff_text: &str,
    workspace_root: &str,
    changed_files: &[String],
) -> Vec<String> {
    let secret_needles = [
        "AKIA",
        "BEGIN PRIVATE KEY",
        "SECRET_KEY",
        "TOKEN=",
        "PASSWORD=",
    ];
    let mut hits = Vec::new();

    if secret_needles
        .iter()
        .any(|needle| diff_text.to_uppercase().contains(&needle.to_uppercase()))
    {
        hits.push("diff_text".to_string());
    }

    for file in changed_files {
        let path = Path::new(workspace_root).join(file);
        if let Ok(contents) = std::fs::read_to_string(path) {
            if secret_needles
                .iter()
                .any(|needle| contents.to_uppercase().contains(&needle.to_uppercase()))
            {
                hits.push(file.clone());
            }
        }
    }

    hits.sort();
    hits.dedup();
    hits
}

fn calculate_review_risk(findings: &[ReviewFinding]) -> ReviewRiskLevel {
    if findings.iter().any(|finding| finding.severity == "high") {
        ReviewRiskLevel::High
    } else if findings.iter().any(|finding| finding.severity == "medium") {
        ReviewRiskLevel::Medium
    } else {
        ReviewRiskLevel::Low
    }
}

fn build_review_summary_markdown(artifact: &ReviewFindingsArtifact, action: &Action) -> String {
    let mut lines = vec![
        "# Review Summary".to_string(),
        String::new(),
        format!("- Capability: `{}`", action.capability),
        format!(
            "- Risk level: `{}`",
            format!("{:?}", artifact.risk_level).to_lowercase()
        ),
        format!("- Changed files: {}", artifact.changed_files.len()),
        String::new(),
        "## Findings".to_string(),
        String::new(),
    ];

    for finding in &artifact.findings {
        lines.push(format!("- **{}** ({})", finding.title, finding.severity));
        lines.push(format!("  {}", finding.detail));
        if !finding.files.is_empty() {
            lines.push(format!("  Files: {}", finding.files.join(", ")));
        }
    }

    lines.join("\n")
}

fn classify_ci_failure(log_text: &str) -> CiTriageArtifact {
    let lower = log_text.to_lowercase();
    let (family, summary, next_steps) = if lower.contains("test failed")
        || lower.contains("assertion failed")
        || lower.contains("failures:")
    {
        (
            CiFailureFamily::Test,
            "CI run failed due to test failures.",
            vec![
                "Inspect the failing test cases and rerun the affected suite.".to_string(),
                "Check whether the change removed required fixtures or mocks.".to_string(),
            ],
        )
    } else if lower.contains("eslint") || lower.contains("clippy") || lower.contains("lint") {
        (
            CiFailureFamily::Lint,
            "CI run failed during lint or static style checks.",
            vec![
                "Run the relevant lint command locally and apply the suggested fixes.".to_string(),
                "Confirm formatter and lint config versions match CI.".to_string(),
            ],
        )
    } else if lower.contains("type error")
        || lower.contains("cannot find type")
        || lower.contains("mismatched types")
        || lower.contains("ts2304")
    {
        (
            CiFailureFamily::Typecheck,
            "CI run failed during type checking.",
            vec![
                "Reproduce the typecheck step locally and inspect the reported symbols."
                    .to_string(),
                "Check recently changed interfaces or generated types.".to_string(),
            ],
        )
    } else if lower.contains("error: could not compile")
        || lower.contains("build failed")
        || lower.contains("linker")
    {
        (
            CiFailureFamily::Build,
            "CI run failed during build or compile steps.",
            vec![
                "Reproduce the build locally with the same target and feature set.".to_string(),
                "Check for missing files, features, or linker/system dependencies.".to_string(),
            ],
        )
    } else if lower.contains("could not resolve")
        || lower.contains("failed to fetch")
        || lower.contains("timed out downloading")
        || lower.contains("npm err")
        || lower.contains("cargo failed to get")
    {
        (
            CiFailureFamily::DependencyInstall,
            "CI run failed while resolving or installing dependencies.",
            vec![
                "Check package registry availability and lockfile consistency.".to_string(),
                "Retry after confirming dependency mirrors and credentials.".to_string(),
            ],
        )
    } else if lower.contains("connection reset")
        || lower.contains("503 service unavailable")
        || lower.contains("timed out")
        || lower.contains("context deadline exceeded")
    {
        (
            CiFailureFamily::InfraTransient,
            "CI run appears to have failed because of transient infrastructure issues.",
            vec![
                "Retry the job to confirm the failure is transient.".to_string(),
                "Inspect runner, network, and remote service status dashboards.".to_string(),
            ],
        )
    } else {
        (
            CiFailureFamily::Unknown,
            "CI run failed, but the deterministic classifier could not categorize it confidently.",
            vec![
                "Inspect the full logs manually and attach the relevant excerpt.".to_string(),
                "Add a new deterministic triage rule if this failure repeats.".to_string(),
            ],
        )
    };

    let evidence = log_text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(5)
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    CiTriageArtifact {
        family,
        summary: summary.to_string(),
        evidence,
        next_steps,
    }
}

fn build_ci_triage_summary_markdown(artifact: &CiTriageArtifact, action: &Action) -> String {
    let mut lines = vec![
        "# CI Triage Summary".to_string(),
        String::new(),
        format!("- Capability: `{}`", action.capability),
        format!(
            "- Failure family: `{}`",
            format!("{:?}", artifact.family).to_lowercase()
        ),
        String::new(),
        "## Summary".to_string(),
        String::new(),
        artifact.summary.clone(),
        String::new(),
        "## Evidence".to_string(),
        String::new(),
    ];

    for line in &artifact.evidence {
        lines.push(format!("- {line}"));
    }

    lines.push(String::new());
    lines.push("## Suggested Next Steps".to_string());
    lines.push(String::new());
    for step in &artifact.next_steps {
        lines.push(format!("- {step}"));
    }

    lines.join("\n")
}

#[derive(Debug, Clone, serde::Deserialize)]
struct IncidentServiceManifest {
    #[serde(default)]
    services: BTreeMap<String, IncidentServiceNode>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct IncidentServiceNode {
    #[serde(default)]
    depends_on: Vec<String>,
}

fn probable_blast_radius(
    service_name: Option<String>,
    alert_name: Option<String>,
    log_text: &str,
    service_manifest: Option<&IncidentServiceManifest>,
) -> Vec<String> {
    let mut seeds = Vec::new();
    if let Some(service_name) = service_name {
        seeds.push(service_name);
    }

    let searchable = format!(
        "{}\n{}",
        log_text.to_lowercase(),
        alert_name.unwrap_or_default().to_lowercase()
    );
    if let Some(manifest) = service_manifest {
        for service in manifest.services.keys() {
            if searchable.contains(&service.to_lowercase()) {
                seeds.push(service.clone());
            }
        }
    }

    seeds.sort();
    seeds.dedup();

    if let Some(manifest) = service_manifest {
        let reverse = build_reverse_dependencies(manifest);
        let mut impacted = seeds.clone();
        let mut queue = seeds;
        while let Some(current) = queue.pop() {
            if let Some(dependents) = reverse.get(&current) {
                for dependent in dependents {
                    if !impacted.contains(dependent) {
                        impacted.push(dependent.clone());
                        queue.push(dependent.clone());
                    }
                }
            }
        }
        impacted.sort();
        impacted.dedup();
        return impacted;
    }

    seeds
}

fn build_reverse_dependencies(manifest: &IncidentServiceManifest) -> BTreeMap<String, Vec<String>> {
    let mut reverse = BTreeMap::new();
    for (service, node) in &manifest.services {
        reverse.entry(service.clone()).or_insert_with(Vec::new);
        for dependency in &node.depends_on {
            reverse
                .entry(dependency.clone())
                .or_insert_with(Vec::new)
                .push(service.clone());
        }
    }
    for dependents in reverse.values_mut() {
        dependents.sort();
        dependents.dedup();
    }
    reverse
}

fn extract_error_signatures(log_text: &str) -> Vec<String> {
    let mut signatures = log_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| {
            let lower = line.to_lowercase();
            [
                "error",
                "exception",
                "panic",
                "timeout",
                "failed",
                "unavailable",
            ]
            .iter()
            .any(|needle| lower.contains(needle))
        })
        .map(|line| line.chars().take(160).collect::<String>())
        .collect::<Vec<_>>();
    signatures.sort();
    signatures.dedup();
    signatures.truncate(5);
    signatures
}

fn extract_repeated_symptoms(log_text: &str) -> Vec<String> {
    let lower = log_text.to_lowercase();
    let mut symptoms = Vec::new();
    if lower.contains("timeout") || lower.contains("deadline exceeded") {
        symptoms.push("timeouts or deadline pressure".to_string());
    }
    if lower.contains("503")
        || lower.contains("service unavailable")
        || lower.contains("reset by peer")
    {
        symptoms.push("downstream service instability".to_string());
    }
    if lower.contains("panic") || lower.contains("segmentation fault") {
        symptoms.push("process crash symptoms".to_string());
    }
    if lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("access denied")
    {
        symptoms.push("authorization failures".to_string());
    }
    if lower.contains("out of memory") || lower.contains("disk full") {
        symptoms.push("resource exhaustion".to_string());
    }
    if lower.contains("connection refused") || lower.contains("dns") {
        symptoms.push("network or name resolution issues".to_string());
    }
    symptoms
}

fn incident_next_steps(
    probable_blast_radius: &[String],
    error_signatures: &[String],
    repeated_symptoms: &[String],
) -> Vec<String> {
    let mut steps = Vec::new();
    if !probable_blast_radius.is_empty() {
        steps.push(format!(
            "Inspect the owners and dashboards for likely impacted services: {}.",
            probable_blast_radius.join(", ")
        ));
    }
    if repeated_symptoms
        .iter()
        .any(|symptom| symptom.contains("network") || symptom.contains("service instability"))
    {
        steps.push(
            "Check network paths, upstream availability, and recent deploys on dependent services."
                .to_string(),
        );
    }
    if repeated_symptoms
        .iter()
        .any(|symptom| symptom.contains("authorization"))
    {
        steps.push(
            "Verify recent credential, policy, and token changes for the failing path.".to_string(),
        );
    }
    if repeated_symptoms
        .iter()
        .any(|symptom| symptom.contains("resource exhaustion"))
    {
        steps.push(
            "Inspect CPU, memory, and disk pressure on the affected worker or service.".to_string(),
        );
    }
    if !error_signatures.is_empty() {
        steps.push(
            "Attach the top error signatures to the operator handoff or incident ticket."
                .to_string(),
        );
    }
    if steps.is_empty() {
        steps.push(
            "Review the full logs and service topology to refine the blast radius.".to_string(),
        );
    }
    steps
}

fn build_incident_summary_markdown(
    artifact: &IncidentEnrichmentArtifact,
    action: &Action,
) -> String {
    let mut lines = vec![
        "# Incident Enrichment Summary".to_string(),
        String::new(),
        format!("- Capability: `{}`", action.capability),
    ];
    if let Some(service_name) = &artifact.service_name {
        lines.push(format!("- Service: `{service_name}`"));
    }
    if let Some(alert_name) = &artifact.alert_name {
        lines.push(format!("- Alert: `{alert_name}`"));
    }
    if let Some(run_url) = &artifact.run_url {
        lines.push(format!("- Run URL: {run_url}"));
    }

    lines.push(String::new());
    lines.push("## Probable Blast Radius".to_string());
    lines.push(String::new());
    if artifact.probable_blast_radius.is_empty() {
        lines.push("- No impacted services could be inferred deterministically.".to_string());
    } else {
        for service in &artifact.probable_blast_radius {
            lines.push(format!("- {service}"));
        }
    }

    lines.push(String::new());
    lines.push("## Error Signatures".to_string());
    lines.push(String::new());
    if artifact.error_signatures.is_empty() {
        lines.push("- No high-confidence error signatures found.".to_string());
    } else {
        for signature in &artifact.error_signatures {
            lines.push(format!("- {signature}"));
        }
    }

    lines.push(String::new());
    lines.push("## Repeated Symptoms".to_string());
    lines.push(String::new());
    if artifact.repeated_symptoms.is_empty() {
        lines.push("- No repeated symptom families detected.".to_string());
    } else {
        for symptom in &artifact.repeated_symptoms {
            lines.push(format!("- {symptom}"));
        }
    }

    lines.push(String::new());
    lines.push("## Suggested Next Steps".to_string());
    lines.push(String::new());
    for step in &artifact.next_steps {
        lines.push(format!("- {step}"));
    }

    lines.join("\n")
}

async fn resolve_owners(
    workspace_root: &Path,
    files: &[String],
) -> anyhow::Result<(BTreeMap<String, Vec<String>>, String)> {
    if let Some((rules, source)) = load_codeowners_rules(workspace_root).await? {
        let mut owners = BTreeMap::new();
        for file in files {
            if let Some(file_owners) = owners_for_path(file, &rules) {
                owners.insert(file.clone(), file_owners);
            }
        }
        return Ok((owners, source));
    }

    let mut owners = BTreeMap::new();
    for file in files {
        let top_level = file
            .split('/')
            .next()
            .map(ToString::to_string)
            .unwrap_or_else(|| "root".to_string());
        owners.insert(file.clone(), vec![format!("@team/{top_level}")]);
    }
    Ok((owners, "heuristic".to_string()))
}

async fn load_codeowners_rules(
    workspace_root: &Path,
) -> anyhow::Result<Option<(Vec<CodeownersRule>, String)>> {
    let candidates = [
        workspace_root.join("CODEOWNERS"),
        workspace_root.join(".github/CODEOWNERS"),
        workspace_root.join("docs/CODEOWNERS"),
    ];

    for candidate in candidates {
        if candidate.exists() {
            let contents = fs::read_to_string(&candidate)
                .await
                .with_context(|| format!("failed to read {}", candidate.display()))?;
            let rules = contents
                .lines()
                .filter_map(parse_codeowners_rule)
                .collect::<Vec<_>>();
            return Ok(Some((rules, candidate.display().to_string())));
        }
    }

    Ok(None)
}

#[derive(Debug, Clone)]
struct CodeownersRule {
    pattern: String,
    owners: Vec<String>,
}

fn parse_codeowners_rule(line: &str) -> Option<CodeownersRule> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut parts = trimmed.split_whitespace();
    let pattern = parts.next()?.trim_start_matches('/').to_string();
    let owners = parts.map(ToString::to_string).collect::<Vec<_>>();
    if owners.is_empty() {
        return None;
    }

    Some(CodeownersRule { pattern, owners })
}

fn owners_for_path(path: &str, rules: &[CodeownersRule]) -> Option<Vec<String>> {
    let mut matched = None;
    for rule in rules {
        if codeowners_pattern_matches(&rule.pattern, path) {
            matched = Some(rule.owners.clone());
        }
    }
    matched
}

fn codeowners_pattern_matches(pattern: &str, path: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if pattern.ends_with('/') {
        let prefix = pattern.trim_start_matches('/');
        return path.starts_with(prefix);
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return path.starts_with(prefix.trim_start_matches('/'));
    }

    let normalized = pattern.trim_start_matches('/');
    path == normalized || path.starts_with(&format!("{normalized}/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawfish_core::now_timestamp;
    use crawfish_types::{
        Action, ActionPhase, ExecutionContract, GoalSpec, IncidentEnrichmentArtifact, OwnerKind,
        OwnerRef, RequesterKind, RequesterRef, ScheduleSpec, TaskPlanArtifact,
        WorkspaceApplyResult,
    };
    use tempfile::tempdir;

    fn action_with_workspace(workspace_root: &Path) -> Action {
        action_with_capability(
            "repo.index",
            BTreeMap::from([(
                "workspace_root".to_string(),
                serde_json::json!(workspace_root.display().to_string()),
            )]),
        )
    }

    fn action_with_capability(
        capability: &str,
        inputs: BTreeMap<String, serde_json::Value>,
    ) -> Action {
        Action {
            id: "action-1".to_string(),
            target_agent_id: match capability {
                "repo.review" => "repo_reviewer".to_string(),
                "ci.triage" => "ci_triage".to_string(),
                "incident.enrich" => "incident_enricher".to_string(),
                "task.plan" | "coding.patch.plan" => "task_planner".to_string(),
                "workspace.patch.apply" => "workspace_editor".to_string(),
                _ => "repo_indexer".to_string(),
            },
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "cli".to_string(),
            },
            initiator_owner: OwnerRef {
                kind: OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            counterparty_refs: Vec::new(),
            goal: GoalSpec {
                summary: capability.to_string(),
                details: None,
            },
            capability: capability.to_string(),
            inputs,
            contract: ExecutionContract::default(),
            execution_strategy: None,
            grant_refs: Vec::new(),
            lease_ref: None,
            encounter_ref: None,
            audit_receipt_ref: None,
            data_boundary: "owner_local".to_string(),
            schedule: ScheduleSpec::default(),
            phase: ActionPhase::Accepted,
            created_at: now_timestamp(),
            started_at: None,
            finished_at: None,
            checkpoint_ref: None,
            continuity_mode: None,
            degradation_profile: None,
            failure_reason: None,
            failure_code: None,
            selected_executor: None,
            recovery_stage: None,
            lock_detail: None,
            external_refs: Vec::new(),
            outputs: ActionOutputs::default(),
        }
    }

    #[tokio::test]
    async fn repo_indexer_writes_artifact() {
        let dir = tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(workspace.join("src")).await.unwrap();
        fs::create_dir_all(workspace.join("tests")).await.unwrap();
        fs::write(
            workspace.join("src/lib.rs"),
            "pub fn value() -> u32 { 42 }\n",
        )
        .await
        .unwrap();
        fs::write(
            workspace.join("tests/lib_test.rs"),
            "#[test] fn smoke() {}\n",
        )
        .await
        .unwrap();
        fs::write(workspace.join("CODEOWNERS"), "src/ @team/core\n")
            .await
            .unwrap();

        let executor = RepoIndexerDeterministicExecutor::new(dir.path().join(".crawfish/state"));
        let outputs = executor
            .execute(&action_with_workspace(&workspace))
            .await
            .unwrap();

        assert_eq!(outputs.artifacts.len(), 1);
        let artifact = fs::read_to_string(&outputs.artifacts[0].path)
            .await
            .unwrap();
        let indexed: RepoIndexArtifact = serde_json::from_str(&artifact).unwrap();
        assert!(indexed.files.contains(&"src/lib.rs".to_string()));
        assert!(indexed
            .test_files
            .contains(&"tests/lib_test.rs".to_string()));
        assert_eq!(
            indexed.owners.get("src/lib.rs").cloned(),
            Some(vec!["@team/core".to_string()])
        );
    }

    #[tokio::test]
    async fn repo_reviewer_emits_structured_findings() {
        let dir = tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(workspace.join("src")).await.unwrap();
        fs::create_dir_all(workspace.join("config")).await.unwrap();
        fs::write(
            workspace.join("src/lib.rs"),
            "pub fn value() -> u32 { 42 } // TODO tighten checks\n",
        )
        .await
        .unwrap();
        fs::write(workspace.join("config/secrets.env"), "PASSWORD=unsafe\n")
            .await
            .unwrap();

        let state_dir = dir.path().join(".crawfish/state");
        let index_executor = RepoIndexerDeterministicExecutor::new(state_dir.clone());
        let index_outputs = index_executor
            .execute(&action_with_workspace(&workspace))
            .await
            .unwrap();
        let repo_index_ref = index_outputs.artifacts[0].clone();
        let repo_index = load_json_artifact::<RepoIndexArtifact>(&repo_index_ref)
            .await
            .unwrap();

        let review_action = action_with_capability(
            "repo.review",
            BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(workspace.display().to_string()),
                ),
                (
                    "changed_files".to_string(),
                    serde_json::json!(["src/lib.rs", "config/secrets.env"]),
                ),
            ]),
        );
        let reviewer =
            RepoReviewerDeterministicExecutor::new(state_dir, repo_index, Some(repo_index_ref));
        let outputs = reviewer.execute(&review_action).await.unwrap();

        assert_eq!(outputs.artifacts.len(), 3);
        let artifact = fs::read_to_string(&outputs.artifacts[1].path)
            .await
            .unwrap();
        let review: ReviewFindingsArtifact = serde_json::from_str(&artifact).unwrap();
        assert_eq!(review.risk_level, ReviewRiskLevel::High);
        assert!(review
            .findings
            .iter()
            .any(|finding| finding.title == "Potential secret material detected"));
    }

    #[tokio::test]
    async fn ci_triage_classifies_failure_family() {
        let dir = tempdir().unwrap();
        let executor = CiTriageDeterministicExecutor::new(dir.path().join(".crawfish/state"));
        let action = action_with_capability(
            "ci.triage",
            BTreeMap::from([(
                "log_text".to_string(),
                serde_json::json!("error: test failed, to rerun pass `cargo test`"),
            )]),
        );

        let outputs = executor.execute(&action).await.unwrap();
        assert_eq!(outputs.artifacts.len(), 2);
        let artifact = fs::read_to_string(&outputs.artifacts[0].path)
            .await
            .unwrap();
        let triage: CiTriageArtifact = serde_json::from_str(&artifact).unwrap();
        assert_eq!(triage.family, CiFailureFamily::Test);
    }

    #[tokio::test]
    async fn incident_enricher_emits_blast_radius_summary() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join(".crawfish/state");
        let manifest_path = dir.path().join("services.toml");
        fs::write(
            &manifest_path,
            r#"[services.api]
depends_on = ["db"]

[services.web]
depends_on = ["api"]

[services.worker]
depends_on = ["api"]

[services.db]
depends_on = []
"#,
        )
        .await
        .unwrap();
        let action = action_with_capability(
            "incident.enrich",
            BTreeMap::from([
                ("service_name".to_string(), serde_json::json!("api")),
                (
                    "alert_name".to_string(),
                    serde_json::json!("api latency high"),
                ),
                (
                    "log_text".to_string(),
                    serde_json::json!(
                        "ERROR timeout contacting db\n503 service unavailable from api\n"
                    ),
                ),
                (
                    "service_manifest_file".to_string(),
                    serde_json::json!(manifest_path.display().to_string()),
                ),
            ]),
        );
        let executor = IncidentEnricherDeterministicExecutor::new(state_dir);
        let outputs = executor.execute(&action).await.unwrap();

        assert_eq!(outputs.artifacts.len(), 2);
        let artifact = fs::read_to_string(&outputs.artifacts[0].path)
            .await
            .unwrap();
        let enrichment: IncidentEnrichmentArtifact = serde_json::from_str(&artifact).unwrap();
        assert!(enrichment
            .probable_blast_radius
            .contains(&"api".to_string()));
        assert!(enrichment
            .probable_blast_radius
            .contains(&"web".to_string()));
        assert!(enrichment
            .probable_blast_radius
            .contains(&"worker".to_string()));
        assert!(!enrichment.error_signatures.is_empty());
    }

    #[tokio::test]
    async fn task_planner_emits_task_plan_artifacts() {
        let dir = tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(workspace.join("src")).await.unwrap();
        fs::create_dir_all(workspace.join("tests")).await.unwrap();
        fs::write(
            workspace.join("src/lib.rs"),
            "pub fn compute() -> u32 { 1 }\n",
        )
        .await
        .unwrap();
        fs::write(
            workspace.join("tests/lib_test.rs"),
            "#[test] fn smoke() { assert_eq!(1, 1); }\n",
        )
        .await
        .unwrap();

        let executor = TaskPlannerDeterministicExecutor::new(dir.path().join(".crawfish/state"));
        let outputs = executor
            .execute(&action_with_capability(
                "task.plan",
                BTreeMap::from([
                    (
                        "workspace_root".to_string(),
                        serde_json::json!(workspace.display().to_string()),
                    ),
                    (
                        "objective".to_string(),
                        serde_json::json!("Add validation to compute and update related tests"),
                    ),
                    (
                        "files_of_interest".to_string(),
                        serde_json::json!(["src/lib.rs", "tests/lib_test.rs"]),
                    ),
                ]),
            ))
            .await
            .unwrap();

        assert_eq!(outputs.artifacts.len(), 2);
        let plan: TaskPlanArtifact = load_json_artifact(&outputs.artifacts[0]).await.unwrap();
        assert!(plan.target_files.contains(&"src/lib.rs".to_string()));
        assert!(!plan.ordered_steps.is_empty());
        let summary = fs::read_to_string(&outputs.artifacts[1].path)
            .await
            .unwrap();
        assert!(summary.contains("# Task Plan"));
        assert!(summary.contains("src/lib.rs"));
    }

    #[tokio::test]
    async fn workspace_patch_apply_applies_and_rejects_safely() {
        let dir = tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(workspace.join("nested")).await.unwrap();
        fs::write(workspace.join("nested/file.txt"), "before\n")
            .await
            .unwrap();
        fs::write(workspace.join("delete.txt"), "remove me\n")
            .await
            .unwrap();

        let replace_sha = sha256_hex(b"before\n");
        let delete_sha = sha256_hex(b"remove me\n");
        let action = action_with_capability(
            "workspace.patch.apply",
            BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(workspace.display().to_string()),
                ),
                (
                    "edits".to_string(),
                    serde_json::json!([
                        {
                            "path": "created.txt",
                            "op": "create",
                            "contents": "created\n"
                        },
                        {
                            "path": "nested/file.txt",
                            "op": "replace",
                            "contents": "after\n",
                            "expected_sha256": replace_sha
                        },
                        {
                            "path": "delete.txt",
                            "op": "delete",
                            "expected_sha256": delete_sha
                        },
                        {
                            "path": "../escape.txt",
                            "op": "create",
                            "contents": "nope\n"
                        }
                    ]),
                ),
            ]),
        );

        let executor =
            WorkspacePatchApplyDeterministicExecutor::new(dir.path().join(".crawfish/state"));
        let outputs = executor.execute(&action).await.unwrap();

        assert_eq!(outputs.artifacts.len(), 1);
        assert_eq!(
            fs::read_to_string(workspace.join("created.txt"))
                .await
                .unwrap(),
            "created\n"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("nested/file.txt"))
                .await
                .unwrap(),
            "after\n"
        );
        assert!(!workspace.join("delete.txt").exists());

        let artifact = fs::read_to_string(&outputs.artifacts[0].path)
            .await
            .unwrap();
        let result: WorkspaceApplyResult = serde_json::from_str(&artifact).unwrap();
        assert_eq!(result.applied.len(), 3);
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].path, "../escape.txt");
    }
}

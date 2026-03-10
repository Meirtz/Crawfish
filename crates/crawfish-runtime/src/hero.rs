use anyhow::{anyhow, Context};
use async_trait::async_trait;
use crawfish_core::DeterministicExecutor;
use crawfish_types::{Action, ActionOutputs, ArtifactRef, RepoIndexArtifact};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use walkdir::WalkDir;

pub struct RepoIndexerDeterministicExecutor {
    state_dir: PathBuf,
}

impl RepoIndexerDeterministicExecutor {
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
        Action, ActionPhase, ExecutionContract, GoalSpec, OwnerKind, OwnerRef, RequesterKind,
        RequesterRef, ScheduleSpec,
    };
    use tempfile::tempdir;

    fn action_with_workspace(workspace_root: &Path) -> Action {
        Action {
            id: "action-1".to_string(),
            target_agent_id: "repo_indexer".to_string(),
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
                summary: "index repository".to_string(),
                details: None,
            },
            capability: "repo.index".to_string(),
            inputs: BTreeMap::from([(
                "workspace_root".to_string(),
                serde_json::json!(workspace_root.display().to_string()),
            )]),
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
            selected_executor: None,
            recovery_stage: None,
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
}

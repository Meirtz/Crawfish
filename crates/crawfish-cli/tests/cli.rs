use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

const CRAWFISH_BIN: &str = env!("CARGO_BIN_EXE_crawfish");

#[test]
fn init_creates_workspace_files() {
    let dir = tempdir().unwrap();
    Command::new(CRAWFISH_BIN)
        .arg("init")
        .arg(dir.path())
        .assert()
        .success();

    assert!(dir.path().join("Crawfish.toml").exists());
    assert!(dir.path().join("agents/repo_reviewer.toml").exists());
}

#[test]
fn run_status_and_policy_validate_work() {
    let dir = tempdir().unwrap();
    fs::create_dir_all(dir.path().join("agents")).unwrap();
    fs::create_dir_all(dir.path().join(".crawfish/state")).unwrap();
    fs::write(
        dir.path().join("Crawfish.toml"),
        r#"[storage]
sqlite_path = ".crawfish/state/control.db"
state_dir = ".crawfish/state"

[fleet]
manifests_dir = "agents"
"#,
    )
    .unwrap();
    fs::write(
        dir.path().join("agents/repo_indexer.toml"),
        include_str!("../../../examples/hero-fleet/agents/repo_indexer.toml"),
    )
    .unwrap();
    fs::write(
        dir.path().join("agents/repo_reviewer.toml"),
        include_str!("../../../examples/hero-fleet/agents/repo_reviewer.toml"),
    )
    .unwrap();

    Command::new(CRAWFISH_BIN)
        .current_dir(dir.path())
        .args(["run", "--once"])
        .assert()
        .success();

    Command::new(CRAWFISH_BIN)
        .current_dir(dir.path())
        .args(["status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("repo_reviewer"));

    Command::new(CRAWFISH_BIN)
        .current_dir(dir.path())
        .args([
            "policy",
            "validate",
            "--target-agent",
            "repo_reviewer",
            "--caller-owner",
            "foreign-user",
            "--capability",
            "repo.review",
            "--workspace-write",
            "--mutating",
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"disposition\": \"deny\""));
}

use assert_cmd::Command;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyperlocal::UnixClientExt;
use predicates::prelude::*;
use std::fs;
use std::path::Path;
use std::process::Child;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Runtime;

const CRAWFISH_BIN: &str = env!("CARGO_BIN_EXE_crawfish");
const CRAWFISHD_BIN: &str = env!("CARGO_BIN_EXE_crawfishd");

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
fn daemon_backed_cli_supports_status_health_and_submit() {
    let dir = tempdir().unwrap();
    fs::create_dir_all(dir.path().join("agents")).unwrap();
    fs::create_dir_all(dir.path().join(".crawfish/state")).unwrap();
    fs::create_dir_all(dir.path().join(".crawfish/run")).unwrap();
    fs::write(
        dir.path().join("Crawfish.toml"),
        r#"[storage]
sqlite_path = ".crawfish/state/control.db"
state_dir = ".crawfish/state"

[fleet]
manifests_dir = "agents"

[api]
socket_path = ".crawfish/run/crawfishd.sock"

[runtime]
reconcile_interval_ms = 100
"#,
    )
    .unwrap();
    for agent in [
        "repo_indexer",
        "repo_reviewer",
        "ci_triage",
        "incident_enricher",
    ] {
        fs::write(
            dir.path().join(format!("agents/{agent}.toml")),
            fs::read_to_string(format!(
                "{}/../../examples/hero-fleet/agents/{agent}.toml",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
    }

    let socket_path = dir.path().join(".crawfish/run/crawfishd.sock");
    let mut daemon = spawn_daemon(dir.path());
    wait_for_socket(&socket_path);

    let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
    let uri: Uri = hyperlocal::Uri::new(&socket_path, "/v1/health").into();
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let runtime = Runtime::new().unwrap();
    let response = runtime
        .block_on(async { client.request(request).await })
        .unwrap();
    let body = runtime
        .block_on(async { response.into_body().collect().await })
        .unwrap()
        .to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("\"status\":\"ok\""));

    Command::new(CRAWFISH_BIN)
        .current_dir(dir.path())
        .args(["status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("repo_reviewer"));

    Command::new(CRAWFISH_BIN)
        .current_dir(dir.path())
        .args([
            "action",
            "submit",
            "--target-agent",
            "repo_reviewer",
            "--capability",
            "repo.review",
            "--goal",
            "review pull request",
            "--caller-owner",
            "local-dev",
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"action_id\""));

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

    daemon.kill().unwrap();
    daemon.wait().unwrap();
}

fn spawn_daemon(dir: &Path) -> Child {
    std::process::Command::new(CRAWFISHD_BIN)
        .current_dir(dir)
        .arg("--config")
        .arg("Crawfish.toml")
        .spawn()
        .unwrap()
}

fn wait_for_socket(path: &Path) {
    for _ in 0..50 {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("socket did not appear at {}", path.display());
}

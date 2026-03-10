use bytes::Bytes;
use clap::{Args, Parser, Subcommand, ValueEnum};
use crawfish_core::{
    ActionDetail, AdminActionResponse, AgentDetail, CrawfishConfig, ExecutionContractPatch,
    FleetStatusResponse, PolicyValidationRequest, PolicyValidationResponse, SubmitActionRequest,
    SubmittedAction,
};
use crawfish_runtime::Supervisor;
use crawfish_types::{
    CounterpartyRef, GoalSpec, Metadata, OwnerKind, OwnerRef, RequesterKind, RequesterRef,
    TrustDomain,
};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyperlocal::{UnixClientExt, UnixConnector};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(name = "crawfish", version, about = "Crawfish operator CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Init(InitCommand),
    Run(RunCommand),
    Status(StatusCommand),
    Inspect(InspectCommand),
    Drain(ConfigCommand),
    Resume(ConfigCommand),
    Policy(PolicyCommand),
    Action(ActionCommand),
}

#[derive(Debug, Subcommand)]
pub enum ActionSubcommands {
    Submit(SubmitActionCommand),
}

#[derive(Debug, Args)]
pub struct ActionCommand {
    #[command(subcommand)]
    pub command: ActionSubcommands,
}

#[derive(Debug, Args)]
pub struct InitCommand {
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

#[derive(Debug, Args, Clone)]
pub struct ConfigCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
}

#[derive(Debug, Args)]
pub struct RunCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub once: bool,
}

#[derive(Debug, Args)]
pub struct StatusCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct InspectCommand {
    pub id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum PolicySubcommands {
    Validate(ValidatePolicyCommand),
}

#[derive(Debug, Args)]
pub struct PolicyCommand {
    #[command(subcommand)]
    pub command: PolicySubcommands,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OwnerKindArg {
    Human,
    Team,
    Org,
    ServiceAccount,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum TrustDomainArg {
    SameOwnerLocal,
    SameDeviceForeignOwner,
    InternalOrg,
    ExternalPartner,
    PublicUnknown,
}

#[derive(Debug, Args)]
pub struct ValidatePolicyCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub target_agent: String,
    #[arg(long)]
    pub caller_owner: String,
    #[arg(long, value_enum, default_value = "human")]
    pub caller_kind: OwnerKindArg,
    #[arg(long)]
    pub capability: String,
    #[arg(long, value_enum, default_value = "same-device-foreign-owner")]
    pub trust_domain: TrustDomainArg,
    #[arg(long)]
    pub workspace_write: bool,
    #[arg(long)]
    pub secret_access: bool,
    #[arg(long)]
    pub mutating: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SubmitActionCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub target_agent: String,
    #[arg(long)]
    pub capability: String,
    #[arg(long)]
    pub goal: String,
    #[arg(long)]
    pub caller_owner: String,
    #[arg(long, value_enum, default_value = "human")]
    pub caller_kind: OwnerKindArg,
    #[arg(long, value_enum, default_value = "same-owner-local")]
    pub trust_domain: TrustDomainArg,
    #[arg(long)]
    pub inputs_json: Option<String>,
    #[arg(long)]
    pub inputs_file: Option<PathBuf>,
    #[arg(long)]
    pub contract_json: Option<String>,
    #[arg(long)]
    pub contract_file: Option<PathBuf>,
    #[arg(long)]
    pub workspace_write: bool,
    #[arg(long)]
    pub secret_access: bool,
    #[arg(long)]
    pub mutating: bool,
    #[arg(long)]
    pub json: bool,
}

impl From<OwnerKindArg> for OwnerKind {
    fn from(value: OwnerKindArg) -> Self {
        match value {
            OwnerKindArg::Human => OwnerKind::Human,
            OwnerKindArg::Team => OwnerKind::Team,
            OwnerKindArg::Org => OwnerKind::Org,
            OwnerKindArg::ServiceAccount => OwnerKind::ServiceAccount,
        }
    }
}

impl From<TrustDomainArg> for TrustDomain {
    fn from(value: TrustDomainArg) -> Self {
        match value {
            TrustDomainArg::SameOwnerLocal => TrustDomain::SameOwnerLocal,
            TrustDomainArg::SameDeviceForeignOwner => TrustDomain::SameDeviceForeignOwner,
            TrustDomainArg::InternalOrg => TrustDomain::InternalOrg,
            TrustDomainArg::ExternalPartner => TrustDomain::ExternalPartner,
            TrustDomainArg::PublicUnknown => TrustDomain::PublicUnknown,
        }
    }
}

pub async fn run_cli() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();

    match cli.command {
        Commands::Init(command) => init_workspace(&command.path),
        Commands::Run(command) => run_command(command).await,
        Commands::Status(command) => status_command(command).await,
        Commands::Inspect(command) => inspect_command(command).await,
        Commands::Drain(command) => drain_command(command).await,
        Commands::Resume(command) => resume_command(command).await,
        Commands::Policy(policy) => match policy.command {
            PolicySubcommands::Validate(command) => validate_policy_command(command).await,
        },
        Commands::Action(action) => match action.command {
            ActionSubcommands::Submit(command) => submit_action_command(command).await,
        },
    }
}

pub async fn run_daemon(config: PathBuf, once: bool) -> anyhow::Result<()> {
    init_tracing();
    let supervisor = Arc::new(Supervisor::from_config_path(&config).await?);
    if once {
        supervisor.run_once().await
    } else {
        supervisor.run_until_signal().await
    }
}

fn init_workspace(path: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(path.join("agents"))?;
    fs::create_dir_all(path.join(".crawfish/state"))?;
    fs::create_dir_all(path.join(".crawfish/run"))?;
    write_if_missing(&path.join("Crawfish.toml"), ROOT_CONFIG_TEMPLATE)?;
    write_if_missing(
        &path.join("agents/repo_indexer.toml"),
        include_str!("../../../examples/hero-fleet/agents/repo_indexer.toml"),
    )?;
    write_if_missing(
        &path.join("agents/repo_reviewer.toml"),
        include_str!("../../../examples/hero-fleet/agents/repo_reviewer.toml"),
    )?;
    write_if_missing(
        &path.join("agents/ci_triage.toml"),
        include_str!("../../../examples/hero-fleet/agents/ci_triage.toml"),
    )?;
    write_if_missing(
        &path.join("agents/incident_enricher.toml"),
        include_str!("../../../examples/hero-fleet/agents/incident_enricher.toml"),
    )?;
    println!("initialized Crawfish workspace at {}", path.display());
    Ok(())
}

async fn run_command(command: RunCommand) -> anyhow::Result<()> {
    run_daemon(command.config, command.once).await
}

async fn status_command(command: StatusCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let status: FleetStatusResponse = client.get_json("/v1/agents").await?;
    if command.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!(
            "queue\taccepted={}\trunning={}\tblocked={}\tcompleted={}\tfailed={}",
            status.queue.accepted,
            status.queue.running,
            status.queue.blocked,
            status.queue.completed,
            status.queue.failed
        );
        for record in status.agents {
            println!(
                "{}\tdesired={:?}\tobserved={:?}\thealth={:?}\tdegraded={:?}\tcontinuity={:?}",
                record.agent_id,
                record.desired_state,
                record.observed_state,
                record.health,
                record.degradation_profile,
                record.continuity_mode
            );
        }
    }
    Ok(())
}

async fn inspect_command(command: InspectCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    if let Ok(agent) = client
        .get_json::<AgentDetail>(&format!("/v1/agents/{}", command.id))
        .await
    {
        print_output(serde_json::to_value(agent)?, command.json)?;
        return Ok(());
    }

    let action: ActionDetail = client
        .get_json(&format!("/v1/actions/{}", command.id))
        .await?;
    print_output(serde_json::to_value(action)?, command.json)?;
    Ok(())
}

async fn drain_command(command: ConfigCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: AdminActionResponse = client
        .post_json("/v1/admin/drain", &serde_json::json!({}))
        .await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

async fn resume_command(command: ConfigCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: AdminActionResponse = client
        .post_json("/v1/admin/resume", &serde_json::json!({}))
        .await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

async fn validate_policy_command(command: ValidatePolicyCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let request = PolicyValidationRequest {
        target_agent_id: command.target_agent,
        caller: CounterpartyRef {
            agent_id: None,
            session_id: Some("cli".to_string()),
            owner: OwnerRef {
                kind: command.caller_kind.into(),
                id: command.caller_owner,
                display_name: None,
            },
            trust_domain: command.trust_domain.into(),
        },
        capability: command.capability,
        workspace_write: command.workspace_write,
        secret_access: command.secret_access,
        mutating: command.mutating,
    };
    let result: PolicyValidationResponse =
        client.post_json("/v1/policy/validate", &request).await?;
    print_output(serde_json::to_value(result)?, command.json)?;
    Ok(())
}

async fn submit_action_command(command: SubmitActionCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let owner = OwnerRef {
        kind: command.caller_kind.into(),
        id: command.caller_owner,
        display_name: None,
    };
    let request = SubmitActionRequest {
        target_agent_id: command.target_agent,
        requester: RequesterRef {
            kind: RequesterKind::User,
            id: "cli".to_string(),
        },
        initiator_owner: owner.clone(),
        capability: command.capability,
        goal: GoalSpec {
            summary: command.goal,
            details: None,
        },
        inputs: load_metadata(command.inputs_json, command.inputs_file)?,
        contract_overrides: load_contract_patch(command.contract_json, command.contract_file)?,
        execution_strategy: None,
        schedule: None,
        counterparty_refs: vec![CounterpartyRef {
            agent_id: None,
            session_id: Some("cli".to_string()),
            owner,
            trust_domain: command.trust_domain.into(),
        }],
        data_boundary: None,
        workspace_write: command.workspace_write,
        secret_access: command.secret_access,
        mutating: command.mutating,
    };
    let submitted: SubmittedAction = client.post_json("/v1/actions", &request).await?;
    print_output(serde_json::to_value(submitted)?, command.json)?;
    Ok(())
}

fn print_output(value: serde_json::Value, _json: bool) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn load_metadata(
    inline_json: Option<String>,
    json_file: Option<PathBuf>,
) -> anyhow::Result<Metadata> {
    if let Some(value) = inline_json {
        return Ok(serde_json::from_str(&value)?);
    }
    if let Some(path) = json_file {
        return Ok(serde_json::from_str(&fs::read_to_string(path)?)?);
    }
    Ok(Metadata::new())
}

fn load_contract_patch(
    inline_json: Option<String>,
    file: Option<PathBuf>,
) -> anyhow::Result<Option<ExecutionContractPatch>> {
    if let Some(value) = inline_json {
        return Ok(Some(serde_json::from_str(&value)?));
    }
    if let Some(path) = file {
        let contents = fs::read_to_string(&path)?;
        if path.extension().and_then(|ext| ext.to_str()) == Some("toml") {
            return Ok(Some(toml::from_str(&contents)?));
        }
        return Ok(Some(serde_json::from_str(&contents)?));
    }
    Ok(None)
}

fn write_if_missing(path: &Path, contents: &str) -> anyhow::Result<()> {
    if !path.exists() {
        fs::write(path, contents)?;
    }
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .try_init();
}

struct DaemonClient {
    socket_path: PathBuf,
    client: Client<UnixConnector, Full<Bytes>>,
}

impl DaemonClient {
    fn from_config(config_path: &Path) -> anyhow::Result<Self> {
        let root = config_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let config = CrawfishConfig::load(config_path)?;
        Ok(Self {
            socket_path: config.socket_path(&root),
            client: Client::unix(),
        })
    }

    async fn get_json<T>(&self, path: &str) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let request = Request::builder()
            .method(Method::GET)
            .uri(self.uri(path))
            .body(Full::new(Bytes::new()))?;
        self.send(request).await
    }

    async fn post_json<T, B>(&self, path: &str, body: &B) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let payload = serde_json::to_vec(body)?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(self.uri(path))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload)))?;
        self.send(request).await
    }

    async fn send<T>(&self, request: Request<Full<Bytes>>) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self.client.request(request).await?;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        if !status.is_success() {
            let payload: serde_json::Value = serde_json::from_slice(&body)
                .unwrap_or_else(|_| serde_json::json!({"error": String::from_utf8_lossy(&body)}));
            anyhow::bail!("daemon request failed with {status}: {payload}");
        }
        Ok(serde_json::from_slice(&body)?)
    }

    fn uri(&self, path: &str) -> Uri {
        hyperlocal::Uri::new(&self.socket_path, path).into()
    }
}

const ROOT_CONFIG_TEMPLATE: &str = r#"[storage]
sqlite_path = ".crawfish/state/control.db"
state_dir = ".crawfish/state"

[fleet]
manifests_dir = "agents"

[api]
socket_path = ".crawfish/run/crawfishd.sock"

[runtime]
reconcile_interval_ms = 5000
"#;

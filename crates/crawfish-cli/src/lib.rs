use clap::{Args, Parser, Subcommand, ValueEnum};
use crawfish_core::SupervisorControl;
use crawfish_runtime::Supervisor;
use crawfish_types::{CounterpartyRef, OwnerKind, OwnerRef, TrustDomain};
use std::fs;
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, ValueEnum)]
pub enum TrustDomainArg {
    SameOwnerLocal,
    SameDeviceForeignOwner,
    InternalOrg,
    ExternalPartner,
    PublicUnknown,
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
    }
}

pub async fn run_daemon(config: PathBuf, once: bool) -> anyhow::Result<()> {
    init_tracing();
    let supervisor = Supervisor::from_config_path(&config).await?;
    if once {
        supervisor.run_once().await
    } else {
        supervisor.run_until_signal().await
    }
}

fn init_workspace(path: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(path.join("agents"))?;
    fs::create_dir_all(path.join(".crawfish/state"))?;
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
    let supervisor = Supervisor::from_config_path(&command.config).await?;
    if command.once {
        supervisor.run_once().await
    } else {
        supervisor.run_until_signal().await
    }
}

async fn status_command(command: StatusCommand) -> anyhow::Result<()> {
    let supervisor = Supervisor::from_config_path(&command.config).await?;
    let records = supervisor.list_status().await?;
    if command.json {
        println!("{}", serde_json::to_string_pretty(&records)?);
    } else {
        for record in records {
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
    let supervisor = Supervisor::from_config_path(&command.config).await?;
    if let Some((manifest, lifecycle)) = supervisor.inspect_agent(&command.id).await? {
        let output = serde_json::json!({
            "kind": "agent",
            "id": manifest.id,
            "owner": manifest.owner,
            "trust_domain": manifest.trust_domain,
            "role": manifest.role,
            "capabilities": manifest.capabilities,
            "dependencies": manifest.dependencies,
            "desired_state": lifecycle.desired_state,
            "observed_state": lifecycle.observed_state,
            "health": lifecycle.health,
            "degradation_profile": lifecycle.degradation_profile,
            "continuity_mode": lifecycle.continuity_mode,
            "transition_reason": lifecycle.transition_reason,
        });
        print_output(output, command.json)?;
        return Ok(());
    }

    if let Some((action, encounter, receipt)) = supervisor.inspect_action(&command.id).await? {
        let output = serde_json::json!({
            "kind": "action",
            "id": action.id,
            "capability": action.capability,
            "phase": action.phase,
            "contract": action.contract,
            "checkpoint_ref": action.checkpoint_ref,
            "lease_ref": action.lease_ref,
            "grant_refs": action.grant_refs,
            "encounter": encounter,
            "audit_receipt": receipt,
        });
        print_output(output, command.json)?;
        return Ok(());
    }

    anyhow::bail!("no agent or action found with id {}", command.id);
}

async fn drain_command(command: ConfigCommand) -> anyhow::Result<()> {
    let supervisor = Supervisor::from_config_path(&command.config).await?;
    supervisor.drain().await?;
    println!("fleet drained");
    Ok(())
}

async fn resume_command(command: ConfigCommand) -> anyhow::Result<()> {
    let supervisor = Supervisor::from_config_path(&command.config).await?;
    supervisor.resume().await?;
    println!("fleet resumed");
    Ok(())
}

async fn validate_policy_command(command: ValidatePolicyCommand) -> anyhow::Result<()> {
    let supervisor = Supervisor::from_config_path(&command.config).await?;
    let result = supervisor
        .validate_policy(
            &command.target_agent,
            CounterpartyRef {
                agent_id: None,
                session_id: Some("cli".to_string()),
                owner: OwnerRef {
                    kind: command.caller_kind.into(),
                    id: command.caller_owner,
                    display_name: None,
                },
                trust_domain: command.trust_domain.into(),
            },
            command.capability,
            command.workspace_write,
            command.secret_access,
            command.mutating,
        )
        .await?;

    let output = serde_json::json!(result);
    print_output(output, command.json)?;
    Ok(())
}

fn print_output(value: serde_json::Value, _json: bool) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
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

const ROOT_CONFIG_TEMPLATE: &str = r#"[storage]
sqlite_path = ".crawfish/state/control.db"
state_dir = ".crawfish/state"

[fleet]
manifests_dir = "agents"

[runtime]
reconcile_interval_ms = 5000
"#;

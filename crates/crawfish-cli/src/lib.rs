use bytes::Bytes;
use clap::{Args, Parser, Subcommand, ValueEnum};
use crawfish_core::{
    AcknowledgeAlertRequest, AcknowledgeAlertResponse, ActionDetail, ActionEvaluationsResponse,
    ActionEventsResponse, ActionListResponse, ActionTraceResponse, AdminActionResponse,
    AgentDetail, AlertListResponse, ApproveActionRequest, CrawfishConfig,
    EvaluationDatasetDetailResponse, EvaluationDatasetsResponse, ExecutionContractPatch,
    ExperimentRunDetailResponse, PolicyValidationRequest, PolicyValidationResponse,
    RejectActionRequest, ResolveReviewQueueItemRequest, ResolveReviewQueueItemResponse,
    ReviewQueueResponse, RevokeLeaseRequest, StartEvaluationRunRequest, StartEvaluationRunResponse,
    SubmitActionRequest, SubmittedAction, SwarmStatusResponse, TreatyDetailResponse,
    TreatyListResponse,
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
    Lease(LeaseCommand),
    Review(ReviewCommand),
    Eval(EvalCommand),
    Alert(AlertCommand),
    Treaty(TreatyCommand),
}

#[derive(Debug, Subcommand)]
pub enum ActionSubcommands {
    List(ListActionsCommand),
    Events(ActionEventsCommand),
    Trace(ActionTraceCommand),
    Evals(ActionEvaluationsCommand),
    Submit(SubmitActionCommand),
    Approve(ApproveActionCommand),
    Reject(RejectActionCommand),
}

#[derive(Debug, Args)]
pub struct ActionCommand {
    #[command(subcommand)]
    pub command: ActionSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum LeaseSubcommands {
    Revoke(RevokeLeaseCommand),
}

#[derive(Debug, Args)]
pub struct LeaseCommand {
    #[command(subcommand)]
    pub command: LeaseSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum ReviewSubcommands {
    List(ListReviewQueueCommand),
    Resolve(ResolveReviewQueueCommand),
}

#[derive(Debug, Args)]
pub struct ReviewCommand {
    #[command(subcommand)]
    pub command: ReviewSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum EvalSubcommands {
    Dataset(EvalDatasetCommand),
    Run(EvalRunCommand),
    RunStatus(EvalRunStatusCommand),
}

#[derive(Debug, Args)]
pub struct EvalCommand {
    #[command(subcommand)]
    pub command: EvalSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum EvalDatasetSubcommands {
    List(EvalDatasetListCommand),
    Show(EvalDatasetShowCommand),
}

#[derive(Debug, Args)]
pub struct EvalDatasetCommand {
    #[command(subcommand)]
    pub command: EvalDatasetSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum AlertSubcommands {
    List(AlertListCommand),
    Ack(AlertAckCommand),
}

#[derive(Debug, Args)]
pub struct AlertCommand {
    #[command(subcommand)]
    pub command: AlertSubcommands,
}

#[derive(Debug, Subcommand)]
pub enum TreatySubcommands {
    List(TreatyListCommand),
    Show(TreatyShowCommand),
}

#[derive(Debug, Args)]
pub struct TreatyCommand {
    #[command(subcommand)]
    pub command: TreatySubcommands,
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

#[derive(Debug, Args)]
pub struct ListActionsCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub phase: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ActionEventsCommand {
    pub action_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ActionTraceCommand {
    pub action_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ActionEvaluationsCommand {
    pub action_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ApproveActionCommand {
    pub action_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub approver: String,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RejectActionCommand {
    pub action_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub approver: String,
    #[arg(long)]
    pub reason: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RevokeLeaseCommand {
    pub lease_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub revoker: String,
    #[arg(long)]
    pub reason: String,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ListReviewQueueCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ResolveReviewQueueCommand {
    pub review_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub resolver: String,
    #[arg(long)]
    pub resolution: String,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct EvalDatasetListCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct EvalDatasetShowCommand {
    pub dataset: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct EvalRunCommand {
    pub dataset: String,
    #[arg(long)]
    pub executor: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct EvalRunStatusCommand {
    pub run_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct AlertListCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct AlertAckCommand {
    pub alert_id: String,
    #[arg(long)]
    pub actor: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct TreatyListCommand {
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct TreatyShowCommand {
    pub treaty_id: String,
    #[arg(long, default_value = "Crawfish.toml")]
    pub config: PathBuf,
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
            ActionSubcommands::List(command) => list_actions_command(command).await,
            ActionSubcommands::Events(command) => action_events_command(command).await,
            ActionSubcommands::Trace(command) => action_trace_command(command).await,
            ActionSubcommands::Evals(command) => action_evaluations_command(command).await,
            ActionSubcommands::Submit(command) => submit_action_command(command).await,
            ActionSubcommands::Approve(command) => approve_action_command(command).await,
            ActionSubcommands::Reject(command) => reject_action_command(command).await,
        },
        Commands::Lease(lease) => match lease.command {
            LeaseSubcommands::Revoke(command) => revoke_lease_command(command).await,
        },
        Commands::Review(review) => match review.command {
            ReviewSubcommands::List(command) => list_review_queue_command(command).await,
            ReviewSubcommands::Resolve(command) => resolve_review_queue_command(command).await,
        },
        Commands::Eval(eval) => match eval.command {
            EvalSubcommands::Dataset(command) => match command.command {
                EvalDatasetSubcommands::List(command) => eval_dataset_list_command(command).await,
                EvalDatasetSubcommands::Show(command) => eval_dataset_show_command(command).await,
            },
            EvalSubcommands::Run(command) => eval_run_command(command).await,
            EvalSubcommands::RunStatus(command) => eval_run_status_command(command).await,
        },
        Commands::Alert(alert) => match alert.command {
            AlertSubcommands::List(command) => alert_list_command(command).await,
            AlertSubcommands::Ack(command) => alert_ack_command(command).await,
        },
        Commands::Treaty(treaty) => match treaty.command {
            TreatySubcommands::List(command) => treaty_list_command(command).await,
            TreatySubcommands::Show(command) => treaty_show_command(command).await,
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
        include_str!("../../../examples/hero-swarm/agents/repo_indexer.toml"),
    )?;
    write_if_missing(
        &path.join("agents/repo_reviewer.toml"),
        include_str!("../../../examples/hero-swarm/agents/repo_reviewer.toml"),
    )?;
    write_if_missing(
        &path.join("agents/ci_triage.toml"),
        include_str!("../../../examples/hero-swarm/agents/ci_triage.toml"),
    )?;
    write_if_missing(
        &path.join("agents/incident_enricher.toml"),
        include_str!("../../../examples/hero-swarm/agents/incident_enricher.toml"),
    )?;
    write_if_missing(
        &path.join("agents/task_planner.toml"),
        include_str!("../../../examples/hero-swarm/agents/task_planner.toml"),
    )?;
    write_if_missing(
        &path.join("agents/workspace_editor.toml"),
        include_str!("../../../examples/hero-swarm/agents/workspace_editor.toml"),
    )?;
    println!("initialized Crawfish workspace at {}", path.display());
    Ok(())
}

async fn run_command(command: RunCommand) -> anyhow::Result<()> {
    run_daemon(command.config, command.once).await
}

async fn status_command(command: StatusCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let status: SwarmStatusResponse = client.get_json("/v1/agents").await?;
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

async fn list_actions_command(command: ListActionsCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let mut path = "/v1/actions".to_string();
    if let Some(phase) = &command.phase {
        path.push_str("?phase=");
        path.push_str(phase);
    }
    let actions: ActionListResponse = client.get_json(&path).await?;
    print_output(serde_json::to_value(actions)?, command.json)?;
    Ok(())
}

async fn action_events_command(command: ActionEventsCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: ActionEventsResponse = client
        .get_json(&format!("/v1/actions/{}/events", command.action_id))
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn action_trace_command(command: ActionTraceCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: ActionTraceResponse = client
        .get_json(&format!("/v1/actions/{}/trace", command.action_id))
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn action_evaluations_command(command: ActionEvaluationsCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: ActionEvaluationsResponse = client
        .get_json(&format!("/v1/actions/{}/evaluations", command.action_id))
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn approve_action_command(command: ApproveActionCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: SubmittedAction = client
        .post_json(
            &format!("/v1/actions/{}/approve", command.action_id),
            &ApproveActionRequest {
                approver_ref: command.approver,
                note: command.note,
            },
        )
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn reject_action_command(command: RejectActionCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: SubmittedAction = client
        .post_json(
            &format!("/v1/actions/{}/reject", command.action_id),
            &RejectActionRequest {
                approver_ref: command.approver,
                reason: command.reason,
            },
        )
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn revoke_lease_command(command: RevokeLeaseCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: AdminActionResponse = client
        .post_json(
            &format!("/v1/leases/{}/revoke", command.lease_id),
            &RevokeLeaseRequest {
                revoker_ref: command.revoker,
                reason: command.reason,
            },
        )
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn list_review_queue_command(command: ListReviewQueueCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: ReviewQueueResponse = client.get_json("/v1/review-queue").await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn resolve_review_queue_command(command: ResolveReviewQueueCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: ResolveReviewQueueItemResponse = client
        .post_json(
            &format!("/v1/review-queue/{}/resolve", command.review_id),
            &ResolveReviewQueueItemRequest {
                resolver_ref: command.resolver,
                resolution: command.resolution,
                note: command.note,
            },
        )
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn eval_dataset_list_command(command: EvalDatasetListCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: EvaluationDatasetsResponse = client.get_json("/v1/evaluation/datasets").await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn eval_dataset_show_command(command: EvalDatasetShowCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: EvaluationDatasetDetailResponse = client
        .get_json(&format!("/v1/evaluation/datasets/{}", command.dataset))
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn eval_run_command(command: EvalRunCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: StartEvaluationRunResponse = client
        .post_json(
            "/v1/evaluation/runs",
            &StartEvaluationRunRequest {
                dataset: command.dataset,
                executor: command.executor,
            },
        )
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn eval_run_status_command(command: EvalRunStatusCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: ExperimentRunDetailResponse = client
        .get_json(&format!("/v1/evaluation/runs/{}", command.run_id))
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn alert_list_command(command: AlertListCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: AlertListResponse = client.get_json("/v1/alerts").await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn alert_ack_command(command: AlertAckCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: AcknowledgeAlertResponse = client
        .post_json(
            &format!("/v1/alerts/{}/ack", command.alert_id),
            &AcknowledgeAlertRequest {
                actor: command.actor,
            },
        )
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn treaty_list_command(command: TreatyListCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: TreatyListResponse = client.get_json("/v1/treaties").await?;
    print_output(serde_json::to_value(response)?, command.json)?;
    Ok(())
}

async fn treaty_show_command(command: TreatyShowCommand) -> anyhow::Result<()> {
    let client = DaemonClient::from_config(&command.config)?;
    let response: TreatyDetailResponse = client
        .get_json(&format!("/v1/treaties/{}", command.treaty_id))
        .await?;
    print_output(serde_json::to_value(response)?, command.json)?;
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

[swarm]
manifests_dir = "agents"

[api]
socket_path = ".crawfish/run/crawfishd.sock"

[openclaw.inbound]
enabled = false
caller_owner_mapping = "required"
default_trust_domain = "same_device_foreign_owner"

[runtime]
reconcile_interval_ms = 5000

[evaluation]
# Built-in profiles are resolved automatically for the current task-plan,
# repo-review, and incident-enrichment reference paths.
"#;

use crawfish_core::{
    authorize_encounter, compile_execution_plan, now_timestamp, owner_policy_for_manifest,
    ActionStore, CrawfishConfig, EncounterDecision, EncounterDisposition, EncounterRequest,
    ExecutionContractPatch, GovernanceContext, SupervisorControl,
};
use crawfish_store_sqlite::SqliteStore;
use crawfish_types::{
    Action, AgentManifest, AgentState, AuditOutcome, AuditReceipt, CapabilityDescriptor,
    CounterpartyRef, DegradedProfileName, EncounterRecord, EncounterState, HealthStatus,
    LifecycleRecord, Mutability, TrustDomain,
};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::time::{sleep, Duration};
use tracing::info;
use uuid::Uuid;

pub struct Supervisor {
    root: PathBuf,
    config: CrawfishConfig,
    store: SqliteStore,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PolicyValidationResult {
    pub disposition: String,
    pub reason: String,
    pub trust_domain: TrustDomain,
    pub target_agent_id: String,
}

impl Supervisor {
    pub async fn from_config_path(path: &Path) -> anyhow::Result<Self> {
        let root = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let config = CrawfishConfig::load(path)?;
        let state_dir = config.state_dir(&root);
        let sqlite_path = config.sqlite_path(&root);
        let store = SqliteStore::connect(&sqlite_path, &state_dir).await?;
        Ok(Self {
            root,
            config,
            store,
        })
    }

    pub async fn run_once(&self) -> anyhow::Result<()> {
        for manifest in self.load_manifests()? {
            self.store.upsert_agent_manifest(&manifest).await?;
            let record = self.reconcile_manifest(&manifest)?;
            self.store.upsert_lifecycle_record(&record).await?;
        }

        info!("reconciled manifests into lifecycle state");
        Ok(())
    }

    pub async fn run_until_signal(&self) -> anyhow::Result<()> {
        loop {
            self.run_once().await?;
            tokio::select! {
                _ = sleep(Duration::from_millis(self.config.runtime.reconcile_interval_ms)) => {}
                result = tokio::signal::ctrl_c() => {
                    result?;
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn validate_policy(
        &self,
        target_agent_id: &str,
        caller: CounterpartyRef,
        capability: String,
        requests_workspace_write: bool,
        requests_secret_access: bool,
        requests_mutating_capability: bool,
    ) -> anyhow::Result<PolicyValidationResult> {
        let manifest = self
            .store
            .get_agent_manifest(target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {target_agent_id}"))?;

        let request = EncounterRequest {
            caller: caller.clone(),
            target_agent_id: target_agent_id.to_string(),
            target_owner: manifest.owner.clone(),
            requested_capabilities: vec![capability],
            requests_workspace_write,
            requests_secret_access,
            requests_mutating_capability,
        };
        let decision = self.authorize(&manifest, &request);

        let encounter = EncounterRecord {
            id: Uuid::new_v4().to_string(),
            initiator_ref: caller,
            target_agent_id: target_agent_id.to_string(),
            target_owner: manifest.owner.clone(),
            trust_domain: manifest.trust_domain.clone(),
            requested_capabilities: request.requested_capabilities.clone(),
            applied_policy_source: "system>owner>trust-domain>manifest".to_string(),
            state: match decision.disposition {
                EncounterDisposition::Deny => EncounterState::Denied,
                EncounterDisposition::AwaitConsent => EncounterState::AwaitingConsent,
                EncounterDisposition::IssueLease => EncounterState::Leased,
            },
            grant_refs: Vec::new(),
            lease_ref: None,
            created_at: now_timestamp(),
        };
        self.store.insert_encounter(&encounter).await?;
        self.store
            .insert_audit_receipt(&AuditReceipt {
                id: Uuid::new_v4().to_string(),
                encounter_ref: encounter.id.clone(),
                grant_refs: Vec::new(),
                lease_ref: None,
                outcome: match decision.disposition {
                    EncounterDisposition::Deny => AuditOutcome::Denied,
                    EncounterDisposition::AwaitConsent => AuditOutcome::Allowed,
                    EncounterDisposition::IssueLease => AuditOutcome::Allowed,
                },
                reason: decision.reason.clone(),
                approver_ref: None,
                emitted_at: now_timestamp(),
            })
            .await?;

        Ok(PolicyValidationResult {
            disposition: format!("{:?}", decision.disposition).to_lowercase(),
            reason: decision.reason,
            trust_domain: manifest.trust_domain,
            target_agent_id: target_agent_id.to_string(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &CrawfishConfig {
        &self.config
    }

    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    fn load_manifests(&self) -> anyhow::Result<Vec<AgentManifest>> {
        let manifests_dir = self.config.manifest_dir(&self.root);
        let mut entries = fs::read_dir(manifests_dir)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());

        let mut manifests = Vec::new();
        for entry in entries {
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("toml") {
                continue;
            }
            let contents = fs::read_to_string(entry.path())?;
            let manifest: AgentManifest = toml::from_str(&contents)?;
            manifests.push(manifest);
        }
        Ok(manifests)
    }

    fn reconcile_manifest(&self, manifest: &AgentManifest) -> anyhow::Result<LifecycleRecord> {
        let dependency_missing = manifest
            .dependencies
            .iter()
            .any(|dependency| !self.manifest_exists(dependency));

        let (observed_state, health, degradation_profile) = if dependency_missing {
            (
                AgentState::Degraded,
                HealthStatus::Degraded,
                Some(DegradedProfileName::DependencyIsolation),
            )
        } else {
            (AgentState::Active, HealthStatus::Healthy, None)
        };

        let compiled = compile_execution_plan(
            &self.config.contracts.org_defaults,
            &manifest.contract_defaults,
            &ExecutionContractPatch::default(),
            &manifest.strategy_defaults,
            manifest
                .capabilities
                .first()
                .map(String::as_str)
                .unwrap_or("default"),
            None,
        )?;

        let continuity_mode = if compiled.contract.execution.preferred_harnesses.is_empty() {
            Some(crawfish_types::ContinuityModeName::DeterministicOnly)
        } else {
            None
        };

        Ok(LifecycleRecord {
            agent_id: manifest.id.clone(),
            desired_state: AgentState::Active,
            observed_state,
            health,
            transition_reason: if dependency_missing {
                Some("dependency missing during reconcile".to_string())
            } else {
                Some("reconciled successfully".to_string())
            },
            last_transition_at: now_timestamp(),
            degradation_profile,
            continuity_mode,
            failure_count: 0,
        })
    }

    fn manifest_exists(&self, dependency: &str) -> bool {
        let path = self
            .config
            .manifest_dir(&self.root)
            .join(format!("{dependency}.toml"));
        path.exists()
    }

    fn authorize(&self, manifest: &AgentManifest, request: &EncounterRequest) -> EncounterDecision {
        authorize_encounter(
            &GovernanceContext {
                system_defaults: self.config.governance.system_defaults.clone(),
                owner_policy: owner_policy_for_manifest(manifest),
                trust_domain_defaults: trust_domain_defaults(request.caller.trust_domain.clone()),
                manifest_policy: manifest.encounter_policy.clone(),
            },
            request,
        )
    }
}

#[async_trait::async_trait]
impl SupervisorControl for Supervisor {
    async fn list_status(&self) -> anyhow::Result<Vec<LifecycleRecord>> {
        self.store.list_lifecycle_records().await
    }

    async fn inspect_agent(
        &self,
        agent_id: &str,
    ) -> anyhow::Result<Option<(AgentManifest, LifecycleRecord)>> {
        let manifest = self.store.get_agent_manifest(agent_id).await?;
        let lifecycle = self.store.get_lifecycle_record(agent_id).await?;
        Ok(match (manifest, lifecycle) {
            (Some(manifest), Some(lifecycle)) => Some((manifest, lifecycle)),
            _ => None,
        })
    }

    async fn inspect_action(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<(Action, Option<EncounterRecord>, Option<AuditReceipt>)>> {
        let action = self.store.get_action(action_id).await?;
        if let Some(action) = action {
            let encounter = self
                .store
                .get_latest_encounter_for_agent(&action.capability)
                .await
                .ok()
                .flatten();
            let receipt = if let Some(encounter) = &encounter {
                self.store
                    .get_audit_receipt_by_encounter(&encounter.id)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };
            Ok(Some((action, encounter, receipt)))
        } else {
            Ok(None)
        }
    }

    async fn drain(&self) -> anyhow::Result<()> {
        for mut record in self.store.list_lifecycle_records().await? {
            record.desired_state = AgentState::Inactive;
            record.observed_state = AgentState::Inactive;
            record.transition_reason = Some("operator drain".to_string());
            record.last_transition_at = now_timestamp();
            self.store.upsert_lifecycle_record(&record).await?;
        }
        Ok(())
    }

    async fn resume(&self) -> anyhow::Result<()> {
        for mut record in self.store.list_lifecycle_records().await? {
            record.desired_state = AgentState::Active;
            if record.health == HealthStatus::Healthy {
                record.observed_state = AgentState::Active;
            }
            record.transition_reason = Some("operator resume".to_string());
            record.last_transition_at = now_timestamp();
            self.store.upsert_lifecycle_record(&record).await?;
        }
        Ok(())
    }
}

fn trust_domain_defaults(trust_domain: TrustDomain) -> crawfish_types::EncounterPolicy {
    let mut policy = crawfish_types::EncounterPolicy::default();
    if trust_domain == TrustDomain::SameDeviceForeignOwner {
        policy.default_disposition = crawfish_types::DefaultDisposition::Deny;
    }
    policy
}

pub fn summarize_capabilities(manifest: &AgentManifest) -> Vec<CapabilityDescriptor> {
    manifest
        .capabilities
        .iter()
        .map(|capability| CapabilityDescriptor {
            namespace: capability.clone(),
            verbs: vec!["run".to_string()],
            executor_class: crawfish_types::ExecutorClass::Hybrid,
            mutability: if capability.contains("patch") || capability.contains("write") {
                Mutability::Mutating
            } else {
                Mutability::ReadOnly
            },
            risk_class: crawfish_types::RiskClass::Medium,
            cost_class: crawfish_types::CostClass::Standard,
            latency_class: crawfish_types::LatencyClass::Background,
            approval_requirements: Vec::new(),
        })
        .collect()
}

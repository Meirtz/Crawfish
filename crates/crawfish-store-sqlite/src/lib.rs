use async_trait::async_trait;
use crawfish_core::{ActionEventRecord, ActionStore, CheckpointStore, QueueSummary};
use crawfish_types::{
    Action, AgentManifest, AlertEvent, AuditReceipt, CapabilityLease, ConsentGrant, DatasetCase,
    DelegationReceipt, EncounterRecord, EvaluationRecord, ExperimentCaseResult, ExperimentRun,
    FeedbackNote, LifecycleRecord, PairwiseCaseResult, PairwiseExperimentRun, PolicyIncident,
    RemoteEvidenceBundle, ReviewQueueItem, TraceBundle,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{ConnectOptions, Pool, Row, Sqlite, SqlitePool};
use std::path::{Path, PathBuf};
use tokio::fs;

const ADMIN_MODE_KEY: &str = "admin_mode";
const ADMIN_MODE_ACTIVE: &str = "active";
const ADMIN_MODE_DRAINING: &str = "draining";

pub struct SqliteStore {
    pool: SqlitePool,
    checkpoint_dir: PathBuf,
}

impl SqliteStore {
    pub async fn connect(database_path: &Path, state_dir: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::create_dir_all(state_dir.join("checkpoints")).await?;

        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .disable_statement_logging();
        let pool = Pool::<Sqlite>::connect_with(options).await?;
        let store = Self {
            pool,
            checkpoint_dir: state_dir.join("checkpoints"),
        };
        store.migrate().await?;
        store.ensure_runtime_state().await?;
        Ok(store)
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn upsert_agent_manifest(&self, manifest: &AgentManifest) -> anyhow::Result<()> {
        let payload = serde_json::to_string(manifest)?;
        sqlx::query(
            r#"
            INSERT INTO agents (id, owner_id, owner_kind, trust_domain, role, manifest_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO UPDATE SET
              owner_id = excluded.owner_id,
              owner_kind = excluded.owner_kind,
              trust_domain = excluded.trust_domain,
              role = excluded.role,
              manifest_json = excluded.manifest_json
            "#,
        )
        .bind(&manifest.id)
        .bind(&manifest.owner.id)
        .bind(format!("{:?}", manifest.owner.kind).to_lowercase())
        .bind(format!("{:?}", manifest.trust_domain).to_lowercase())
        .bind(&manifest.role)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_agent_manifests(&self) -> anyhow::Result<Vec<AgentManifest>> {
        let rows = sqlx::query("SELECT manifest_json FROM agents ORDER BY id")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("manifest_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn get_agent_manifest(
        &self,
        agent_id: &str,
    ) -> anyhow::Result<Option<AgentManifest>> {
        let row = sqlx::query("SELECT manifest_json FROM agents WHERE id = ?1")
            .bind(agent_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("manifest_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn upsert_lifecycle_record(&self, record: &LifecycleRecord) -> anyhow::Result<()> {
        let payload = serde_json::to_string(record)?;
        sqlx::query(
            r#"
            INSERT INTO lifecycle_records (
              agent_id,
              desired_state,
              observed_state,
              health,
              transition_reason,
              last_transition_at,
              degradation_profile,
              continuity_mode,
              failure_count,
              record_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(agent_id) DO UPDATE SET
              desired_state = excluded.desired_state,
              observed_state = excluded.observed_state,
              health = excluded.health,
              transition_reason = excluded.transition_reason,
              last_transition_at = excluded.last_transition_at,
              degradation_profile = excluded.degradation_profile,
              continuity_mode = excluded.continuity_mode,
              failure_count = excluded.failure_count,
              record_json = excluded.record_json
            "#,
        )
        .bind(&record.agent_id)
        .bind(format!("{:?}", record.desired_state).to_lowercase())
        .bind(format!("{:?}", record.observed_state).to_lowercase())
        .bind(format!("{:?}", record.health).to_lowercase())
        .bind(&record.transition_reason)
        .bind(&record.last_transition_at)
        .bind(record.degradation_profile.as_ref().map(enum_to_snake))
        .bind(record.continuity_mode.as_ref().map(enum_to_snake))
        .bind(i64::from(record.failure_count))
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_lifecycle_records(&self) -> anyhow::Result<Vec<LifecycleRecord>> {
        let rows = sqlx::query("SELECT record_json FROM lifecycle_records ORDER BY agent_id")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("record_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn get_lifecycle_record(
        &self,
        agent_id: &str,
    ) -> anyhow::Result<Option<LifecycleRecord>> {
        let row = sqlx::query("SELECT record_json FROM lifecycle_records WHERE agent_id = ?1")
            .bind(agent_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("record_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn insert_encounter(&self, encounter: &EncounterRecord) -> anyhow::Result<()> {
        let payload = serde_json::to_string(encounter)?;
        sqlx::query(
            r#"
            INSERT INTO encounters (id, target_agent_id, trust_domain, state, encounter_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              target_agent_id = excluded.target_agent_id,
              trust_domain = excluded.trust_domain,
              state = excluded.state,
              encounter_json = excluded.encounter_json
            "#,
        )
        .bind(&encounter.id)
        .bind(&encounter.target_agent_id)
        .bind(format!("{:?}", encounter.trust_domain).to_lowercase())
        .bind(format!("{:?}", encounter.state).to_lowercase())
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_encounter(
        &self,
        encounter_id: &str,
    ) -> anyhow::Result<Option<EncounterRecord>> {
        let row = sqlx::query("SELECT encounter_json FROM encounters WHERE id = ?1")
            .bind(encounter_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("encounter_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn insert_audit_receipt(&self, receipt: &AuditReceipt) -> anyhow::Result<()> {
        let payload = serde_json::to_string(receipt)?;
        sqlx::query(
            r#"
            INSERT INTO audit_receipts (id, encounter_ref, outcome, audit_json)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(id) DO UPDATE SET
              encounter_ref = excluded.encounter_ref,
              outcome = excluded.outcome,
              audit_json = excluded.audit_json
            "#,
        )
        .bind(&receipt.id)
        .bind(&receipt.encounter_ref)
        .bind(format!("{:?}", receipt.outcome).to_lowercase())
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_audit_receipt(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<AuditReceipt>> {
        let row = sqlx::query("SELECT audit_json FROM audit_receipts WHERE id = ?1")
            .bind(receipt_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("audit_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn upsert_consent_grant(&self, grant: &ConsentGrant) -> anyhow::Result<()> {
        let payload = serde_json::to_string(grant)?;
        sqlx::query(
            r#"
            INSERT INTO consent_grants (id, grantor_id, grantee_id, expires_at, grant_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              grantor_id = excluded.grantor_id,
              grantee_id = excluded.grantee_id,
              expires_at = excluded.expires_at,
              grant_json = excluded.grant_json
            "#,
        )
        .bind(&grant.id)
        .bind(&grant.grantor.id)
        .bind(&grant.grantee.id)
        .bind(&grant.expires_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_consent_grant(&self, grant_id: &str) -> anyhow::Result<Option<ConsentGrant>> {
        let row = sqlx::query("SELECT grant_json FROM consent_grants WHERE id = ?1")
            .bind(grant_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("grant_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn list_consent_grants(
        &self,
        grant_ids: &[String],
    ) -> anyhow::Result<Vec<ConsentGrant>> {
        let mut grants = Vec::new();
        for grant_id in grant_ids {
            if let Some(grant) = self.get_consent_grant(grant_id).await? {
                grants.push(grant);
            }
        }
        Ok(grants)
    }

    pub async fn upsert_capability_lease(&self, lease: &CapabilityLease) -> anyhow::Result<()> {
        let payload = serde_json::to_string(lease)?;
        sqlx::query(
            r#"
            INSERT INTO capability_leases (
              id,
              grant_ref,
              lessor_id,
              lessee_id,
              expires_at,
              revocation_reason,
              lease_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
              grant_ref = excluded.grant_ref,
              lessor_id = excluded.lessor_id,
              lessee_id = excluded.lessee_id,
              expires_at = excluded.expires_at,
              revocation_reason = excluded.revocation_reason,
              lease_json = excluded.lease_json
            "#,
        )
        .bind(&lease.id)
        .bind(&lease.grant_ref)
        .bind(&lease.lessor.id)
        .bind(&lease.lessee.id)
        .bind(&lease.expires_at)
        .bind(&lease.revocation_reason)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_capability_lease(
        &self,
        lease_id: &str,
    ) -> anyhow::Result<Option<CapabilityLease>> {
        let row = sqlx::query("SELECT lease_json FROM capability_leases WHERE id = ?1")
            .bind(lease_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("lease_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn upsert_trace_bundle(&self, bundle: &TraceBundle) -> anyhow::Result<()> {
        let payload = serde_json::to_string(bundle)?;
        sqlx::query(
            r#"
            INSERT INTO trace_bundles (action_id, trace_id, trace_json, created_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(action_id) DO UPDATE SET
              trace_id = excluded.trace_id,
              trace_json = excluded.trace_json,
              created_at = excluded.created_at
            "#,
        )
        .bind(&bundle.action_id)
        .bind(&bundle.id)
        .bind(payload)
        .bind(&bundle.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_trace_bundle(&self, action_id: &str) -> anyhow::Result<Option<TraceBundle>> {
        let row = sqlx::query("SELECT trace_json FROM trace_bundles WHERE action_id = ?1")
            .bind(action_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("trace_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn upsert_remote_evidence_bundle(
        &self,
        bundle: &RemoteEvidenceBundle,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(bundle)?;
        sqlx::query(
            r#"
            INSERT INTO remote_evidence_bundles (id, action_id, attempt, created_at, bundle_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              attempt = excluded.attempt,
              created_at = excluded.created_at,
              bundle_json = excluded.bundle_json
            "#,
        )
        .bind(&bundle.id)
        .bind(&bundle.action_id)
        .bind(i64::from(bundle.attempt))
        .bind(&bundle.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_remote_evidence_bundle(
        &self,
        bundle_id: &str,
    ) -> anyhow::Result<Option<RemoteEvidenceBundle>> {
        let row = sqlx::query("SELECT bundle_json FROM remote_evidence_bundles WHERE id = ?1")
            .bind(bundle_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("bundle_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn list_remote_evidence_bundles(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<RemoteEvidenceBundle>> {
        let rows = sqlx::query(
            "SELECT bundle_json FROM remote_evidence_bundles WHERE action_id = ?1 ORDER BY attempt ASC, created_at ASC, id ASC",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("bundle_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn insert_evaluation_record(
        &self,
        evaluation: &EvaluationRecord,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(evaluation)?;
        sqlx::query(
            r#"
            INSERT INTO evaluations (id, action_id, evaluator, status, created_at, evaluation_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              evaluator = excluded.evaluator,
              status = excluded.status,
              created_at = excluded.created_at,
              evaluation_json = excluded.evaluation_json
            "#,
        )
        .bind(&evaluation.id)
        .bind(&evaluation.action_id)
        .bind(&evaluation.evaluator)
        .bind(format!("{:?}", evaluation.status).to_lowercase())
        .bind(&evaluation.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_evaluation_records(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<EvaluationRecord>> {
        let rows = sqlx::query(
            "SELECT evaluation_json FROM evaluations WHERE action_id = ?1 ORDER BY created_at ASC, id ASC",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("evaluation_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn upsert_review_queue_item(&self, item: &ReviewQueueItem) -> anyhow::Result<()> {
        let payload = serde_json::to_string(item)?;
        sqlx::query(
            r#"
            INSERT INTO review_queue_items (
              id,
              action_id,
              status,
              summary,
              created_at,
              resolved_at,
              review_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              status = excluded.status,
              summary = excluded.summary,
              created_at = excluded.created_at,
              resolved_at = excluded.resolved_at,
              review_json = excluded.review_json
            "#,
        )
        .bind(&item.id)
        .bind(&item.action_id)
        .bind(format!("{:?}", item.status).to_lowercase())
        .bind(&item.summary)
        .bind(&item.created_at)
        .bind(&item.resolved_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_review_queue_items(&self) -> anyhow::Result<Vec<ReviewQueueItem>> {
        let rows = sqlx::query(
            "SELECT review_json FROM review_queue_items ORDER BY created_at ASC, id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("review_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn insert_feedback_note(&self, note: &FeedbackNote) -> anyhow::Result<()> {
        let payload = serde_json::to_string(note)?;
        sqlx::query(
            r#"
            INSERT INTO feedback_notes (id, action_id, created_at, feedback_json)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              created_at = excluded.created_at,
              feedback_json = excluded.feedback_json
            "#,
        )
        .bind(&note.id)
        .bind(&note.action_id)
        .bind(&note.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_feedback_note(&self, note_id: &str) -> anyhow::Result<Option<FeedbackNote>> {
        let row = sqlx::query("SELECT feedback_json FROM feedback_notes WHERE id = ?1")
            .bind(note_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("feedback_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn insert_policy_incident(&self, incident: &PolicyIncident) -> anyhow::Result<()> {
        let payload = serde_json::to_string(incident)?;
        sqlx::query(
            r#"
            INSERT INTO policy_incidents (id, action_id, code, created_at, incident_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              code = excluded.code,
              created_at = excluded.created_at,
              incident_json = excluded.incident_json
            "#,
        )
        .bind(&incident.id)
        .bind(&incident.action_id)
        .bind(&incident.reason_code)
        .bind(&incident.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_policy_incidents(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<PolicyIncident>> {
        let rows = sqlx::query(
            "SELECT incident_json FROM policy_incidents WHERE action_id = ?1 ORDER BY created_at ASC, id ASC",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("incident_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn insert_dataset_case(&self, case: &DatasetCase) -> anyhow::Result<()> {
        let payload = serde_json::to_string(case)?;
        sqlx::query(
            r#"
            INSERT INTO dataset_cases (id, dataset_name, capability, source_action_id, created_at, case_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO UPDATE SET
              dataset_name = excluded.dataset_name,
              capability = excluded.capability,
              source_action_id = excluded.source_action_id,
              created_at = excluded.created_at,
              case_json = excluded.case_json
            "#,
        )
        .bind(&case.id)
        .bind(&case.dataset_name)
        .bind(&case.capability)
        .bind(&case.source_action_id)
        .bind(&case.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_dataset_cases(&self, dataset_name: &str) -> anyhow::Result<Vec<DatasetCase>> {
        let rows = sqlx::query(
            "SELECT case_json FROM dataset_cases WHERE dataset_name = ?1 ORDER BY created_at ASC, id ASC",
        )
        .bind(dataset_name)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("case_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn insert_experiment_run(&self, run: &ExperimentRun) -> anyhow::Result<()> {
        let payload = serde_json::to_string(run)?;
        sqlx::query(
            r#"
            INSERT INTO experiment_runs (id, dataset_name, status, created_at, run_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              dataset_name = excluded.dataset_name,
              status = excluded.status,
              created_at = excluded.created_at,
              run_json = excluded.run_json
            "#,
        )
        .bind(&run.id)
        .bind(&run.dataset_name)
        .bind(format!("{:?}", run.status).to_lowercase())
        .bind(&run.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_experiment_run(&self, run_id: &str) -> anyhow::Result<Option<ExperimentRun>> {
        let row = sqlx::query("SELECT run_json FROM experiment_runs WHERE id = ?1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("run_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn insert_experiment_case_result(
        &self,
        result: &ExperimentCaseResult,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(result)?;
        sqlx::query(
            r#"
            INSERT INTO experiment_case_results (id, run_id, dataset_case_id, created_at, result_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              run_id = excluded.run_id,
              dataset_case_id = excluded.dataset_case_id,
              created_at = excluded.created_at,
              result_json = excluded.result_json
            "#,
        )
        .bind(&result.id)
        .bind(&result.run_id)
        .bind(&result.dataset_case_id)
        .bind(&result.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_experiment_case_results(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<ExperimentCaseResult>> {
        let rows = sqlx::query(
            "SELECT result_json FROM experiment_case_results WHERE run_id = ?1 ORDER BY created_at ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("result_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn insert_pairwise_experiment_run(
        &self,
        run: &PairwiseExperimentRun,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(run)?;
        sqlx::query(
            r#"
            INSERT INTO pairwise_experiment_runs (id, dataset_name, status, created_at, run_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              dataset_name = excluded.dataset_name,
              status = excluded.status,
              created_at = excluded.created_at,
              run_json = excluded.run_json
            "#,
        )
        .bind(&run.id)
        .bind(&run.dataset_name)
        .bind(format!("{:?}", run.status).to_lowercase())
        .bind(&run.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_pairwise_experiment_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<PairwiseExperimentRun>> {
        let row = sqlx::query("SELECT run_json FROM pairwise_experiment_runs WHERE id = ?1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("run_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn insert_pairwise_case_result(
        &self,
        result: &PairwiseCaseResult,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(result)?;
        sqlx::query(
            r#"
            INSERT INTO pairwise_case_results (id, pairwise_run_id, dataset_case_id, created_at, result_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              pairwise_run_id = excluded.pairwise_run_id,
              dataset_case_id = excluded.dataset_case_id,
              created_at = excluded.created_at,
              result_json = excluded.result_json
            "#,
        )
        .bind(&result.id)
        .bind(&result.pairwise_run_id)
        .bind(&result.dataset_case_id)
        .bind(&result.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_pairwise_case_result(
        &self,
        case_result_id: &str,
    ) -> anyhow::Result<Option<PairwiseCaseResult>> {
        let row = sqlx::query("SELECT result_json FROM pairwise_case_results WHERE id = ?1")
            .bind(case_result_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("result_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn list_pairwise_case_results(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<PairwiseCaseResult>> {
        let rows = sqlx::query(
            "SELECT result_json FROM pairwise_case_results WHERE pairwise_run_id = ?1 ORDER BY created_at ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("result_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn insert_alert_event(&self, alert: &AlertEvent) -> anyhow::Result<()> {
        let payload = serde_json::to_string(alert)?;
        sqlx::query(
            r#"
            INSERT INTO alert_events (id, action_id, rule_id, severity, created_at, acknowledged_at, alert_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              rule_id = excluded.rule_id,
              severity = excluded.severity,
              created_at = excluded.created_at,
              acknowledged_at = excluded.acknowledged_at,
              alert_json = excluded.alert_json
            "#,
        )
        .bind(&alert.id)
        .bind(&alert.action_id)
        .bind(&alert.rule_id)
        .bind(&alert.severity)
        .bind(&alert.created_at)
        .bind(&alert.acknowledged_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_alert_events(&self) -> anyhow::Result<Vec<AlertEvent>> {
        let rows =
            sqlx::query("SELECT alert_json FROM alert_events ORDER BY created_at DESC, id DESC")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("alert_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn latest_completed_action(
        &self,
        target_agent_id: &str,
        capability: &str,
    ) -> anyhow::Result<Option<Action>> {
        let row = sqlx::query(
            r#"
            SELECT action_json
            FROM actions
            WHERE target_agent_id = ?1
              AND capability = ?2
              AND phase = 'completed'
            ORDER BY COALESCE(finished_at, created_at) DESC
            LIMIT 1
            "#,
        )
        .bind(target_agent_id)
        .bind(capability)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let payload: String = row.try_get("action_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    pub async fn list_actions(&self, phase: Option<&str>) -> anyhow::Result<Vec<Action>> {
        let rows = if let Some(phase) = phase {
            sqlx::query(
                r#"
                SELECT action_json
                FROM actions
                WHERE phase = ?1
                ORDER BY created_at DESC
                "#,
            )
            .bind(phase)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT action_json
                FROM actions
                ORDER BY created_at DESC
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(|row| {
                let payload: String = row.try_get("action_json")?;
                Ok(serde_json::from_str(&payload)?)
            })
            .collect()
    }

    pub async fn set_admin_mode_draining(&self, draining: bool) -> anyhow::Result<()> {
        let value = if draining {
            ADMIN_MODE_DRAINING
        } else {
            ADMIN_MODE_ACTIVE
        };
        sqlx::query(
            r#"
            INSERT INTO runtime_state (key, value)
            VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            "#,
        )
        .bind(ADMIN_MODE_KEY)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn is_draining(&self) -> anyhow::Result<bool> {
        let row = sqlx::query("SELECT value FROM runtime_state WHERE key = ?1")
            .bind(ADMIN_MODE_KEY)
            .fetch_optional(&self.pool)
            .await?;
        let value = row
            .map(|row| row.try_get::<String, _>("value"))
            .transpose()?
            .unwrap_or_else(|| ADMIN_MODE_ACTIVE.to_string());
        Ok(value == ADMIN_MODE_DRAINING)
    }

    async fn ensure_runtime_state(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO runtime_state (key, value)
            VALUES (?1, ?2)
            ON CONFLICT(key) DO NOTHING
            "#,
        )
        .bind(ADMIN_MODE_KEY)
        .bind(ADMIN_MODE_ACTIVE)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_delegation_receipt(
        &self,
        receipt: &DelegationReceipt,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(receipt)?;
        sqlx::query(
            r#"
            INSERT INTO delegation_receipts (id, action_id, treaty_pack_id, created_at, receipt_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
              action_id = excluded.action_id,
              treaty_pack_id = excluded.treaty_pack_id,
              created_at = excluded.created_at,
              receipt_json = excluded.receipt_json
            "#,
        )
        .bind(&receipt.id)
        .bind(&receipt.action_id)
        .bind(&receipt.treaty_pack_id)
        .bind(&receipt.created_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_delegation_receipt(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<DelegationReceipt>> {
        let row = sqlx::query("SELECT receipt_json FROM delegation_receipts WHERE id = ?1")
            .bind(receipt_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("receipt_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }
}

#[async_trait]
impl ActionStore for SqliteStore {
    async fn upsert_action(&self, action: &Action) -> anyhow::Result<()> {
        let payload = serde_json::to_string(action)?;
        sqlx::query(
            r#"
            INSERT INTO actions (
              id,
              target_agent_id,
              capability,
              phase,
              encounter_ref,
              audit_receipt_ref,
              checkpoint_ref,
              continuity_mode,
              failure_reason,
              created_at,
              claimed_at,
              started_at,
              finished_at,
              action_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ON CONFLICT(id) DO UPDATE SET
              target_agent_id = excluded.target_agent_id,
              capability = excluded.capability,
              phase = excluded.phase,
              encounter_ref = excluded.encounter_ref,
              audit_receipt_ref = excluded.audit_receipt_ref,
              checkpoint_ref = excluded.checkpoint_ref,
              continuity_mode = excluded.continuity_mode,
              failure_reason = excluded.failure_reason,
              created_at = excluded.created_at,
              claimed_at = excluded.claimed_at,
              started_at = excluded.started_at,
              finished_at = excluded.finished_at,
              action_json = excluded.action_json
            "#,
        )
        .bind(&action.id)
        .bind(&action.target_agent_id)
        .bind(&action.capability)
        .bind(enum_to_snake(&action.phase))
        .bind(&action.encounter_ref)
        .bind(&action.audit_receipt_ref)
        .bind(&action.checkpoint_ref)
        .bind(action.continuity_mode.as_ref().map(enum_to_snake))
        .bind(&action.failure_reason)
        .bind(&action.created_at)
        .bind(&action.started_at)
        .bind(&action.started_at)
        .bind(&action.finished_at)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn append_action_event(
        &self,
        action_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO action_events (action_id, event_type, payload_json, created_at) VALUES (?1, ?2, ?3, strftime('%s','now'))",
        )
        .bind(action_id)
        .bind(event_type)
        .bind(payload.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_action(&self, action_id: &str) -> anyhow::Result<Option<Action>> {
        let row = sqlx::query("SELECT action_json FROM actions WHERE id = ?1")
            .bind(action_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            let payload: String = row.try_get("action_json")?;
            Ok(serde_json::from_str(&payload)?)
        })
        .transpose()
    }

    async fn list_action_events(&self, action_id: &str) -> anyhow::Result<Vec<ActionEventRecord>> {
        let rows = sqlx::query(
            "SELECT id, action_id, event_type, payload_json, created_at FROM action_events WHERE action_id = ?1 ORDER BY id ASC",
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let payload_json: String = row.try_get("payload_json")?;
                Ok(ActionEventRecord {
                    id: row.try_get("id")?,
                    action_id: row.try_get("action_id")?,
                    event_type: row.try_get("event_type")?,
                    payload: serde_json::from_str(&payload_json)?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect()
    }

    async fn list_actions_by_phase(&self, phase: Option<&str>) -> anyhow::Result<Vec<Action>> {
        self.list_actions(phase).await
    }

    async fn claim_next_accepted_action(&self) -> anyhow::Result<Option<Action>> {
        let row = sqlx::query(
            "SELECT action_json FROM actions WHERE phase = 'accepted' ORDER BY created_at ASC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        let mut action = match row {
            Some(row) => {
                let payload: String = row.try_get("action_json")?;
                serde_json::from_str::<Action>(&payload)?
            }
            None => return Ok(None),
        };

        action.phase = crawfish_types::ActionPhase::Running;
        let now = current_timestamp();
        action.started_at = Some(now.clone());
        self.upsert_action(&action).await?;
        self.append_action_event(
            &action.id,
            "running",
            serde_json::json!({"phase": "running", "started_at": now}),
        )
        .await?;
        Ok(Some(action))
    }

    async fn queue_summary(&self) -> anyhow::Result<QueueSummary> {
        let rows = sqlx::query("SELECT phase, COUNT(*) AS count FROM actions GROUP BY phase")
            .fetch_all(&self.pool)
            .await?;

        let mut summary = QueueSummary::default();
        for row in rows {
            let phase: String = row.try_get("phase")?;
            let count: i64 = row.try_get("count")?;
            match phase.as_str() {
                "accepted" => summary.accepted = count as u64,
                "running" => summary.running = count as u64,
                "blocked" => summary.blocked = count as u64,
                "awaiting_approval" => summary.awaiting_approval = count as u64,
                "completed" => summary.completed = count as u64,
                "failed" => summary.failed = count as u64,
                "expired" => summary.expired = count as u64,
                _ => {}
            }
        }

        Ok(summary)
    }

    async fn put_trace_bundle(&self, bundle: &TraceBundle) -> anyhow::Result<()> {
        self.upsert_trace_bundle(bundle).await
    }

    async fn get_trace_bundle(&self, action_id: &str) -> anyhow::Result<Option<TraceBundle>> {
        SqliteStore::get_trace_bundle(self, action_id).await
    }

    async fn insert_remote_evidence_bundle(
        &self,
        bundle: &RemoteEvidenceBundle,
    ) -> anyhow::Result<()> {
        SqliteStore::upsert_remote_evidence_bundle(self, bundle).await
    }

    async fn get_remote_evidence_bundle(
        &self,
        bundle_id: &str,
    ) -> anyhow::Result<Option<RemoteEvidenceBundle>> {
        SqliteStore::get_remote_evidence_bundle(self, bundle_id).await
    }

    async fn list_remote_evidence_bundles(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<RemoteEvidenceBundle>> {
        SqliteStore::list_remote_evidence_bundles(self, action_id).await
    }

    async fn insert_evaluation(&self, evaluation: &EvaluationRecord) -> anyhow::Result<()> {
        self.insert_evaluation_record(evaluation).await
    }

    async fn list_evaluations(&self, action_id: &str) -> anyhow::Result<Vec<EvaluationRecord>> {
        self.list_evaluation_records(action_id).await
    }

    async fn insert_review_queue_item(&self, item: &ReviewQueueItem) -> anyhow::Result<()> {
        self.upsert_review_queue_item(item).await
    }

    async fn list_review_queue_items(&self) -> anyhow::Result<Vec<ReviewQueueItem>> {
        SqliteStore::list_review_queue_items(self).await
    }

    async fn resolve_review_queue_item(&self, item: &ReviewQueueItem) -> anyhow::Result<()> {
        self.upsert_review_queue_item(item).await
    }

    async fn insert_feedback_note(&self, note: &FeedbackNote) -> anyhow::Result<()> {
        SqliteStore::insert_feedback_note(self, note).await
    }

    async fn get_feedback_note(&self, note_id: &str) -> anyhow::Result<Option<FeedbackNote>> {
        SqliteStore::get_feedback_note(self, note_id).await
    }

    async fn insert_policy_incident(&self, incident: &PolicyIncident) -> anyhow::Result<()> {
        SqliteStore::insert_policy_incident(self, incident).await
    }

    async fn list_policy_incidents(&self, action_id: &str) -> anyhow::Result<Vec<PolicyIncident>> {
        SqliteStore::list_policy_incidents(self, action_id).await
    }

    async fn insert_dataset_case(&self, case: &DatasetCase) -> anyhow::Result<()> {
        SqliteStore::insert_dataset_case(self, case).await
    }

    async fn list_dataset_cases(&self, dataset_name: &str) -> anyhow::Result<Vec<DatasetCase>> {
        SqliteStore::list_dataset_cases(self, dataset_name).await
    }

    async fn insert_experiment_run(&self, run: &ExperimentRun) -> anyhow::Result<()> {
        SqliteStore::insert_experiment_run(self, run).await
    }

    async fn get_experiment_run(&self, run_id: &str) -> anyhow::Result<Option<ExperimentRun>> {
        SqliteStore::get_experiment_run(self, run_id).await
    }

    async fn update_experiment_run(&self, run: &ExperimentRun) -> anyhow::Result<()> {
        SqliteStore::insert_experiment_run(self, run).await
    }

    async fn insert_experiment_case_result(
        &self,
        result: &ExperimentCaseResult,
    ) -> anyhow::Result<()> {
        SqliteStore::insert_experiment_case_result(self, result).await
    }

    async fn list_experiment_case_results(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<ExperimentCaseResult>> {
        SqliteStore::list_experiment_case_results(self, run_id).await
    }

    async fn insert_pairwise_experiment_run(
        &self,
        run: &PairwiseExperimentRun,
    ) -> anyhow::Result<()> {
        SqliteStore::insert_pairwise_experiment_run(self, run).await
    }

    async fn get_pairwise_experiment_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<PairwiseExperimentRun>> {
        SqliteStore::get_pairwise_experiment_run(self, run_id).await
    }

    async fn update_pairwise_experiment_run(
        &self,
        run: &PairwiseExperimentRun,
    ) -> anyhow::Result<()> {
        SqliteStore::insert_pairwise_experiment_run(self, run).await
    }

    async fn insert_pairwise_case_result(&self, result: &PairwiseCaseResult) -> anyhow::Result<()> {
        SqliteStore::insert_pairwise_case_result(self, result).await
    }

    async fn get_pairwise_case_result(
        &self,
        case_result_id: &str,
    ) -> anyhow::Result<Option<PairwiseCaseResult>> {
        SqliteStore::get_pairwise_case_result(self, case_result_id).await
    }

    async fn update_pairwise_case_result(&self, result: &PairwiseCaseResult) -> anyhow::Result<()> {
        SqliteStore::insert_pairwise_case_result(self, result).await
    }

    async fn list_pairwise_case_results(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<PairwiseCaseResult>> {
        SqliteStore::list_pairwise_case_results(self, run_id).await
    }

    async fn insert_alert_event(&self, alert: &AlertEvent) -> anyhow::Result<()> {
        SqliteStore::insert_alert_event(self, alert).await
    }

    async fn list_alert_events(&self) -> anyhow::Result<Vec<AlertEvent>> {
        SqliteStore::list_alert_events(self).await
    }

    async fn acknowledge_alert_event(&self, alert: &AlertEvent) -> anyhow::Result<()> {
        SqliteStore::insert_alert_event(self, alert).await
    }

    async fn insert_delegation_receipt(&self, receipt: &DelegationReceipt) -> anyhow::Result<()> {
        SqliteStore::insert_delegation_receipt(self, receipt).await
    }

    async fn get_delegation_receipt(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<DelegationReceipt>> {
        SqliteStore::get_delegation_receipt(self, receipt_id).await
    }

    async fn count_running_actions_for_agent(&self, agent_id: &str) -> anyhow::Result<u64> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS cnt FROM actions WHERE target_agent_id = ?1 AND phase = 'running'",
        )
        .bind(agent_id)
        .fetch_one(&self.pool)
        .await?;
        let count: i64 = row.try_get("cnt")?;
        Ok(count as u64)
    }
}

#[async_trait]
impl CheckpointStore for SqliteStore {
    async fn put_checkpoint(
        &self,
        action_id: &str,
        checkpoint_ref: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let action_dir = self.checkpoint_dir.join(action_id);
        fs::create_dir_all(&action_dir).await?;
        let blob_path = action_dir.join(format!("{checkpoint_ref}.bin"));
        fs::write(&blob_path, payload).await?;

        sqlx::query(
            r#"
            INSERT INTO checkpoints (action_id, checkpoint_ref, blob_path, created_at)
            VALUES (?1, ?2, ?3, strftime('%s','now'))
            ON CONFLICT(action_id) DO UPDATE SET
              checkpoint_ref = excluded.checkpoint_ref,
              blob_path = excluded.blob_path,
              created_at = excluded.created_at
            "#,
        )
        .bind(action_id)
        .bind(checkpoint_ref)
        .bind(blob_path.display().to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_checkpoint(&self, action_id: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT blob_path FROM checkpoints WHERE action_id = ?1")
            .bind(action_id)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => {
                let path: String = row.try_get("blob_path")?;
                Ok(Some(fs::read(path).await?))
            }
            None => Ok(None),
        }
    }
}

fn current_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn enum_to_snake<T: std::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
        .chars()
        .flat_map(|c| match c {
            'A'..='Z' => vec!['_', c.to_ascii_lowercase()],
            _ => vec![c],
        })
        .collect::<String>()
        .trim_start_matches('_')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawfish_core::now_timestamp;
    use crawfish_types::{
        ActionOutputs, ActionPhase, AgentManifest, AgentState, AuditOutcome, CapabilityLease,
        ConsentGrant, CounterpartyRef, EncounterState, ExecutionContract, GoalSpec, HealthStatus,
        LifecycleRecord, OwnerKind, OwnerRef, RequesterKind, RequesterRef, RuntimeProfile,
        TrustDomain,
    };
    use tempfile::tempdir;

    fn owner(id: &str) -> OwnerRef {
        OwnerRef {
            kind: OwnerKind::Human,
            id: id.to_string(),
            display_name: None,
        }
    }

    fn manifest(id: &str) -> AgentManifest {
        AgentManifest {
            id: id.to_string(),
            owner: owner("alice"),
            trust_domain: TrustDomain::SameOwnerLocal,
            role: "reviewer".to_string(),
            capabilities: vec!["repo.review".to_string()],
            exposed_capabilities: vec!["repo.review".to_string()],
            dependencies: Vec::new(),
            runtime: RuntimeProfile::default(),
            lifecycle: Default::default(),
            encounter_policy: Default::default(),
            contract_defaults: ExecutionContract::default(),
            adapters: Vec::new(),
            workspace_policy: Default::default(),
            default_data_boundaries: vec!["owner_local".to_string()],
            strategy_defaults: Default::default(),
        }
    }

    fn action(id: &str) -> Action {
        Action {
            id: id.to_string(),
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "user-1".to_string(),
            },
            initiator_owner: owner("alice"),
            counterparty_refs: Vec::new(),
            goal: GoalSpec {
                summary: "review pull request".to_string(),
                details: None,
            },
            capability: "repo.review".to_string(),
            inputs: Default::default(),
            contract: ExecutionContract::default(),
            execution_strategy: None,
            grant_refs: Vec::new(),
            lease_ref: None,
            encounter_ref: Some("enc-1".to_string()),
            audit_receipt_ref: Some("audit-1".to_string()),
            data_boundary: "owner_local".to_string(),
            schedule: Default::default(),
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
    async fn migration_and_lifecycle_persistence_work() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("control.db");
        let state_dir = dir.path().join("state");
        let store = SqliteStore::connect(&db, &state_dir).await.unwrap();

        store
            .upsert_agent_manifest(&manifest("repo_reviewer"))
            .await
            .unwrap();
        store
            .upsert_lifecycle_record(&LifecycleRecord {
                agent_id: "repo_reviewer".to_string(),
                desired_state: AgentState::Active,
                observed_state: AgentState::Active,
                health: HealthStatus::Healthy,
                transition_reason: None,
                last_transition_at: now_timestamp(),
                degradation_profile: None,
                continuity_mode: None,
                failure_count: 0,
            })
            .await
            .unwrap();

        assert_eq!(store.list_agent_manifests().await.unwrap().len(), 1);
        assert_eq!(store.list_lifecycle_records().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn action_claim_and_checkpoint_round_trip() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("control.db");
        let state_dir = dir.path().join("state");
        let store = SqliteStore::connect(&db, &state_dir).await.unwrap();

        store.upsert_action(&action("action-1")).await.unwrap();
        store
            .append_action_event(
                "action-1",
                "accepted",
                serde_json::json!({"phase": "accepted"}),
            )
            .await
            .unwrap();
        let claimed = store.claim_next_accepted_action().await.unwrap().unwrap();
        assert_eq!(claimed.phase, ActionPhase::Running);
        store
            .put_checkpoint("action-1", "ckpt-1", b"checkpoint")
            .await
            .unwrap();

        assert_eq!(
            store
                .get_action("action-1")
                .await
                .unwrap()
                .expect("action")
                .id,
            "action-1"
        );
        assert_eq!(
            store
                .get_checkpoint("action-1")
                .await
                .unwrap()
                .expect("checkpoint"),
            b"checkpoint"
        );
        assert_eq!(store.queue_summary().await.unwrap().running, 1);
    }

    #[tokio::test]
    async fn encounter_audit_and_runtime_state_round_trip() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("control.db");
        let state_dir = dir.path().join("state");
        let store = SqliteStore::connect(&db, &state_dir).await.unwrap();

        let encounter = EncounterRecord {
            id: "enc-1".to_string(),
            initiator_ref: CounterpartyRef {
                agent_id: None,
                session_id: Some("sess-1".to_string()),
                owner: owner("foreign"),
                trust_domain: TrustDomain::SameDeviceForeignOwner,
            },
            target_agent_id: "repo_reviewer".to_string(),
            target_owner: owner("alice"),
            trust_domain: TrustDomain::SameDeviceForeignOwner,
            requested_capabilities: vec!["repo.review".to_string()],
            applied_policy_source: "system_defaults".to_string(),
            state: EncounterState::Denied,
            grant_refs: Vec::new(),
            lease_ref: None,
            created_at: now_timestamp(),
        };

        let receipt = AuditReceipt {
            id: "audit-1".to_string(),
            encounter_ref: "enc-1".to_string(),
            grant_refs: Vec::new(),
            lease_ref: None,
            outcome: AuditOutcome::Denied,
            reason: "policy denied".to_string(),
            approver_ref: None,
            emitted_at: now_timestamp(),
        };

        store.insert_encounter(&encounter).await.unwrap();
        store.insert_audit_receipt(&receipt).await.unwrap();
        store.set_admin_mode_draining(true).await.unwrap();

        assert_eq!(
            store
                .get_encounter("enc-1")
                .await
                .unwrap()
                .expect("encounter")
                .id,
            "enc-1"
        );
        assert_eq!(
            store
                .get_audit_receipt("audit-1")
                .await
                .unwrap()
                .expect("receipt")
                .id,
            "audit-1"
        );
        assert!(store.is_draining().await.unwrap());
    }

    #[tokio::test]
    async fn consent_grant_and_capability_lease_round_trip() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("control.db");
        let state_dir = dir.path().join("state");
        let store = SqliteStore::connect(&db, &state_dir).await.unwrap();

        let grant = ConsentGrant {
            id: "grant-1".to_string(),
            grantor: owner("alice"),
            grantee: owner("bob"),
            purpose: "review repo".to_string(),
            scope: vec!["repo.review".to_string()],
            issued_at: now_timestamp(),
            expires_at: now_timestamp(),
            revocable: true,
            approver_ref: Some("alice".to_string()),
        };
        let lease = CapabilityLease {
            id: "lease-1".to_string(),
            grant_ref: grant.id.clone(),
            lessor: owner("alice"),
            lessee: owner("bob"),
            capability_refs: vec!["repo.review".to_string()],
            scope: vec!["repo.review".to_string()],
            issued_at: now_timestamp(),
            expires_at: now_timestamp(),
            revocation_reason: None,
            audit_receipt_ref: "audit-1".to_string(),
        };

        store.upsert_consent_grant(&grant).await.unwrap();
        store.upsert_capability_lease(&lease).await.unwrap();

        assert_eq!(
            store
                .get_consent_grant("grant-1")
                .await
                .unwrap()
                .expect("grant")
                .id,
            "grant-1"
        );
        assert_eq!(
            store
                .get_capability_lease("lease-1")
                .await
                .unwrap()
                .expect("lease")
                .id,
            "lease-1"
        );
    }

    #[tokio::test]
    async fn list_actions_can_filter_by_phase() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("control.db");
        let state_dir = dir.path().join("state");
        let store = SqliteStore::connect(&db, &state_dir).await.unwrap();

        let mut awaiting = action("action-awaiting");
        awaiting.phase = ActionPhase::AwaitingApproval;
        let mut completed = action("action-completed");
        completed.phase = ActionPhase::Completed;

        store.upsert_action(&awaiting).await.unwrap();
        store.upsert_action(&completed).await.unwrap();

        assert_eq!(store.list_actions(None).await.unwrap().len(), 2);
        let awaiting_actions = store.list_actions(Some("awaiting_approval")).await.unwrap();
        assert_eq!(awaiting_actions.len(), 1);
        assert_eq!(awaiting_actions[0].id, "action-awaiting");
    }
}

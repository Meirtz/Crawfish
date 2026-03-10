ALTER TABLE actions ADD COLUMN target_agent_id TEXT;
ALTER TABLE actions ADD COLUMN encounter_ref TEXT;
ALTER TABLE actions ADD COLUMN audit_receipt_ref TEXT;
ALTER TABLE actions ADD COLUMN continuity_mode TEXT;
ALTER TABLE actions ADD COLUMN failure_reason TEXT;
ALTER TABLE actions ADD COLUMN created_at TEXT;
ALTER TABLE actions ADD COLUMN claimed_at TEXT;
ALTER TABLE actions ADD COLUMN started_at TEXT;
ALTER TABLE actions ADD COLUMN finished_at TEXT;

CREATE TABLE IF NOT EXISTS runtime_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

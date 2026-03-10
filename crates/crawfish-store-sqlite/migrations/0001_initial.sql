CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  owner_kind TEXT NOT NULL,
  trust_domain TEXT NOT NULL,
  role TEXT NOT NULL,
  manifest_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lifecycle_records (
  agent_id TEXT PRIMARY KEY,
  desired_state TEXT NOT NULL,
  observed_state TEXT NOT NULL,
  health TEXT NOT NULL,
  transition_reason TEXT,
  last_transition_at TEXT NOT NULL,
  degradation_profile TEXT,
  continuity_mode TEXT,
  failure_count INTEGER NOT NULL DEFAULT 0,
  record_json TEXT NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);

CREATE TABLE IF NOT EXISTS actions (
  id TEXT PRIMARY KEY,
  capability TEXT NOT NULL,
  phase TEXT NOT NULL,
  checkpoint_ref TEXT,
  action_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS action_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoints (
  action_id TEXT PRIMARY KEY,
  checkpoint_ref TEXT NOT NULL,
  blob_path TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS encounters (
  id TEXT PRIMARY KEY,
  target_agent_id TEXT NOT NULL,
  trust_domain TEXT NOT NULL,
  state TEXT NOT NULL,
  encounter_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_receipts (
  id TEXT PRIMARY KEY,
  encounter_ref TEXT NOT NULL,
  outcome TEXT NOT NULL,
  audit_json TEXT NOT NULL
);

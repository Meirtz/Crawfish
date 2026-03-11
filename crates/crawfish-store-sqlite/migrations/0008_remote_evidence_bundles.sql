CREATE TABLE IF NOT EXISTS remote_evidence_bundles (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  bundle_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_remote_evidence_bundles_action_id
  ON remote_evidence_bundles(action_id, created_at, id);

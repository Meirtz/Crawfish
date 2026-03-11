CREATE TABLE IF NOT EXISTS remote_followup_requests (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  request_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_remote_followup_requests_action_id
  ON remote_followup_requests(action_id, created_at, id);

CREATE TABLE IF NOT EXISTS remote_attempt_records (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  completed_at TEXT,
  record_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_remote_attempt_records_action_id
  ON remote_attempt_records(action_id, attempt, created_at, id);

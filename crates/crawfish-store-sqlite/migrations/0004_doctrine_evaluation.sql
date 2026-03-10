CREATE TABLE IF NOT EXISTS trace_bundles (
  action_id TEXT PRIMARY KEY,
  trace_id TEXT NOT NULL,
  trace_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evaluations (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  evaluator TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  evaluation_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_queue_items (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  status TEXT NOT NULL,
  summary TEXT NOT NULL,
  created_at TEXT NOT NULL,
  resolved_at TEXT,
  review_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS feedback_notes (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  feedback_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_incidents (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  code TEXT NOT NULL,
  created_at TEXT NOT NULL,
  incident_json TEXT NOT NULL
);

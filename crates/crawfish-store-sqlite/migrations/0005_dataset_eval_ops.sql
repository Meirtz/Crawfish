CREATE TABLE IF NOT EXISTS dataset_cases (
  id TEXT PRIMARY KEY,
  dataset_name TEXT NOT NULL,
  capability TEXT NOT NULL,
  source_action_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  case_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dataset_cases_dataset_name
  ON dataset_cases(dataset_name, created_at);

CREATE TABLE IF NOT EXISTS experiment_runs (
  id TEXT PRIMARY KEY,
  dataset_name TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  run_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiment_case_results (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  dataset_case_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  result_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_experiment_case_results_run_id
  ON experiment_case_results(run_id, created_at);

CREATE TABLE IF NOT EXISTS alert_events (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  severity TEXT NOT NULL,
  created_at TEXT NOT NULL,
  acknowledged_at TEXT,
  alert_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_events_created_at
  ON alert_events(created_at, id);

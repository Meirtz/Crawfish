CREATE TABLE IF NOT EXISTS pairwise_experiment_runs (
  id TEXT PRIMARY KEY,
  dataset_name TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  run_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pairwise_case_results (
  id TEXT PRIMARY KEY,
  pairwise_run_id TEXT NOT NULL,
  dataset_case_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  result_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pairwise_case_results_run_id
  ON pairwise_case_results(pairwise_run_id, created_at);

CREATE TABLE IF NOT EXISTS delegation_receipts (
  id TEXT PRIMARY KEY,
  action_id TEXT NOT NULL,
  treaty_pack_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  receipt_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_delegation_receipts_action_id
  ON delegation_receipts(action_id, created_at);

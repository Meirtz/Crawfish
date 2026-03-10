CREATE TABLE IF NOT EXISTS consent_grants (
  id TEXT PRIMARY KEY,
  grantor_id TEXT NOT NULL,
  grantee_id TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  grant_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capability_leases (
  id TEXT PRIMARY KEY,
  grant_ref TEXT NOT NULL,
  lessor_id TEXT NOT NULL,
  lessee_id TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  revocation_reason TEXT,
  lease_json TEXT NOT NULL
);

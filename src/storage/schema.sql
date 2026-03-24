PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seed_name TEXT NOT NULL,
    creativity_level TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS input_names (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    input_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS name_variants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    variant_name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    creativity_level TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS organisations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    registry_type TEXT NOT NULL,
    registry_number TEXT NOT NULL,
    suffix INTEGER NOT NULL DEFAULT 0,
    organisation_number INTEGER,
    name TEXT NOT NULL,
    status TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(registry_type, registry_number, suffix)
);

CREATE TABLE IF NOT EXISTS addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL,
    normalized_key TEXT NOT NULL UNIQUE,
    postcode TEXT,
    country TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS organisation_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organisation_id INTEGER NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    address_id INTEGER NOT NULL REFERENCES addresses(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    relationship_phrase TEXT NOT NULL DEFAULT 'is registered at',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(organisation_id, address_id)
);

CREATE TABLE IF NOT EXISTS run_organisations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    organisation_id INTEGER NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    source TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(run_id, organisation_id, stage, source)
);

CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    source_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
    source_person_name TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS person_org_roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    organisation_id INTEGER NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    role_type TEXT NOT NULL,
    role_label TEXT NOT NULL,
    relationship_kind TEXT NOT NULL DEFAULT '',
    relationship_phrase TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL,
    confidence_class TEXT NOT NULL DEFAULT 'medium',
    edge_weight REAL NOT NULL DEFAULT 1.0,
    start_date TEXT,
    end_date TEXT,
    provenance_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(person_id, organisation_id, role_label, source)
);

CREATE TABLE IF NOT EXISTS evidence_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    snippet TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, source, source_key)
);

CREATE TABLE IF NOT EXISTS candidate_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    variant_name TEXT NOT NULL,
    candidate_name TEXT NOT NULL,
    organisation_name TEXT NOT NULL,
    registry_type TEXT,
    registry_number TEXT,
    suffix INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL,
    evidence_id INTEGER REFERENCES evidence_items(id) ON DELETE SET NULL,
    feature_payload_json TEXT NOT NULL,
    score REAL NOT NULL,
    raw_payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    candidate_match_id INTEGER NOT NULL REFERENCES candidate_matches(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    confidence REAL NOT NULL,
    canonical_name TEXT NOT NULL,
    explanation TEXT NOT NULL,
    rule_score REAL NOT NULL,
    alias_status TEXT NOT NULL DEFAULT 'none',
    llm_payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_candidate_matches_run_id ON candidate_matches(run_id);
CREATE INDEX IF NOT EXISTS idx_resolution_decisions_run_id ON resolution_decisions(run_id);
CREATE INDEX IF NOT EXISTS idx_roles_org_id ON person_org_roles(organisation_id);
CREATE INDEX IF NOT EXISTS idx_roles_person_id ON person_org_roles(person_id);
CREATE INDEX IF NOT EXISTS idx_run_orgs_run_id ON run_organisations(run_id);
CREATE INDEX IF NOT EXISTS idx_run_orgs_org_id ON run_organisations(organisation_id);
CREATE INDEX IF NOT EXISTS idx_org_addresses_org_id ON organisation_addresses(organisation_id);
CREATE INDEX IF NOT EXISTS idx_org_addresses_address_id ON organisation_addresses(address_id);

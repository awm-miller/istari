PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS negative_news_batch_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_hash TEXT NOT NULL UNIQUE,
    config_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    offset_value INTEGER NOT NULL DEFAULT 0,
    limit_value INTEGER NOT NULL DEFAULT 0,
    total_clusters INTEGER NOT NULL DEFAULT 0,
    completed_clusters INTEGER NOT NULL DEFAULT 0,
    output_path TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS negative_news_cluster_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_run_id INTEGER NOT NULL REFERENCES negative_news_batch_runs(id) ON DELETE CASCADE,
    cluster_rank INTEGER NOT NULL,
    cluster_id TEXT NOT NULL,
    label TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    interesting_count INTEGER NOT NULL DEFAULT 0,
    category_counts_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    error_text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batch_run_id, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_negative_news_cluster_results_batch_run_id
ON negative_news_cluster_results(batch_run_id);

from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path
from typing import Any


class NegativeNewsStore:
    def __init__(self, database_path: Path, schema_path: Path) -> None:
        self.database_path = Path(database_path)
        self.schema_path = Path(schema_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def init_db(self) -> None:
        schema = self.schema_path.read_text(encoding="utf-8")
        with self.connect() as connection:
            connection.executescript(schema)

    @staticmethod
    def config_hash(config: dict[str, Any]) -> str:
        return sha256(json.dumps(config, sort_keys=True).encode("utf-8")).hexdigest()

    def get_or_create_batch_run(
        self,
        *,
        config: dict[str, Any],
        offset_value: int,
        limit_value: int,
        total_clusters: int,
        output_path: str,
    ) -> int:
        config_json = json.dumps(config, sort_keys=True, ensure_ascii=False)
        config_hash = self.config_hash(config)
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT id
                FROM negative_news_batch_runs
                WHERE config_hash = ?
                """,
                (config_hash,),
            ).fetchone()
            if row:
                batch_run_id = int(row["id"])
                connection.execute(
                    """
                    UPDATE negative_news_batch_runs
                    SET config_json = ?,
                        status = 'running',
                        offset_value = ?,
                        limit_value = ?,
                        total_clusters = ?,
                        output_path = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        config_json,
                        offset_value,
                        limit_value,
                        total_clusters,
                        output_path,
                        batch_run_id,
                    ),
                )
                return batch_run_id
            cursor = connection.execute(
                """
                INSERT INTO negative_news_batch_runs(
                    config_hash,
                    config_json,
                    status,
                    offset_value,
                    limit_value,
                    total_clusters,
                    completed_clusters,
                    output_path
                ) VALUES(?, ?, 'running', ?, ?, ?, 0, ?)
                """,
                (config_hash, config_json, offset_value, limit_value, total_clusters, output_path),
            )
            return int(cursor.lastrowid)

    def get_batch_run(self, batch_run_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM negative_news_batch_runs
                WHERE id = ?
                """,
                (batch_run_id,),
            ).fetchone()

    def get_batch_run_by_config(self, config: dict[str, Any]) -> sqlite3.Row | None:
        config_hash = self.config_hash(config)
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM negative_news_batch_runs
                WHERE config_hash = ?
                """,
                (config_hash,),
            ).fetchone()

    def get_completed_cluster_ids(self, batch_run_id: int) -> set[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT cluster_id
                FROM negative_news_cluster_results
                WHERE batch_run_id = ?
                  AND status = 'completed'
                """,
                (batch_run_id,),
            ).fetchall()
        return {str(row["cluster_id"]) for row in rows}

    def get_cluster_results(self, batch_run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM negative_news_cluster_results
                WHERE batch_run_id = ?
                ORDER BY cluster_rank ASC, id ASC
                """,
                (batch_run_id,),
            ).fetchall()

    def upsert_cluster_result(
        self,
        *,
        batch_run_id: int,
        cluster_rank: int,
        cluster_id: str,
        label: str,
        status: str,
        interesting_count: int,
        category_counts: dict[str, int],
        result: dict[str, Any],
        error_text: str = "",
    ) -> None:
        result_json = json.dumps(result, ensure_ascii=False, default=str)
        category_counts_json = json.dumps(category_counts, ensure_ascii=False, sort_keys=True)
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO negative_news_cluster_results(
                    batch_run_id,
                    cluster_rank,
                    cluster_id,
                    label,
                    status,
                    interesting_count,
                    category_counts_json,
                    result_json,
                    error_text,
                    updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(batch_run_id, cluster_id) DO UPDATE SET
                    cluster_rank = excluded.cluster_rank,
                    label = excluded.label,
                    status = excluded.status,
                    interesting_count = excluded.interesting_count,
                    category_counts_json = excluded.category_counts_json,
                    result_json = excluded.result_json,
                    error_text = excluded.error_text,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    batch_run_id,
                    cluster_rank,
                    cluster_id,
                    label,
                    status,
                    int(interesting_count),
                    category_counts_json,
                    result_json,
                    str(error_text or ""),
                ),
            )
            connection.execute(
                """
                UPDATE negative_news_batch_runs
                SET completed_clusters = (
                    SELECT COUNT(*)
                    FROM negative_news_cluster_results
                    WHERE batch_run_id = ?
                      AND status = 'completed'
                ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (batch_run_id, batch_run_id),
            )

    def mark_batch_completed(self, batch_run_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE negative_news_batch_runs
                SET completed_clusters = (
                        SELECT COUNT(*)
                        FROM negative_news_cluster_results
                        WHERE batch_run_id = ?
                          AND status = 'completed'
                    ),
                    status = CASE
                        WHEN (
                            SELECT COUNT(*)
                            FROM negative_news_cluster_results
                            WHERE batch_run_id = ?
                              AND status = 'completed'
                        ) >= total_clusters THEN 'completed'
                        ELSE 'partial'
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (batch_run_id, batch_run_id, batch_run_id),
            )

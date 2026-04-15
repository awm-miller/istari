from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path
from typing import Any

from src.search.queries import generate_name_variants, normalize_name


def person_ids_fingerprint(person_ids: Any) -> str:
    values: set[int] = set()
    for value in person_ids or []:
        try:
            values.add(int(value))
        except (TypeError, ValueError):
            continue
    if not values:
        return ""
    return ",".join(str(value) for value in sorted(values))


def result_person_ids_fingerprint(result: Any) -> str:
    if not isinstance(result, dict):
        return ""
    return person_ids_fingerprint(result.get("person_ids"))


def cluster_lookup_key(cluster: Any) -> str:
    if not isinstance(cluster, dict):
        return ""
    cluster_kind = str(cluster.get("cluster_kind") or cluster.get("kind") or "").strip().lower()
    if not cluster_kind:
        cluster_id = str(cluster.get("cluster_id") or cluster.get("id") or "").strip().lower()
        if cluster_id.startswith("identity:") or cluster_id.startswith("identity_cluster:"):
            cluster_kind = "seed_alias"
        elif cluster_id.startswith("merged_person:") or cluster_id.startswith("person:"):
            cluster_kind = "person"
        elif cluster.get("identity_keys"):
            cluster_kind = "seed_alias"
        elif cluster.get("person_ids") or cluster.get("aliases") or cluster.get("label"):
            cluster_kind = "person"
    if cluster_kind not in {"seed_alias", "person"}:
        return ""

    if cluster_kind == "seed_alias":
        identity_keys = sorted(
            {
                str(value).strip()
                for value in (cluster.get("identity_keys") or [])
                if str(value).strip()
            }
        )
        if identity_keys:
            digest = sha256("\n".join(identity_keys).encode("utf-8")).hexdigest()[:16]
            return f"seed_alias:identity_keys:{digest}"

    raw_names = [
        str(value).strip()
        for value in [cluster.get("label"), *(cluster.get("aliases") or [])]
        if str(value or "").strip()
    ]
    names_set: set[str] = set()
    for value in raw_names:
        normalized = normalize_name(value)
        if normalized:
            names_set.add(normalized)
        if cluster_kind == "person":
            for variant in generate_name_variants(value, "balanced"):
                variant_normalized = normalize_name(str(variant.name or ""))
                if variant_normalized:
                    names_set.add(variant_normalized)
    names = sorted(names_set)
    names = [name for name in names if name]
    if not names:
        return ""
    digest = sha256("\n".join(names).encode("utf-8")).hexdigest()[:16]
    return f"{cluster_kind}:names:{digest}"


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

    def get_latest_completed_results_by_cluster_id(self) -> dict[str, dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    cluster_id,
                    label,
                    result_json,
                    batch_run_id,
                    updated_at,
                    id
                FROM negative_news_cluster_results
                WHERE status = 'completed'
                ORDER BY batch_run_id DESC, updated_at DESC, id DESC
                """
            ).fetchall()
        results: dict[str, dict[str, Any]] = {}
        for row in rows:
            cluster_id = str(row["cluster_id"] or "").strip()
            if not cluster_id or cluster_id in results:
                continue
            try:
                result = json.loads(str(row["result_json"] or "{}"))
            except json.JSONDecodeError:
                result = {}
            results[cluster_id] = {
                "cluster_id": cluster_id,
                "label": str(row["label"] or ""),
                "batch_run_id": int(row["batch_run_id"] or 0),
                "updated_at": str(row["updated_at"] or ""),
                "result": result if isinstance(result, dict) else {},
            }
        return results

    def get_latest_completed_results_by_person_ids(self) -> dict[str, dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    cluster_id,
                    label,
                    result_json,
                    batch_run_id,
                    updated_at,
                    id
                FROM negative_news_cluster_results
                WHERE status = 'completed'
                ORDER BY batch_run_id DESC, updated_at DESC, id DESC
                """
            ).fetchall()
        results: dict[str, dict[str, Any]] = {}
        for row in rows:
            try:
                result = json.loads(str(row["result_json"] or "{}"))
            except json.JSONDecodeError:
                continue
            fingerprint = result_person_ids_fingerprint(result)
            if not fingerprint or fingerprint in results:
                continue
            results[fingerprint] = {
                "cluster_id": str(row["cluster_id"] or ""),
                "label": str(row["label"] or ""),
                "batch_run_id": int(row["batch_run_id"] or 0),
                "updated_at": str(row["updated_at"] or ""),
                "result": result if isinstance(result, dict) else {},
            }
        return results

    def get_latest_completed_results_by_cluster_lookup_key(self) -> dict[str, dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    cluster_id,
                    label,
                    result_json,
                    batch_run_id,
                    updated_at,
                    id
                FROM negative_news_cluster_results
                WHERE status = 'completed'
                ORDER BY batch_run_id DESC, updated_at DESC, id DESC
                """
            ).fetchall()
        results: dict[str, dict[str, Any]] = {}
        for row in rows:
            try:
                result = json.loads(str(row["result_json"] or "{}"))
            except json.JSONDecodeError:
                continue
            lookup_key = cluster_lookup_key(result)
            if not lookup_key or lookup_key in results:
                continue
            results[lookup_key] = {
                "cluster_id": str(row["cluster_id"] or ""),
                "label": str(row["label"] or ""),
                "batch_run_id": int(row["batch_run_id"] or 0),
                "updated_at": str(row["updated_at"] or ""),
                "result": result if isinstance(result, dict) else {},
            }
        return results

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

from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from src.config import Settings


_CATEGORY_PRIORITY = {
    "explicit_mb_connection": 0,
    "writes_for_mb_outlet": 1,
    "other_mb_alignment": 2,
}


def resolve_negative_news_db_path(settings: Settings) -> Path:
    raw = os.getenv("NEGATIVE_NEWS_DB_PATH", "").strip()
    if raw:
        return Path(raw)
    return settings.project_root / "data" / "negative_news.sqlite"


def _claim_sort_key(claim: dict[str, Any]) -> tuple[int, float, str]:
    category = str(claim.get("category") or "").strip()
    confidence = float(claim.get("confidence") or 0.0)
    title = str(claim.get("title") or "").strip().lower()
    return (_CATEGORY_PRIORITY.get(category, 99), -confidence, title)


def _extract_claims_from_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for article in result.get("articles", []):
        if not isinstance(article, dict):
            continue
        classification = article.get("classification") or {}
        category = str(classification.get("category") or "").strip()
        if not category or category == "reject":
            continue
        search = article.get("search") or {}
        claims.append(
            {
                "category": category,
                "confidence": classification.get("confidence"),
                "short_rationale": classification.get("short_rationale"),
                "evidence_quote": classification.get("evidence_quote"),
                "url": search.get("url"),
                "title": search.get("title"),
            }
        )
    claims.sort(key=_claim_sort_key)
    return claims


def _latest_cluster_claims(database_path: Path) -> dict[str, list[dict[str, Any]]]:
    if not database_path.exists():
        return {}
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                cluster_id,
                result_json,
                batch_run_id,
                updated_at
            FROM negative_news_cluster_results
            WHERE status = 'completed'
            ORDER BY batch_run_id DESC, updated_at DESC, id DESC
            """
        ).fetchall()
    finally:
        connection.close()

    claims_by_cluster: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        cluster_id = str(row["cluster_id"] or "").strip()
        if not cluster_id or cluster_id in claims_by_cluster:
            continue
        try:
            result = json.loads(str(row["result_json"] or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(result, dict):
            continue
        claims = _extract_claims_from_result(result)
        if claims:
            claims_by_cluster[cluster_id] = claims
    return claims_by_cluster


def _adverse_media_warning(claims: list[dict[str, Any]]) -> str:
    counts = Counter(str(claim.get("category") or "").strip() for claim in claims)
    parts = []
    for category in ("explicit_mb_connection", "writes_for_mb_outlet", "other_mb_alignment"):
        count = counts.get(category, 0)
        if count:
            parts.append(f"{count} {category.replace('_', ' ')}")
    suffix = f": {', '.join(parts)}" if parts else ""
    return f"\u26a0\ufe0f <strong>ADVERSE MEDIA</strong>{suffix}"


def annotate_graph_with_adverse_media(
    data: dict[str, Any],
    *,
    database_path: Path,
) -> dict[str, Any]:
    claims_by_cluster = _latest_cluster_claims(database_path)
    if not claims_by_cluster:
        return data

    for node in data.get("nodes", []):
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        claims = claims_by_cluster.get(node_id)
        if not claims:
            continue
        node["adverse_media_hit"] = True
        node["adverse_media_count"] = len(claims)
        node["adverse_media_claims"] = claims
        tooltip_lines = list(node.get("tooltip_lines") or [])
        warning = _adverse_media_warning(claims)
        if not tooltip_lines or tooltip_lines[0] != warning:
            node["tooltip_lines"] = [warning, *tooltip_lines]
    return data

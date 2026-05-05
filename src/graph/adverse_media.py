from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.search.queries import normalize_name
from src.storage.negative_news_store import cluster_lookup_key, database_source_key, person_ids_fingerprint


_CATEGORY_PRIORITY = {
    "explicit_mb_connection": 0,
    "writes_for_mb_outlet": 1,
    "other_mb_alignment": 2,
}
_ARABIC_SCRIPT_RE = re.compile(r"[\u0600-\u06FF]")


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


def _needs_title_translation(title: str) -> bool:
    return bool(_ARABIC_SCRIPT_RE.search(title or ""))


def _translate_title_to_english(*, gemini: GeminiClient, title: str) -> str:
    prompt = f"""Translate this news headline into concise natural English.

Return only the English headline text, with no quotes and no explanation.
If it is already in English, return it unchanged.

Headline:
{title}
"""
    response = gemini.generate(model="gemini-2.0-flash", prompt=prompt, temperature=0.0)
    return extract_gemini_text(response).strip()


def _result_source_key(row: sqlite3.Row) -> str:
    try:
        config = json.loads(str(row["config_json"] or "{}"))
    except json.JSONDecodeError:
        return ""
    if not isinstance(config, dict):
        return ""
    return str(config.get("source_database_key") or "").strip()


def _latest_cluster_claims(
    database_path: Path,
    *,
    source_database_key: str = "",
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
]:
    if not database_path.exists():
        return {}, {}, {}, {}
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                r.cluster_id,
                r.result_json,
                r.batch_run_id,
                r.updated_at,
                b.config_json
            FROM negative_news_cluster_results r
            JOIN negative_news_batch_runs b ON b.id = r.batch_run_id
            WHERE r.status = 'completed'
            ORDER BY r.batch_run_id DESC, r.updated_at DESC, r.id DESC
            """
        ).fetchall()
    finally:
        connection.close()

    source_key = str(source_database_key or "").strip()
    if source_key:
        scoped_rows = [row for row in rows if _result_source_key(row) == source_key]
        rows = scoped_rows if scoped_rows else [row for row in rows if not _result_source_key(row)]

    claims_by_cluster: dict[str, list[dict[str, Any]]] = {}
    claims_by_lookup_key: dict[str, list[dict[str, Any]]] = {}
    claims_by_person_ids: dict[str, list[dict[str, Any]]] = {}
    claims_by_person_name: dict[str, list[dict[str, Any]]] = {}
    person_name_owner: dict[str, str] = {}
    ambiguous_person_names: set[str] = set()
    for row in rows:
        cluster_id = str(row["cluster_id"] or "").strip()
        try:
            result = json.loads(str(row["result_json"] or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(result, dict):
            continue
        claims = _extract_claims_from_result(result)
        if not claims:
            continue
        if cluster_id and cluster_id not in claims_by_cluster:
            claims_by_cluster[cluster_id] = claims
        lookup_key = cluster_lookup_key(result)
        if lookup_key and lookup_key not in claims_by_lookup_key:
            claims_by_lookup_key[lookup_key] = claims
        fingerprint = person_ids_fingerprint(result.get("person_ids"))
        if fingerprint and fingerprint not in claims_by_person_ids:
            claims_by_person_ids[fingerprint] = claims
        if _historical_cluster_kind(cluster_id, result) != "person":
            continue
        for name_key in _person_name_keys(result):
            if name_key in ambiguous_person_names:
                continue
            owner = person_name_owner.get(name_key)
            if owner and owner != cluster_id:
                ambiguous_person_names.add(name_key)
                claims_by_person_name.pop(name_key, None)
                person_name_owner.pop(name_key, None)
                continue
            person_name_owner[name_key] = cluster_id
            claims_by_person_name[name_key] = claims
    return claims_by_cluster, claims_by_lookup_key, claims_by_person_ids, claims_by_person_name


def _historical_cluster_kind(cluster_id: str, result: dict[str, Any]) -> str:
    cluster_kind = str(result.get("cluster_kind") or result.get("kind") or "").strip().lower()
    if cluster_kind in {"seed_alias", "person"}:
        return cluster_kind
    lowered = str(cluster_id or "").strip().lower()
    if lowered.startswith("identity:") or lowered.startswith("identity_cluster:"):
        return "seed_alias"
    if lowered.startswith("merged_person:") or lowered.startswith("person:"):
        return "person"
    return ""


def _person_name_keys(payload: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in [payload.get("label"), *(payload.get("aliases") or [])]:
        normalized = normalize_name(str(value or ""))
        if normalized:
            keys.add(normalized)
    return keys


def _can_use_direct_cluster_match(node: dict[str, Any]) -> bool:
    node_id = str(node.get("id") or "").strip().lower()
    node_kind = str(node.get("kind") or "").strip().lower()
    if node_kind == "person":
        return False
    if node_id.startswith("merged_person:") or node_id.startswith("person:"):
        return False
    return True


def _translate_claim_titles(
    claims_by_cluster: dict[str, list[dict[str, Any]]],
    *,
    settings: Settings,
) -> dict[str, list[dict[str, Any]]]:
    if not settings.gemini_api_key:
        return claims_by_cluster
    gemini = GeminiClient(
        api_key=settings.gemini_api_key,
        cache_dir=settings.cache_dir / "negative_news" / "title_translate",
        timeout_seconds=20.0,
        attempts=2,
    )
    translated: dict[str, list[dict[str, Any]]] = {}
    for cluster_id, claims in claims_by_cluster.items():
        next_claims: list[dict[str, Any]] = []
        for claim in claims:
            next_claim = dict(claim)
            title = str(claim.get("title") or "").strip()
            if title and _needs_title_translation(title):
                try:
                    translated_title = _translate_title_to_english(gemini=gemini, title=title)
                except Exception:
                    translated_title = ""
                if translated_title:
                    next_claim["translated_title"] = translated_title
            next_claims.append(next_claim)
        translated[cluster_id] = next_claims
    return translated

def annotate_graph_with_adverse_media(
    data: dict[str, Any],
    *,
    settings: Settings,
    database_path: Path,
) -> dict[str, Any]:
    claims_by_cluster, claims_by_lookup_key, claims_by_person_ids, claims_by_person_name = _latest_cluster_claims(
        database_path,
        source_database_key=database_source_key(settings.database_path),
    )
    if not claims_by_cluster and not claims_by_lookup_key and not claims_by_person_ids and not claims_by_person_name:
        return data
    claims_by_cluster = _translate_claim_titles(claims_by_cluster, settings=settings)
    claims_by_lookup_key = _translate_claim_titles(claims_by_lookup_key, settings=settings)
    claims_by_person_ids = _translate_claim_titles(claims_by_person_ids, settings=settings)
    claims_by_person_name = _translate_claim_titles(claims_by_person_name, settings=settings)

    for node in data.get("nodes", []):
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("id") or "").strip()
        lookup_key = cluster_lookup_key(node)
        fingerprint = person_ids_fingerprint(node.get("person_ids"))
        claims = claims_by_cluster.get(node_id) if _can_use_direct_cluster_match(node) else None
        if not claims:
            claims = claims_by_lookup_key.get(lookup_key) if lookup_key else None
        if not claims:
            claims = claims_by_person_ids.get(fingerprint) if fingerprint else None
        if not claims and str(node.get("kind") or "") == "person":
            candidate_claims: list[dict[str, Any]] | None = None
            for name_key in sorted(_person_name_keys(node)):
                current_claims = claims_by_person_name.get(name_key)
                if not current_claims:
                    continue
                if candidate_claims is None:
                    candidate_claims = current_claims
                    continue
                if candidate_claims != current_claims:
                    candidate_claims = None
                    break
            claims = candidate_claims
        if not claims:
            continue
        node["adverse_media_hit"] = True
        node["adverse_media_count"] = len(claims)
        node["adverse_media_claims"] = claims
    return data

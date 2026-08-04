from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from src.config import Settings
from src.models import CandidateMatch
from src.resolution.matcher import HybridMatcher
from src.search.queries import normalize_name

_STATUSES = ("match", "maybe_match", "no_match")


def benchmark_historical_decisions(
    *,
    database_path: Path,
    settings: Settings,
    per_status: int = 4,
) -> dict[str, Any]:
    samples = _load_balanced_samples(database_path, per_status=per_status)
    benchmark_settings = replace(
        settings,
        resolution_provider="openrouter",
        cache_dir=settings.cache_dir / "resolution_benchmark",
    )
    matcher = HybridMatcher(benchmark_settings)
    workers = min(settings.resolution_workers, max(1, len(samples)))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="istari-benchmark") as executor:
        new_decisions = list(
            executor.map(
                lambda sample: matcher.resolve(sample["seed_name"], sample["candidate"]),
                samples,
            )
        )

    comparisons = []
    for sample, decision in zip(samples, new_decisions, strict=True):
        comparisons.append(
            {
                "candidate_id": sample["candidate_id"],
                "seed_name": sample["seed_name"],
                "candidate_name": sample["candidate"].candidate_name,
                "organisation_name": sample["candidate"].organisation_name,
                "rule_score": sample["candidate"].score,
                "baseline": sample["baseline"],
                "openrouter": asdict(decision),
                "agrees": sample["baseline"]["status"] == decision.status,
            }
        )

    agreements = sum(1 for row in comparisons if row["agrees"])
    return {
        "database": str(database_path),
        "model": settings.openrouter_resolution_model,
        "sample_count": len(comparisons),
        "agreement_count": agreements,
        "agreement_rate": round(agreements / len(comparisons), 4) if comparisons else 0.0,
        "comparisons": comparisons,
    }


def _load_balanced_samples(database_path: Path, *, per_status: int) -> list[dict[str, Any]]:
    uri = f"{database_path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT rd.candidate_match_id, rd.llm_payload_json, r.seed_name,
                   cm.variant_name, cm.candidate_name, cm.organisation_name,
                   cm.registry_type, cm.registry_number, cm.suffix, cm.source,
                   cm.evidence_id, cm.feature_payload_json, cm.score, cm.raw_payload_json
            FROM resolution_decisions rd
            JOIN candidate_matches cm ON cm.id = rd.candidate_match_id
            JOIN runs r ON r.id = rd.run_id
            WHERE rd.llm_payload_json NOT IN ('{}', 'null', '')
            ORDER BY rd.id
            """
        ).fetchall()
    finally:
        connection.close()

    selected: dict[str, list[dict[str, Any]]] = {status: [] for status in _STATUSES}
    seen_name_pairs: set[tuple[str, str]] = set()
    for row in rows:
        llm_payload = _json_object(row["llm_payload_json"])
        baseline = llm_payload.get("document")
        if not isinstance(baseline, dict):
            continue
        status = str(baseline.get("status") or "")
        if status not in selected or len(selected[status]) >= per_status:
            continue
        raw_payload = _json_object(row["raw_payload_json"])
        seed_name = str(
            raw_payload.get("discovery_frontier_name")
            or row["variant_name"]
            or row["seed_name"]
        )
        name_pair = (normalize_name(seed_name), normalize_name(str(row["candidate_name"])))
        if name_pair in seen_name_pairs:
            continue
        seen_name_pairs.add(name_pair)
        candidate = CandidateMatch(
            name_variant=str(row["variant_name"]),
            candidate_name=str(row["candidate_name"]),
            organisation_name=str(row["organisation_name"]),
            registry_type=row["registry_type"],
            registry_number=row["registry_number"],
            suffix=int(row["suffix"]),
            source=str(row["source"]),
            evidence_id=row["evidence_id"],
            feature_payload=_json_object(row["feature_payload_json"]),
            score=float(row["score"]),
            raw_payload=raw_payload,
        )
        selected[status].append(
            {
                "candidate_id": int(row["candidate_match_id"]),
                "seed_name": seed_name,
                "candidate": candidate,
                "baseline": baseline,
            }
        )
        if all(len(group) >= per_status for group in selected.values()):
            break
    return [sample for status in _STATUSES for sample in selected[status]]


def _json_object(value: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}

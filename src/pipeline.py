from __future__ import annotations

from typing import Any

from src.charity_commission.client import CharityCommissionClient
from src.resolution.matcher import HybridMatcher
from src.search.provider import SearchProvider
from src.services.mvp_pipeline import (
    add_organisation_to_run,
    run_registry_only_mvp,
    step1_expand_seed,
    step2_expand_connected_organisations,
    step2b_enrich_from_pdfs,
    step3_expand_connected_people,
    step4_ofac_screening,
)
from src.storage.repository import Repository


def run_name_pipeline(
    *,
    repository: Repository,
    settings: Any,
    charity_client: CharityCommissionClient,
    search_providers: list[SearchProvider],
    matcher: HybridMatcher,
    seed_name: str,
    creativity_level: str,
    limit: int,
) -> dict[str, Any]:
    return run_registry_only_mvp(
        repository=repository,
        settings=settings,
        charity_client=charity_client,
        search_providers=search_providers,
        matcher=matcher,
        seed_name=seed_name,
        creativity_level=creativity_level,
        limit=limit,
    )


def run_seed_batch_pipeline(
    *,
    repository: Repository,
    settings: Any,
    charity_client: CharityCommissionClient,
    search_providers: list[SearchProvider],
    matcher: HybridMatcher,
    seed_names: list[str],
    creativity_level: str,
    limit: int,
    overlap_limit: int = 25,
) -> dict[str, Any]:
    cleaned_seeds: list[str] = []
    seen: set[str] = set()
    for value in seed_names:
        seed = " ".join(str(value).split()).strip()
        if not seed:
            continue
        key = seed.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned_seeds.append(seed)

    runs: list[dict[str, Any]] = []
    run_ids: list[int] = []
    resolution_metrics_total: dict[str, int] = {}
    for seed in cleaned_seeds:
        result = run_name_pipeline(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            search_providers=search_providers,
            matcher=matcher,
            seed_name=seed,
            creativity_level=creativity_level,
            limit=limit,
        )
        run_id = int(result["run_id"])
        run_ids.append(run_id)
        metrics = result.get("resolution_metrics", {})
        for key, value in metrics.items():
            resolution_metrics_total[key] = int(resolution_metrics_total.get(key, 0)) + int(value)
        runs.append(
            {
                "seed_name": seed,
                "run_id": run_id,
                "decision_count": int(result.get("decision_count", 0)),
                "search_summary": result.get("search_summary", {}),
                "resolution_metrics": metrics,
                "top_ranking": list(result.get("ranking", []))[:10],
            }
        )

    overlap_people = repository.get_overlap_people_for_runs(run_ids, limit=overlap_limit)
    overlap_orgs = repository.get_overlap_organisations_for_runs(run_ids, limit=overlap_limit)
    return {
        "mode": "multi_seed",
        "seed_names": cleaned_seeds,
        "run_ids": run_ids,
        "runs": runs,
        "overlap_people": _decorate_overlap_people([dict(row) for row in overlap_people]),
        "overlap_organisations": _decorate_overlap_organisations([dict(row) for row in overlap_orgs]),
        "aggregate_resolution_metrics": resolution_metrics_total,
    }


def _decorate_overlap_people(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(rows, 1):
        seed_count = int(row.get("seed_count") or 0)
        weighted = float(row.get("weighted_organisation_score") or 0.0)
        confidence_sum = float(row.get("confidence_sum") or 0.0)
        row["overlap_priority_score"] = round((seed_count * 100.0) + weighted + (confidence_sum * 0.01), 4)
        row["rank"] = rank
        out.append(row)
    return out


def _decorate_overlap_organisations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(rows, 1):
        seed_count = int(row.get("seed_count") or 0)
        person_count = int(row.get("person_count") or 0)
        weighted = float(row.get("weighted_organisation_score") or 0.0)
        row["overlap_priority_score"] = round((seed_count * 100.0) + (person_count * 5.0) + weighted, 4)
        row["rank"] = rank
        out.append(row)
    return out


__all__ = [
    "run_name_pipeline",
    "run_seed_batch_pipeline",
    "add_organisation_to_run",
    "step1_expand_seed",
    "step2_expand_connected_organisations",
    "step2b_enrich_from_pdfs",
    "step3_expand_connected_people",
    "step4_ofac_screening",
]

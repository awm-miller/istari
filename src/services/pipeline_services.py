from __future__ import annotations

import json
import logging
from dataclasses import asdict
from typing import Any

from src.models import NameVariant, OrganisationRecord, ResolutionDecision
from src.ranking import rank_people
from src.resolution.features import build_candidate_match, build_person_identity_key
from src.resolution.matcher import HybridMatcher
from src.search.provider import SearchProvider
from src.search.queries import generate_name_variants, normalize_name
from src.services.registry_ingestion import ingest_registry_evidence_items
from src.services.relation_semantics import (
    apply_birth_month_year_guard,
    apply_conflicting_middle_name_guard,
    candidate_birth_month_year,
    candidate_matches_known_birth_month_year,
    apply_low_information_name_guard,
    apply_weak_name_match_guard,
    candidate_relationship_kind,
    candidate_relationship_phrase,
    candidate_role_label,
    candidate_role_type,
)
from src.storage.repository import Repository

log = logging.getLogger("istari.pipeline")


class VariantService:
    def generate_and_store(
        self,
        repository: Repository,
        run_id: int,
        seed_name: str,
        creativity_level: str,
        settings: Any | None = None,
    ) -> list[NameVariant]:
        seed_variant = NameVariant(
            name=" ".join(str(seed_name).split()).strip(),
            strategy="seed_input",
            creativity_level=creativity_level,
        )
        generated = generate_name_variants(seed_name, creativity_level)
        seen = {seed_variant.name.lower()}
        variants = [seed_variant]
        for v in generated:
            if v.name.lower() not in seen:
                seen.add(v.name.lower())
                variants.append(v)
        repository.insert_name_variants(run_id, [asdict(variant) for variant in variants])
        log.info(
            "Generated %d variants for '%s' at level=%s",
            len(variants),
            seed_name,
            creativity_level,
        )
        return variants


class DiscoveryService:
    def search_name(
        self,
        *,
        repository: Repository,
        charity_client: Any,
        search_providers: list[SearchProvider],
        run_id: int,
        variants: list[NameVariant],
    ) -> dict[str, Any]:
        evidence_count = 0
        candidate_count = 0
        search_provider_metrics: dict[str, dict[str, int]] = {}

        log.info(
            "Searching %d direct seed variants across %d registry providers",
            len(variants),
            len(search_providers),
        )

        for search_provider in search_providers:
            provider_name = type(search_provider).__name__
            log.info("Running %s ...", provider_name)
            try:
                provider_items = search_provider.search(variants)
            except RuntimeError as exc:
                log.warning("  %s failed: %s", provider_name, exc)
                continue
            provider_metrics = getattr(search_provider, "metrics", None)
            if isinstance(provider_metrics, dict):
                search_provider_metrics[provider_name] = {
                    str(key): int(value) for key, value in provider_metrics.items()
                }
            log.info("  %s returned %d evidence items", provider_name, len(provider_items))
            added_evidence, added_candidates = ingest_registry_evidence_items(
                repository=repository,
                run_id=run_id,
                items=provider_items,
                provider_name=provider_name,
                log=log,
            )
            evidence_count += added_evidence
            candidate_count += added_candidates

        log.info(
            "Search complete: %d evidence items, %d candidates",
            evidence_count,
            candidate_count,
        )
        return {
            "evidence_count": evidence_count,
            "candidate_count": candidate_count,
            "provider_metrics": search_provider_metrics,
        }


class ResolutionService:
    def __init__(self) -> None:
        self.last_metrics: dict[str, int] = _blank_resolution_metrics()

    def resolve_candidates(
        self,
        *,
        repository: Repository,
        matcher: HybridMatcher,
        run_id: int,
    ) -> list[dict[str, Any]]:
        run = repository.get_run(run_id)
        if run is None:
            raise RuntimeError(f"Run {run_id} does not exist.")

        decisions: list[dict[str, Any]] = []
        unresolved = list(repository.get_unresolved_candidate_matches(run_id))
        metrics = _blank_resolution_metrics()
        metrics["unresolved_candidates"] = len(unresolved)
        grouped: dict[tuple[str, str, str, str, int, str], list[tuple[Any, Any]]] = {}
        for row in unresolved:
            candidate = _candidate_from_row(row)
            key = _resolution_group_key(candidate)
            grouped.setdefault(key, []).append((row, candidate))

        unique_groups = list(grouped.values())
        metrics["unique_resolution_groups"] = len(unique_groups)
        metrics["dedupe_saved_candidates"] = max(0, len(unresolved) - len(unique_groups))
        log.info(
            "Resolving %d unresolved candidates for run %d (%d unique groups)",
            len(unresolved),
            run_id,
            len(unique_groups),
        )

        total = len(unresolved)
        processed = 0
        known_seed_birth_month_years: set[tuple[int, int]] = set()
        for group in unique_groups:
            _representative_row, representative_candidate = max(
                group,
                key=lambda pair: float(pair[0]["score"]),
            )
            base_decision = matcher.resolve(str(run["seed_name"]), representative_candidate)
            base_decision = apply_low_information_name_guard(
                seed_name=str(run["seed_name"]),
                candidate=representative_candidate,
                decision=base_decision,
            )
            minimum_similarity = 0.4 if candidate_matches_known_birth_month_year(
                candidate=representative_candidate,
                known_birth_month_years=known_seed_birth_month_years,
            ) else 0.55
            base_decision = apply_weak_name_match_guard(
                seed_name=str(run["seed_name"]),
                candidate=representative_candidate,
                decision=base_decision,
                minimum_similarity=minimum_similarity,
            )
            base_decision = apply_conflicting_middle_name_guard(
                seed_name=str(run["seed_name"]),
                candidate=representative_candidate,
                decision=base_decision,
            )
            base_decision = apply_birth_month_year_guard(
                candidate=representative_candidate,
                decision=base_decision,
                known_birth_month_years=known_seed_birth_month_years,
            )
            group_size = len(group)
            if _decision_used_llm(base_decision):
                metrics["groups_resolved_by_llm"] += 1
                metrics["candidates_resolved_by_llm"] += group_size
                metrics["llm_calls"] += 1
                metrics["dedupe_saved_llm_calls"] += max(0, group_size - 1)
            else:
                metrics["groups_resolved_by_rule"] += 1
                metrics["candidates_resolved_by_rule"] += group_size

            for row, candidate in group:
                processed += 1
                decision = _decision_for_duplicate(base_decision, candidate)
                decision.alias_status = "none"
                log.info(
                    "  [%d/%d] '%s' @ '%s' -> %s (%.2f)",
                    processed,
                    total,
                    candidate.candidate_name,
                    candidate.organisation_name,
                    decision.status,
                    decision.confidence,
                )
                repository.insert_resolution_decision(run_id, int(row["id"]), decision)

                if (
                    candidate.registry_type
                    and candidate.registry_number
                    and decision.status == "match"
                ):
                    organisation_id = repository.upsert_organisation(
                        OrganisationRecord(
                            registry_type=candidate.registry_type,
                            registry_number=candidate.registry_number,
                            suffix=candidate.suffix,
                            name=candidate.organisation_name,
                            metadata={"source": candidate.source},
                        )
                    )
                    person_id = repository.upsert_person(
                        decision.canonical_name,
                        identity_key=decision.person_identity_key,
                    )
                    confidence_class, edge_weight = _candidate_confidence(decision)
                    repository.upsert_role(
                        person_id=person_id,
                        organisation_id=organisation_id,
                        role_type=candidate_role_type(candidate),
                        role_label=candidate_role_label(candidate),
                        relationship_kind=candidate_relationship_kind(candidate),
                        relationship_phrase=candidate_relationship_phrase(candidate),
                        source=candidate.source,
                        confidence_class=confidence_class,
                        edge_weight=edge_weight,
                        provenance={
                            "candidate_match": candidate.raw_payload,
                            "decision": {
                                "status": decision.status,
                                "confidence": decision.confidence,
                                **(
                                    {"llm_payload": decision.llm_payload}
                                    if decision.llm_payload
                                    else {}
                                ),
                            },
                        },
                    )
                    birth_month, birth_year = candidate_birth_month_year(candidate)
                    if (
                        candidate.source.startswith("companies_house")
                        and birth_month
                        and birth_year
                    ):
                        known_seed_birth_month_years.add((birth_month, birth_year))

                decisions.append(
                    {
                        "candidate_name": candidate.candidate_name,
                        "organisation_name": candidate.organisation_name,
                        "status": decision.status,
                        "confidence": decision.confidence,
                        "alias_status": "none",
                    }
                )

        matches = sum(1 for decision in decisions if decision["status"] == "match")
        maybes = sum(1 for decision in decisions if decision["status"] == "maybe_match")
        rejects = sum(1 for decision in decisions if decision["status"] == "no_match")
        log.info(
            "Resolution done: %d match, %d maybe, %d rejected",
            matches,
            maybes,
            rejects,
        )
        metrics["match_count"] = matches
        metrics["maybe_match_count"] = maybes
        metrics["no_match_count"] = rejects
        self.last_metrics = metrics
        log.info("Resolution metrics: %s", metrics)
        return decisions


class RankingService:
    def rank(self, repository: Repository, run_id: int, limit: int) -> list[dict[str, Any]]:
        return [
            asdict(entry)
            for entry in rank_people(repository, limit=limit, run_id=run_id)
        ]


def _candidate_from_row(row: Any) -> Any:
    return build_candidate_match(
        name_variant=str(row["variant_name"]),
        candidate_name=str(row["candidate_name"]),
        organisation_name=str(row["organisation_name"]),
        registry_type=row["registry_type"],
        registry_number=row["registry_number"],
        suffix=int(row["suffix"]),
        source=str(row["source"]),
        evidence_id=row["evidence_id"],
        raw_payload=json.loads(row["raw_payload_json"]),
    )


def _decision_for_duplicate(base_decision: ResolutionDecision, candidate: Any) -> ResolutionDecision:
    canonical_name = str(base_decision.canonical_name or candidate.candidate_name).strip()
    if not canonical_name:
        canonical_name = str(candidate.candidate_name or "").strip()
    return ResolutionDecision(
        status=base_decision.status,
        confidence=float(base_decision.confidence),
        canonical_name=canonical_name,
        person_identity_key=build_person_identity_key(
            canonical_name,
            source=str(candidate.source or ""),
            raw_payload=dict(candidate.raw_payload or {}),
        ),
        explanation=str(base_decision.explanation),
        rule_score=float(base_decision.rule_score),
        alias_status="none",
        llm_payload=dict(base_decision.llm_payload) if base_decision.llm_payload else {},
    )


def _candidate_confidence(decision: ResolutionDecision) -> tuple[str, float]:
    if decision.status == "match":
        return ("verified", 1.0)
    return ("tentative", 0.35)


def _decision_used_llm(decision: ResolutionDecision) -> bool:
    payload = decision.llm_payload or {}
    return bool(payload.get("response_id")) or bool(payload.get("document"))


def _blank_resolution_metrics() -> dict[str, int]:
    return {
        "unresolved_candidates": 0,
        "unique_resolution_groups": 0,
        "dedupe_saved_candidates": 0,
        "groups_resolved_by_rule": 0,
        "groups_resolved_by_llm": 0,
        "candidates_resolved_by_rule": 0,
        "candidates_resolved_by_llm": 0,
        "llm_calls": 0,
        "dedupe_saved_llm_calls": 0,
        "match_count": 0,
        "maybe_match_count": 0,
        "no_match_count": 0,
    }


def _resolution_group_key(candidate: Any) -> tuple[str, str, str, str, int, str]:
    return (
        normalize_name(str(candidate.candidate_name or "")),
        normalize_name(str(candidate.organisation_name or "")),
        str(candidate.registry_type or ""),
        str(candidate.registry_number or ""),
        int(candidate.suffix or 0),
        build_person_identity_key(
            str(candidate.candidate_name or ""),
            source=str(candidate.source or ""),
            raw_payload=dict(candidate.raw_payload or {}),
        ),
    )

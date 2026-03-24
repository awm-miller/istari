from __future__ import annotations

from dataclasses import asdict
from difflib import SequenceMatcher
from typing import Any

from src.models import CandidateMatch
from src.search.queries import normalize_name


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, normalize_name(left), normalize_name(right)).ratio()


def person_name_similarity(left: str, right: str) -> float:
    left_normalized = normalize_name(left)
    right_normalized = normalize_name(right)
    if not left_normalized or not right_normalized:
        return 0.0

    score = SequenceMatcher(None, left_normalized, right_normalized).ratio()
    left_tokens = left_normalized.split()
    right_tokens = right_normalized.split()

    if left_tokens and right_tokens and left_tokens[-1] != right_tokens[-1]:
        score *= 0.5
    if left_tokens and right_tokens and left_tokens[0][0] != right_tokens[0][0]:
        score *= 0.7

    return round(score, 4)


def build_candidate_match(
    *,
    name_variant: str,
    candidate_name: str,
    organisation_name: str,
    registry_type: str | None,
    registry_number: str | None,
    suffix: int,
    source: str,
    evidence_id: int | None,
    raw_payload: dict[str, Any],
) -> CandidateMatch:
    name_similarity = person_name_similarity(name_variant, candidate_name)
    organisation_similarity = similarity(
        organisation_name,
        raw_payload.get("organisation_name") or organisation_name,
    )
    has_registry_number = 1.0 if registry_number else 0.0
    source_weight = 0.55 if source == "charity_commission_search" else 1.0

    feature_payload = {
        "name_similarity": round(name_similarity, 4),
        "organisation_similarity": round(organisation_similarity, 4),
        "registry_type": registry_type,
        "has_registry_number": has_registry_number,
        "source_weight": source_weight,
        "source": source,
    }

    score = round(
        (
            (name_similarity * 0.7)
            + (organisation_similarity * 0.2)
            + (has_registry_number * 0.1)
        )
        * source_weight,
        4,
    )

    return CandidateMatch(
        name_variant=name_variant,
        candidate_name=candidate_name,
        organisation_name=organisation_name,
        registry_type=registry_type,
        registry_number=registry_number,
        suffix=suffix,
        source=source,
        evidence_id=evidence_id,
        feature_payload=feature_payload,
        score=score,
        raw_payload=raw_payload,
    )


def candidate_to_dict(candidate: CandidateMatch) -> dict[str, Any]:
    return asdict(candidate)

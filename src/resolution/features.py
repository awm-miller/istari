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


def extract_birth_month_year(raw_payload: dict[str, Any]) -> tuple[int | None, int | None]:
    evidence = raw_payload.get("evidence", {}) if isinstance(raw_payload, dict) else {}
    officer_search_item = evidence.get("officer_search_item", {}) if isinstance(evidence, dict) else {}
    date_of_birth = {}
    if isinstance(officer_search_item, dict):
        date_of_birth = officer_search_item.get("date_of_birth", {}) or {}
    if not date_of_birth and isinstance(raw_payload, dict):
        date_of_birth = raw_payload.get("date_of_birth", {}) or {}
    month = date_of_birth.get("month")
    year = date_of_birth.get("year")
    try:
        month_value = int(month) if month not in (None, "") else None
    except (TypeError, ValueError):
        month_value = None
    try:
        year_value = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        year_value = None
    return month_value, year_value


def build_person_identity_key(
    canonical_name: str,
    *,
    source: str = "",
    raw_payload: dict[str, Any] | None = None,
) -> str:
    normalized_name = normalize_name(canonical_name)
    if not normalized_name:
        raise ValueError("Person name is required.")

    payload = raw_payload or {}
    evidence = payload.get("evidence", {}) if isinstance(payload, dict) else {}
    officer_id = payload.get("officer_id") or (evidence.get("officer_id") if isinstance(evidence, dict) else None)
    birth_month, birth_year = extract_birth_month_year(payload)

    if officer_id and str(source or "").startswith("companies_house"):
        key = f"ch-officer:{str(officer_id).strip().lower()}"
        if birth_month and birth_year:
            key += f":{birth_year:04d}-{birth_month:02d}"
        return key

    if str(source or "").startswith("companies_house") and birth_month and birth_year:
        return f"ch-name-dob:{normalized_name}:{birth_year:04d}-{birth_month:02d}"

    return f"name:{normalized_name}"


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
    birth_month, birth_year = extract_birth_month_year(raw_payload)
    person_identity_key = build_person_identity_key(
        candidate_name,
        source=source,
        raw_payload=raw_payload,
    )

    feature_payload = {
        "name_similarity": round(name_similarity, 4),
        "organisation_similarity": round(organisation_similarity, 4),
        "registry_type": registry_type,
        "has_registry_number": has_registry_number,
        "source_weight": source_weight,
        "source": source,
        "birth_month": birth_month,
        "birth_year": birth_year,
        "person_identity_key": person_identity_key,
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

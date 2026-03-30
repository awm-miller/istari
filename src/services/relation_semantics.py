from __future__ import annotations

from typing import Any

from src.models import ResolutionDecision
from src.resolution.features import extract_birth_month_year, person_name_similarity
from src.search.queries import is_low_information_person_name, normalize_name


def _middle_name_tokens_conflict(left: str, right: str) -> bool:
    left_tokens = normalize_name(left).split()
    right_tokens = normalize_name(right).split()
    if len(left_tokens) < 3 or len(right_tokens) < 3:
        return False
    if left_tokens[0] != right_tokens[0]:
        return False
    if left_tokens[-1] != right_tokens[-1]:
        return False
    left_middle = set(left_tokens[1:-1])
    right_middle = set(right_tokens[1:-1])
    if not left_middle or not right_middle:
        return False
    return left_middle.isdisjoint(right_middle)


def candidate_role_type(candidate: Any) -> str:
    direct = str(candidate.raw_payload.get("role_type") or "").strip()
    if direct:
        return direct
    if candidate.source.startswith("companies_house"):
        return "company_officer"
    return "candidate_link"


def candidate_role_label(candidate: Any) -> str:
    direct = str(candidate.raw_payload.get("role_label") or "").strip()
    if direct:
        return direct
    if candidate.source.startswith("companies_house"):
        appointment = candidate.raw_payload.get("evidence", {}).get("appointment", {})
        return appointment.get("officer_role") or "company_officer"
    return "possible_association"


def candidate_relationship_kind(candidate: Any) -> str:
    direct = str(candidate.raw_payload.get("relationship_kind") or "").strip()
    if direct:
        return direct
    role_type = candidate_role_type(candidate).lower()
    if "trustee" in role_type:
        return "trustee_of"
    if "director" in role_type:
        return "director_of"
    if "secretary" in role_type:
        return "secretary_of"
    if "accountant" in role_type or "auditor" in role_type or "examiner" in role_type:
        return "accountant_of"
    return "linked_to"


def candidate_relationship_phrase(candidate: Any) -> str:
    direct = str(candidate.raw_payload.get("relationship_phrase") or "").strip()
    if direct:
        return direct
    role_type = candidate_role_type(candidate).lower()
    if "trustee" in role_type:
        return "is a trustee of"
    if "director" in role_type:
        return "is a director of"
    if "secretary" in role_type:
        return "is a secretary of"
    if "accountant" in role_type or "auditor" in role_type or "examiner" in role_type:
        return "is listed in governance/finance documents for"
    if candidate.source.startswith("companies_house"):
        return "is listed at Companies House for"
    if candidate.source.startswith("charity_commission"):
        return "is linked in Charity Commission records to"
    return "is linked to"


def apply_low_information_name_guard(
    *,
    seed_name: str,
    candidate: Any,
    decision: ResolutionDecision,
) -> ResolutionDecision:
    candidate_name = str(candidate.candidate_name or decision.canonical_name or "").strip()
    canonical_name = str(decision.canonical_name or candidate_name).strip()
    if not is_low_information_person_name(candidate_name) and not is_low_information_person_name(
        canonical_name
    ):
        return decision
    if normalize_name(candidate_name) == normalize_name(seed_name):
        return decision
    return ResolutionDecision(
        status="no_match",
        confidence=min(float(decision.confidence or 0.0), 0.2),
        canonical_name=candidate_name or canonical_name,
        explanation=(
            "Rejected because the candidate name is too low-information "
            "(for example a repeated generic name) to treat as a reliable identity."
        ),
        rule_score=decision.rule_score,
        alias_status="none",
        llm_payload=dict(decision.llm_payload) if decision.llm_payload else {},
    )


def apply_weak_name_match_guard(
    *,
    seed_name: str,
    candidate: Any,
    decision: ResolutionDecision,
    minimum_similarity: float = 0.55,
) -> ResolutionDecision:
    candidate_name = str(candidate.candidate_name or decision.canonical_name or "").strip()
    canonical_name = str(decision.canonical_name or candidate_name).strip()
    if not candidate_name:
        return decision
    if normalize_name(candidate_name) == normalize_name(seed_name):
        return decision
    if canonical_name and normalize_name(canonical_name) == normalize_name(seed_name):
        return decision

    similarity = float(candidate.feature_payload.get("name_similarity") or 0.0)
    if similarity <= 0.0:
        similarity = person_name_similarity(seed_name, candidate_name)
    if similarity >= minimum_similarity:
        return decision

    return ResolutionDecision(
        status="no_match",
        confidence=min(float(decision.confidence or 0.0), 0.2),
        canonical_name=candidate_name or canonical_name,
        explanation=(
            "Rejected because the candidate name is too dissimilar to the seed name "
            "to treat shared organisation metadata as identity evidence."
        ),
        rule_score=decision.rule_score,
        alias_status="none",
        llm_payload=dict(decision.llm_payload) if decision.llm_payload else {},
    )


def apply_conflicting_middle_name_guard(
    *,
    seed_name: str,
    candidate: Any,
    decision: ResolutionDecision,
) -> ResolutionDecision:
    candidate_name = str(candidate.candidate_name or decision.canonical_name or "").strip()
    canonical_name = str(decision.canonical_name or candidate_name).strip()
    if not candidate_name:
        return decision
    if normalize_name(candidate_name) == normalize_name(seed_name):
        return decision
    if canonical_name and normalize_name(canonical_name) == normalize_name(seed_name):
        return decision
    if not _middle_name_tokens_conflict(seed_name, candidate_name):
        return decision

    return ResolutionDecision(
        status="no_match",
        confidence=min(float(decision.confidence or 0.0), 0.2),
        canonical_name=candidate_name or canonical_name,
        explanation=(
            "Rejected because the seed and candidate share first name and surname "
            "but have conflicting middle-name tokens."
        ),
        rule_score=decision.rule_score,
        alias_status="none",
        llm_payload=dict(decision.llm_payload) if decision.llm_payload else {},
    )


def candidate_birth_month_year(candidate: Any) -> tuple[int | None, int | None]:
    raw_payload = candidate.raw_payload if isinstance(getattr(candidate, "raw_payload", None), dict) else {}
    return extract_birth_month_year(raw_payload)


def candidate_matches_known_birth_month_year(
    *,
    candidate: Any,
    known_birth_month_years: set[tuple[int, int]],
) -> bool:
    if not known_birth_month_years:
        return False
    birth_month, birth_year = candidate_birth_month_year(candidate)
    if not birth_month or not birth_year:
        return False
    return (birth_month, birth_year) in known_birth_month_years


def apply_birth_month_year_guard(
    *,
    candidate: Any,
    decision: ResolutionDecision,
    known_birth_month_years: set[tuple[int, int]],
) -> ResolutionDecision:
    if not known_birth_month_years:
        return decision

    birth_month, birth_year = candidate_birth_month_year(candidate)
    if not birth_month or not birth_year:
        return decision
    if (birth_month, birth_year) in known_birth_month_years:
        return decision

    candidate_name = str(candidate.candidate_name or decision.canonical_name or "").strip()
    canonical_name = str(decision.canonical_name or candidate_name).strip()
    return ResolutionDecision(
        status="no_match",
        confidence=min(float(decision.confidence or 0.0), 0.2),
        canonical_name=candidate_name or canonical_name,
        explanation=(
            "Rejected because the candidate Companies House birth month/year "
            "conflicts with an already-confirmed birth month/year for this seed."
        ),
        rule_score=decision.rule_score,
        alias_status="none",
        llm_payload=dict(decision.llm_payload) if decision.llm_payload else {},
    )

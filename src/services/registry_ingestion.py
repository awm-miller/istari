from __future__ import annotations

import re
from typing import Any

from src.charity_commission.identifiers import (
    extract_charity_number_from_payload,
    extract_charity_number_from_url,
    looks_like_charity_url,
)
from src.models import CandidateMatch, EvidenceItem
from src.resolution.features import build_candidate_match


def ingest_registry_evidence_items(
    *,
    repository: Any,
    run_id: int,
    items: list[EvidenceItem],
    provider_name: str,
    log: Any,
) -> tuple[int, int]:
    evidence_count = 0
    candidate_count = 0
    total_items = len(items)
    for item_index, item in enumerate(items, 1):
        if item_index == 1 or item_index % 50 == 0 or item_index == total_items:
            log.info(
                "  %s ingest progress: %d/%d evidence items",
                provider_name,
                item_index,
                total_items,
            )
        evidence_id = repository.insert_evidence_item(run_id, item)
        evidence_count += 1
        candidate = candidate_from_evidence_item(item=item, evidence_id=evidence_id)
        repository.insert_candidate_match(run_id, candidate)
        candidate_count += 1
    return evidence_count, candidate_count


def candidate_from_evidence_item(*, item: EvidenceItem, evidence_id: int) -> CandidateMatch:
    candidate_name = (
        item.raw_payload.get("candidate_name")
        or item.raw_payload.get("matched_name")
        or item.raw_payload.get("appointment", {}).get("name")
        or item.raw_payload.get("officer_search_item", {}).get("title")
        or item.raw_payload.get("variant", "")
    )
    organisation_name = (
        item.raw_payload.get("organisation_name")
        or item.raw_payload.get("appointment", {}).get("appointed_to", {}).get("company_name")
        or item.title
    )
    return build_candidate_match(
        name_variant=item.raw_payload.get("variant", ""),
        candidate_name=candidate_name,
        organisation_name=organisation_name,
        registry_type=extract_registry_type(item),
        registry_number=extract_registry_number(item),
        suffix=int(item.raw_payload.get("suffix") or 0),
        source=item.source,
        evidence_id=evidence_id,
        raw_payload={
            "candidate_name": candidate_name,
            "organisation_name": organisation_name,
            "officer_id": item.raw_payload.get("officer_id"),
            "role_type": item.raw_payload.get("role_type", ""),
            "role_label": item.raw_payload.get("role_label", ""),
            "relationship_kind": item.raw_payload.get("relationship_kind", ""),
            "relationship_phrase": item.raw_payload.get("relationship_phrase", ""),
            "evidence": item.raw_payload,
        },
    )


def extract_registry_type(item: EvidenceItem) -> str | None:
    direct = item.raw_payload.get("registry_type")
    if direct:
        return str(direct)
    if item.source.startswith("companies_house"):
        return "company"
    if looks_like_charity_url(item.url or ""):
        return "charity"
    url = (item.url or "").lower()
    if "company-information.service.gov.uk" in url:
        return "company"
    return None


def extract_registry_number(item: EvidenceItem) -> str | None:
    direct = item.raw_payload.get("registry_number")
    if direct not in (None, ""):
        return str(direct)
    if item.source.startswith("companies_house"):
        appointment = item.raw_payload.get("appointment", {})
        appointed_to = appointment.get("appointed_to", {})
        company_number = appointed_to.get("company_number")
        if company_number:
            return str(company_number)
        return None
    url = item.url or ""
    charity_number = extract_charity_number_from_url(url)
    if charity_number:
        return charity_number
    company_match = re.search(r"/company/([A-Z0-9]+)", url)
    if company_match:
        return company_match.group(1)
    return extract_charity_number_from_payload(item.raw_payload.get("result", {}))

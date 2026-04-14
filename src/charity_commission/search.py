from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.charity_commission.client import CharityCommissionClient
from src.charity_commission.identifiers import extract_charity_number_from_payload
from src.config import Settings
from src.models import EvidenceItem, NameVariant, OrganisationRecord
from src.search.queries import normalize_name

log = logging.getLogger("istari.charity_commission")


def _significant_org_tokens(value: str) -> set[str]:
    stop = {"the", "of", "and", "for", "in", "a", "an", "ltd", "limited", "plc", "llp", "cic", "foundation"}
    return {
        token
        for token in normalize_name(value).split()
        if token and token not in stop
    }


def _is_strong_org_name_match(query: str, candidate: str) -> bool:
    query_normalized = normalize_name(query)
    candidate_normalized = normalize_name(candidate)
    if not query_normalized or not candidate_normalized:
        return False
    if query_normalized == candidate_normalized:
        return True
    query_tokens = _significant_org_tokens(query)
    candidate_tokens = _significant_org_tokens(candidate)
    if len(query_tokens) < 2 or len(candidate_tokens) < 2:
        return False
    return query_tokens == candidate_tokens


def search_name_to_organisation(
    charity_client: CharityCommissionClient,
    organisation_name: str,
) -> OrganisationRecord | None:
    try:
        matches = charity_client.search_charities_by_name(organisation_name)
    except RuntimeError:
        return None
    if not matches:
        return None

    top_match = matches[0]
    top_match_name = str(top_match.get("charity_name") or organisation_name).strip()
    if not _is_strong_org_name_match(organisation_name, top_match_name):
        return None
    charity_number = extract_charity_number_from_payload(top_match)
    if not charity_number:
        return None

    return OrganisationRecord(
        registry_type="charity",
        registry_number=charity_number,
        suffix=int(top_match.get("group_subsid_suffix") or 0),
        organisation_number=top_match.get("organisation_number"),
        name=top_match_name,
        status=top_match.get("reg_status"),
        metadata=top_match,
    )


@dataclass(slots=True)
class CharityCommissionSearchProvider:
    settings: Settings
    client: CharityCommissionClient = field(init=False)

    def __post_init__(self) -> None:
        self.client = CharityCommissionClient(self.settings)

    def search(self, variants: list[NameVariant]) -> list[EvidenceItem]:
        evidence: list[EvidenceItem] = []
        for index, variant in enumerate(variants, 1):
            log.info("  CCEW name search [%d/%d] '%s'", index, len(variants), variant.name)
            matches = self.client.search_charities_by_name(variant.name)
            for match in matches:
                charity_number = extract_charity_number_from_payload(match)
                if not charity_number:
                    continue
                suffix = int(match.get("group_subsid_suffix") or 0)
                charity_name = str(match.get("charity_name") or variant.name).strip()
                evidence.append(
                    EvidenceItem(
                        source="charity_commission_search",
                        source_key=f"{variant.name}:{charity_number}:{suffix}",
                        title=charity_name,
                        url=(
                            "https://register-of-charities.charitycommission.gov.uk/"
                            f"charity-details/?regid={charity_number}&subid=0"
                        ),
                        snippet=f"Charity Commission name search match for {variant.name}",
                        raw_payload={
                            "variant": variant.name,
                            "candidate_name": variant.name,
                            "organisation_name": charity_name,
                            "registry_type": "charity",
                            "registry_number": charity_number,
                            "suffix": suffix,
                            "organisation_number": match.get("organisation_number"),
                            "match": match,
                        },
                    )
                )
        return evidence

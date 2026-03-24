from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.charity_commission.client import CharityCommissionClient
from src.charity_commission.identifiers import extract_charity_number_from_payload
from src.config import Settings
from src.models import EvidenceItem, NameVariant, OrganisationRecord

log = logging.getLogger("istari.charity_commission")


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
    charity_number = extract_charity_number_from_payload(top_match)
    if not charity_number:
        return None

    return OrganisationRecord(
        registry_type="charity",
        registry_number=charity_number,
        suffix=int(top_match.get("group_subsid_suffix") or 0),
        organisation_number=top_match.get("organisation_number"),
        name=top_match.get("charity_name") or organisation_name,
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
                            f"charity-search/-/charity-details/{charity_number}"
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

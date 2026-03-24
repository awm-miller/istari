from __future__ import annotations

import logging
from typing import Any

from src.companies_house.client import CompaniesHouseClient
from src.companies_house.relationships import (
    company_relationship_kind,
    company_relationship_phrase,
    company_role_type,
)
from src.models import OrganisationRecord
from src.storage.repository import Repository

log = logging.getLogger("istari.companies_house")


def expand_company_people(
    *,
    repository: Repository,
    companies_house_client: CompaniesHouseClient,
    company_number: str,
    confidence_class: str = "expanded",
    edge_weight: float = 0.65,
) -> dict[str, int]:
    profile = companies_house_client.get_company_profile(company_number)
    officers = companies_house_client.get_company_officers(company_number)
    company_name = str(profile.get("company_name") or company_number).strip()
    organisation_id = repository.upsert_organisation(
        OrganisationRecord(
            registry_type="company",
            registry_number=str(company_number),
            suffix=0,
            name=company_name,
            status=profile.get("company_status"),
            metadata=profile,
        )
    )

    inserted_roles = 0
    for item in officers.get("items", []):
        if not isinstance(item, dict):
            continue
        person_name = str(item.get("name") or "").strip()
        if not person_name:
            continue
        officer_role = item.get("officer_role")
        person_id = repository.upsert_person(person_name)
        repository.upsert_role(
            person_id=person_id,
            organisation_id=organisation_id,
            role_type=company_role_type(officer_role),
            role_label=str(officer_role or "company_officer"),
            relationship_kind=company_relationship_kind(officer_role),
            relationship_phrase=company_relationship_phrase(officer_role),
            source="companies_house_company_officers",
            confidence_class=confidence_class,
            edge_weight=edge_weight,
            provenance=item,
            start_date=item.get("appointed_on"),
            end_date=item.get("resigned_on"),
        )
        inserted_roles += 1

    log.info(
        "  Company %s: inserted %d connected people roles",
        company_number,
        inserted_roles,
    )
    return {
        "organisation_id": organisation_id,
        "inserted_roles": inserted_roles,
    }

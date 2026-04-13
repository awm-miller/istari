from __future__ import annotations

import logging
from typing import Any

from src.charity_commission.client import CharityCommissionClient
from src.models import OrganisationRecord
from src.storage.repository import Repository

log = logging.getLogger("istari.charity_commission")


def build_charity_record(
    details: dict[str, Any],
    *,
    charity_number: int,
    suffix: int = 0,
) -> OrganisationRecord:
    return OrganisationRecord(
        registry_type="charity",
        registry_number=str(charity_number),
        suffix=suffix,
        organisation_number=details.get("organisation_number"),
        name=details.get("charity_name") or details.get("CharityName") or str(charity_number),
        status=details.get("reg_status") or details.get("RegistrationStatus"),
        metadata=details,
    )


def expand_charity_connected_organisations(
    *,
    repository: Repository,
    charity_client: CharityCommissionClient,
    charity_number: int,
    suffix: int = 0,
) -> list[dict[str, Any]]:
    details = charity_client.get_all_charity_details(charity_number, suffix)
    linked_charities = charity_client.get_charity_linked_charities(charity_number, suffix)
    linked_charity = charity_client.get_charity_linked_charity(charity_number, suffix)

    charity_record = build_charity_record(
        details,
        charity_number=charity_number,
        suffix=suffix,
    )
    repository.upsert_organisation(charity_record)

    connected: list[dict[str, Any]] = []
    linked_rows = extract_linked_charity_rows([linked_charities, linked_charity])
    for linked in linked_rows:
        linked_number = linked.get("registry_number")
        linked_suffix = int(linked.get("suffix") or 0)
        if linked_number == str(charity_number) and linked_suffix == int(suffix):
            continue
        linked_record = OrganisationRecord(
            registry_type="charity",
            registry_number=str(linked_number),
            suffix=linked_suffix,
            name=str(linked.get("name") or "").strip(),
            metadata={
                "source": "charity_commission_linked_charities",
                "parent_charity_number": charity_number,
                "parent_suffix": suffix,
                "linked_payload": linked.get("payload") or {},
            },
        )
        organisation_id = repository.upsert_organisation(linked_record)
        connected.append(
            {
                "organisation_id": organisation_id,
                "registry_type": linked_record.registry_type,
                "registry_number": linked_record.registry_number,
                "suffix": linked_record.suffix,
                "name": linked_record.name,
                "source": "charity_commission_linked_charities",
            }
        )

    if connected:
        log.info(
            "  Charity %d: found %d connected organisations",
            charity_number,
            len(connected),
        )
    return connected


def expand_charity_people(
    *,
    repository: Repository,
    charity_client: CharityCommissionClient,
    charity_number: int,
    suffix: int = 0,
    confidence_class: str = "expanded",
    edge_weight: float = 0.65,
) -> dict[str, int]:
    details = charity_client.get_all_charity_details(charity_number, suffix)
    trustees = charity_client.get_charity_trustee_information(charity_number, suffix)
    trustee_names = charity_client.get_charity_trustee_names(charity_number, suffix)

    organisation_id = repository.upsert_organisation(
        build_charity_record(
            details,
            charity_number=charity_number,
            suffix=suffix,
        )
    )
    inserted_roles = 0

    for trustee in trustees:
        person_name = (
            trustee.get("TrusteeName")
            or trustee.get("trustee_name")
            or trustee.get("name")
            or ""
        )
        if not person_name:
            continue
        person_id = repository.upsert_person(str(person_name))
        repository.upsert_role(
            person_id=person_id,
            organisation_id=organisation_id,
            role_type="trustee",
            role_label=trustee.get("Role") or trustee.get("role") or "trustee",
            relationship_kind="trustee_of",
            relationship_phrase="is a trustee of",
            source="charity_commission_trustee_information",
            confidence_class=confidence_class,
            edge_weight=edge_weight,
            provenance=trustee,
            start_date=trustee.get("StartDate") or trustee.get("start_date"),
            end_date=trustee.get("EndDate") or trustee.get("end_date"),
        )
        inserted_roles += 1

    for trustee_name in trustee_names:
        person_id = repository.upsert_person(str(trustee_name))
        repository.upsert_role(
            person_id=person_id,
            organisation_id=organisation_id,
            role_type="trustee_name",
            role_label="trustee_name_only",
            relationship_kind="named_trustee_of",
            relationship_phrase="is named as a trustee of",
            source="charity_commission_trustee_names",
            confidence_class=confidence_class,
            edge_weight=max(0.3, edge_weight - 0.15),
            provenance={"name": trustee_name},
        )
        inserted_roles += 1

    log.info(
        "  Charity %d: inserted %d connected people roles",
        charity_number,
        inserted_roles,
    )
    return {
        "organisation_id": organisation_id,
        "inserted_roles": inserted_roles,
    }


def extract_linked_charity_rows(payloads: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()

    def append_row(item: dict[str, Any]) -> None:
        number = (
            item.get("linked_charity_number")
            or item.get("LinkedCharityNumber")
            or item.get("reg_charity_number")
            or item.get("RegisteredNumber")
            or item.get("charity_number")
            or item.get("CharityNumber")
        )
        if number in (None, ""):
            return
        try:
            registry_number = str(int(number))
        except (TypeError, ValueError):
            return

        raw_suffix = (
            item.get("linked_charity_suffix")
            or item.get("LinkedCharitySuffix")
            or item.get("group_subsid_suffix")
            or item.get("suffix")
            or item.get("Suffix")
            or 0
        )
        try:
            parsed_suffix = int(raw_suffix)
        except (TypeError, ValueError):
            parsed_suffix = 0

        key = (registry_number, parsed_suffix)
        if key in seen:
            return
        seen.add(key)

        rows.append(
            {
                "registry_number": registry_number,
                "suffix": parsed_suffix,
                "name": (
                    item.get("linked_charity_name")
                    or item.get("LinkedCharityName")
                    or item.get("charity_name")
                    or item.get("CharityName")
                    or item.get("name")
                    or ""
                ),
                "payload": item,
            }
        )

    for payload in payloads:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    append_row(item)
            continue
        if not isinstance(payload, dict):
            continue

        candidate_lists: list[list[Any]] = []
        for key in (
            "linked_charities",
            "LinkedCharities",
            "linked_charity",
            "LinkedCharity",
            "items",
            "data",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                candidate_lists.append(value)
            elif isinstance(value, dict):
                candidate_lists.append([value])

        if not candidate_lists:
            candidate_lists.append([payload])

        for candidate_list in candidate_lists:
            for item in candidate_list:
                if isinstance(item, dict):
                    append_row(item)

    return rows

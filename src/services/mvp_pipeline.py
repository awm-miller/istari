from __future__ import annotations

import logging
from typing import Any

from src.address_pivot import AddressPivotSearcher, build_organisation_record
from src.addresses import extract_addresses_for_organisation
from src.charity_commission.client import CharityCommissionClient
from src.charity_commission.expansion import (
    build_charity_record,
    expand_charity_connected_organisations,
    expand_charity_people,
)
from src.companies_house.client import CompaniesHouseClient
from src.companies_house.expansion import expand_company_people
from src.config import Settings
from src.models import OrganisationRecord
from src.ofac.screening import OFACScreener
from src.resolution.matcher import HybridMatcher
from src.search.provider import SearchProvider
from src.services.pipeline_services import (
    DiscoveryService,
    RankingService,
    ResolutionService,
    VariantService,
)
from src.storage.repository import Repository

log = logging.getLogger("istari.pipeline")

STEP1_STAGE = "step1_seed_match"
STEP2_STAGE = "step2_connected_org"


def step1_expand_seed(
    *,
    repository: Repository,
    charity_client: CharityCommissionClient,
    search_providers: list[SearchProvider],
    matcher: HybridMatcher,
    seed_name: str,
    creativity_level: str,
) -> dict[str, Any]:
    variant_service = VariantService()
    discovery_service = DiscoveryService()
    resolution_service = ResolutionService()

    run_id = repository.create_run(seed_name, creativity_level)
    variants = variant_service.generate_and_store(
        repository,
        run_id,
        seed_name,
        creativity_level,
        settings=charity_client.settings,
    )
    search_summary = discovery_service.search_name(
        repository=repository,
        charity_client=charity_client,
        search_providers=search_providers,
        run_id=run_id,
        variants=variants,
    )
    decisions = resolution_service.resolve_candidates(
        repository=repository,
        matcher=matcher,
        run_id=run_id,
    )
    matched_organisations = repository.get_matched_organisations_for_run(run_id)
    for organisation in matched_organisations:
        repository.link_run_organisation(
            run_id,
            int(organisation["id"]),
            stage=STEP1_STAGE,
            source="resolution_match",
            metadata={
                "registry_type": organisation["registry_type"],
                "registry_number": organisation["registry_number"],
                "suffix": organisation["suffix"],
            },
        )
    return {
        "run_id": run_id,
        "variant_count": len(variants),
        "search_summary": search_summary,
        "decision_count": len(decisions),
        "resolution_metrics": dict(getattr(resolution_service, "last_metrics", {})),
        "matched_organisation_count": len(matched_organisations),
    }


def step2_expand_connected_organisations(
    *,
    repository: Repository,
    charity_client: CharityCommissionClient,
    run_id: int,
) -> dict[str, Any]:
    seed_organisations = repository.get_run_organisations(run_id, stages=[STEP1_STAGE])
    processed = 0
    linked_count = 0
    for organisation in seed_organisations:
        processed += 1
        if organisation["registry_type"] != "charity":
            continue
        connected = expand_charity_connected_organisations(
            repository=repository,
            charity_client=charity_client,
            charity_number=int(organisation["registry_number"]),
            suffix=int(organisation["suffix"]),
        )
        for linked in connected:
            repository.link_run_organisation(
                run_id,
                int(linked["organisation_id"]),
                stage=STEP2_STAGE,
                source=str(linked["source"]),
                metadata={
                    "parent_registry_type": organisation["registry_type"],
                    "parent_registry_number": organisation["registry_number"],
                    "parent_suffix": organisation["suffix"],
                },
            )
            linked_count += 1

    address_count = 0
    address_pivot_count = 0
    settings = getattr(charity_client, "settings", None)
    if isinstance(settings, Settings):
        companies_house_client = CompaniesHouseClient(settings)
        address_searcher = AddressPivotSearcher(
            settings=settings,
            charity_client=charity_client,
            companies_house_client=companies_house_client,
        )
        scoped_organisations = repository.get_run_organisations(run_id, stages=[STEP1_STAGE, STEP2_STAGE])
        seen_address_keys: set[str] = set()
        for organisation in scoped_organisations:
            hydrated = _hydrate_organisation_for_addresses(
                repository=repository,
                charity_client=charity_client,
                companies_house_client=companies_house_client,
                organisation=organisation,
            )
            if not hydrated:
                continue
            stored_addresses = _store_organisation_addresses(
                repository=repository,
                organisation_id=int(hydrated["organisation_id"]),
                registry_type=str(hydrated["registry_type"]),
                metadata=dict(hydrated["metadata"]),
                source=str(hydrated["source"]),
            )
            address_count += len(stored_addresses)
            for stored in stored_addresses:
                address = stored["address"]
                if address.normalized_key in seen_address_keys:
                    continue
                seen_address_keys.add(address.normalized_key)
                related_orgs = address_searcher.find_related_organisations(
                    address=address,
                    source_registry_type=str(hydrated["registry_type"]),
                    source_registry_number=str(hydrated["registry_number"]),
                    source_suffix=int(hydrated["suffix"]),
                )
                for related in related_orgs:
                    related_record = build_organisation_record(related)
                    related_org_id = repository.upsert_organisation(related_record)
                    repository.link_organisation_address(
                        related_org_id,
                        int(stored["address_id"]),
                        source=str(related.get("source") or "address_pivot"),
                        relationship_phrase="is registered at",
                        metadata={
                            "verified_by": str(related.get("source") or "address_pivot"),
                            "normalized_key": address.normalized_key,
                        },
                    )
                    repository.link_run_organisation(
                        run_id,
                        related_org_id,
                        stage=STEP2_STAGE,
                        source=str(related.get("source") or "address_pivot"),
                        metadata={
                            "parent_registry_type": str(hydrated["registry_type"]),
                            "parent_registry_number": str(hydrated["registry_number"]),
                            "parent_suffix": int(hydrated["suffix"]),
                            "parent_name": str(hydrated["name"]),
                            "address_id": int(stored["address_id"]),
                            "address_label": address.label,
                            "connection_phrase": "shares an address with",
                        },
                    )
                    address_pivot_count += 1
    return {
        "run_id": run_id,
        "processed_organisation_count": processed,
        "connected_organisation_count": len(
            repository.get_run_organisations(run_id, stages=[STEP2_STAGE])
        ),
        "linked_insert_attempts": linked_count,
        "address_count": address_count,
        "address_pivot_insert_attempts": address_pivot_count,
    }


def step3_expand_connected_people(
    *,
    repository: Repository,
    settings: Settings,
    charity_client: CharityCommissionClient,
    run_id: int,
    limit: int = 25,
) -> dict[str, Any]:
    companies_house_client = CompaniesHouseClient(settings)
    ranking_service = RankingService()
    scoped_organisations = repository.get_run_organisations(
        run_id,
        stages=[STEP1_STAGE, STEP2_STAGE],
    )
    processed = 0
    inserted_roles = 0
    for organisation in scoped_organisations:
        processed += 1
        if organisation["registry_type"] == "charity":
            summary = expand_charity_people(
                repository=repository,
                charity_client=charity_client,
                charity_number=int(organisation["registry_number"]),
                suffix=int(organisation["suffix"]),
            )
            inserted_roles += int(summary["inserted_roles"])
            continue
        if organisation["registry_type"] == "company":
            try:
                summary = expand_company_people(
                    repository=repository,
                    companies_house_client=companies_house_client,
                    company_number=str(organisation["registry_number"]),
                )
                inserted_roles += int(summary["inserted_roles"])
            except RuntimeError as exc:
                log.warning(
                    "  Skipping company %s: %s",
                    organisation["registry_number"],
                    exc,
                )
    return {
        "run_id": run_id,
        "processed_organisation_count": processed,
        "inserted_roles": inserted_roles,
        "ranking": ranking_service.rank(repository, run_id=run_id, limit=limit),
    }


def step4_ofac_screening(
    *,
    settings: Settings,
    ranking: list[dict[str, Any]],
) -> dict[str, Any]:
    screener = OFACScreener()
    sdn_path = settings.project_root / "data" / "sdn.csv"
    if not sdn_path.exists():
        log.info("OFAC SDN file not found at %s — downloading ...", sdn_path)
        screener.download_and_load(sdn_path.parent)
    else:
        screener.load_csv(sdn_path)

    if not screener.loaded:
        log.warning("OFAC screening skipped — no SDN data available")
        return {"ofac_hits": {}, "screened_count": 0, "sdn_entry_count": 0}

    names = [str(entry.get("canonical_name", "")) for entry in ranking if entry.get("canonical_name")]
    hits = screener.screen_names(names)

    for entry in ranking:
        name = str(entry.get("canonical_name", ""))
        entry["ofac_hit"] = name in hits
        entry["ofac_matches"] = hits.get(name, [])

    log.info(
        "OFAC screening: %d names checked, %d hits from %d SDN entries",
        len(names),
        len(hits),
        screener.entry_count,
    )
    return {
        "ofac_hits": hits,
        "screened_count": len(names),
        "sdn_entry_count": screener.entry_count,
    }


def run_registry_only_mvp(
    *,
    repository: Repository,
    settings: Settings,
    charity_client: CharityCommissionClient,
    search_providers: list[SearchProvider],
    matcher: HybridMatcher,
    seed_name: str,
    creativity_level: str,
    limit: int,
) -> dict[str, Any]:
    step1 = step1_expand_seed(
        repository=repository,
        charity_client=charity_client,
        search_providers=search_providers,
        matcher=matcher,
        seed_name=seed_name,
        creativity_level=creativity_level,
    )
    run_id = int(step1["run_id"])
    step2 = step2_expand_connected_organisations(
        repository=repository,
        charity_client=charity_client,
        run_id=run_id,
    )
    step3 = step3_expand_connected_people(
        repository=repository,
        settings=settings,
        charity_client=charity_client,
        run_id=run_id,
        limit=limit,
    )

    ranking = step3["ranking"]
    step4 = step4_ofac_screening(settings=settings, ranking=ranking)

    return {
        "mode": "registry_only_mvp",
        "run_id": run_id,
        "step1": step1,
        "step2": step2,
        "step3": step3,
        "step4": step4,
        "search_summary": step1["search_summary"],
        "decision_count": step1["decision_count"],
        "resolution_metrics": step1["resolution_metrics"],
        "alias_rounds": 0,
        "alias_variant_count": 0,
        "ranking": ranking,
    }


def _hydrate_organisation_for_addresses(
    *,
    repository: Repository,
    charity_client: CharityCommissionClient,
    companies_house_client: CompaniesHouseClient,
    organisation: Any,
) -> dict[str, Any] | None:
    registry_type = str(organisation["registry_type"])
    registry_number = str(organisation["registry_number"])
    suffix = int(organisation["suffix"] or 0)
    if registry_type == "charity":
        details = charity_client.get_all_charity_details(int(registry_number), suffix)
        organisation_id = repository.upsert_organisation(
            build_charity_record(
                details,
                charity_number=int(registry_number),
                suffix=suffix,
            )
        )
        return {
            "organisation_id": organisation_id,
            "registry_type": registry_type,
            "registry_number": registry_number,
            "suffix": suffix,
            "name": str(details.get("charity_name") or details.get("CharityName") or organisation["name"]),
            "metadata": details,
            "source": "charity_commission_all_details",
        }
    if registry_type == "company":
        try:
            profile = companies_house_client.get_company_profile(registry_number)
        except RuntimeError as exc:
            log.warning("  Skipping company address enrichment for %s: %s", registry_number, exc)
            return None
        organisation_id = repository.upsert_organisation(
            OrganisationRecord(
                registry_type="company",
                registry_number=registry_number,
                suffix=suffix,
                name=str(profile.get("company_name") or organisation["name"] or registry_number).strip(),
                status=profile.get("company_status"),
                metadata=profile,
            )
        )
        return {
            "organisation_id": organisation_id,
            "registry_type": registry_type,
            "registry_number": registry_number,
            "suffix": suffix,
            "name": str(profile.get("company_name") or organisation["name"] or registry_number).strip(),
            "metadata": profile,
            "source": "companies_house_company_profile",
        }
    return None


def _store_organisation_addresses(
    *,
    repository: Repository,
    organisation_id: int,
    registry_type: str,
    metadata: dict[str, Any],
    source: str,
) -> list[dict[str, Any]]:
    stored: list[dict[str, Any]] = []
    for address in extract_addresses_for_organisation(registry_type, metadata):
        address_id = repository.upsert_address(
            label=address.label,
            normalized_key=address.normalized_key,
            postcode=address.postcode,
            country=address.country,
            metadata=dict(address.metadata),
        )
        repository.link_organisation_address(
            organisation_id,
            address_id,
            source=source,
            relationship_phrase="is registered at",
            metadata={
                "normalized_key": address.normalized_key,
                **dict(address.metadata),
            },
        )
        stored.append({"address_id": address_id, "address": address})
    return stored

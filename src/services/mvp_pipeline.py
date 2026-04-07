from __future__ import annotations

import logging
import json
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
from src.ofac.screening import extract_identity_key_birth_month_year
from src.resolution.features import build_candidate_match
from src.resolution.matcher import HybridMatcher
from src.search.provider import SearchProvider
from src.services.pipeline_services import (
    DiscoveryService,
    RankingService,
    ResolutionService,
    VariantService,
)
from src.services.pdf_enrichment import enrich_run_from_pdfs
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
    matcher = HybridMatcher(settings)
    resolution_service = ResolutionService()
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
    stage3_resolution = _resolve_stage3_people(
        repository=repository,
        matcher=matcher,
        resolution_service=resolution_service,
        run_id=run_id,
    )
    return {
        "run_id": run_id,
        "processed_organisation_count": processed,
        "inserted_roles": inserted_roles,
        "stage3_resolution": stage3_resolution,
        "ranking": ranking_service.rank(repository, run_id=run_id, limit=limit),
    }


def step2b_enrich_from_pdfs(
    *,
    repository: Repository,
    settings: Settings,
    charity_client: CharityCommissionClient,
    run_id: int,
) -> dict[str, Any]:
    scoped_organisations = repository.get_run_organisations(run_id, stages=[STEP1_STAGE, STEP2_STAGE])
    return enrich_run_from_pdfs(
        repository=repository,
        settings=settings,
        charity_client=charity_client,
        run_id=run_id,
        organisations=scoped_organisations,
    )


def step4_ofac_screening(
    *,
    repository: Repository,
    settings: Settings,
    ranking: list[dict[str, Any]],
    enable_remote_sources: bool = True,
) -> dict[str, Any]:
    screener = OFACScreener(enable_remote_sources=enable_remote_sources)
    data_dir = settings.project_root / "data"
    try:
        screener.ensure_local_sources(data_dir)
    except Exception as exc:
        log.warning("Sanctions list download failed: %s", exc)
        screener.load_sources(data_dir)

    if not screener.loaded:
        log.warning("Sanctions screening skipped — no local sanctions data available")
        return {
            "ofac_hits": {},
            "sanctions_hits": {},
            "screened_count": 0,
            "sdn_entry_count": 0,
            "sanctions_entry_count": 0,
        }

    for entry in ranking:
        name = str(entry.get("canonical_name", ""))
        person_id = int(entry.get("person_id") or 0)
        birth_month, birth_year = extract_identity_key_birth_month_year(
            str(entry.get("identity_key", ""))
        )
        hits = screener.screen_name(
            name,
            birth_month=birth_month,
            birth_year=birth_year,
        )
        entry["sanctions_hit"] = bool(hits)
        entry["sanctions_matches"] = hits
        entry["ofac_hit"] = bool(hits)
        entry["ofac_matches"] = hits
        entry["sanctions_birth_month"] = birth_month
        entry["sanctions_birth_year"] = birth_year
        entry["ofac_birth_month"] = birth_month
        entry["ofac_birth_year"] = birth_year
        if person_id:
            repository.upsert_person_sanctions(
                person_id=person_id,
                screened_name=name,
                screened_birth_month=birth_month,
                screened_birth_year=birth_year,
                matches=hits,
            )

    log.info(
        "Sanctions screening: %d names checked, %d hits from %d local entries",
        len(ranking),
        sum(1 for entry in ranking if entry.get("ofac_hit")),
        screener.entry_count,
    )
    sanctions_hits = {
        str(entry.get("canonical_name", "")): list(entry.get("ofac_matches") or [])
        for entry in ranking
        if entry.get("ofac_hit")
    }
    return {
        "ofac_hits": sanctions_hits,
        "sanctions_hits": sanctions_hits,
        "screened_count": len(ranking),
        "sdn_entry_count": screener.entry_count,
        "sanctions_entry_count": screener.entry_count,
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
    step2b = step2b_enrich_from_pdfs(
        repository=repository,
        settings=settings,
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
    step4 = step4_ofac_screening(repository=repository, settings=settings, ranking=ranking)

    return {
        "mode": "registry_only_mvp",
        "run_id": run_id,
        "step1": step1,
        "step2": step2,
        "step2b": step2b,
        "step3": step3,
        "step4": step4,
        "search_summary": step1["search_summary"],
        "decision_count": step1["decision_count"],
        "resolution_metrics": step1["resolution_metrics"],
        "alias_rounds": 0,
        "alias_variant_count": 0,
        "ranking": ranking,
    }


def add_organisation_to_run(
    *,
    repository: Repository,
    settings: Settings,
    charity_client: CharityCommissionClient,
    run_id: int,
    registry_type: str,
    registry_number: str,
    suffix: int = 0,
    limit: int = 25,
    rerun_downstream: bool = True,
) -> dict[str, Any]:
    run_row = repository.get_run(run_id)
    if run_row is None:
        raise ValueError(f"Run {run_id} does not exist.")

    organisation = _load_registry_organisation(
        settings=settings,
        charity_client=charity_client,
        registry_type=registry_type,
        registry_number=registry_number,
        suffix=suffix,
    )
    organisation_id = repository.upsert_organisation(organisation)
    repository.link_run_organisation(
        run_id,
        organisation_id,
        stage=STEP1_STAGE,
        source="manual_add",
        metadata={
            "registry_type": organisation.registry_type,
            "registry_number": organisation.registry_number,
            "suffix": organisation.suffix,
            "name": organisation.name,
        },
    )

    result: dict[str, Any] = {
        "run_id": run_id,
        "organisation_id": organisation_id,
        "registry_type": organisation.registry_type,
        "registry_number": organisation.registry_number,
        "suffix": organisation.suffix,
        "name": organisation.name,
        "reran_downstream": rerun_downstream,
    }
    if not rerun_downstream:
        return result

    step2 = step2_expand_connected_organisations(
        repository=repository,
        charity_client=charity_client,
        run_id=run_id,
    )
    step2b = step2b_enrich_from_pdfs(
        repository=repository,
        settings=settings,
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
    result.update(
        {
            "step2": step2,
            "step2b": step2b,
            "step3": step3,
            "ranking": step3["ranking"],
        }
    )
    return result


def _load_registry_organisation(
    *,
    settings: Settings,
    charity_client: CharityCommissionClient,
    registry_type: str,
    registry_number: str,
    suffix: int = 0,
) -> OrganisationRecord:
    cleaned_type = str(registry_type).strip().lower()
    cleaned_number = str(registry_number).strip()
    if cleaned_type == "charity":
        details = charity_client.get_all_charity_details(int(cleaned_number), int(suffix))
        return build_charity_record(
            details,
            charity_number=int(cleaned_number),
            suffix=int(suffix),
        )
    if cleaned_type == "company":
        companies_house_client = CompaniesHouseClient(settings)
        profile = companies_house_client.get_company_profile(cleaned_number)
        return OrganisationRecord(
            registry_type="company",
            registry_number=cleaned_number,
            suffix=0,
            name=str(profile.get("company_name") or cleaned_number).strip(),
            status=profile.get("company_status"),
            metadata=profile,
        )
    raise ValueError(f"Unsupported registry type: {registry_type}")


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


def _resolve_stage3_people(
    *,
    repository: Repository,
    matcher: HybridMatcher,
    resolution_service: ResolutionService,
    run_id: int,
) -> dict[str, Any]:
    run = repository.get_run(run_id)
    if run is None:
        raise RuntimeError(f"Run {run_id} does not exist.")

    repository.delete_stage3_candidate_matches(run_id)
    expanded_people = repository.get_expanded_people_for_run(run_id, limit=5000)
    inserted_candidates = 0
    seed_name = str(run["seed_name"] or "").strip()
    seed_name_key = seed_name.lower()
    for row in expanded_people:
        candidate_name = str(row["person_name"] or "").strip()
        if not candidate_name or candidate_name.lower() == seed_name_key:
            continue
        raw_payload = _build_stage3_candidate_payload(row)
        candidate = build_candidate_match(
            name_variant=seed_name,
            candidate_name=candidate_name,
            organisation_name=str(row["organisation_name"] or "").strip(),
            registry_type=str(row["registry_type"] or "").strip() or None,
            registry_number=str(row["registry_number"] or "").strip() or None,
            suffix=int(row["suffix"] or 0),
            source=str(row["source"] or "").strip(),
            evidence_id=None,
            raw_payload=raw_payload,
        )
        repository.insert_candidate_match(run_id, candidate)
        inserted_candidates += 1

    decisions = resolution_service.resolve_candidates(
        repository=repository,
        matcher=matcher,
        run_id=run_id,
    ) if inserted_candidates else []
    return {
        "candidate_count": inserted_candidates,
        "decision_count": len(decisions),
        "resolution_metrics": dict(getattr(resolution_service, "last_metrics", {})),
    }


def _build_stage3_candidate_payload(row: Any) -> dict[str, Any]:
    try:
        provenance = json.loads(str(row["provenance_json"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        provenance = {}
    return {
        "stage3_resolution": True,
        "organisation_name": str(row["organisation_name"] or "").strip(),
        "role_type": str(row["role_type"] or "").strip(),
        "role_label": str(row["role_label"] or "").strip(),
        "relationship_phrase": str(row["relationship_phrase"] or "").strip(),
        "registry_type": str(row["registry_type"] or "").strip(),
        "registry_number": str(row["registry_number"] or "").strip(),
        "source": str(row["source"] or "").strip(),
        **(provenance if isinstance(provenance, dict) else {}),
    }

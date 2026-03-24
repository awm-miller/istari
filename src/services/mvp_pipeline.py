from __future__ import annotations

import logging
from typing import Any

from src.charity_commission.client import CharityCommissionClient
from src.charity_commission.expansion import (
    expand_charity_connected_organisations,
    expand_charity_people,
)
from src.companies_house.client import CompaniesHouseClient
from src.companies_house.expansion import expand_company_people
from src.config import Settings
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
    return {
        "run_id": run_id,
        "processed_organisation_count": processed,
        "connected_organisation_count": len(
            repository.get_run_organisations(run_id, stages=[STEP2_STAGE])
        ),
        "linked_insert_attempts": linked_count,
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

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.charity_commission.search import CharityCommissionSearchProvider
from src.config import Settings
from src.models import EvidenceItem, NameVariant, OrganisationRecord, ResolutionDecision
from src.ofac.screening import OFACScreener
from src.pipeline import (
    add_organisation_to_run,
    run_name_pipeline,
    step1_expand_seed,
    step2_expand_connected_organisations,
    step3_expand_connected_people,
    step4_ofac_screening,
)
from src.storage.repository import Repository


def build_test_settings(root: Path) -> Settings:
    return Settings(
        project_root=root,
        database_path=root / "test.sqlite",
        cache_dir=root / "cache",
        charity_api_key="test-cc",
        charity_api_base_url="https://example.test/cc",
        charity_api_key_header="X-Test-Key",
        companies_house_api_key="test-ch",
        companies_house_base_url="https://example.test/ch",
        gemini_api_key=None,
        gemini_resolution_model="gemini-test",
        openai_api_key=None,
        openai_search_model="gpt-test",
        openai_resolution_model="gpt-test",
        openai_base_url="https://example.test/openai",
        openai_web_search_context="medium",
        resolution_provider="gemini",
        serper_api_key=None,
        serper_base_url="https://example.test/serper",
        user_agent="project-istari-test/1.0",
        pdf_enrichment_enabled=True,
        pdf_enrichment_model="gemini-test",
        pdf_enrichment_max_documents=3,
        pdf_enrichment_max_chunks=4,
    )


class FakeCharityClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def search_charities_by_name(self, charity_name: str) -> list[dict[str, object]]:
        if charity_name != "Alex Smith":
            return []
        return [
            {
                "organisation_number": 1001,
                "reg_charity_number": 123456,
                "group_subsid_suffix": 0,
                "charity_name": "Alex Smith Foundation",
                "reg_status": "R",
            }
        ]

    def get_all_charity_details(self, charity_number: int, suffix: int = 0) -> dict[str, object]:
        if charity_number == 123456:
            return {
                "organisation_number": 1001,
                "charity_name": "Alex Smith Foundation",
                "reg_status": "R",
                "address_line_one": "1 Charity Street",
                "address_line_two": "London",
                "address_post_code": "E1 1AA",
            }
        if charity_number == 654321:
            return {
                "organisation_number": None,
                "charity_name": "Linked Relief Trust",
                "reg_status": "R",
                "address_line_one": "2 Linked Road",
                "address_line_two": "London",
                "address_post_code": "E2 2BB",
            }
        raise RuntimeError(f"unexpected charity lookup: {charity_number}/{suffix}")

    def get_charity_trustee_information(
        self,
        charity_number: int,
        suffix: int = 0,
    ) -> list[dict[str, object]]:
        if charity_number == 123456:
            return [{"TrusteeName": "Jane Trustee", "Role": "Trustee"}]
        if charity_number == 654321:
            return [{"TrusteeName": "John Linked", "Role": "Chair"}]
        return []

    def get_charity_trustee_names(self, charity_number: int, suffix: int = 0) -> list[str]:
        if charity_number == 123456:
            return ["Mary Trustee"]
        if charity_number == 654321:
            return ["Peter Linked"]
        return []

    def get_charity_linked_charities(self, charity_number: int, suffix: int = 0) -> list[dict[str, object]]:
        if charity_number != 123456:
            return []
        return [
            {
                "linked_charity_number": 654321,
                "linked_charity_suffix": 0,
                "linked_charity_name": "Linked Relief Trust",
            }
        ]

    def get_charity_linked_charity(self, charity_number: int, suffix: int = 0) -> dict[str, object]:
        return {}


class SeedSearchProvider:
    def __init__(self) -> None:
        self.seen_variant_batches: list[list[str]] = []

    def search(self, variants: list[object]) -> list[EvidenceItem]:
        names = [variant.name for variant in variants]
        self.seen_variant_batches.append(names)
        if "Alex Smith" not in names:
            return []
        return [
            EvidenceItem(
                source="companies_house_officer_appointments",
                source_key="Alex Smith:alpha-company",
                title="Alpha Ltd",
                url="https://find-and-update.company-information.service.gov.uk/company/001",
                snippet="Alex Smith linked to Alpha Ltd via Companies House as director",
                raw_payload={
                    "variant": "Alex Smith",
                    "officer_search_item": {"title": "Alex Smith"},
                    "appointment": {
                        "officer_role": "director",
                        "appointed_to": {
                            "company_name": "Alpha Ltd",
                            "company_number": "001",
                        },
                    },
                },
            ),
            EvidenceItem(
                source="charity_commission_search",
                source_key="Alex Smith:123456:0",
                title="Alex Smith Foundation",
                url="https://example.test/charity/123456",
                snippet="Charity Commission name search match for Alex Smith",
                raw_payload={
                    "variant": "Alex Smith",
                    "candidate_name": "Alex Smith",
                    "organisation_name": "Alex Smith Foundation",
                    "registry_type": "charity",
                    "registry_number": "123456",
                    "suffix": 0,
                },
            ),
        ]


class AlwaysMatchMatcher:
    def resolve(self, seed_name: str, candidate: object) -> ResolutionDecision:
        return ResolutionDecision(
            status="match",
            confidence=0.99,
            canonical_name=str(candidate.candidate_name),
            explanation="Test matcher accepts the candidate.",
            rule_score=float(candidate.score),
        )


class AlwaysMaybeMatcher:
    def resolve(self, seed_name: str, candidate: object) -> ResolutionDecision:
        return ResolutionDecision(
            status="maybe_match",
            confidence=0.6,
            canonical_name=str(candidate.candidate_name),
            explanation="Test matcher leaves the candidate unverified.",
            rule_score=float(candidate.score),
        )


class PipelineTests(unittest.TestCase):
    def _repository(self, root: Path) -> Repository:
        repository = Repository(
            database_path=root / "test.sqlite",
            schema_path=Path(__file__).resolve().parents[1] / "src" / "storage" / "schema.sql",
        )
        repository.init_db()
        return repository

    def test_charity_commission_search_provider_uses_registry_number_in_source_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_test_settings(Path(temp_dir))
            provider = CharityCommissionSearchProvider(settings)
            provider.client = FakeCharityClient(settings)

            rows = provider.search([NameVariant(name="Alex Smith", strategy="seed_input", creativity_level="balanced")])

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].source, "charity_commission_search")
            self.assertEqual(rows[0].source_key, "Alex Smith:123456:0")

    def test_step_flow_expands_seed_orgs_then_connected_orgs_then_people(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)
            search_provider = SeedSearchProvider()

            step1 = step1_expand_seed(
                repository=repository,
                charity_client=charity_client,
                search_providers=[search_provider],
                matcher=AlwaysMatchMatcher(),
                seed_name="Alex Smith",
                creativity_level="balanced",
            )
            run_id = int(step1["run_id"])

            self.assertEqual(step1["matched_organisation_count"], 2)
            self.assertEqual(len(repository.get_run_organisations(run_id, stages=["step1_seed_match"])), 2)

            with (
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_profile",
                    side_effect=lambda *args, **kwargs: {
                        "company_name": "Alpha Ltd" if str(args[-1]) == "001" else "Alex Smith Foundation Ltd",
                        "company_status": "active",
                        "registered_office_address": {
                            "address_line_1": "3 Company Lane",
                            "locality": "London",
                            "postal_code": "E3 3CC",
                        },
                    },
                ),
                patch(
                    "src.address_pivot.AddressPivotSearcher.find_related_organisations",
                    return_value=[],
                ),
            ):
                step2 = step2_expand_connected_organisations(
                    repository=repository,
                    charity_client=charity_client,
                    run_id=run_id,
                )

            self.assertEqual(step2["connected_organisation_count"], 1)
            scoped_connected = repository.get_run_organisations(run_id, stages=["step2_connected_org"])
            self.assertEqual(len(scoped_connected), 1)

            with (
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_profile",
                    side_effect=lambda *args, **kwargs: {
                        "company_name": "Alpha Ltd" if str(args[-1]) == "001" else "Alex Smith Foundation Ltd",
                        "company_status": "active",
                        "registered_office_address": {
                            "address_line_1": "3 Company Lane",
                            "locality": "London",
                            "postal_code": "E3 3CC",
                        },
                    },
                ),
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_officers",
                    side_effect=lambda *args, **kwargs: {
                        "items": [
                            {
                                "name": "Alice Director" if str(args[-1]) == "001" else "Bob Director",
                                "officer_role": "director",
                                "appointed_on": "2020-01-01",
                            }
                        ]
                    },
                ),
            ):
                step3 = step3_expand_connected_people(
                    repository=repository,
                    settings=settings,
                    charity_client=charity_client,
                    run_id=run_id,
                    limit=10,
                )

            self.assertGreaterEqual(step3["inserted_roles"], 5)
            ranked_names = [row["canonical_name"] for row in step3["ranking"]]
            self.assertIn("Jane Trustee", ranked_names)
            self.assertIn("Alice Director", ranked_names)

    def test_run_name_pipeline_is_registry_only_and_alias_free(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)
            search_provider = SeedSearchProvider()

            with (
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_profile",
                    return_value={
                        "company_name": "Alpha Ltd",
                        "company_status": "active",
                        "registered_office_address": {
                            "address_line_1": "3 Company Lane",
                            "locality": "London",
                            "postal_code": "E3 3CC",
                        },
                    },
                ),
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_officers",
                    return_value={
                        "items": [
                            {
                                "name": "Alice Director",
                                "officer_role": "director",
                                "appointed_on": "2020-01-01",
                            }
                        ]
                    },
                ),
                patch(
                    "src.address_pivot.AddressPivotSearcher.find_related_organisations",
                    return_value=[],
                ),
            ):
                result = run_name_pipeline(
                    repository=repository,
                    settings=settings,
                    charity_client=charity_client,
                    search_providers=[search_provider],
                    matcher=AlwaysMatchMatcher(),
                    seed_name="Alex Smith",
                    creativity_level="balanced",
                    limit=10,
                )

            self.assertEqual(result["mode"], "registry_only_mvp")
            self.assertEqual(result["alias_rounds"], 0)
            self.assertEqual(result["alias_variant_count"], 0)
            self.assertEqual(len(repository.get_confirmed_alias_rows(int(result["run_id"]))), 0)
            self.assertEqual(len(search_provider.seen_variant_batches), 1)
            self.assertIn("Alex Smith", search_provider.seen_variant_batches[0])
            self.assertNotIn("land_registry_address_pivot", result["search_summary"])

    def test_step3_keeps_same_name_company_officers_separate_by_birth_month_year(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)

            run_id = repository.create_run("Alex Smith", "balanced")
            company_id = repository.upsert_organisation(
                OrganisationRecord(
                    registry_type="company",
                    registry_number="001",
                    name="Alpha Ltd",
                )
            )
            repository.link_run_organisation(
                run_id,
                company_id,
                stage="step1_seed_match",
                source="test_seed",
                metadata={},
            )

            with (
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_profile",
                    return_value={
                        "company_name": "Alpha Ltd",
                        "company_status": "active",
                    },
                ),
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_officers",
                    return_value={
                        "items": [
                            {
                                "name": "Alex Smith",
                                "officer_role": "director",
                                "appointed_on": "2020-01-01",
                                "date_of_birth": {"month": 1, "year": 1980},
                            },
                            {
                                "name": "Alex Smith",
                                "officer_role": "director",
                                "appointed_on": "2021-01-01",
                                "date_of_birth": {"month": 2, "year": 1980},
                            },
                        ]
                    },
                ),
            ):
                step3_expand_connected_people(
                    repository=repository,
                    settings=settings,
                    charity_client=charity_client,
                    run_id=run_id,
                    limit=10,
                )

            connection = repository.connect()
            try:
                rows = connection.execute(
                    """
                    SELECT id, canonical_name, identity_key
                    FROM people
                    WHERE canonical_name = 'Alex Smith'
                    ORDER BY identity_key
                    """
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual(len(rows), 2)
            self.assertNotEqual(rows[0]["identity_key"], rows[1]["identity_key"])

    def test_step2_address_pivot_links_related_org_with_connection_phrase(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)
            search_provider = SeedSearchProvider()

            step1 = step1_expand_seed(
                repository=repository,
                charity_client=charity_client,
                search_providers=[search_provider],
                matcher=AlwaysMatchMatcher(),
                seed_name="Alex Smith",
                creativity_level="balanced",
            )
            run_id = int(step1["run_id"])

            with (
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_profile",
                    return_value={
                        "company_name": "Alpha Ltd",
                        "company_status": "active",
                        "registered_office_address": {
                            "address_line_1": "3 Company Lane",
                            "locality": "London",
                            "postal_code": "E3 3CC",
                        },
                    },
                ),
                patch(
                    "src.address_pivot.AddressPivotSearcher.find_related_organisations",
                    return_value=[
                        {
                            "registry_type": "company",
                            "registry_number": "999",
                            "suffix": 0,
                            "name": "Beta Ltd",
                            "status": "active",
                            "metadata": {
                                "company_name": "Beta Ltd",
                                "company_status": "active",
                                "registered_office_address": {
                                    "address_line_1": "3 Company Lane",
                                    "locality": "London",
                                    "postal_code": "E3 3CC",
                                },
                            },
                            "source": "address_pivot_company",
                        }
                    ],
                ),
            ):
                step2_expand_connected_organisations(
                    repository=repository,
                    charity_client=charity_client,
                    run_id=run_id,
                )

            connected = repository.get_run_organisations(run_id, stages=["step2_connected_org"])
            beta_rows = [row for row in connected if row["name"] == "Beta Ltd"]
            self.assertEqual(len(beta_rows), 1)
            metadata = beta_rows[0]["run_metadata_json"]
            self.assertIn("shares an address with", str(metadata))
            address_edges = repository.get_run_address_edges(run_id)
            self.assertTrue(any(row["organisation_name"] == "Beta Ltd" for row in address_edges))

    def test_step1_does_not_scope_maybe_matches_as_seed_orgs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)
            search_provider = SeedSearchProvider()

            step1 = step1_expand_seed(
                repository=repository,
                charity_client=charity_client,
                search_providers=[search_provider],
                matcher=AlwaysMaybeMatcher(),
                seed_name="Alex Smith",
                creativity_level="balanced",
            )

            run_id = int(step1["run_id"])
            self.assertEqual(step1["matched_organisation_count"], 0)
            self.assertEqual(len(repository.get_run_organisations(run_id, stages=["step1_seed_match"])), 0)

    def test_step3_runs_resolution_over_expanded_people(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)

            run_id = repository.create_run("Alex Smith", "balanced")
            company_id = repository.upsert_organisation(
                OrganisationRecord(
                    registry_type="company",
                    registry_number="001",
                    name="Alpha Ltd",
                )
            )
            repository.link_run_organisation(
                run_id,
                company_id,
                stage="step1_seed_match",
                source="test_seed",
                metadata={},
            )

            with (
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_profile",
                    return_value={
                        "company_name": "Alpha Ltd",
                        "company_status": "active",
                    },
                ),
                patch(
                    "src.companies_house.client.CompaniesHouseClient.get_company_officers",
                    return_value={
                        "items": [
                            {
                                "name": "Alex Smith",
                                "officer_role": "director",
                                "appointed_on": "2020-01-01",
                                "date_of_birth": {"month": 1, "year": 1980},
                            },
                            {
                                "name": "Jamie Carter",
                                "officer_role": "director",
                                "appointed_on": "2020-01-01",
                            },
                        ]
                    },
                ),
            ):
                summary = step3_expand_connected_people(
                    repository=repository,
                    settings=settings,
                    charity_client=charity_client,
                    run_id=run_id,
                    limit=10,
                )

            self.assertGreaterEqual(summary["stage3_resolution"]["candidate_count"], 1)
            self.assertGreaterEqual(summary["stage3_resolution"]["decision_count"], 1)

            connection = repository.connect()
            try:
                rows = connection.execute(
                    """
                    SELECT candidate_name, raw_payload_json
                    FROM candidate_matches
                    WHERE run_id = ?
                    ORDER BY id ASC
                    """,
                    (run_id,),
                ).fetchall()
            finally:
                connection.close()

            self.assertTrue(any("Jamie Carter" == str(row["candidate_name"]) for row in rows))
            self.assertTrue(any('"stage3_resolution": true' in str(row["raw_payload_json"]) for row in rows))

    def test_add_organisation_to_run_links_org_and_reruns_downstream_steps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            charity_client = FakeCharityClient(settings)
            run_id = repository.create_run("Manual Run", "balanced")

            with patch(
                "src.address_pivot.AddressPivotSearcher.find_related_organisations",
                return_value=[],
            ):
                result = add_organisation_to_run(
                    repository=repository,
                    settings=settings,
                    charity_client=charity_client,
                    run_id=run_id,
                    registry_type="charity",
                    registry_number="123456",
                    suffix=0,
                    limit=10,
                )

            scoped_seed_orgs = repository.get_run_organisations(run_id, stages=["step1_seed_match"])
            scoped_connected_orgs = repository.get_run_organisations(run_id, stages=["step2_connected_org"])

            self.assertEqual(result["registry_type"], "charity")
            self.assertEqual(result["registry_number"], "123456")
            self.assertTrue(result["reran_downstream"])
            self.assertEqual(len(scoped_seed_orgs), 1)
            self.assertEqual(scoped_seed_orgs[0]["name"], "Alex Smith Foundation")
            self.assertEqual(len(scoped_connected_orgs), 1)
            ranked_names = [row["canonical_name"] for row in result["ranking"]]
            self.assertIn("Jane Trustee", ranked_names)
            self.assertIn("John Linked", ranked_names)

    def test_step4_ofac_screening_persists_person_sanctions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_test_settings(root)
            repository = self._repository(root)
            person_id = repository.upsert_person("John Doe", "person:john-doe")
            ranking = [
                {
                    "person_id": person_id,
                    "canonical_name": "John Doe",
                    "identity_key": "person:john-doe",
                    "organisation_count": 1,
                    "role_count": 1,
                    "weighted_organisation_score": 1.0,
                }
            ]

            with patch.object(
                OFACScreener,
                "ensure_local_sources",
                new=lambda self, target_dir: setattr(self, "_entries", [{"name": "DOE, John"}]),
            ), patch.object(
                OFACScreener,
                "screen_name",
                return_value=[{"source": "OFAC SDN", "source_id": "1", "name": "DOE, John"}],
            ):
                result = step4_ofac_screening(
                    repository=repository,
                    settings=settings,
                    ranking=ranking,
                )

            sanctions = repository.get_person_sanctions([person_id])
            self.assertTrue(result["sanctions_hits"]["John Doe"])
            self.assertTrue(sanctions[person_id]["is_sanctioned"])
            self.assertEqual(sanctions[person_id]["screened_name"], "John Doe")

    def test_person_sanctions_persistence_handles_set_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repository = self._repository(root)
            person_id = repository.upsert_person("John Doe", "person:john-doe")
            repository.upsert_person_sanctions(
                person_id=person_id,
                screened_name="John Doe",
                screened_birth_month=10,
                screened_birth_year=1952,
                matches=[
                    {
                        "source": "Direction Generale du Tresor",
                        "birth_month_years": {(10, 1952)},
                        "_prepared_norms": frozenset({"john doe"}),
                    }
                ],
            )

            sanctions = repository.get_person_sanctions([person_id])
            self.assertTrue(sanctions[person_id]["is_sanctioned"])
            self.assertEqual(
                sanctions[person_id]["matches"][0]["birth_month_years"],
                [[10, 1952]],
            )


if __name__ == "__main__":
    unittest.main()

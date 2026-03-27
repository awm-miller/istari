from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.config import Settings
from src.models import OrganisationRecord, PdfExtractedEntity, PdfSourceDocument
from src.services.pdf_enrichment import (
    PdfEnrichmentService,
    chunk_markdown,
    parse_pdf_entities_document,
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
        gemini_api_key="test-gemini",
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
        return []


class PdfEnrichmentTests(unittest.TestCase):
    def test_chunk_markdown_splits_large_input(self) -> None:
        text = ("alpha\n\n" * 2000).strip()
        chunks = chunk_markdown(text, max_chars=200)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk for chunk in chunks))
        self.assertTrue(all(len(chunk) <= 200 for chunk in chunks))

    def test_parse_pdf_entities_document_normalizes_rows(self) -> None:
        rows = parse_pdf_entities_document(
            {
                "entities": [
                    {
                        "name": "  Jane Trustee ",
                        "entity_type": "person",
                        "role_category": "person",
                        "role_label": "trustee",
                        "connection_phrase": "is named as a trustee of",
                        "notes": "The trustees section lists Jane Trustee by name.",
                        "confidence": 0.9,
                    },
                    {
                        "name": "ignored row",
                        "entity_type": "weird",
                        "role_category": "mystery",
                    },
                    {"name": "   "},
                ]
            },
            organisation_name="Known Org",
            source_document_url="https://example.test/report.pdf",
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].name, "Jane Trustee")
        self.assertEqual(rows[0].organisation_name, "Known Org")
        self.assertEqual(rows[0].connection_phrase, "is named as a trustee of")
        self.assertEqual(rows[0].notes, "The trustees section lists Jane Trustee by name.")
        self.assertEqual(rows[1].entity_type, "other")
        self.assertEqual(rows[1].role_category, "ignore")

    def test_enrich_run_adds_roles_and_links_resolved_organisations(self) -> None:
        root = Path(tempfile.mkdtemp())
        settings = build_test_settings(root)
        repository = Repository(
            settings.database_path,
            settings.project_root / "src" / "storage" / "schema.sql",
        )
        schema_src = Path(__file__).resolve().parents[1] / "src" / "storage" / "schema.sql"
        target_schema = settings.project_root / "src" / "storage" / "schema.sql"
        target_schema.parent.mkdir(parents=True, exist_ok=True)
        target_schema.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
        repository.init_db()

        parent_org_id = repository.upsert_organisation(
            OrganisationRecord(
                registry_type="charity",
                registry_number="123456",
                suffix=0,
                organisation_number=1001,
                name="Known Org",
                status="R",
                metadata={},
            )
        )
        run_id = repository.create_run("Alex Smith", "balanced")
        repository.link_run_organisation(run_id, parent_org_id, stage="step1_seed_match", source="seed")

        service = PdfEnrichmentService(
            settings=settings,
            repository=repository,
            charity_client=FakeCharityClient(settings),
        )
        document = PdfSourceDocument(
            organisation_name="Known Org",
            document_url="https://example.test/report.pdf",
            title="Known Org Annual Report",
            source_provider="companies_house_filing",
            local_pdf_path=str(root / "report.pdf"),
            markdown_path=str(root / "report.md"),
            markdown_text="Annual report for Known Org with trustees and accounts.",
            filing_description="accounts (2023-01-01)",
        )

        with (
            patch.object(service, "find_documents_for_organisation", return_value=[document]),
            patch.object(service, "_prepare_document", return_value=document),
            patch.object(
                service,
                "extract_entities_from_document",
                return_value=[
                    PdfExtractedEntity(
                        name="Jane Trustee",
                        entity_type="person",
                        role_category="person",
                        role_label="trustee",
                        organisation_name="Known Org",
                        source_document_url=document.document_url,
                        connection_phrase="is named as a trustee of",
                        notes="The annual report trustee section lists Jane Trustee.",
                        confidence=0.9,
                    ),
                    PdfExtractedEntity(
                        name="Acme Audit Limited",
                        entity_type="organisation",
                        role_category="organisation",
                        role_label="auditor",
                        organisation_name="Known Org",
                        source_document_url=document.document_url,
                        connection_phrase="is named as auditor for",
                        notes="The accounts section names Acme Audit Limited as auditor.",
                        confidence=0.9,
                    ),
                ],
            ),
            patch.object(
                service.org_resolver,
                "resolve",
                return_value=OrganisationRecord(
                    registry_type="company",
                    registry_number="ACME123",
                    suffix=0,
                    organisation_number=None,
                    name="Acme Audit Limited",
                    status="active",
                    metadata={},
                ),
            ),
        ):
            summary = service.enrich_run(
                run_id=run_id,
                organisations=repository.get_run_organisations(run_id, stages=["step1_seed_match"]),
            )

        self.assertEqual(summary["document_count"], 1)
        self.assertEqual(summary["entity_count"], 2)
        self.assertEqual(summary["people_added"], 1)
        self.assertEqual(summary["organisation_mentions_resolved"], 1)

        with repository.connect() as connection:
            roles = connection.execute(
                "SELECT role_label, relationship_phrase, provenance_json, source FROM person_org_roles WHERE organisation_id = ?",
                (parent_org_id,),
            ).fetchall()
            self.assertEqual(len(roles), 1)
            self.assertEqual(str(roles[0]["source"]), "pdf_gemini_extraction")
            self.assertEqual(str(roles[0]["relationship_phrase"]), "is named as a trustee of")
            self.assertIn("trustee section", str(roles[0]["provenance_json"]))

            linked = connection.execute(
                """
                SELECT organisations.name, run_organisations.metadata_json
                FROM run_organisations
                JOIN organisations ON organisations.id = run_organisations.organisation_id
                WHERE run_organisations.run_id = ? AND run_organisations.source = 'pdf_org_mention'
                """,
                (run_id,),
            ).fetchall()
            self.assertEqual([str(row["name"]) for row in linked], ["Acme Audit Limited"])
            self.assertIn("is named as auditor for", str(linked[0]["metadata_json"]))
            self.assertIn("accounts section", str(linked[0]["metadata_json"]))

        del service
        del repository

    def test_enrich_run_ignores_notice_boilerplate_entities(self) -> None:
        root = Path(tempfile.mkdtemp())
        settings = build_test_settings(root)
        repository = Repository(
            settings.database_path,
            settings.project_root / "src" / "storage" / "schema.sql",
        )
        schema_src = Path(__file__).resolve().parents[1] / "src" / "storage" / "schema.sql"
        target_schema = settings.project_root / "src" / "storage" / "schema.sql"
        target_schema.parent.mkdir(parents=True, exist_ok=True)
        target_schema.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
        repository.init_db()

        parent_org_id = repository.upsert_organisation(
            OrganisationRecord(
                registry_type="company",
                registry_number="11301882",
                suffix=0,
                organisation_number=None,
                name="BILAL YASIN PHOTOGRAPHY LTD",
                status="active",
                metadata={},
            )
        )
        run_id = repository.create_run("Bilal Khalil Hasan Yasin", "balanced")
        repository.link_run_organisation(run_id, parent_org_id, stage="step1_seed_match", source="seed")

        service = PdfEnrichmentService(
            settings=settings,
            repository=repository,
            charity_client=FakeCharityClient(settings),
        )
        document = PdfSourceDocument(
            organisation_name="BILAL YASIN PHOTOGRAPHY LTD",
            document_url="https://example.test/gazette.pdf",
            title="BILAL YASIN PHOTOGRAPHY LTD - gazette notice",
            source_provider="companies_house_filing",
            local_pdf_path=str(root / "gazette.pdf"),
            markdown_path=str(root / "gazette.md"),
            markdown_text="The Registrar of Companies gives notice that the company will be struck off.",
            filing_description="gazette-notice-compulsory (2024-07-09)",
        )

        with (
            patch.object(service, "find_documents_for_organisation", return_value=[document]),
            patch.object(service, "_prepare_document", return_value=document),
            patch.object(
                service,
                "extract_entities_from_document",
                return_value=[
                    PdfExtractedEntity(
                        name="The Registrar of Companies",
                        entity_type="organisation",
                        role_category="other_professional",
                        role_label="Registrar of Companies",
                        organisation_name="BILAL YASIN PHOTOGRAPHY LTD",
                        source_document_url=document.document_url,
                        connection_phrase="gives notice regarding",
                        notes="The official body issuing the gazette notice for striking off the company.",
                        confidence=1.0,
                    ),
                ],
            ),
        ):
            summary = service.enrich_run(
                run_id=run_id,
                organisations=repository.get_run_organisations(run_id, stages=["step1_seed_match"]),
            )

        self.assertEqual(summary["entity_count"], 0)
        self.assertEqual(summary["people_added"], 0)
        self.assertEqual(summary["organisation_mentions_seen"], 0)
        self.assertEqual(summary["organisation_mentions_resolved"], 0)

        with repository.connect() as connection:
            roles = connection.execute("SELECT COUNT(*) AS count FROM person_org_roles").fetchone()
            linked = connection.execute(
                "SELECT COUNT(*) AS count FROM run_organisations WHERE run_id = ? AND source = 'pdf_org_mention'",
                (run_id,),
            ).fetchone()
            self.assertEqual(int(roles["count"]), 0)
            self.assertEqual(int(linked["count"]), 0)

        del service
        del repository


if __name__ == "__main__":
    unittest.main()

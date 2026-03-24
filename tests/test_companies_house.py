from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from src.companies_house.client import CompaniesHouseClient, extract_officer_id
from src.config import Settings


class CompaniesHouseTests(unittest.TestCase):
    def test_extract_officer_id_from_self_link(self) -> None:
        officer_id = extract_officer_id(
            {"links": {"self": "/officers/abc123def456/appointments"}}
        )
        self.assertEqual(officer_id, "abc123def456")

    def test_search_companies_builds_expected_url(self) -> None:
        settings = Settings(
            project_root=Path("."),
            database_path=Path("test.sqlite"),
            cache_dir=Path(".cache"),
            charity_api_key=None,
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
        client = CompaniesHouseClient(settings)
        seen: list[str] = []

        def fake_get_json(url: str) -> dict:
            seen.append(url)
            return {"items": []}

        with patch.object(CompaniesHouseClient, "_get_json", side_effect=fake_get_json):
            result = client.search_companies("1 High Street SW1A 1AA", items_per_page=20)

        self.assertEqual(result, {"items": []})
        self.assertEqual(
            seen[0],
            "https://example.test/ch/search/companies?q=1%20High%20Street%20SW1A%201AA&items_per_page=20",
        )


if __name__ == "__main__":
    unittest.main()

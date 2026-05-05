from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import builder_api
from src.config import Settings


class _NoopExecutor:
    def submit(self, *_args, **_kwargs):
        return None


def _empty_settings(root: Path) -> Settings:
    return Settings(
        project_root=root,
        database_path=root / "data.sqlite",
        cache_dir=root / "cache",
        charity_api_key=None,
        charity_api_base_url="https://example.test/charity",
        charity_api_key_header="Ocp-Apim-Subscription-Key",
        companies_house_api_key=None,
        companies_house_base_url="https://example.test/companies",
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
        user_agent="project-istari-test",
        pdf_enrichment_enabled=False,
        pdf_enrichment_model="gemini-test",
        pdf_enrichment_max_documents=1,
        pdf_enrichment_max_chunks=1,
    )


class BuilderApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._executor = builder_api.EXECUTOR
        builder_api.JOB_DIR = Path(self._tmp.name)
        builder_api.EXECUTOR = _NoopExecutor()
        self.client = builder_api.create_app().test_client()

    def tearDown(self) -> None:
        builder_api.EXECUTOR = self._executor
        self._tmp.cleanup()

    def test_gemini_key_test_requires_server_side_key(self) -> None:
        with patch("src.builder_api.load_settings", return_value=_empty_settings(Path(self._tmp.name))):
            response = self.client.post("/api/key-tests/gemini", json={})

        self.assertEqual(400, response.status_code)
        self.assertFalse(response.get_json()["ok"])

    def test_tree_job_response_never_echoes_credentials(self) -> None:
        response = self.client.post(
            "/api/tree-jobs",
            json={
                "mode": "name_seed",
                "seed_name": "Alice Example",
                "credentials": {
                    "gemini_api_key": "secret-gemini",
                    "serper_api_key": "secret-serper",
                },
            },
        )

        self.assertEqual(202, response.status_code)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertNotIn("credentials", body["job"]["request"])
        self.assertNotIn("secret-gemini", str(body))
        self.assertNotIn("secret-serper", str(body))

    def test_invalid_tree_job_is_rejected_before_queueing(self) -> None:
        response = self.client.post("/api/tree-jobs", json={"mode": "org_rooted"})

        self.assertEqual(400, response.status_code)
        self.assertFalse(response.get_json()["ok"])


if __name__ == "__main__":
    unittest.main()

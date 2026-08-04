from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.cases.openrouter import DEFAULT_CASE_MODEL, OpenRouterCaseParser
from src.openrouter_api import OpenRouterChatClient


class _Response:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class OpenRouterCaseParserTest(unittest.TestCase):
    def test_parser_uses_latest_deepseek_with_zdr_and_validates_result(self) -> None:
        response = _Response(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "version": 1,
                                    "id": "park-avenue",
                                    "title": "Park Avenue",
                                    "recipe": "address-network",
                                    "inputs": [
                                        {"kind": "address", "value": "94 Park Avenue North, London, NW10 1JY"}
                                    ],
                                }
                            )
                        }
                    }
                ]
            }
        )
        with patch("src.openrouter_api.request.urlopen", return_value=response) as urlopen:
            spec = OpenRouterCaseParser(api_key="test-key").parse("Fan out from 94 Park Avenue North")

        sent = json.loads(urlopen.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(DEFAULT_CASE_MODEL, sent["model"])
        self.assertEqual({"type": "json_object"}, sent["response_format"])
        self.assertEqual({"effort": "medium"}, sent["reasoning"])
        self.assertEqual({"data_collection": "deny", "zdr": True}, sent["provider"])
        self.assertIn("A company input must be a Companies House number", sent["messages"][0]["content"])
        self.assertIn('"depth N"', sent["messages"][0]["content"])
        self.assertEqual("address-network", spec.recipe)
        self.assertEqual("address", spec.inputs[0].kind)

    def test_parser_requires_api_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "OPENROUTER_API_KEY"):
            OpenRouterCaseParser(api_key="").parse("Find Alice Example")

    def test_parser_requires_model(self) -> None:
        with self.assertRaisesRegex(ValueError, "model is required"):
            OpenRouterCaseParser(api_key="test-key", model="").parse("Find Alice Example")

    def test_parser_rejects_company_name_misclassified_as_person(self) -> None:
        response = _Response(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "version": 1,
                                    "id": "acme",
                                    "title": "ACME",
                                    "recipe": "registry-light",
                                    "inputs": [{"kind": "person", "value": "ACME Holdings Limited"}],
                                }
                            )
                        }
                    }
                ]
            }
        )
        with (
            patch("src.openrouter_api.request.urlopen", return_value=response),
            self.assertRaisesRegex(ValueError, "Companies House number"),
        ):
            OpenRouterCaseParser(api_key="test-key").parse("Investigate ACME Holdings Limited")

    def test_client_retries_provider_error_returned_with_http_200(self) -> None:
        client = OpenRouterChatClient(
            api_key="test-key",
            base_url="https://example.test",
            user_agent="test",
        )
        provider_error = _Response({"error": {"message": "Worker request limit reached"}})
        completion = _Response({"choices": [{"message": {"content": "{}"}}]})
        with (
            patch("src.openrouter_api.request.urlopen", side_effect=[provider_error, completion]) as urlopen,
            patch("src.openrouter_api.time.sleep"),
        ):
            response = client.create_chat_completion(
                model=DEFAULT_CASE_MODEL,
                messages=[{"role": "user", "content": "test"}],
            )

        self.assertEqual(2, urlopen.call_count)
        self.assertEqual("{}", response["choices"][0]["message"]["content"])

    def test_client_ignores_cached_provider_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = OpenRouterChatClient(
                api_key="test-key",
                base_url="https://example.test",
                user_agent="test",
                cache_dir=Path(temp_dir),
            )
            request_payload = {
                "model": DEFAULT_CASE_MODEL,
                "messages": [{"role": "user", "content": "test"}],
                "temperature": 0.0,
                "stream": False,
                "provider": {"data_collection": "deny", "zdr": True},
            }
            cache_path = client._cache_path(request_payload)
            assert cache_path is not None
            cache_path.write_text(json.dumps({"error": {"message": "busy"}}), encoding="utf-8")
            completion = _Response({"choices": [{"message": {"content": "{}"}}]})

            with patch("src.openrouter_api.request.urlopen", return_value=completion) as urlopen:
                response = client.create_chat_completion(
                    model=DEFAULT_CASE_MODEL,
                    messages=request_payload["messages"],
                )

            self.assertEqual(1, urlopen.call_count)
            self.assertEqual("{}", response["choices"][0]["message"]["content"])


if __name__ == "__main__":
    unittest.main()

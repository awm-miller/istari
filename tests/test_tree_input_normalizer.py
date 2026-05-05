from __future__ import annotations

import json
import unittest

from src.tree_input_normalizer import normalize_builder_payload, resolve_organisation_roots


class _FakeCharityClient:
    def __init__(self, results: dict[str, list[dict]]) -> None:
        self.results = results
        self.calls: list[str] = []

    def search_charities_by_name(self, charity_name: str) -> list[dict]:
        self.calls.append(charity_name)
        return self.results.get(charity_name, [])


class _FakeCompaniesClient:
    def __init__(self, results: dict[str, list[dict]]) -> None:
        self.results = results
        self.calls: list[str] = []

    def search_companies(self, query: str, items_per_page: int = 20) -> dict:
        self.calls.append(query)
        return {"items": self.results.get(query, [])}


class _FakeGeminiClient:
    def __init__(self, items: list[dict[str, str]]) -> None:
        self.items = items
        self.calls: list[str] = []

    def generate(self, *, model: str, prompt: str, temperature: float = 0.0) -> dict:
        self.calls.append(prompt)
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {
                                "text": json.dumps({"items": self.items}),
                            }
                        ]
                    }
                }
            ]
        }


class TreeInputNormalizerTest(unittest.TestCase):
    def test_explicit_roots_are_passed_through_and_deduped(self) -> None:
        roots = resolve_organisation_roots(
            ["charity:293802", "charity:293802:0", "company:07162533"],
        )

        self.assertEqual(["charity:293802", "company:07162533"], [f"{item.root.registry_type}:{item.root.registry_number}" for item in roots])

    def test_resolves_organisation_names_with_registry_search(self) -> None:
        payload = normalize_builder_payload(
            {
                "mode": "org_rooted",
                "roots": ["1. AHLULBAYT FOUNDATION - charity entry from report"],
                "target_names": ["Trustee: HASHEM DARWISH (director)"],
            },
            charity_client=_FakeCharityClient(
                {
                    "AHLULBAYT FOUNDATION": [
                        {
                            "charity_name": "AHLULBAYT FOUNDATION",
                            "reg_charity_number": 1136006,
                            "group_subsid_suffix": 0,
                        }
                    ]
                }
            ),
            companies_house_client=_FakeCompaniesClient({}),
        )

        self.assertEqual(["charity:1136006"], payload["roots"])
        self.assertEqual(["HASHEM DARWISH"], payload["target_names"])

    def test_normal_seed_can_use_seed_names_input(self) -> None:
        payload = normalize_builder_payload(
            {
                "mode": "name_seed",
                "seed_name": "",
                "seed_names": ["Name: Ahmed Vaezi - Iran report row"],
            },
        )

        self.assertEqual("Ahmed Vaezi", payload["seed_name"])
        self.assertEqual([], payload["seed_names"])

    def test_resolves_company_names_with_registry_search(self) -> None:
        payload = normalize_builder_payload(
            {
                "mode": "org_chained",
                "seed_names": ["Name: Aliasghar Ramezanpour - report row"],
                "roots": ["Company: AHLULBAYT FOUNDATION"],
            },
            charity_client=_FakeCharityClient({}),
            companies_house_client=_FakeCompaniesClient(
                {
                    "AHLULBAYT FOUNDATION": [
                        {
                            "title": "AHLULBAYT FOUNDATION",
                            "company_number": "07162533",
                        }
                    ]
                }
            ),
        )

        self.assertEqual(["Aliasghar Ramezanpour"], payload["seed_names"])
        self.assertEqual(["company:07162533"], payload["roots"])

    def test_ambiguous_organisation_name_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Could not resolve organisation row"):
            normalize_builder_payload(
                {
                    "mode": "org_rooted",
                    "roots": ["AHLULBAYT"],
                },
                charity_client=_FakeCharityClient(
                    {
                        "AHLULBAYT": [
                            {"charity_name": "AHLULBAYT FOUNDATION", "reg_charity_number": 1136006},
                            {"charity_name": "AHLULBAYT TRUST", "reg_charity_number": 999999},
                        ]
                    }
                ),
                companies_house_client=_FakeCompaniesClient({}),
            )

    def test_gemini_fallback_extracts_messy_organisation_label(self) -> None:
        payload = normalize_builder_payload(
            {
                "mode": "org_rooted",
                "roots": ["Row contains mixed prose with no clean organisation marker"],
            },
            charity_client=_FakeCharityClient(
                {
                    "ABRAR ISLAMIC FOUNDATION": [
                        {"charity_name": "ABRAR ISLAMIC FOUNDATION", "reg_charity_number": 293802}
                    ]
                }
            ),
            companies_house_client=_FakeCompaniesClient({}),
            gemini_client=_FakeGeminiClient(
                [
                    {
                        "row": "Row contains mixed prose with no clean organisation marker",
                        "value": "ABRAR ISLAMIC FOUNDATION",
                    }
                ]
            ),
            gemini_model="gemini-test",
        )

        self.assertEqual(["charity:293802"], payload["roots"])


if __name__ == "__main__":
    unittest.main()

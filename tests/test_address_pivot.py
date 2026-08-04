from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

from src.address_pivot import AddressPivotSearcher
from src.addresses import normalize_address_label
from src.config import load_settings


class AddressPivotSearcherTest(unittest.TestCase):
    def test_company_address_search_uses_location_and_prefilters_results(self) -> None:
        companies_house = MagicMock()
        companies_house.search_companies_advanced.return_value = {
            "hits": 2,
            "items": [
                {
                    "company_number": "00000001",
                    "company_name": "Wrong Address Limited",
                    "registered_office_address": {
                        "address_line_1": "95 Park Avenue North",
                        "locality": "London",
                        "postal_code": "NW10 1JY",
                    },
                },
                {
                    "company_number": "00000002",
                    "company_name": "Exact Address Limited",
                    "company_status": "active",
                    "registered_office_address": {
                        "address_line_1": "94 Park Avenue North",
                        "locality": "London",
                        "postal_code": "NW10 1JY",
                    },
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(
                load_settings(),
                cache_dir=Path(tmp),
                companies_house_api_key="test-key",
                serper_api_key=None,
            )
            searcher = AddressPivotSearcher(
                settings=settings,
                charity_client=MagicMock(),
                companies_house_client=companies_house,
            )
            rows = searcher.find_organisations(
                normalize_address_label("94 Park Avenue North, London, NW10 1JY")
            )

        self.assertEqual(["00000002"], [row["registry_number"] for row in rows])
        companies_house.search_companies_advanced.assert_called_once_with(
            location="94 Park Avenue North, London, NW10 1JY",
            size=500,
        )

    def test_numbered_street_without_postcode_matches_full_registry_address(self) -> None:
        companies_house = MagicMock()
        companies_house.search_companies_advanced.return_value = {
            "hits": 1,
            "items": [
                {
                    "company_number": "00000032",
                    "company_name": "Store Street Limited",
                    "registered_office_address": {
                        "premises": "32",
                        "address_line_1": "Store Street",
                        "locality": "London",
                        "postal_code": "WC1E 7BS",
                    },
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(load_settings(), cache_dir=Path(tmp), serper_api_key=None)
            searcher = AddressPivotSearcher(
                settings=settings,
                charity_client=MagicMock(),
                companies_house_client=companies_house,
            )
            rows = searcher.find_organisations(normalize_address_label("32 Store Street"))

        self.assertEqual(["00000032"], [row["registry_number"] for row in rows])

    def test_company_source_failure_is_not_reported_as_no_matches(self) -> None:
        companies_house = MagicMock()
        companies_house.search_companies_advanced.side_effect = RuntimeError("Invalid Authorization header")

        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(load_settings(), cache_dir=Path(tmp), serper_api_key=None)
            searcher = AddressPivotSearcher(
                settings=settings,
                charity_client=MagicMock(),
                companies_house_client=companies_house,
            )
            with self.assertRaisesRegex(RuntimeError, "Companies House address search failed"):
                searcher.find_organisations(normalize_address_label("32 Store Street"))


if __name__ == "__main__":
    unittest.main()

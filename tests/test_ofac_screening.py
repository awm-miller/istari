from __future__ import annotations

import json
import unittest

from src.ofac.screening import OFACScreener


class OFACScreenerTests(unittest.TestCase):
    def test_screen_name_returns_public_json_safe_hits(self) -> None:
        screener = OFACScreener()
        screener._append_entry(
            {
                "ent_num": "example-1",
                "name": "Jane Doe",
                "aliases": [],
                "program": "Example programme",
                "remarks": "DOB January 1980",
                "source": "Example source",
                "source_id": "example-1",
            }
        )

        hits = screener.screen_name("Jane Doe")

        self.assertEqual(hits[0]["birth_month_years"], [[1, 1980]])
        self.assertFalse(any(key.startswith("_") for key in hits[0]))
        json.dumps(hits)


if __name__ == "__main__":
    unittest.main()

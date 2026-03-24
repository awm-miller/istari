from __future__ import annotations

import unittest

from src.companies_house.client import extract_officer_id


class CompaniesHouseTests(unittest.TestCase):
    def test_extract_officer_id_from_self_link(self) -> None:
        officer_id = extract_officer_id(
            {"links": {"self": "/officers/abc123def456/appointments"}}
        )
        self.assertEqual(officer_id, "abc123def456")


if __name__ == "__main__":
    unittest.main()

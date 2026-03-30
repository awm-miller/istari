from __future__ import annotations

import unittest

from src.ofac.screening import OFACScreener, extract_identity_key_birth_month_year


class OfacScreeningTests(unittest.TestCase):
    def test_matches_majid_al_zeer_variants(self) -> None:
        screener = OFACScreener()
        screener._entries = [
            {
                "ent_num": "1",
                "name": "AL-ZEER, Majed",
                "program": "SDGT",
                "remarks": "DOB 12 Oct 1959",
            }
        ]
        screener._normalized_names = set()
        screener._token_sets = {frozenset({"majed", "zeer"}): "AL-ZEER, Majed"}
        screener._entry_birth_month_years = {"1": {(10, 1959)}}

        self.assertTrue(screener.screen_name("Majid al-Zeer", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("Majed Khalil Al-Zeer", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("Majed Alzeer", birth_month=10, birth_year=1959))
        self.assertTrue(screener.screen_name("AL-ZEER, Majed Khalil", birth_month=10, birth_year=1959))
        self.assertFalse(screener.screen_name("Majid al-Zeer"))
        self.assertFalse(screener.screen_name("Majid al-Zeer", birth_month=9, birth_year=1959))

    def test_load_csv_extracts_birth_month_year_from_remarks(self) -> None:
        screener = OFACScreener()
        screener._entries = [
            {
                "ent_num": "1",
                "name": "DOE, John",
                "program": "SDGT",
                "remarks": "DOB 14 Feb 1980; alt. DOB 1981-03-01",
            }
        ]
        screener._normalized_names = {"doe john"}
        screener._token_sets = {frozenset({"doe", "john"}): "DOE, John"}
        screener._entry_birth_month_years = {"1": {(2, 1980), (3, 1981)}}

        self.assertTrue(screener.screen_name("John Doe", birth_month=2, birth_year=1980))
        self.assertTrue(screener.screen_name("John Doe", birth_month=3, birth_year=1981))

    def test_extract_identity_key_birth_month_year_reads_companies_house_keys(self) -> None:
        self.assertEqual(
            extract_identity_key_birth_month_year("ch-name-dob:john_doe:1980-02"),
            (2, 1980),
        )
        self.assertEqual(
            extract_identity_key_birth_month_year("ch-officer:abc123:1975-11"),
            (11, 1975),
        )
        self.assertEqual(
            extract_identity_key_birth_month_year("name:john_doe"),
            (None, None),
        )


if __name__ == "__main__":
    unittest.main()

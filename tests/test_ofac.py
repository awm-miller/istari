from __future__ import annotations

import unittest

from src.ofac.screening import OFACScreener


class OfacScreeningTests(unittest.TestCase):
    def test_matches_majid_al_zeer_variants(self) -> None:
        screener = OFACScreener()
        screener._entries = [
            {
                "ent_num": "1",
                "name": "AL-ZEER, Majed",
                "program": "SDGT",
                "remarks": "",
            }
        ]
        screener._normalized_names = set()
        screener._token_sets = {frozenset({"majed", "zeer"}): "AL-ZEER, Majed"}

        self.assertTrue(screener.screen_name("Majid al-Zeer"))
        self.assertTrue(screener.screen_name("Majed Khalil Al-Zeer"))
        self.assertTrue(screener.screen_name("Majed Alzeer"))
        self.assertTrue(screener.screen_name("AL-ZEER, Majed Khalil"))


if __name__ == "__main__":
    unittest.main()

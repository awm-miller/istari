from __future__ import annotations

import unittest

from scripts.consolidate_and_graph import _role_phrase


class RolePhraseNormalizationTest(unittest.TestCase):
    def test_shareholder_and_initial_shareholder_normalize_together(self) -> None:
        shareholder_edge = {
            "relationship_phrase": "",
            "role_type": "shareholder",
            "role_label": "shareholder",
            "end_date": "",
        }
        initial_shareholder_edge = {
            "relationship_phrase": "",
            "role_type": "company_shareholding",
            "role_label": "initial shareholder",
            "end_date": "",
        }

        self.assertEqual(_role_phrase(shareholder_edge), "is a shareholder of")
        self.assertEqual(_role_phrase(initial_shareholder_edge), "is a shareholder of")


if __name__ == "__main__":
    unittest.main()

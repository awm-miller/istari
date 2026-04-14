from __future__ import annotations

import unittest

from scripts.consolidate_and_graph import _role_key


class RoleCountDedupTest(unittest.TestCase):
    def test_role_key_dedupes_same_visible_role_across_org_ids(self) -> None:
        company_edge = {
            "organisation_id": 154,
            "organisation_name": "MUSLIM WELFARE HOUSE",
            "relationship_phrase": "is a trustee of",
            "role_type": "trustee",
            "role_label": "trustee",
            "end_date": "",
        }
        charity_edge = {
            "organisation_id": 1069,
            "organisation_name": "MUSLIM WELFARE HOUSE",
            "relationship_phrase": "is a trustee of",
            "role_type": "trustee",
            "role_label": "trustee",
            "end_date": "",
        }

        self.assertEqual(_role_key(company_edge), _role_key(charity_edge))


if __name__ == "__main__":
    unittest.main()

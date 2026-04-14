from __future__ import annotations

import unittest

from scripts.consolidate_and_graph import _merged_org_key


class OrgCountDedupTest(unittest.TestCase):
    def test_merged_org_key_dedupes_same_visible_org_across_ids(self) -> None:
        org_nodes = {
            "154": {"label": "MUSLIM WELFARE HOUSE"},
            "1069": {"label": "MUSLIM WELFARE HOUSE"},
        }

        self.assertEqual(
            _merged_org_key("154", org_nodes),
            _merged_org_key("1069", org_nodes),
        )


if __name__ == "__main__":
    unittest.main()

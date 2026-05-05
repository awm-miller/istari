from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from src.negative_news import partition_negative_news_clusters_by_history
from src.storage.negative_news_store import database_source_key


class _FakeStore:
    def get_latest_completed_results_by_cluster_id(self, **_kwargs):
        return {
            "merged_person:429": {
                "cluster_id": "merged_person:429",
                "label": "Daoud Khan",
                "result": {
                    "cluster_id": "merged_person:429",
                    "label": "Daoud Khan",
                    "person_ids": [4510],
                },
            }
        }

    def get_latest_completed_results_by_cluster_lookup_key(self, **_kwargs):
        return {}

    def get_latest_completed_results_by_person_ids(self, **_kwargs):
        return {}


class NegativeNewsHistoryReuseTest(unittest.TestCase):
    def test_person_cluster_id_reuse_requires_matching_fingerprint(self) -> None:
        cluster = {
            "cluster_id": "merged_person:429",
            "cluster_kind": "person",
            "label": "MR BILAL KHALIL HASAN YASIN",
            "aliases": [
                "MR BILAL KHALIL HASAN YASIN",
                "BILAL YASIN",
            ],
            "person_ids": [1, 27, 44, 124],
        }

        partition = partition_negative_news_clusters_by_history(_FakeStore(), [cluster])

        self.assertEqual(1, len(partition["pending_clusters"]))
        self.assertEqual(0, len(partition["reused_clusters"]))

    def test_database_source_key_changes_when_database_is_rebuilt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            database_path = Path(tmp) / "builder.sqlite"
            database_path.write_bytes(b"first-db")
            first_key = database_source_key(database_path)

            database_path.write_bytes(b"second-db-with-different-content")
            second_key = database_source_key(database_path)

        self.assertNotEqual(first_key, second_key)


if __name__ == "__main__":
    unittest.main()

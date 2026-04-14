from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.graph.adverse_media import annotate_graph_with_adverse_media
from src.negative_news import partition_negative_news_clusters_by_history
from src.storage.negative_news_store import NegativeNewsStore


class NegativeNewsGraphAnnotationsTest(unittest.TestCase):
    def _make_store(self) -> tuple[tempfile.TemporaryDirectory[str], NegativeNewsStore]:
        temp_dir = tempfile.TemporaryDirectory()
        root = Path(temp_dir.name)
        store = NegativeNewsStore(
            root / "negative_news.sqlite",
            Path("c:/Users/Alex/GitHub/project-istari/src/storage/negative_news_schema.sql"),
        )
        store.init_db()
        return temp_dir, store

    def test_identity_clusters_are_not_reused_from_person_history(self) -> None:
        temp_dir, store = self._make_store()
        self.addCleanup(temp_dir.cleanup)
        batch_run_id = store.get_or_create_batch_run(
            config={"mode": "test"},
            offset_value=0,
            limit_value=1,
            total_clusters=1,
            output_path="",
        )
        store.upsert_cluster_result(
            batch_run_id=batch_run_id,
            cluster_rank=1,
            cluster_id="merged_person:1",
            label="Merged Person",
            status="completed",
            interesting_count=1,
            category_counts={"other_mb_alignment": 1},
            result={
                "cluster_id": "merged_person:1",
                "cluster_kind": "person",
                "label": "Merged Person",
                "person_ids": [101],
                "articles": [
                    {
                        "classification": {"category": "other_mb_alignment"},
                        "search": {"title": "Merged article", "url": "https://example.com/merged"},
                    }
                ],
            },
        )

        partition = partition_negative_news_clusters_by_history(
            store,
            [
                {
                    "cluster_id": "identity:5:seed_alias:1",
                    "cluster_kind": "seed_alias",
                    "label": "Alias Identity",
                    "person_ids": [101],
                },
                {
                    "cluster_id": "merged_person:2",
                    "cluster_kind": "person",
                    "label": "Merged Person Copy",
                    "person_ids": [101],
                },
            ],
        )

        pending_ids = [cluster["cluster_id"] for cluster in partition["pending_clusters"]]
        reused_ids = [cluster["cluster_id"] for cluster in partition["reused_clusters"]]
        self.assertEqual(pending_ids, ["identity:5:seed_alias:1"])
        self.assertEqual(reused_ids, ["merged_person:2"])

    def test_cluster_specific_adverse_media_wins_over_person_fingerprint_fallback(self) -> None:
        temp_dir, store = self._make_store()
        self.addCleanup(temp_dir.cleanup)
        batch_run_id = store.get_or_create_batch_run(
            config={"mode": "test"},
            offset_value=0,
            limit_value=2,
            total_clusters=2,
            output_path="",
        )
        store.upsert_cluster_result(
            batch_run_id=batch_run_id,
            cluster_rank=1,
            cluster_id="merged_person:1",
            label="Merged Person",
            status="completed",
            interesting_count=1,
            category_counts={"other_mb_alignment": 1},
            result={
                "cluster_id": "merged_person:1",
                "cluster_kind": "person",
                "label": "Merged Person",
                "person_ids": [101],
                "articles": [
                    {
                        "classification": {
                            "category": "other_mb_alignment",
                            "confidence": 0.7,
                        },
                        "search": {
                            "title": "Merged person article",
                            "url": "https://example.com/merged",
                        },
                    }
                ],
            },
        )
        store.upsert_cluster_result(
            batch_run_id=batch_run_id,
            cluster_rank=2,
            cluster_id="identity:5:seed_alias:1",
            label="Alias Identity",
            status="completed",
            interesting_count=1,
            category_counts={"explicit_mb_connection": 1},
            result={
                "cluster_id": "identity:5:seed_alias:1",
                "cluster_kind": "seed_alias",
                "label": "Alias Identity",
                "person_ids": [101],
                "articles": [
                    {
                        "classification": {
                            "category": "explicit_mb_connection",
                            "confidence": 0.9,
                        },
                        "search": {
                            "title": "Identity article",
                            "url": "https://example.com/identity",
                        },
                    }
                ],
            },
        )

        data = {
            "nodes": [
                {"id": "merged_person:1", "kind": "person", "person_ids": [101]},
                {"id": "identity:5:seed_alias:1", "kind": "seed_alias", "person_ids": [101]},
            ]
        }
        settings = SimpleNamespace(gemini_api_key=None, cache_dir=Path(temp_dir.name))

        annotated = annotate_graph_with_adverse_media(
            data,
            settings=settings,
            database_path=store.database_path,
        )

        merged_node = next(node for node in annotated["nodes"] if node["id"] == "merged_person:1")
        identity_node = next(node for node in annotated["nodes"] if node["id"] == "identity:5:seed_alias:1")
        self.assertEqual(merged_node["adverse_media_claims"][0]["title"], "Merged person article")
        self.assertEqual(identity_node["adverse_media_claims"][0]["title"], "Identity article")


if __name__ == "__main__":
    unittest.main()

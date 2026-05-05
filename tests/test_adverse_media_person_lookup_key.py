from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.config import load_settings
from src.graph.adverse_media import annotate_graph_with_adverse_media
from src.storage.negative_news_store import database_source_key


class AdverseMediaPersonLookupKeyTest(unittest.TestCase):
    def test_person_node_maps_by_stable_name_lookup_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            database_path = temp_path / "negative_news.sqlite"
            schema_path = Path("src/storage/negative_news_schema.sql")

            connection = sqlite3.connect(database_path)
            connection.executescript(schema_path.read_text(encoding="utf-8"))
            result = {
                "cluster_id": "merged_person:443",
                "cluster_kind": "person",
                "label": "MR BILAL KHALIL HASAN YASIN",
                "aliases": [
                    "MR BILAL KHALIL HASAN YASIN",
                    "BILAL YASIN",
                    "Bilal Khalil Hasan YASIN",
                    "YASIN, Bilal Khalil Hasan",
                ],
                "person_ids": [1, 27, 44, 124],
                "articles": [
                    {
                        "search": {
                            "title": "عربي21 example",
                            "url": "https://www.arabi21.com/story/example",
                        },
                        "classification": {
                            "category": "other_mb_alignment",
                            "confidence": 0.91,
                            "short_rationale": "Example adverse-media hit",
                            "evidence_quote": "Example quote",
                        },
                    }
                ],
            }
            with connection:
                connection.execute(
                    """
                    INSERT INTO negative_news_batch_runs(
                        config_hash, config_json, status, offset_value, limit_value, total_clusters, completed_clusters, output_path
                    ) VALUES(?, ?, 'completed', 0, 1, 1, 1, '')
                    """,
                    ("test", "{}",),
                )
                connection.execute(
                    """
                    INSERT INTO negative_news_cluster_results(
                        batch_run_id, cluster_rank, cluster_id, label, status, interesting_count,
                        category_counts_json, result_json, error_text
                    ) VALUES(1, 1, ?, ?, 'completed', 1, ?, ?, '')
                    """,
                    (
                        "merged_person:443",
                        "MR BILAL KHALIL HASAN YASIN",
                        json.dumps({"other_mb_alignment": 1}),
                        json.dumps(result, ensure_ascii=False),
                    ),
                )
            connection.close()

            data = {
                "nodes": [
                    {
                        "id": "merged_person:2",
                        "kind": "person",
                        "label": "MR BILAL KHALIL HASAN YASIN",
                        "aliases": [
                            "BILAL YASIN",
                            "Bilal Khalil Hasan YASIN",
                            "YASIN, Bilal Khalil Hasan",
                        ],
                        "person_ids": [9991, 9992],
                    }
                ],
                "edges": [],
            }

            annotated = annotate_graph_with_adverse_media(
                data,
                settings=load_settings(Path(".").resolve()),
                database_path=database_path,
            )

            node = annotated["nodes"][0]
            self.assertTrue(node["adverse_media_hit"])
            self.assertEqual(1, node["adverse_media_count"])
            self.assertEqual("https://www.arabi21.com/story/example", node["adverse_media_claims"][0]["url"])

    def test_person_node_maps_by_name_lookup_when_historical_result_lacks_cluster_kind(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            database_path = temp_path / "negative_news.sqlite"
            schema_path = Path("src/storage/negative_news_schema.sql")

            connection = sqlite3.connect(database_path)
            connection.executescript(schema_path.read_text(encoding="utf-8"))
            result = {
                "cluster_id": "merged_person:23",
                "label": "SAFFOUR, Walid",
                "aliases": ["SAFFOUR, Walid"],
                "person_ids": [294],
                "articles": [
                    {
                        "search": {
                            "title": "Example article",
                            "url": "https://example.com/walid",
                        },
                        "classification": {
                            "category": "other_mb_alignment",
                            "confidence": 0.82,
                            "short_rationale": "Example adverse-media hit",
                            "evidence_quote": "Example quote",
                        },
                    }
                ],
            }
            with connection:
                connection.execute(
                    """
                    INSERT INTO negative_news_batch_runs(
                        config_hash, config_json, status, offset_value, limit_value, total_clusters, completed_clusters, output_path
                    ) VALUES(?, ?, 'completed', 0, 1, 1, 1, '')
                    """,
                    ("test", "{}",),
                )
                connection.execute(
                    """
                    INSERT INTO negative_news_cluster_results(
                        batch_run_id, cluster_rank, cluster_id, label, status, interesting_count,
                        category_counts_json, result_json, error_text
                    ) VALUES(1, 1, ?, ?, 'completed', 1, ?, ?, '')
                    """,
                    (
                        "merged_person:23",
                        "SAFFOUR, Walid",
                        json.dumps({"other_mb_alignment": 1}),
                        json.dumps(result, ensure_ascii=False),
                    ),
                )
            connection.close()

            data = {
                "nodes": [
                    {
                        "id": "merged_person:182",
                        "kind": "person",
                        "label": "SAFFOUR, Walid",
                        "aliases": ["Walid Saffour"],
                        "person_ids": [294, 5466],
                    }
                ],
                "edges": [],
            }

            annotated = annotate_graph_with_adverse_media(
                data,
                settings=load_settings(Path(".").resolve()),
                database_path=database_path,
            )

            node = annotated["nodes"][0]
            self.assertTrue(node["adverse_media_hit"])
            self.assertEqual(1, node["adverse_media_count"])
            self.assertEqual("https://example.com/walid", node["adverse_media_claims"][0]["url"])

    def test_stale_merged_person_cluster_id_does_not_attach_to_different_person(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            database_path = temp_path / "negative_news.sqlite"
            schema_path = Path("src/storage/negative_news_schema.sql")

            connection = sqlite3.connect(database_path)
            connection.executescript(schema_path.read_text(encoding="utf-8"))
            result = {
                "cluster_id": "merged_person:23",
                "label": "SAFFOUR, Walid",
                "aliases": ["SAFFOUR, Walid"],
                "person_ids": [294],
                "articles": [
                    {
                        "search": {
                            "title": "Example article",
                            "url": "https://example.com/walid",
                        },
                        "classification": {
                            "category": "other_mb_alignment",
                            "confidence": 0.82,
                            "short_rationale": "Example adverse-media hit",
                            "evidence_quote": "Example quote",
                        },
                    }
                ],
            }
            with connection:
                connection.execute(
                    """
                    INSERT INTO negative_news_batch_runs(
                        config_hash, config_json, status, offset_value, limit_value, total_clusters, completed_clusters, output_path
                    ) VALUES(?, ?, 'completed', 0, 1, 1, 1, '')
                    """,
                    ("test", "{}",),
                )
                connection.execute(
                    """
                    INSERT INTO negative_news_cluster_results(
                        batch_run_id, cluster_rank, cluster_id, label, status, interesting_count,
                        category_counts_json, result_json, error_text
                    ) VALUES(1, 1, ?, ?, 'completed', 1, ?, ?, '')
                    """,
                    (
                        "merged_person:23",
                        "SAFFOUR, Walid",
                        json.dumps({"other_mb_alignment": 1}),
                        json.dumps(result, ensure_ascii=False),
                    ),
                )
            connection.close()

            data = {
                "nodes": [
                    {
                        "id": "merged_person:23",
                        "kind": "person",
                        "label": "YORK PLACE COMPANY SECRETARIES LIMITED",
                        "aliases": ["YORK PLACE COMPANY SECRETARIES LIMITED"],
                        "person_ids": [1068],
                    },
                    {
                        "id": "merged_person:182",
                        "kind": "person",
                        "label": "SAFFOUR, Walid",
                        "aliases": ["Walid Saffour"],
                        "person_ids": [294, 5466],
                    },
                ],
                "edges": [],
            }

            annotated = annotate_graph_with_adverse_media(
                data,
                settings=load_settings(Path(".").resolve()),
                database_path=database_path,
            )

            wrong_node, walid_node = annotated["nodes"]
            self.assertFalse(wrong_node.get("adverse_media_hit", False))
            self.assertTrue(walid_node["adverse_media_hit"])
            self.assertEqual(1, walid_node["adverse_media_count"])
            self.assertEqual("https://example.com/walid", walid_node["adverse_media_claims"][0]["url"])

    def test_scoped_results_from_other_database_do_not_attach(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            database_path = temp_path / "negative_news.sqlite"
            schema_path = Path("src/storage/negative_news_schema.sql")
            graph_database_path = temp_path / "current_graph.db"
            other_database_path = temp_path / "other_graph.db"

            connection = sqlite3.connect(database_path)
            connection.executescript(schema_path.read_text(encoding="utf-8"))
            result = {
                "cluster_id": "merged_person:23",
                "cluster_kind": "person",
                "source_database_key": database_source_key(other_database_path),
                "label": "SAFFOUR, Walid",
                "aliases": ["SAFFOUR, Walid", "Walid Saffour"],
                "person_ids": [294],
                "articles": [
                    {
                        "search": {
                            "title": "Wrong database article",
                            "url": "https://example.com/wrong-db",
                        },
                        "classification": {
                            "category": "other_mb_alignment",
                            "confidence": 0.82,
                            "short_rationale": "Should not attach across databases",
                            "evidence_quote": "Example quote",
                        },
                    }
                ],
            }
            with connection:
                connection.execute(
                    """
                    INSERT INTO negative_news_batch_runs(
                        config_hash, config_json, status, offset_value, limit_value, total_clusters, completed_clusters, output_path
                    ) VALUES(?, ?, 'completed', 0, 1, 1, 1, '')
                    """,
                    (
                        "other-db",
                        json.dumps(
                            {
                                "source_database_path": str(other_database_path),
                                "source_database_key": database_source_key(other_database_path),
                            }
                        ),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO negative_news_cluster_results(
                        batch_run_id, cluster_rank, cluster_id, label, status, interesting_count,
                        category_counts_json, result_json, error_text
                    ) VALUES(1, 1, ?, ?, 'completed', 1, ?, ?, '')
                    """,
                    (
                        "merged_person:23",
                        "SAFFOUR, Walid",
                        json.dumps({"other_mb_alignment": 1}),
                        json.dumps(result, ensure_ascii=False),
                    ),
                )
            connection.close()

            data = {
                "nodes": [
                    {
                        "id": "merged_person:23",
                        "kind": "person",
                        "label": "SAFFOUR, Walid",
                        "aliases": ["Walid Saffour"],
                        "person_ids": [294],
                    }
                ],
                "edges": [],
            }

            settings = replace(load_settings(Path(".").resolve()), database_path=graph_database_path)
            annotated = annotate_graph_with_adverse_media(
                data,
                settings=settings,
                database_path=database_path,
            )

            self.assertFalse(annotated["nodes"][0].get("adverse_media_hit", False))


if __name__ == "__main__":
    unittest.main()

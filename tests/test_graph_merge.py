from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


_MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "consolidate_and_graph.py"
_SPEC = importlib.util.spec_from_file_location("consolidate_and_graph", _MODULE_PATH)
assert _SPEC and _SPEC.loader
cg = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cg)


class GraphMergeTests(unittest.TestCase):
    def test_matches_seed_alias_uses_aliases_not_just_label(self) -> None:
        self.assertTrue(
            cg.matches_seed_alias(
                "Mohamed Kozbar",
                [
                    "KOZBAR, Mohamad Abdul Karim",
                    "KOZBAR, Mohamad",
                    "Mohammed KOZBAR",
                ],
            )
        )

    def test_multi_run_merges_equivalent_addresses(self) -> None:
        run_one = {
            "seed_name": "Seed One",
            "run_id": 1,
            "nodes": [
                {"id": "org:1", "label": "Org One", "kind": "organisation", "lane": 2, "tooltip_lines": []},
                {
                    "id": "addr:10",
                    "label": "32 Shore Street, London, E1 8AA, United Kingdom",
                    "kind": "address",
                    "lane": 3,
                    "normalized_key": "32 SHORE STREET|LONDON|E18AA|UNITED KINGDOM",
                    "postcode": "E1 8AA",
                    "country": "United Kingdom",
                    "tooltip_lines": [],
                },
            ],
            "edges": [
                {
                    "source": "org:1",
                    "target": "addr:10",
                    "kind": "address_link",
                    "phrase": "is registered at",
                    "source_provider": "test",
                    "confidence": "high",
                    "weight": 0.8,
                    "tooltip": "Org One is registered at 32 Shore Street",
                }
            ],
            "consolidated": [],
        }
        run_two = {
            "seed_name": "Seed Two",
            "run_id": 2,
            "nodes": [
                {"id": "org:2", "label": "Org Two", "kind": "organisation", "lane": 2, "tooltip_lines": []},
                {
                    "id": "addr:11",
                    "label": "32 Shore St, London, E1 8AA",
                    "kind": "address",
                    "lane": 3,
                    "normalized_key": "32 SHORE ST|LONDON|E18AA",
                    "postcode": "E1 8AA",
                    "country": "",
                    "tooltip_lines": [],
                },
            ],
            "edges": [
                {
                    "source": "org:2",
                    "target": "addr:11",
                    "kind": "address_link",
                    "phrase": "is registered at",
                    "source_provider": "test",
                    "confidence": "high",
                    "weight": 0.8,
                    "tooltip": "Org Two is registered at 32 Shore St",
                }
            ],
            "consolidated": [],
        }

        with patch.object(cg, "consolidate_run", side_effect=[run_one, run_two]):
            merged = cg.consolidate_multi_run([1, 2])

        address_nodes = [node for node in merged["nodes"] if node["kind"] == "address"]
        self.assertEqual(len(address_nodes), 1)
        self.assertEqual(
            sorted(address_nodes[0].get("aliases") or []),
            [
                "32 Shore St, London, E1 8AA",
                "32 Shore Street, London, E1 8AA, United Kingdom",
            ],
        )

        address_edges = [edge for edge in merged["edges"] if edge["kind"] == "address_link"]
        self.assertEqual(len(address_edges), 2)
        self.assertEqual({edge["target"] for edge in address_edges}, {address_nodes[0]["id"]})

    def test_multi_run_preserves_pdf_evidence_on_role_edges(self) -> None:
        run_one = {
            "seed_name": "Seed One",
            "run_id": 1,
            "nodes": [
                {"id": "org:1", "label": "Org One", "kind": "organisation", "lane": 2, "tooltip_lines": []},
                {"id": "person:10", "label": "Jane Trustee", "kind": "person", "lane": 4, "tooltip_lines": []},
            ],
            "edges": [
                {
                    "source": "org:1",
                    "target": "person:10",
                    "kind": "role",
                    "phrase": "is named as a trustee of",
                    "source_provider": "pdf_gemini_extraction",
                    "confidence": "medium",
                    "weight": 0.45,
                    "evidence": {
                        "title": "Org One Annual Report",
                        "document_url": "https://example.test/report.pdf",
                        "page_hint": "page 14",
                        "page_number": 14,
                    },
                }
            ],
            "consolidated": [
                {
                    "group_id": "person:10",
                    "label": "Jane Trustee",
                    "aliases": ["Jane Trustee"],
                    "person_ids": [],
                    "org_count": 1,
                    "role_count": 1,
                    "score": 0.45,
                    "is_seed_alias": False,
                }
            ],
        }

        with patch.object(cg, "consolidate_run", return_value=run_one):
            merged = cg.consolidate_multi_run([1])

        role_edges = [edge for edge in merged["edges"] if edge["kind"] == "role"]
        self.assertEqual(len(role_edges), 1)
        self.assertEqual(role_edges[0]["evidence"]["title"], "Org One Annual Report")
        self.assertEqual(role_edges[0]["evidence"]["page_number"], 14)


if __name__ == "__main__":
    unittest.main()

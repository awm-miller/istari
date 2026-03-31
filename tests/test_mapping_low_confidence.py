from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from src.mapping_low_confidence import (
    MappingStore,
    build_low_confidence_edge_details,
    build_low_confidence_group_details,
    build_low_confidence_overlay,
    import_mapping_workbooks,
)


class MappingLowConfidenceTests(unittest.TestCase):
    def test_import_mapping_workbooks_extracts_entities_links_and_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workbook_path = root / "sample.xlsx"
            database_path = root / "mapping.sqlite"

            workbook = Workbook()
            worksheet = workbook.active
            worksheet.title = "Combined"
            worksheet.append(
                [
                    "Label",
                    "Type",
                    "Description",
                    "",
                    "",
                    "From",
                    "To",
                    "Type",
                    "Description",
                ]
            )
            worksheet.append(
                [
                    "Campaign A",
                    "Campaign",
                    "Campaign description",
                    "",
                    "",
                    "Person One",
                    "Campaign A",
                    "Signatory",
                    "Signed [source](https://example.test/source) and https://example.test/extra",
                ]
            )
            workbook.save(workbook_path)

            summary = import_mapping_workbooks(root, database_path)

            self.assertEqual(summary["workbook_count"], 1)
            self.assertEqual(summary["entity_count"], 1)
            self.assertEqual(summary["link_count"], 1)
            self.assertEqual(summary["evidence_count"], 2)

            store = MappingStore(database_path)
            entities = store.list_entities()
            links = store.list_links()
            evidence = store.list_evidence()

            self.assertEqual(len(entities), 1)
            self.assertEqual(entities[0]["label"], "Campaign A")
            self.assertEqual(len(links), 1)
            self.assertEqual(links[0]["from_label"], "Person One")
            self.assertEqual(links[0]["to_label"], "Campaign A")
            self.assertEqual([row["url"] for row in evidence], ["https://example.test/source", "https://example.test/extra"])

    def test_build_low_confidence_overlay_matches_exact_nodes_and_keeps_unmatched_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            store = MappingStore(database_path)
            store.init_db()
            import_id = store.create_import(root)
            link_id = store.insert_link(
                import_id=import_id,
                workbook_name="sample.xlsx",
                sheet_name="Links",
                row_number=2,
                from_label="Person One",
                to_label="Campaign A",
                link_type="Signatory",
                description="Signed [source](https://example.test/source)",
                raw_row=["Person One", "Campaign A", "Signatory"],
            )
            store.insert_evidence(
                mapping_link_id=link_id,
                ordinal=1,
                evidence_kind="markdown_link",
                title="source",
                url="https://example.test/source",
                snippet="Signed [source](https://example.test/source)",
            )
            store.insert_entity(
                import_id=import_id,
                workbook_name="sample.xlsx",
                sheet_name="Entities",
                row_number=4,
                label="Campaign A",
                entity_type="Campaign",
                description="Imported campaign node",
                raw_row=["Campaign A", "Campaign", "Imported campaign node"],
            )

            overlay = build_low_confidence_overlay(
                main_data={
                    "nodes": [
                        {
                            "id": "person:1",
                            "label": "Person One",
                            "kind": "person",
                            "lane": 4,
                            "aliases": ["P. One"],
                        }
                    ],
                    "edges": [],
                },
                database_path=database_path,
                run_key="run-1",
            )

            self.assertEqual(len(overlay["nodes"]), 1)
            self.assertEqual(overlay["nodes"][0]["label"], "Campaign A")
            self.assertTrue(overlay["nodes"][0]["is_low_confidence"])
            self.assertEqual(len(overlay["edges"]), 1)
            self.assertEqual(overlay["edges"][0]["source"], "person:1")
            self.assertEqual(overlay["edges"][0]["target"], overlay["nodes"][0]["id"])
            self.assertTrue(overlay["edges"][0]["is_low_confidence"])
            self.assertTrue(overlay["edges"][0]["detail_available"])
            self.assertEqual(
                overlay["edges"][0]["tooltip"],
                "Imported from sample.xlsx / Links / row 2",
            )

            matches = store.list_matches("run-1")
            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0]["endpoint"], "from")
            self.assertEqual(matches[0]["matched_node_id"], "person:1")

            details = build_low_confidence_edge_details(database_path=database_path)
            self.assertEqual(
                details["mapping-link:1"]["evidence_items"][0]["document_url"],
                "https://example.test/source",
            )

    def test_build_low_confidence_overlay_aggregates_large_member_groups(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            store = MappingStore(database_path)
            store.init_db()
            import_id = store.create_import(root)
            store.insert_entity(
                import_id=import_id,
                workbook_name="sample.xlsx",
                sheet_name="Entities",
                row_number=2,
                label="Campaign A",
                entity_type="Campaign",
                description="Imported campaign node",
                raw_row=["Campaign A", "Campaign", "Imported campaign node"],
            )
            for index in range(1, 7):
                store.insert_link(
                    import_id=import_id,
                    workbook_name="sample.xlsx",
                    sheet_name="Links",
                    row_number=10 + index,
                    from_label=f"Person {index}",
                    to_label="Campaign A",
                    link_type="Signatory",
                    description="Signed [source](https://example.test/source)",
                    raw_row=[f"Person {index}", "Campaign A", "Signatory"],
                )

            overlay = build_low_confidence_overlay(
                main_data={"nodes": [], "edges": []},
                database_path=database_path,
                run_key="run-1",
            )

            self.assertEqual(len(overlay["nodes"]), 2)
            self.assertEqual(len(overlay["edges"]), 1)
            self.assertEqual(overlay["summary"]["aggregated_group_count"], 1)
            group_node = next(node for node in overlay["nodes"] if node.get("is_low_confidence_group"))
            self.assertEqual(group_node["aggregate_member_count"], 6)

            group_details = build_low_confidence_group_details(
                main_data={"nodes": [], "edges": []},
                database_path=database_path,
                run_key="run-1",
            )
            self.assertEqual(len(group_details), 1)
            detail = group_details[group_node["id"]]
            self.assertEqual(detail["summary"]["member_count"], 6)
            self.assertEqual(len(detail["edges"]), 6)


if __name__ == "__main__":
    unittest.main()

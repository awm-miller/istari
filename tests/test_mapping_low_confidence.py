from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from src.config import Settings
from src.mapping_evidence_enrichment import (
    MappingDocumentContext,
    _GENERATED_WORKBOOK_NAME,
    MappingEvidenceDocument,
    MappingEvidenceEnricher,
    _build_document_context,
    _parse_signatory_payload,
    _signatory_payload_to_entities_links,
    _select_best_summary,
    _select_relevant_chunks,
    _select_signatory_chunks,
    _document_key,
)
from src.mapping_low_confidence import (
    MappingStore,
    build_low_confidence_overlay,
    default_overlay_mapping_db_path,
    import_mapping_workbooks,
    rebuild_overlay_mapping_db,
)


def _test_settings(root: Path) -> Settings:
    return Settings(
        project_root=root,
        database_path=root / "charity.sqlite",
        cache_dir=root / "cache",
        charity_api_key=None,
        charity_api_base_url="https://example.test/charity",
        charity_api_key_header="x-test-key",
        companies_house_api_key=None,
        companies_house_base_url="https://example.test/ch",
        gemini_api_key="test-key",
        gemini_resolution_model="gemini-test",
        openai_api_key=None,
        openai_search_model="gpt-test",
        openai_resolution_model="gpt-test",
        openai_base_url="https://example.test/openai",
        openai_web_search_context="medium",
        resolution_provider="gemini",
        serper_api_key=None,
        serper_base_url="https://example.test/serper",
        user_agent="project-istari-test/0.1",
        pdf_enrichment_enabled=True,
        pdf_enrichment_model="gemini-test",
        pdf_enrichment_max_documents=3,
        pdf_enrichment_max_chunks=4,
    )


class FakeMappingEvidenceEnricher(MappingEvidenceEnricher):
    def __init__(
        self,
        *,
        settings: Settings,
        database_path: Path,
        summary_text: str,
        entities: list[dict[str, object]] | None = None,
        links: list[dict[str, object]] | None = None,
    ) -> None:
        self._summary_text = summary_text
        self._entities = list(entities or [])
        self._links = list(links or [])
        super().__init__(settings=settings, database_path=database_path)

    def _prepare_document(self, *, url: str, title: str) -> MappingEvidenceDocument:
        return MappingEvidenceDocument(
            url=url,
            title=title,
            source_type="html",
            text="Example source text",
        )

    def _extract_document(
        self,
        document: MappingEvidenceDocument,
        *,
        context: MappingDocumentContext | None = None,
    ) -> tuple[str, list[dict[str, object]], list[dict[str, object]]]:
        return (self._summary_text, list(self._entities), list(self._links))


class MappingLowConfidenceTests(unittest.TestCase):
    def test_rebuild_overlay_mapping_db_combines_clean_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            data_dir = root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            source_paths = [
                data_dir / "mapping_links.signatory-clean.sqlite",
                data_dir / "mapping_links.normalize-after-rest.sqlite",
            ]
            for index, path in enumerate(source_paths, start=1):
                store = MappingStore(path)
                store.init_db()
                import_id = store.create_import(root / f"source-{index}")
                shared_link_id = store.insert_link(
                    import_id=import_id,
                    workbook_name="shared.xlsx",
                    sheet_name="Links",
                    row_number=1,
                    from_label="Person One",
                    to_label="Campaign A",
                    link_type="signatory",
                    description="Shared row",
                    raw_row=["Person One", "Campaign A", "signatory"],
                )
                store.insert_evidence(
                    mapping_link_id=shared_link_id,
                    ordinal=1,
                    evidence_kind="plain_url",
                    title="shared",
                    url="https://example.test/shared",
                    snippet="Shared evidence",
                )
                store.insert_entity(
                    import_id=import_id,
                    workbook_name="shared.xlsx",
                    sheet_name="Entities",
                    row_number=1,
                    label="Campaign A",
                    entity_type="Campaign",
                    description="Shared campaign",
                    raw_row=["Campaign A", "Campaign"],
                )
            second_store = MappingStore(source_paths[1])
            second_import_id = second_store.create_import(root / "source-2-extra")
            unique_link_id = second_store.insert_link(
                import_id=second_import_id,
                workbook_name="generated.xlsx",
                sheet_name="Links",
                row_number=2,
                from_label="Person Two",
                to_label="Campaign B",
                link_type="affiliate",
                description="Unique row",
                raw_row=["Person Two", "Campaign B", "affiliate"],
            )
            second_store.insert_evidence(
                mapping_link_id=unique_link_id,
                ordinal=1,
                evidence_kind="plain_url",
                title="unique",
                url="https://example.test/unique",
                snippet="Unique evidence",
            )

            combined_path = rebuild_overlay_mapping_db(root)

            self.assertEqual(combined_path, default_overlay_mapping_db_path(root))
            combined_store = MappingStore(combined_path)
            self.assertEqual(len(combined_store.list_links()), 2)
            self.assertEqual(len(combined_store.list_evidence()), 2)
            self.assertEqual(len(combined_store.list_entities()), 1)

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
            self.assertEqual(entities[0]["entity_type"], "campaign")
            self.assertEqual(len(links), 1)
            self.assertEqual(links[0]["from_label"], "Person One")
            self.assertEqual(links[0]["to_label"], "Campaign A")
            self.assertEqual(links[0]["link_type"], "signatory")
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
                description=(
                    "Signed [source](https://example.test/source) after issuing a long public statement "
                    "about the campaign and its wider political context. This trailing text should not "
                    "be copied into the rendered tooltip in full."
                ),
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
            self.assertEqual(overlay["edges"][0]["evidence_items"][0]["document_url"], "https://example.test/source")
            self.assertNotIn("https://example.test/source", overlay["edges"][0]["tooltip"])
            self.assertLess(len(overlay["edges"][0]["tooltip"]), 220)

            matches = store.list_matches("run-1")
            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0]["endpoint"], "from")
            self.assertEqual(matches[0]["matched_node_id"], "person:1")

    def test_build_low_confidence_overlay_marks_signatory_target_as_expandable_document(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            store = MappingStore(database_path)
            store.init_db()
            import_id = store.create_import(root)
            document_label = "Statement of support for Moazzam Begg (February 2014)"
            matched_link_id = store.insert_link(
                import_id=import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name="doc-1",
                row_number=1,
                from_label="Mohammed Kozbar",
                to_label=document_label,
                link_type="signatory",
                description="Matched signatory row",
                raw_row=["Mohammed Kozbar", document_label, "signatory"],
            )
            store.insert_evidence(
                mapping_link_id=matched_link_id,
                ordinal=1,
                evidence_kind="plain_url",
                title="source",
                url="https://example.test/cage",
                snippet="Matched signatory row",
                document_summary="Support statement with signatories and affiliations.",
            )
            unmatched_link_id = store.insert_link(
                import_id=import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name="doc-1",
                row_number=2,
                from_label="New Signatory",
                to_label=document_label,
                link_type="signatory",
                description="Unmatched signatory row",
                raw_row=["New Signatory", document_label, "signatory"],
            )
            store.insert_evidence(
                mapping_link_id=unmatched_link_id,
                ordinal=1,
                evidence_kind="plain_url",
                title="source",
                url="https://example.test/cage",
                snippet="Unmatched signatory row",
                document_summary="Support statement with signatories and affiliations.",
            )

            overlay = build_low_confidence_overlay(
                main_data={
                    "nodes": [
                        {
                            "id": "person:1",
                            "label": "Mohammed Kozbar",
                            "kind": "person",
                            "lane": 4,
                            "aliases": [],
                        }
                    ],
                    "edges": [],
                },
                database_path=database_path,
                run_key="run-1",
                include_unmatched=True,
                include_generated_links=True,
            )

            document_node = next(node for node in overlay["nodes"] if node["label"] == document_label)
            self.assertEqual(document_node["registry_type"], "other")
            self.assertTrue(document_node["low_confidence_expandable"])
            self.assertEqual(document_node["mapping_entity_type"], "other organisation")
            self.assertEqual(len(overlay["edges"]), 2)

    def test_build_low_confidence_overlay_resolves_person_via_seed_name_variants(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            store = MappingStore(database_path)
            store.init_db()
            import_id = store.create_import(root)
            store.insert_entity(
                import_id=import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name="doc-1",
                row_number=1,
                label="Mohammed Kozbar",
                entity_type="individual",
                description="Signer",
                raw_row=["Mohammed Kozbar", "individual"],
            )
            link_id = store.insert_link(
                import_id=import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name="doc-1",
                row_number=1,
                from_label="Mohammed Kozbar",
                to_label="Statement of support for Moazzam Begg (February 2014)",
                link_type="signatory",
                description="Matched signatory row",
                raw_row=["Mohammed Kozbar", "Statement of support for Moazzam Begg (February 2014)", "signatory"],
            )
            store.insert_evidence(
                mapping_link_id=link_id,
                ordinal=1,
                evidence_kind="plain_url",
                title="source",
                url="https://example.test/cage",
                snippet="Matched signatory row",
                document_summary="Support statement with signatories and affiliations.",
            )

            overlay = build_low_confidence_overlay(
                main_data={
                    "nodes": [
                        {
                            "id": "seed:14",
                            "label": "Mohamed Kozbar",
                            "kind": "seed",
                            "lane": 0,
                            "aliases": [],
                        },
                        {
                            "id": "identity:14:person:365",
                            "label": "KOZBAR, Mohamad Abdul Karim",
                            "kind": "seed_alias",
                            "lane": 1,
                            "aliases": ["Mohamad Abdul Karim Kozbar", "Mohammed KOZBAR"],
                        },
                    ],
                    "edges": [
                        {
                            "kind": "alias",
                            "source": "seed:14",
                            "target": "identity:14:person:365",
                        }
                    ],
                },
                database_path=database_path,
                run_key="run-1",
                include_unmatched=True,
                include_generated_links=True,
            )

            self.assertTrue(any(
                edge["source"] == "seed:14" or edge["target"] == "seed:14"
                for edge in overlay["edges"]
            ))

    def test_build_low_confidence_overlay_skips_fully_unmatched_links_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            store = MappingStore(database_path)
            store.init_db()
            import_id = store.create_import(root)
            store.insert_link(
                import_id=import_id,
                workbook_name="sample.xlsx",
                sheet_name="Links",
                row_number=2,
                from_label="Matched Person",
                to_label="Matched Campaign",
                link_type="signatory",
                description="Matched link",
                raw_row=["Matched Person", "Matched Campaign", "signatory"],
            )
            store.insert_link(
                import_id=import_id,
                workbook_name="sample.xlsx",
                sheet_name="Links",
                row_number=3,
                from_label="Unknown Person",
                to_label="Unknown Campaign",
                link_type="signatory",
                description="Unmatched link",
                raw_row=["Unknown Person", "Unknown Campaign", "signatory"],
            )
            store.insert_link(
                import_id=import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name="doc-1",
                row_number=1,
                from_label="Matched Person",
                to_label="Generated Campaign",
                link_type="affiliate",
                description="Generated link",
                raw_row=["Matched Person", "Generated Campaign", "affiliate"],
            )
            store.insert_entity(
                import_id=import_id,
                workbook_name="sample.xlsx",
                sheet_name="Entities",
                row_number=4,
                label="Matched Campaign",
                entity_type="Campaign",
                description="Imported campaign node",
                raw_row=["Matched Campaign", "Campaign", "Imported campaign node"],
            )

            default_overlay = build_low_confidence_overlay(
                main_data={
                    "nodes": [
                        {
                            "id": "person:1",
                            "label": "Matched Person",
                            "kind": "person",
                            "lane": 4,
                            "aliases": [],
                        }
                    ],
                    "edges": [],
                },
                database_path=database_path,
                run_key="run-1",
            )
            full_overlay = build_low_confidence_overlay(
                main_data={
                    "nodes": [
                        {
                            "id": "person:1",
                            "label": "Matched Person",
                            "kind": "person",
                            "lane": 4,
                            "aliases": [],
                        }
                    ],
                    "edges": [],
                },
                database_path=database_path,
                run_key="run-2",
                include_unmatched=True,
            )
            generated_overlay = build_low_confidence_overlay(
                main_data={
                    "nodes": [
                        {
                            "id": "person:1",
                            "label": "Matched Person",
                            "kind": "person",
                            "lane": 4,
                            "aliases": [],
                        }
                    ],
                    "edges": [],
                },
                database_path=database_path,
                run_key="run-3",
                include_unmatched=True,
                include_generated_links=True,
            )

            self.assertEqual(len(default_overlay["edges"]), 1)
            self.assertEqual(len(full_overlay["edges"]), 2)
            self.assertEqual(len(generated_overlay["edges"]), 3)
            self.assertEqual(default_overlay["summary"]["matched_link_count"], 1)

    def test_enrich_updates_evidence_without_generating_rows_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            settings = _test_settings(root)
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
                link_type="signatory",
                description="Original description",
                raw_row=["Person One", "Campaign A", "signatory"],
            )
            url = "https://example.test/source"
            store.insert_evidence(
                mapping_link_id=link_id,
                ordinal=1,
                evidence_kind="plain_url",
                title="source",
                url=url,
                snippet="Original description",
            )
            doc_key = _document_key(url)
            generated_import_id = store.create_import(Path("mapping_evidence_enrichment"))
            store.insert_entity(
                import_id=generated_import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name=doc_key,
                row_number=1,
                label="Generated Org",
                entity_type="Organisation",
                description="Generated entity",
                raw_row=["Generated Org"],
            )
            store.insert_link(
                import_id=generated_import_id,
                workbook_name=_GENERATED_WORKBOOK_NAME,
                sheet_name=doc_key,
                row_number=1,
                from_label="Generated Person",
                to_label="Generated Org",
                link_type="affiliate",
                description="Generated link",
                raw_row=["Generated Person", "Generated Org", "affiliate"],
            )

            enricher = FakeMappingEvidenceEnricher(
                settings=settings,
                database_path=database_path,
                summary_text="This document is a short, concrete summary of the relationship.",
                entities=[
                    {
                        "name": "Generated Org",
                        "entity_type": "organisation",
                        "organisation_type_hint": "",
                        "description": "Generated entity",
                        "confidence": 0.8,
                    }
                ],
                links=[
                    {
                        "from_name": "Generated Person",
                        "from_type": "person",
                        "from_role_or_title": "",
                        "to_name": "Generated Org",
                        "to_type": "organisation",
                        "link_type": "affiliate",
                        "description": "Generated link",
                        "confidence": 0.8,
                    }
                ],
            )
            summary = enricher.enrich(only_urls=[url])

            self.assertFalse(summary["allow_generated_rows"])
            self.assertEqual(summary["generated_link_count"], 0)
            self.assertEqual(summary["removed_generated_entity_count"], 1)
            self.assertEqual(summary["removed_generated_link_count"], 1)
            with store.managed_connection() as connection:
                generated_link_count = connection.execute(
                    "SELECT COUNT(*) FROM mapping_links WHERE workbook_name = ?",
                    (_GENERATED_WORKBOOK_NAME,),
                ).fetchone()[0]
                evidence_row = connection.execute(
                    "SELECT document_summary FROM mapping_evidence WHERE mapping_link_id = ?",
                    (link_id,),
                ).fetchone()
            self.assertEqual(generated_link_count, 0)
            self.assertEqual(
                evidence_row[0],
                "This document is a short, concrete summary of the relationship.",
            )

    def test_enrich_rejects_generic_document_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            database_path = root / "mapping.sqlite"
            settings = _test_settings(root)
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
                link_type="signatory",
                description="Original description",
                raw_row=["Person One", "Campaign A", "signatory"],
            )
            url = "https://example.test/source"
            store.insert_evidence(
                mapping_link_id=link_id,
                ordinal=1,
                evidence_kind="plain_url",
                title="source",
                url=url,
                snippet="Original description",
            )
            enricher = FakeMappingEvidenceEnricher(
                settings=settings,
                database_path=database_path,
                summary_text="This document chunk provides the navigation menu for the site and its main content sections.",
            )

            enricher.enrich(only_urls=[url])

            with store.managed_connection() as connection:
                evidence_row = connection.execute(
                    "SELECT document_summary FROM mapping_evidence WHERE mapping_link_id = ?",
                    (link_id,),
                ).fetchone()
            self.assertEqual(evidence_row[0], "")

    def test_select_relevant_chunks_prefers_claim_related_text(self) -> None:
        claim_texts = [
            "On 10 February 2016, Richard Haley joined 450+ academics, students and human rights activists in signing an open letter on Prevent."
        ]
        chunks = [
            "News Opinion Sport Culture Lifestyle The Guardian navigation menu and account links.",
            "The open letter argues Prevent is damaging the fabric of trust in society and was signed by academics and activists.",
            "Crossword newsletters podcasts and subscriptions.",
        ]

        selected = _select_relevant_chunks(chunks, claim_texts, max_chunks=1)

        self.assertEqual(len(selected), 1)
        self.assertIn("damaging the fabric of trust", selected[0])

    def test_select_best_summary_prefers_relevant_usable_summary(self) -> None:
        claim_texts = [
            "Open letter urgent call to repeal the Prevent legislation signed by academics and human rights activists."
        ]
        candidate_summaries = [
            "This document chunk is from The Guardian website, displaying its navigation menu and main content sections such as News, Opinion, Sport, Culture, and Lifestyle.",
            "The document is an open letter arguing Prevent is demonising Muslims and damaging trust in society.",
        ]

        selected = _select_best_summary(candidate_summaries, claim_texts)

        self.assertEqual(
            selected,
            "The document is an open letter arguing Prevent is demonising Muslims and damaging trust in society.",
        )

    def test_build_document_context_classifies_signatory_documents(self) -> None:
        context = _build_document_context(
            [
                {
                    "from_label": "Person One",
                    "to_label": "Open letter on Prevent",
                    "link_type": "signatory",
                    "link_description": "Person One signed an open letter on Prevent.",
                    "workbook_name": "sample.xlsx",
                    "sheet_name": "Letters",
                }
            ],
            document_title="Open letter on Prevent",
        )

        self.assertEqual(context.document_kind, "signatory_list")
        self.assertEqual(context.document_label, "Open letter on Prevent")

    def test_select_signatory_chunks_prefers_list_like_content(self) -> None:
        claim_texts = [
            "A statement of solidarity from British Muslim communities signed by named signatories."
        ]
        chunks = [
            "News Opinion Sport Culture Lifestyle navigation account menu.",
            "Signatories\\nDr A Person, Some Foundation\\nImam B Person, City Mosque\\nOrganisation C",
            "Footer privacy cookies newsletters.",
        ]

        selected = _select_signatory_chunks(chunks, claim_texts, max_chunks=1)

        self.assertEqual(len(selected), 1)
        self.assertIn("Signatories", selected[0])

    def test_parse_signatory_payload_and_convert_to_links(self) -> None:
        summary, signatories = _parse_signatory_payload(
            {
                "summary": "A public statement signed by several named signatories.",
                "signatories": [
                    {
                        "signer_name": "Dr Person One",
                        "signer_type": "person",
                        "signer_role_or_title": "Director",
                        "affiliation_name": "Example Foundation",
                        "affiliation_type": "organisation",
                        "affiliation_role_or_type": "Director",
                        "signatory_line": "Dr Person One, Director, Example Foundation",
                        "confidence": 0.9,
                    },
                    {
                        "signer_name": "Example Organisation",
                        "signer_type": "organisation",
                        "signer_role_or_title": "",
                        "affiliation_name": "",
                        "affiliation_type": "",
                        "affiliation_role_or_type": "",
                        "signatory_line": "Example Organisation",
                        "confidence": 0.8,
                    },
                ],
            }
        )
        entities, links = _signatory_payload_to_entities_links(
            signatories=signatories,
            document_label="Statement of Solidarity",
            document_summary=summary,
        )

        self.assertEqual(summary, "A public statement signed by several named signatories.")
        self.assertEqual(len(signatories), 2)
        self.assertTrue(any(entity["name"] == "Dr Person One" for entity in entities))
        self.assertTrue(any(entity["name"] == "Example Foundation" for entity in entities))
        self.assertTrue(
            any(
                link["from_name"] == "Dr Person One"
                and link["to_name"] == "Statement of Solidarity"
                and link["link_type"] == "signatory"
                for link in links
            )
        )
        self.assertTrue(
            any(
                link["from_name"] == "Dr Person One"
                and link["to_name"] == "Example Foundation"
                and link["link_type"] == "affiliate"
                and "Director" in link["description"]
                for link in links
            )
        )
        self.assertTrue(
            any(
                link["from_name"] == "Example Organisation"
                and link["to_name"] == "Statement of Solidarity"
                and link["link_type"] == "signatory"
                for link in links
            )
        )
        self.assertTrue(
            all(link.get("evidence_snippet") for link in links)
        )

if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from types import SimpleNamespace

from scripts.consolidate_and_graph import _is_notice_org_mention
from src.charity_commission.search import search_name_to_organisation
from src.services.pdf_enrichment import _is_notice_boilerplate_entity


class FakeCharityClient:
    def search_charities_by_name(self, charity_name: str):
        return [
            {
                "charity_name": "JESUS CHRIST THE CROWN OF LIFE MINISTRIES",
                "reg_charity_number": 1162171,
                "group_subsid_suffix": 0,
                "organisation_number": 123,
                "reg_status": "R",
            }
        ]


class CrownBonaVacantiaFiltersTest(unittest.TestCase):
    def test_charity_search_rejects_loose_crown_match(self) -> None:
        result = search_name_to_organisation(FakeCharityClient(), "The Crown")
        self.assertIsNone(result)

    def test_pdf_entity_filter_rejects_bona_vacantia_crown_entity(self) -> None:
        entity = SimpleNamespace(
            name="The Crown",
            role_label="",
            connection_phrase="will receive assets of",
            notes="Upon the Company's dissolution, all property and rights are deemed to be bona vacantia and will belong to the Crown.",
        )
        self.assertTrue(_is_notice_boilerplate_entity(entity))

    def test_graph_filter_rejects_stored_crown_org_mentions(self) -> None:
        metadata = {
            "entity_name": "The Crown",
            "connection_phrase": "will receive assets of",
            "connection_detail": "Upon the Company's dissolution, all property and rights are deemed to be bona vacantia and will belong to the Crown.",
        }
        self.assertTrue(_is_notice_org_mention("pdf_org_mention", metadata))


if __name__ == "__main__":
    unittest.main()

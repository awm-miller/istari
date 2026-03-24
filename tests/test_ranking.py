from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.models import OrganisationRecord
from src.ranking import rank_people
from src.storage.repository import Repository


class RankingTests(unittest.TestCase):
    def test_people_rank_by_distinct_organisations_then_role_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repository = Repository(
                database_path=root / "test.sqlite",
                schema_path=Path(__file__).resolve().parents[1] / "src" / "storage" / "schema.sql",
            )
            repository.init_db()

            org_a = repository.upsert_organisation(
                OrganisationRecord(
                    registry_type="charity",
                    registry_number="1001",
                    name="Alpha Trust",
                )
            )
            org_b = repository.upsert_organisation(
                OrganisationRecord(
                    registry_type="company",
                    registry_number="1002",
                    name="Beacon Ltd",
                )
            )
            org_c = repository.upsert_organisation(
                OrganisationRecord(
                    registry_type="charity",
                    registry_number="1003",
                    name="Cedar Trust",
                )
            )

            alex = repository.upsert_person("Alex Smith")
            blair = repository.upsert_person("Blair Jones")

            repository.upsert_role(
                person_id=alex,
                organisation_id=org_a,
                role_type="trustee",
                role_label="trustee",
                source="test",
                confidence_class="verified",
                edge_weight=1.0,
                provenance={},
            )
            repository.upsert_role(
                person_id=alex,
                organisation_id=org_b,
                role_type="trustee",
                role_label="trustee",
                source="test",
                confidence_class="verified_alias",
                edge_weight=0.95,
                provenance={},
            )
            repository.upsert_role(
                person_id=blair,
                organisation_id=org_c,
                role_type="trustee",
                role_label="trustee",
                source="test",
                confidence_class="tentative",
                edge_weight=0.35,
                provenance={},
            )

            ranked = rank_people(repository, limit=10)

            self.assertEqual(ranked[0].canonical_name, "Alex Smith")
            self.assertEqual(ranked[0].organisation_count, 2)
            self.assertGreater(ranked[0].weighted_organisation_score, ranked[1].weighted_organisation_score)
            self.assertEqual(ranked[1].canonical_name, "Blair Jones")
            self.assertEqual(ranked[1].organisation_count, 1)


if __name__ == "__main__":
    unittest.main()

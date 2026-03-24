from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.resolution.features import build_candidate_match


class ResolutionFeatureTests(unittest.TestCase):
    def test_fixture_scores(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "resolution_cases.json"
        cases = json.loads(fixture_path.read_text(encoding="utf-8"))

        for case in cases:
            with self.subTest(case=case["name"]):
                candidate = build_candidate_match(
                    name_variant=case["seed_name"],
                    candidate_name=case["candidate_name"],
                    organisation_name=case["organisation_name"],
                    registry_type=case["registry_type"],
                    registry_number=case["registry_number"],
                    suffix=case["suffix"],
                    source=case["source"],
                    evidence_id=None,
                    raw_payload={
                        "candidate_name": case["candidate_name"],
                        "organisation_name": case["organisation_name"],
                    },
                )
                if "expected_min_score" in case:
                    self.assertGreaterEqual(candidate.score, case["expected_min_score"])
                if "expected_max_score" in case:
                    self.assertLessEqual(candidate.score, case["expected_max_score"])


if __name__ == "__main__":
    unittest.main()

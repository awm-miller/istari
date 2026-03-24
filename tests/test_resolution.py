from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.models import ResolutionDecision
from src.resolution.features import build_candidate_match
from src.services.relation_semantics import apply_weak_name_match_guard


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

    def test_weak_name_match_guard_rejects_different_person(self) -> None:
        candidate = build_candidate_match(
            name_variant="Alex Smith",
            candidate_name="Zaher Khalid Birawi",
            organisation_name="River Trust",
            registry_type="company",
            registry_number="12345678",
            suffix=0,
            source="companies_house_officer_appointments",
            evidence_id=None,
            raw_payload={
                "candidate_name": "Zaher Khalid Birawi",
                "organisation_name": "River Trust",
            },
        )
        decision = ResolutionDecision(
            status="maybe_match",
            confidence=0.6,
            canonical_name="Zaher Khalid Birawi",
            explanation="Borderline match",
            rule_score=candidate.score,
        )

        guarded = apply_weak_name_match_guard(
            seed_name="Alex Smith",
            candidate=candidate,
            decision=decision,
        )

        self.assertEqual(guarded.status, "no_match")
        self.assertLessEqual(guarded.confidence, 0.2)

    def test_weak_name_match_guard_keeps_exact_name(self) -> None:
        candidate = build_candidate_match(
            name_variant="Alex Smith",
            candidate_name="Alex Smith",
            organisation_name="River Trust",
            registry_type="company",
            registry_number="12345678",
            suffix=0,
            source="companies_house_officer_appointments",
            evidence_id=None,
            raw_payload={
                "candidate_name": "Alex Smith",
                "organisation_name": "River Trust",
            },
        )
        decision = ResolutionDecision(
            status="maybe_match",
            confidence=0.6,
            canonical_name="Alex Smith",
            explanation="Borderline match",
            rule_score=candidate.score,
        )

        guarded = apply_weak_name_match_guard(
            seed_name="Alex Smith",
            candidate=candidate,
            decision=decision,
        )

        self.assertEqual(guarded.status, "maybe_match")


if __name__ == "__main__":
    unittest.main()

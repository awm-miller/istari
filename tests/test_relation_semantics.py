from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.models import ResolutionDecision
from src.services.relation_semantics import (
    apply_birth_month_year_guard,
    apply_conflicting_middle_name_guard,
    candidate_matches_known_birth_month_year,
)


class RelationSemanticsTests(unittest.TestCase):
    def test_conflicting_middle_name_guard_rejects_conflicting_ch_name(self) -> None:
        candidate = SimpleNamespace(
            candidate_name="Bilal Mohammed YASIN",
            feature_payload={"name_similarity": 0.82},
            raw_payload={},
        )
        decision = ResolutionDecision(
            status="match",
            confidence=0.95,
            canonical_name="Bilal Mohammed YASIN",
            explanation="LLM accepted the candidate.",
            rule_score=0.82,
        )

        guarded = apply_conflicting_middle_name_guard(
            seed_name="Bilal Khalil Hasan Yasin",
            candidate=candidate,
            decision=decision,
        )

        self.assertEqual(guarded.status, "no_match")
        self.assertLessEqual(guarded.confidence, 0.2)
        self.assertIn("conflicting middle-name", guarded.explanation)

    def test_conflicting_middle_name_guard_allows_missing_middle_name_case(self) -> None:
        candidate = SimpleNamespace(
            candidate_name="Bilal YASIN",
            feature_payload={"name_similarity": 0.9},
            raw_payload={},
        )
        decision = ResolutionDecision(
            status="match",
            confidence=0.95,
            canonical_name="Bilal YASIN",
            explanation="LLM accepted the candidate.",
            rule_score=0.9,
        )

        guarded = apply_conflicting_middle_name_guard(
            seed_name="Bilal Khalil Hasan Yasin",
            candidate=candidate,
            decision=decision,
        )

        self.assertEqual(guarded.status, "match")

    def test_birth_month_year_guard_rejects_conflicting_known_dob(self) -> None:
        candidate = SimpleNamespace(
            candidate_name="Omer Hasem EL-HAMDOON",
            feature_payload={"name_similarity": 0.9},
            raw_payload={"date_of_birth": {"month": 11, "year": 1974}},
        )
        decision = ResolutionDecision(
            status="match",
            confidence=0.95,
            canonical_name="Omer Hasem EL-HAMDOON",
            explanation="LLM accepted the candidate.",
            rule_score=0.9,
        )

        guarded = apply_birth_month_year_guard(
            candidate=candidate,
            decision=decision,
            known_birth_month_years={(10, 1974)},
        )

        self.assertEqual(guarded.status, "no_match")
        self.assertLessEqual(guarded.confidence, 0.2)
        self.assertIn("birth month/year conflicts", guarded.explanation)

    def test_birth_month_year_guard_allows_matching_known_dob(self) -> None:
        candidate = SimpleNamespace(
            candidate_name="Omer Hasem EL-HAMDOON",
            feature_payload={"name_similarity": 0.42},
            raw_payload={"date_of_birth": {"month": 11, "year": 1974}},
        )
        decision = ResolutionDecision(
            status="match",
            confidence=0.95,
            canonical_name="Omer Hasem EL-HAMDOON",
            explanation="LLM accepted the candidate.",
            rule_score=0.42,
        )

        guarded = apply_birth_month_year_guard(
            candidate=candidate,
            decision=decision,
            known_birth_month_years={(11, 1974)},
        )

        self.assertEqual(guarded.status, "match")
        self.assertTrue(
            candidate_matches_known_birth_month_year(
                candidate=candidate,
                known_birth_month_years={(11, 1974)},
            )
        )


if __name__ == "__main__":
    unittest.main()

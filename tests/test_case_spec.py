from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.addresses import NormalizedAddress, addresses_match, normalize_address_label
from src.cases.spec import CaseSpec, load_case_spec, write_case_spec


class CaseSpecTest(unittest.TestCase):
    def test_address_case_uses_explicit_bounded_policy(self) -> None:
        spec = CaseSpec.from_dict(
            {
                "title": "94 Park Ave",
                "recipe": "address-network",
                "inputs": [
                    {"kind": "address", "value": "94 Park Avenue North, London, NW10 1JY"},
                ],
            }
        )

        self.assertEqual("94-park-ave", spec.id)
        self.assertEqual(3, spec.policy.max_rounds)
        self.assertEqual(("address", "company", "charity"), spec.policy.pivot_kinds)
        self.assertEqual(("person",), spec.policy.leaf_kinds)

    def test_people_cannot_become_pivots_yet(self) -> None:
        with self.assertRaisesRegex(ValueError, "pivot_kinds"):
            CaseSpec.from_dict(
                {
                    "title": "Unsafe recursion",
                    "inputs": [{"kind": "person", "value": "Alice Example"}],
                    "policy": {"pivot_kinds": ["person"]},
                }
            )

    def test_yaml_round_trip_preserves_case(self) -> None:
        spec = CaseSpec.from_dict(
            {
                "id": "known-company",
                "title": "Known Company",
                "inputs": [{"kind": "company", "value": "01234567"}],
                "enrichments": {"documents": True},
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "case.yaml"
            write_case_spec(spec, path)
            loaded = load_case_spec(path)

        self.assertEqual(spec, loaded)

    def test_address_match_ignores_country_and_component_boundaries(self) -> None:
        target = normalize_address_label("94 Park Avenue North, London, NW10 1JY")
        registry = NormalizedAddress(
            label="94, Park Avenue North, London, NW10 1JY, United Kingdom",
            normalized_key="94|PARK AVENUE NORTH|LONDON|NW101JY|UNITED KINGDOM",
            postcode="NW10 1JY",
        )
        other = normalize_address_label("74 Park Avenue North, London, NW10 1JY")

        self.assertTrue(addresses_match(target, registry))
        self.assertFalse(addresses_match(target, other))


if __name__ == "__main__":
    unittest.main()

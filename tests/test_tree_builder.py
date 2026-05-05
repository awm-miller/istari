from __future__ import annotations

import unittest

from src.tree_builder import (
    TreeBuildRequest,
    execute_tree_build,
    normalize_tree_build_request,
    parse_org_root_spec,
)


class _FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, TreeBuildRequest]] = []

    def run_name(self, request: TreeBuildRequest) -> dict[str, object]:
        self.calls.append(("name", request))
        return {"mode": "name_seed", "run_id": 1}

    def run_org_rooted(self, request: TreeBuildRequest) -> dict[str, object]:
        self.calls.append(("org", request))
        return {"mode": "org_rooted", "run_id": 2}

    def run_org_chained(self, request: TreeBuildRequest) -> dict[str, object]:
        self.calls.append(("chained", request))
        return {"mode": "org_chained", "run_ids": [3]}


class TreeBuilderTest(unittest.TestCase):
    def test_parse_org_root_spec_accepts_company_and_suffix(self) -> None:
        root = parse_org_root_spec("company:01234567:2")

        self.assertEqual("company", root.registry_type)
        self.assertEqual("01234567", root.registry_number)
        self.assertEqual(2, root.suffix)

    def test_normal_seed_request_requires_seed_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "seed name"):
            normalize_tree_build_request({"mode": "name_seed"})

    def test_normal_seed_request_accepts_multiple_seed_names(self) -> None:
        request = normalize_tree_build_request(
            {
                "mode": "name_seed",
                "seed_names": [" Alice Example ", "Bob Example", "alice example"],
            }
        )

        self.assertEqual("Alice Example", request.seed_name)
        self.assertEqual(("Alice Example", "Bob Example"), request.seed_names)

    def test_org_rooted_request_normalizes_roots(self) -> None:
        request = normalize_tree_build_request(
            {
                "mode": "org_rooted",
                "seed_name": "Known org tree",
                "roots": ["charity:1095626", "charity:1095626:0"],
                "target_names": [" Alice Example ", "alice example"],
            }
        )

        self.assertEqual("org_rooted", request.mode)
        self.assertEqual(1, len(request.roots))
        self.assertEqual(("Alice Example",), request.target_names)

    def test_org_chained_request_requires_seed_and_roots(self) -> None:
        with self.assertRaisesRegex(ValueError, "seed name"):
            normalize_tree_build_request({"mode": "org_chained", "roots": ["charity:1095626"]})
        with self.assertRaisesRegex(ValueError, "organisation root"):
            normalize_tree_build_request({"mode": "org_chained", "seed_names": ["Alice Example"]})

    def test_execute_tree_build_dispatches_by_mode(self) -> None:
        runner = _FakeRunner()
        request = normalize_tree_build_request(
            {
                "mode": "org_chained",
                "seed_names": ["Alice Example"],
                "roots": ["company:01234567"],
            }
        )

        result = execute_tree_build(request, runner)

        self.assertEqual({"mode": "org_chained", "run_ids": [3]}, result)
        self.assertEqual([("chained", request)], runner.calls)


if __name__ == "__main__":
    unittest.main()

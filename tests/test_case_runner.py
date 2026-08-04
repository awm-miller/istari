from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.cases.runner import CaseRunner
from src.cases.spec import CaseSpec
from src.config import load_settings


class CaseRunnerTest(unittest.TestCase):
    def test_runner_uses_isolated_database_and_resumes_completed_case(self) -> None:
        spec = CaseSpec.from_dict(
            {
                "id": "alice-example",
                "title": "Alice Example",
                "inputs": [{"kind": "person", "value": "Alice Example"}],
                "policy": {"max_rounds": 1, "max_entities": 50},
                "enrichments": {"sanctions": False},
            }
        )

        def seed_side_effect(*, repository, seed_name, creativity_level, **_kwargs):
            return {"run_id": repository.create_run(seed_name, creativity_level)}

        with tempfile.TemporaryDirectory() as tmp, patch(
            "src.cases.runner.step1_expand_seed",
            side_effect=seed_side_effect,
        ) as step1, patch(
            "src.cases.runner.step2_expand_connected_organisations",
            return_value={"processed_organisation_count": 0},
        ), patch(
            "src.cases.runner.step3_expand_connected_people",
            return_value={"processed_organisation_count": 0, "inserted_roles": 0, "ranking": []},
        ):
            runner = CaseRunner(workspace=Path(tmp), settings=load_settings())
            first = runner.run(spec)
            second = runner.run(spec)

            case_root = Path(tmp) / spec.id
            self.assertTrue((case_root / "case.db").exists())
            self.assertTrue((case_root / "events.jsonl").exists())
            self.assertTrue((case_root / "artifacts" / spec.id / "versions" / "v1" / "graph-data.json").exists())

        self.assertEqual("completed", first["status"])
        self.assertEqual(first["artifact"]["version"], second["artifact"]["version"])
        self.assertEqual(1, step1.call_count)

    def test_runner_refuses_concurrent_case_process(self) -> None:
        spec = CaseSpec.from_dict(
            {
                "id": "locked-case",
                "title": "Locked Case",
                "inputs": [{"kind": "person", "value": "Alice Example"}],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            case_root = Path(tmp) / spec.id
            case_root.mkdir(parents=True)
            (case_root / "case.lock").write_text(str(os.getpid()), encoding="ascii")

            with self.assertRaisesRegex(RuntimeError, "already running"):
                CaseRunner(workspace=Path(tmp), settings=load_settings()).run(spec)


if __name__ == "__main__":
    unittest.main()

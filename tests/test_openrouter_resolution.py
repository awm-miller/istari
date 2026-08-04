from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config import load_settings
from src.models import CandidateMatch, ResolutionDecision
from src.resolution.matcher import HybridMatcher
from src.services.pipeline_services import ResolutionService


class OpenRouterResolutionTest(unittest.TestCase):
    def test_hybrid_matcher_uses_latest_deepseek_v4_flash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = replace(
                load_settings(),
                cache_dir=Path(tmp),
                resolution_provider="openrouter",
                openrouter_api_key="test-key",
                openrouter_resolution_model="~deepseek/deepseek-v4-flash-latest",
            )
            matcher = HybridMatcher(settings)
            candidate = CandidateMatch(
                name_variant="Alex Smith",
                candidate_name="Alexander Smith",
                organisation_name="Example Limited",
                registry_type="company",
                registry_number="00000001",
                suffix=0,
                source="test",
                evidence_id=None,
                feature_payload={},
                score=0.7,
                raw_payload={},
            )
            response = {
                "id": "generation-1",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "status": "match",
                                    "confidence": 0.91,
                                    "canonical_name": "Alexander Smith",
                                    "explanation": "Name variant is consistent.",
                                }
                            )
                        }
                    }
                ],
            }
            with patch(
                "src.openrouter_api.OpenRouterChatClient.create_chat_completion",
                return_value=response,
            ) as create_completion:
                decision = matcher.resolve("Alex Smith", candidate)

        self.assertEqual("match", decision.status)
        self.assertEqual(0.91, decision.confidence)
        self.assertEqual(
            "~deepseek/deepseek-v4-flash-latest",
            create_completion.call_args.kwargs["model"],
        )

    def test_resolution_calls_overlap_but_repository_writes_do_not(self) -> None:
        repository = _RepositoryStub()
        matcher = _ConcurrentMatcherStub()
        service = ResolutionService()
        caller_thread = threading.get_ident()

        decisions = service.resolve_candidates(repository=repository, matcher=matcher, run_id=1)

        self.assertEqual(4, len(decisions))
        self.assertGreater(matcher.max_active, 1)
        self.assertEqual({caller_thread}, set(repository.write_threads))


class _ConcurrentMatcherStub:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(resolution_workers=4)
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def resolve(self, _seed_name, candidate) -> ResolutionDecision:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.03)
        with self.lock:
            self.active -= 1
        return ResolutionDecision(
            status="maybe_match",
            confidence=candidate.score,
            canonical_name=candidate.candidate_name,
            explanation="Test decision.",
            rule_score=candidate.score,
            llm_payload={"response_id": candidate.candidate_name},
        )


class _RepositoryStub:
    def __init__(self) -> None:
        self.write_threads: list[int] = []

    def get_run(self, _run_id):
        return {"seed_name": "Alex Smith"}

    def get_unresolved_candidate_matches(self, _run_id):
        return [
            {
                "id": index,
                "variant_name": "Alex Smith",
                "candidate_name": f"Alex Smith {index}",
                "organisation_name": f"Example {index} Limited",
                "registry_type": "company",
                "registry_number": f"{index:08d}",
                "suffix": 0,
                "source": "test",
                "evidence_id": None,
                "score": 0.7,
                "raw_payload_json": "{}",
            }
            for index in range(1, 5)
        ]

    def insert_resolution_decision(self, _run_id, _candidate_id, _decision):
        self.write_threads.append(threading.get_ident())


if __name__ == "__main__":
    unittest.main()

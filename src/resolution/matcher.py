from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.models import CandidateMatch, ResolutionDecision
from src.openai_api import OpenAIResponsesClient, extract_json_document, extract_output_text

log = logging.getLogger("istari.matcher")


def _build_resolution_prompt(seed_name: str, candidate: CandidateMatch) -> str:
    return f"""\
Decide whether the seed name and candidate refer to the same real person in a charity/company context.
Return JSON only with this shape:
{{
  "status": "match" | "maybe_match" | "no_match",
  "confidence": 0.0,
  "canonical_name": "",
  "explanation": ""
}}

Seed name: {seed_name}
Candidate name: {candidate.candidate_name}
Organisation: {candidate.organisation_name}
Source: {candidate.source}
Rule score: {candidate.score}
Features: {candidate.feature_payload}
Raw payload: {candidate.raw_payload}"""


@dataclass(slots=True)
class HybridMatcher:
    settings: Settings
    low_threshold: float = 0.3
    high_threshold: float = 0.92
    _gemini: GeminiClient | None = field(init=False, default=None)
    _openai: OpenAIResponsesClient | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.settings.resolution_provider == "gemini" and self.settings.gemini_api_key:
            self._gemini = GeminiClient(
                api_key=self.settings.gemini_api_key,
                cache_dir=self.settings.cache_dir / "gemini_resolution",
            )
            log.info("Resolution LLM: Gemini (%s)", self.settings.gemini_resolution_model)
        elif self.settings.openai_api_key:
            self._openai = OpenAIResponsesClient(
                api_key=self.settings.openai_api_key,
                base_url=self.settings.openai_base_url,
                cache_dir=self.settings.cache_dir / "openai_resolution",
                user_agent=self.settings.user_agent,
            )
            log.info("Resolution LLM: OpenAI (%s)", self.settings.openai_resolution_model)
        else:
            log.warning("No LLM configured for resolution -- middle-band candidates will be marked maybe_match")

    @property
    def _has_llm(self) -> bool:
        return self._gemini is not None or self._openai is not None

    def resolve(self, seed_name: str, candidate: CandidateMatch) -> ResolutionDecision:
        if candidate.score < self.low_threshold:
            return ResolutionDecision(
                status="no_match",
                confidence=round(1 - candidate.score, 4),
                canonical_name=candidate.candidate_name,
                explanation="Rejected by deterministic threshold.",
                rule_score=candidate.score,
            )

        if candidate.score >= self.high_threshold:
            return ResolutionDecision(
                status="match",
                confidence=candidate.score,
                canonical_name=candidate.candidate_name,
                explanation="Accepted by deterministic threshold.",
                rule_score=candidate.score,
            )

        if not self._has_llm:
            return ResolutionDecision(
                status="maybe_match",
                confidence=candidate.score,
                canonical_name=candidate.candidate_name,
                explanation="No LLM key configured, leaving candidate for manual review.",
                rule_score=candidate.score,
            )

        return self._resolve_with_llm(seed_name, candidate)

    def _resolve_with_llm(
        self,
        seed_name: str,
        candidate: CandidateMatch,
    ) -> ResolutionDecision:
        prompt = _build_resolution_prompt(seed_name, candidate)

        if self._gemini is not None:
            return self._resolve_gemini(prompt, candidate)
        return self._resolve_openai(prompt, seed_name, candidate)

    def _resolve_gemini(
        self, prompt: str, candidate: CandidateMatch
    ) -> ResolutionDecision:
        response = self._gemini.generate(
            model=self.settings.gemini_resolution_model,
            prompt=prompt,
        )
        text = extract_gemini_text(response)
        try:
            document = extract_json_document(text)
        except (ValueError, KeyError):
            document = {}

        return self._decision_from_document(document, candidate, response)

    def _resolve_openai(
        self, prompt: str, seed_name: str, candidate: CandidateMatch
    ) -> ResolutionDecision:
        response = self._openai.create_response(
            model=self.settings.openai_resolution_model,
            input_text=prompt,
            metadata={"task": "entity_resolution", "seed_name": seed_name},
        )
        try:
            document = extract_json_document(extract_output_text(response))
        except (ValueError, KeyError):
            document = {}

        return self._decision_from_document(document, candidate, response)

    @staticmethod
    def _decision_from_document(
        document: dict[str, Any],
        candidate: CandidateMatch,
        response: dict[str, Any],
    ) -> ResolutionDecision:
        return ResolutionDecision(
            status=document.get("status", "maybe_match"),
            confidence=float(document.get("confidence", candidate.score)),
            canonical_name=document.get("canonical_name", candidate.candidate_name),
            explanation=document.get("explanation", "No explanation provided."),
            rule_score=candidate.score,
            llm_payload={"response_id": response.get("id"), "document": document},
        )

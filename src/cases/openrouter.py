from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.cases.spec import CaseSpec
from src.openai_api import extract_json_document
from src.openrouter_api import OpenRouterChatClient, extract_chat_content


DEFAULT_CASE_MODEL = "~deepseek/deepseek-v4-flash-latest"
DEFAULT_REASONING_EFFORT = "medium"
_COMPANY_SUFFIX_RE = re.compile(r"\b(?:LIMITED|LTD|PLC|LLP|LP)\.?$", re.IGNORECASE)

SYSTEM_PROMPT = """Translate a natural-language OSINT request into an executable Istari case specification.
Return JSON only. Never invent or complete registry numbers, names, organisations, or addresses.

How Istari works:
- A person input is a name. Istari searches Companies House and the Charity Commission for organisations linked to that person.
- An address input is the full address supplied by the user. Istari finds exact-address companies and charities.
- A company input must be a Companies House number explicitly present in the request, not a company name.
- A charity input must be a registered charity number explicitly present in the request, not a charity name.
- Starting organisations expand to connected companies and charities for max_rounds rounds.
- People connected to the resulting organisations are added after expansion as leaf nodes. Istari never expands through a person.
- registry-light is for person inputs or explicit company/charity numbers.
- address-network is for requests principally starting from an address.

Schema:
{
  "version": 1,
  "id": "short-lowercase-slug",
  "title": "short human title",
  "recipe": "registry-light or address-network",
  "inputs": [{"kind": "person|company|charity|address", "value": "verbatim input"}],
  "policy": {
    "pivot_kinds": ["address", "company", "charity"],
    "leaf_kinds": ["person"],
    "max_rounds": 1-10,
    "max_entities": 1-10000
  },
  "enrichments": {
    "sanctions": true|false,
    "documents": true|false,
    "negative_news": true|false
  }
}

Rules:
- Include every concrete starting entity stated by the user, but do not treat descriptive words as inputs.
- If a company or charity is named without its registry number, do not mislabel the name as a numbered registry input.
- Interpret "depth N" or "N rounds" as max_rounds N.
- "Do not pivot through people" describes Istari's default and still allows people as leaf nodes.
- "Exclude people" or "organisations only" means leaf_kinds is empty.
- Default to 2 rounds and 500 entities. Use 3 rounds and 750 entities for an address-network request.
- Enable documents or negative news only when explicitly requested.
- Keep sanctions enabled unless explicitly disabled.
- Preserve input spelling exactly in each value.

Examples:
User: Fan out from 94 Park Avenue North, London, NW10 1JY to depth 3, but do not pivot people.
Result: recipe address-network; one address input with the verbatim address; max_rounds 3; max_entities 750; pivot_kinds address, company, charity; leaf_kinds person.

User: Investigate company 01234567 and charity 1095626, organisations only, one round.
Result: recipe registry-light; company input 01234567; charity input 1095626; max_rounds 1; max_entities 500; pivot_kinds company, charity, address; leaf_kinds empty.
"""


@dataclass(slots=True)
class OpenRouterCaseParser:
    api_key: str
    base_url: str = "https://openrouter.ai/api/v1"
    model: str = DEFAULT_CASE_MODEL
    reasoning_effort: str = DEFAULT_REASONING_EFFORT
    timeout_seconds: float = 60.0
    cache_dir: Path | None = None

    def parse(self, query: str, *, case_id: str = "", title: str = "") -> CaseSpec:
        clean_query = " ".join(str(query or "").split()).strip()
        if not clean_query:
            raise ValueError("Natural-language case query cannot be empty.")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY must be configured to parse natural-language cases.")
        response = self._chat(clean_query)
        content = _response_content(response)
        payload = extract_json_document(content)
        if not isinstance(payload, dict):
            raise ValueError("OpenRouter returned a non-object case specification.")
        if case_id:
            payload["id"] = case_id
        if title:
            payload["title"] = title
        spec = CaseSpec.from_dict(payload)
        _validate_input_semantics(spec)
        return spec

    @classmethod
    def from_settings(cls, settings: Any, *, model: str = "") -> "OpenRouterCaseParser":
        return cls(
            api_key=str(getattr(settings, "openrouter_api_key", None) or ""),
            base_url=str(getattr(settings, "openrouter_base_url", "https://openrouter.ai/api/v1")),
            model=model or str(getattr(settings, "openrouter_model", DEFAULT_CASE_MODEL)),
            reasoning_effort=str(
                getattr(settings, "openrouter_case_reasoning_effort", DEFAULT_REASONING_EFFORT)
            ),
            cache_dir=Path(getattr(settings, "cache_dir")) / "openrouter_case_plans",
        )

    def _chat(self, query: str) -> dict[str, Any]:
        client = OpenRouterChatClient(
            api_key=self.api_key,
            base_url=self.base_url,
            user_agent="project-istari/0.1",
            cache_dir=self.cache_dir,
            timeout_seconds=self.timeout_seconds,
        )
        return client.create_chat_completion(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": query},
            ],
            response_format={"type": "json_object"},
            reasoning={"effort": self.reasoning_effort},
            temperature=0.0,
        )


def _response_content(response: dict[str, Any]) -> str:
    return extract_chat_content(response)


def _validate_input_semantics(spec: CaseSpec) -> None:
    misclassified = [
        item.value
        for item in spec.inputs
        if item.kind == "person" and _COMPANY_SUFFIX_RE.search(item.value)
    ]
    if misclassified:
        names = ", ".join(misclassified)
        raise ValueError(f"Company names require a Companies House number before discovery: {names}.")

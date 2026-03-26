from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
import logging
import re
from typing import Any, Mapping

from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.openai_api import OpenAIResponsesClient, extract_json_document, extract_output_text

log = logging.getLogger("istari.address_resolution")

_TOKEN_REPLACEMENTS = {
    "ST": "STREET",
    "ST.": "STREET",
    "RD": "ROAD",
    "RD.": "ROAD",
    "AVE": "AVENUE",
    "AVE.": "AVENUE",
    "LN": "LANE",
    "LN.": "LANE",
    "CT": "COURT",
    "CTR": "CENTRE",
    "CTR.": "CENTRE",
    "PL": "PLACE",
    "PL.": "PLACE",
    "SQ": "SQUARE",
    "SQ.": "SQUARE",
    "APT": "APARTMENT",
    "FL": "FLAT",
}


def address_bucket_keys(entry: Mapping[str, Any]) -> set[str]:
    postcode = _clean_postcode(entry.get("postcode"))
    first_line = first_line_signature(str(entry.get("label") or ""))
    house_number = house_number_signature(str(entry.get("label") or ""))
    keys: set[str] = set()
    if postcode:
        keys.add(f"postcode:{postcode}")
    if first_line:
        keys.add(f"line:{first_line}")
        if postcode:
            keys.add(f"postcode_line:{postcode}:{first_line}")
    if postcode and house_number:
        keys.add(f"postcode_number:{postcode}:{house_number}")
    return keys


def addresses_match(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    matcher: AddressMergeMatcher | None = None,
) -> bool:
    deterministic = _deterministic_match(left, right)
    if deterministic is not None:
        return deterministic
    if matcher is None:
        return False
    return matcher.matches(left, right)


def first_line_signature(label: str) -> str:
    first_line = next((part.strip() for part in str(label).split(",") if part.strip()), "")
    return _canonical_address_text(first_line)


def house_number_signature(label: str) -> str:
    first_line = first_line_signature(label)
    for token in first_line.split():
        if any(ch.isdigit() for ch in token):
            return token
    return ""


def _deterministic_match(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool | None:
    left_key = str(left.get("normalized_key") or "").strip()
    right_key = str(right.get("normalized_key") or "").strip()
    if left_key and right_key and left_key == right_key:
        return True

    left_postcode = _clean_postcode(left.get("postcode"))
    right_postcode = _clean_postcode(right.get("postcode"))
    if left_postcode and right_postcode and left_postcode != right_postcode:
        return False

    left_country = _clean_country(left.get("country"))
    right_country = _clean_country(right.get("country"))
    if left_country and right_country and left_country != right_country:
        return False

    left_first_line = first_line_signature(str(left.get("label") or ""))
    right_first_line = first_line_signature(str(right.get("label") or ""))
    if left_first_line and right_first_line and left_first_line == right_first_line:
        return True

    left_full = _canonical_address_text(str(left.get("label") or ""))
    right_full = _canonical_address_text(str(right.get("label") or ""))
    if left_full and right_full and left_full == right_full:
        return True

    left_number = house_number_signature(str(left.get("label") or ""))
    right_number = house_number_signature(str(right.get("label") or ""))
    if left_number and right_number and left_number != right_number:
        return False

    if not left_first_line or not right_first_line:
        return False

    similarity = SequenceMatcher(None, left_first_line, right_first_line).ratio()
    if similarity < 0.72:
        return False
    return None


def _should_ask_llm(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_postcode = _clean_postcode(left.get("postcode"))
    right_postcode = _clean_postcode(right.get("postcode"))
    if left_postcode and right_postcode and left_postcode != right_postcode:
        return False

    left_country = _clean_country(left.get("country"))
    right_country = _clean_country(right.get("country"))
    if left_country and right_country and left_country != right_country:
        return False

    left_first_line = first_line_signature(str(left.get("label") or ""))
    right_first_line = first_line_signature(str(right.get("label") or ""))
    if not left_first_line or not right_first_line:
        return False

    left_number = house_number_signature(str(left.get("label") or ""))
    right_number = house_number_signature(str(right.get("label") or ""))
    if left_number and right_number and left_number != right_number:
        return False

    return SequenceMatcher(None, left_first_line, right_first_line).ratio() >= 0.72


def _build_prompt(left: Mapping[str, Any], right: Mapping[str, Any]) -> str:
    return f"""\
Decide whether these two records refer to the same real-world postal address.
Be strict: only return true when they are clearly the same physical place.
Return JSON only with this shape:
{{
  "same_address": true,
  "confidence": 0.0,
  "canonical_label": "",
  "explanation": ""
}}

Left address:
- label: {str(left.get("label") or "")}
- normalized_key: {str(left.get("normalized_key") or "")}
- postcode: {str(left.get("postcode") or "")}
- country: {str(left.get("country") or "")}

Right address:
- label: {str(right.get("label") or "")}
- normalized_key: {str(right.get("normalized_key") or "")}
- postcode: {str(right.get("postcode") or "")}
- country: {str(right.get("country") or "")}
"""


def _clean_postcode(value: Any) -> str:
    text = str(value or "").upper().strip()
    compact = re.sub(r"[^A-Z0-9]", "", text)
    if len(compact) > 3:
        return f"{compact[:-3]} {compact[-3:]}"
    return compact


def _clean_country(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").upper()).strip(" ,")


def _canonical_address_text(value: str) -> str:
    text = re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()
    tokens = []
    for token in text.split():
        tokens.append(_TOKEN_REPLACEMENTS.get(token, token))
    return " ".join(tokens)


@dataclass(slots=True)
class AddressMergeMatcher:
    settings: Settings
    _gemini: GeminiClient | None = field(init=False, default=None)
    _openai: OpenAIResponsesClient | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.settings.resolution_provider == "gemini" and self.settings.gemini_api_key:
            self._gemini = GeminiClient(
                api_key=self.settings.gemini_api_key,
                cache_dir=self.settings.cache_dir / "gemini_address_resolution",
            )
        elif self.settings.openai_api_key:
            self._openai = OpenAIResponsesClient(
                api_key=self.settings.openai_api_key,
                base_url=self.settings.openai_base_url,
                cache_dir=self.settings.cache_dir / "openai_address_resolution",
                user_agent=self.settings.user_agent,
            )

    @property
    def has_llm(self) -> bool:
        return self._gemini is not None or self._openai is not None

    def matches(self, left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
        if not self.has_llm or not _should_ask_llm(left, right):
            return False

        prompt = _build_prompt(left, right)
        try:
            if self._gemini is not None:
                response = self._gemini.generate(
                    model=self.settings.gemini_resolution_model,
                    prompt=prompt,
                )
                document = extract_json_document(extract_gemini_text(response))
            else:
                response = self._openai.create_response(
                    model=self.settings.openai_resolution_model,
                    input_text=prompt,
                    metadata={"task": "address_resolution"},
                )
                document = extract_json_document(extract_output_text(response))
        except Exception as exc:
            log.warning("Address merge LLM call failed: %s", exc)
            return False

        return bool(document.get("same_address"))

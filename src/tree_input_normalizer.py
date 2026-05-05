from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Protocol

from src.gemini_api import extract_gemini_text
from src.openai_api import extract_json_document
from src.search.queries import normalize_name
from src.tree_builder import OrgRootSpec, parse_org_root_spec


class CharitySearchClient(Protocol):
    def search_charities_by_name(self, charity_name: str) -> list[dict[str, Any]]:
        ...


class CompaniesSearchClient(Protocol):
    def search_companies(self, query: str, items_per_page: int = 20) -> dict[str, Any]:
        ...


class GeminiEntityClient(Protocol):
    def generate(self, *, model: str, prompt: str, temperature: float = 0.0) -> dict[str, Any]:
        ...


@dataclass(frozen=True, slots=True)
class ResolvedRoot:
    root: OrgRootSpec
    label: str
    score: float
    source: str


_BULLET_RE = re.compile(r"^\s*(?:[-*•]+|\(?\d{1,3}\)?[.)-]?|[a-zA-Z][.)-])\s+")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_BRACKET_RE = re.compile(r"\[[^\]]*\]|\([^)]{0,80}\)")
_EXPLICIT_ROOT_RE = re.compile(r"\b(charity|company)\s*:\s*([A-Za-z0-9]+)(?:\s*:\s*(\d+))?\b", re.IGNORECASE)
_CHARITY_NUMBER_RE = re.compile(
    r"\b(?:registered\s+)?(?:charity|charity\s+(?:no|number|commission\s+number|registration))\D{0,20}(\d{5,8})(?:\D{0,20}(?:suffix|subsidiary)\D{0,10}(\d+))?",
    re.IGNORECASE,
)
_COMPANY_NUMBER_RE = re.compile(
    r"\b(?:company|companies\s+house|company\s+(?:no|number|registration))\D{0,20}([A-Z]{2}\d{6}|\d{8})\b",
    re.IGNORECASE,
)
_ORG_HINT_RE = re.compile(
    r"\b(foundation|trust|charity|association|centre|center|council|institute|society|mosque|academy|ltd|limited|cic|plc|llp)\b",
    re.IGNORECASE,
)
_PERSON_PREFIX_RE = re.compile(r"^(?:name|person|individual|trustee|director|officer|target|seed)\s*[:\-]\s*", re.IGNORECASE)
_ORG_PREFIX_RE = re.compile(r"^(?:organisation|organization|org|company|charity|entity|root)\s*[:\-]\s*", re.IGNORECASE)
_TRAILING_ID_RE = re.compile(
    r"\b(?:charity|company|companies\s+house|registered|registration|number|no|id|suffix)\b.*$",
    re.IGNORECASE,
)
_ORG_SUFFIX_WORDS = {
    "charity",
    "charitable",
    "cic",
    "company",
    "incorporated",
    "limited",
    "ltd",
    "plc",
    "the",
}


def normalize_builder_payload(
    payload: dict[str, Any],
    *,
    charity_client: CharitySearchClient | None = None,
    companies_house_client: CompaniesSearchClient | None = None,
    gemini_client: GeminiEntityClient | None = None,
    gemini_model: str = "",
) -> dict[str, Any]:
    """Return a cleaned builder payload while preserving unrelated options."""
    normalized = dict(payload)
    mode = str(normalized.get("mode") or "").strip().lower()

    if mode == "name_seed":
        seed_names = _clean_people(
            normalized.get("seed_names") or [],
            gemini_client=gemini_client,
            gemini_model=gemini_model,
        )
        seed_name = _first_clean_person(
            normalized.get("seed_name"),
            gemini_client=gemini_client,
            gemini_model=gemini_model,
        )
        if seed_name:
            seed_names = _dedupe([seed_name, *seed_names])
        normalized["seed_name"] = seed_names[0] if seed_names else ""
        normalized["seed_names"] = seed_names
        normalized["roots"] = []
        normalized["target_names"] = []
        return normalized

    if mode == "org_chained":
        normalized["seed_names"] = _clean_people(
            normalized.get("seed_names") or [normalized.get("seed_name")],
            gemini_client=gemini_client,
            gemini_model=gemini_model,
        )
    else:
        normalized["seed_names"] = []

    if mode == "org_rooted":
        normalized["target_names"] = _clean_people(
            normalized.get("target_names") or [],
            gemini_client=gemini_client,
            gemini_model=gemini_model,
        )
    else:
        normalized["target_names"] = []

    if mode in {"org_rooted", "org_chained"}:
        normalized["roots"] = [
            _root_to_payload(root)
            for root in resolve_organisation_roots(
                normalized.get("roots") or [],
                charity_client=charity_client,
                companies_house_client=companies_house_client,
                gemini_client=gemini_client,
                gemini_model=gemini_model,
            )
        ]
    else:
        normalized["roots"] = []

    normalized["seed_name"] = _clean_person_row(normalized.get("seed_name"))
    return normalized


def resolve_organisation_roots(
    rows: Any,
    *,
    charity_client: CharitySearchClient | None = None,
    companies_house_client: CompaniesSearchClient | None = None,
    gemini_client: GeminiEntityClient | None = None,
    gemini_model: str = "",
) -> list[ResolvedRoot]:
    resolved: list[ResolvedRoot] = []
    unresolved: list[str] = []

    for raw_row in _as_values(rows):
        root = _explicit_root(raw_row)
        if root:
            resolved.append(ResolvedRoot(root=root, label=_clean_row(raw_row), score=1.0, source="explicit"))
            continue

        label = _clean_organisation_label(raw_row)
        if not label:
            unresolved.append(str(raw_row))
            continue

        match = _resolve_organisation_label(
            label,
            charity_client=charity_client,
            companies_house_client=companies_house_client,
        )
        if match:
            resolved.append(match)
            continue
        unresolved.append(str(raw_row))

    if unresolved and gemini_client is not None:
        extracted = _ai_extract_entities(
            unresolved,
            entity_key="organisations",
            gemini_client=gemini_client,
            gemini_model=gemini_model,
        )
        still_unresolved: list[str] = []
        for raw_row in unresolved:
            label = extracted.get(raw_row)
            if not label and len(unresolved) == 1 and len(extracted) == 1:
                label = next(iter(extracted.values()))
            if not label:
                still_unresolved.append(raw_row)
                continue
            match = _resolve_organisation_label(
                label,
                charity_client=charity_client,
                companies_house_client=companies_house_client,
            )
            if match:
                resolved.append(match)
            else:
                still_unresolved.append(raw_row)
        unresolved = still_unresolved

    if unresolved:
        rows_text = "; ".join(_clean_row(row) or str(row) for row in unresolved)
        raise ValueError(f"Could not resolve organisation row: {rows_text}")

    deduped: list[ResolvedRoot] = []
    seen: set[tuple[str, str, int]] = set()
    for item in resolved:
        key = (item.root.registry_type, item.root.registry_number, item.root.suffix)
        if key not in seen:
            deduped.append(item)
            seen.add(key)
    return deduped


def _first_clean_person(value: Any, *, gemini_client: GeminiEntityClient | None, gemini_model: str) -> str:
    cleaned = _clean_person_row(value)
    if cleaned:
        return cleaned
    names = _clean_people([value], gemini_client=gemini_client, gemini_model=gemini_model)
    return names[0] if names else ""


def _clean_people(
    rows: Any,
    *,
    gemini_client: GeminiEntityClient | None = None,
    gemini_model: str = "",
) -> list[str]:
    values = _as_values(rows)
    cleaned: list[str] = []
    unresolved: list[str] = []
    for value in values:
        person = _clean_person_row(value)
        if person:
            cleaned.append(person)
        elif str(value or "").strip():
            unresolved.append(str(value))

    if unresolved and gemini_client is not None:
        extracted = _ai_extract_entities(
            unresolved,
            entity_key="people",
            gemini_client=gemini_client,
            gemini_model=gemini_model,
        )
        cleaned.extend(name for name in extracted.values() if name)

    return _dedupe(cleaned)


def _clean_person_row(value: Any) -> str:
    text = _clean_row(value)
    if not text:
        return ""
    text = _PERSON_PREFIX_RE.sub("", text)
    text = re.split(r"\s+(?:[|–—]|-)\s+|\t", text, maxsplit=1)[0]
    text = _TRAILING_ID_RE.sub("", text).strip(" ,-;:")
    text = re.sub(r"\s{2,}", " ", text).strip()
    normalized = normalize_name(text)
    if len(normalized.split()) < 2:
        return ""
    return text


def _clean_organisation_label(value: Any) -> str:
    text = _clean_row(value)
    if not text:
        return ""
    text = _ORG_PREFIX_RE.sub("", text)
    parts = [part.strip(" ,-;:") for part in re.split(r"\t|\s+(?:[|–—]|-)\s+", text) if part.strip(" ,-;:")]
    hinted = [part for part in parts if _ORG_HINT_RE.search(part)]
    text = hinted[0] if hinted else parts[0] if parts else text
    text = _TRAILING_ID_RE.sub("", text).strip(" ,-;:")
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text if len(_normalise_org_label(text).split()) >= 1 else ""


def _clean_row(value: Any) -> str:
    text = " ".join(str(value or "").replace("\r", "\n").split())
    text = _URL_RE.sub(" ", text)
    text = _BULLET_RE.sub("", text)
    text = _BRACKET_RE.sub(" ", text)
    text = text.replace("“", "\"").replace("”", "\"").replace("’", "'")
    return re.sub(r"\s{2,}", " ", text).strip(" ,-;:")


def _explicit_root(value: Any) -> OrgRootSpec | None:
    if isinstance(value, dict):
        registry_type = str(value.get("registry_type") or "").strip().lower()
        registry_number = str(value.get("registry_number") or "").strip()
        if registry_type and registry_number:
            return OrgRootSpec(registry_type=registry_type, registry_number=registry_number, suffix=int(value.get("suffix") or 0))
        return None

    text = _clean_row(value)
    if not text:
        return None
    try:
        root = parse_org_root_spec(text)
        if _is_valid_root_number(root):
            return root
    except ValueError:
        pass

    explicit_match = _EXPLICIT_ROOT_RE.search(text)
    if explicit_match:
        root = OrgRootSpec(
            registry_type=explicit_match.group(1).lower(),
            registry_number=explicit_match.group(2).upper() if explicit_match.group(1).lower() == "company" else explicit_match.group(2),
            suffix=int(explicit_match.group(3) or 0),
        )
        if _is_valid_root_number(root):
            return root

    charity_match = _CHARITY_NUMBER_RE.search(text)
    if charity_match:
        return OrgRootSpec("charity", charity_match.group(1), int(charity_match.group(2) or 0))

    company_match = _COMPANY_NUMBER_RE.search(text)
    if company_match:
        return OrgRootSpec("company", company_match.group(1).upper(), 0)
    return None


def _is_valid_root_number(root: OrgRootSpec) -> bool:
    if root.registry_type == "charity":
        return bool(re.fullmatch(r"\d{5,8}", root.registry_number))
    if root.registry_type == "company":
        return bool(re.fullmatch(r"(?:[A-Z]{2}\d{6}|\d{8})", root.registry_number.upper()))
    return False


def _resolve_organisation_label(
    label: str,
    *,
    charity_client: CharitySearchClient | None,
    companies_house_client: CompaniesSearchClient | None,
) -> ResolvedRoot | None:
    candidates: list[ResolvedRoot] = []
    if charity_client is not None:
        try:
            for item in charity_client.search_charities_by_name(label)[:8]:
                candidate = _charity_candidate(label, item)
                if candidate:
                    candidates.append(candidate)
        except Exception:
            pass

    if companies_house_client is not None:
        try:
            payload = companies_house_client.search_companies(label, items_per_page=8)
            items = payload.get("items") if isinstance(payload, dict) else []
            for item in items or []:
                candidate = _company_candidate(label, item)
                if candidate:
                    candidates.append(candidate)
        except Exception:
            pass

    if not candidates:
        return None
    candidates.sort(key=lambda item: item.score, reverse=True)
    top = candidates[0]
    second_score = candidates[1].score if len(candidates) > 1 else 0.0
    if top.score >= 0.96 or (top.score >= 0.86 and top.score - second_score >= 0.04):
        return top
    return None


def _charity_candidate(query: str, item: dict[str, Any]) -> ResolvedRoot | None:
    name = _first_value(item, "charity_name", "CharityName", "name", "title")
    number = _first_value(item, "reg_charity_number", "registered_charity_number", "charity_number", "RegisteredCharityNumber")
    suffix_text = _first_value(item, "group_subsid_suffix", "suffix", "GroupSubsidSuffix") or "0"
    if not name or not number:
        return None
    try:
        suffix = int(str(suffix_text or "0").strip() or "0")
    except ValueError:
        suffix = 0
    return ResolvedRoot(
        root=OrgRootSpec("charity", str(number).strip(), suffix),
        label=name,
        score=_org_similarity(query, name),
        source="charity_commission",
    )


def _company_candidate(query: str, item: dict[str, Any]) -> ResolvedRoot | None:
    name = _first_value(item, "title", "company_name", "name")
    number = _first_value(item, "company_number")
    if not name or not number:
        return None
    return ResolvedRoot(
        root=OrgRootSpec("company", str(number).strip().upper(), 0),
        label=name,
        score=_org_similarity(query, name),
        source="companies_house",
    )


def _org_similarity(query: str, candidate: str) -> float:
    left = _normalise_org_label(query)
    right = _normalise_org_label(candidate)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    token_score = len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)
    ratio = SequenceMatcher(None, left, right).ratio()
    return max(token_score, ratio)


def _normalise_org_label(value: str) -> str:
    normalized = normalize_name(value)
    tokens = [token for token in normalized.split() if token not in _ORG_SUFFIX_WORDS]
    return " ".join(tokens)


def _ai_extract_entities(
    rows: list[str],
    *,
    entity_key: str,
    gemini_client: GeminiEntityClient,
    gemini_model: str,
) -> dict[str, str]:
    if not rows:
        return {}
    prompt = f"""Extract {entity_key} from these pasted report rows.
Return JSON only with this exact shape:
{{"items":[{{"row":"original row","value":"extracted value or empty string"}}]}}

Rows:
{chr(10).join(f"- {row}" for row in rows)}
"""
    response = gemini_client.generate(model=gemini_model, prompt=prompt, temperature=0.0)
    document = extract_json_document(extract_gemini_text(response))
    items = document.get("items") if isinstance(document, dict) else []
    extracted: dict[str, str] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        row = str(item.get("row") or "").strip()
        value = _clean_row(item.get("value"))
        if row and value:
            extracted[row] = value
    return extracted


def _root_to_payload(item: ResolvedRoot) -> str:
    root = item.root
    if root.registry_type == "charity" and root.suffix:
        return f"charity:{root.registry_number}:{root.suffix}"
    return f"{root.registry_type}:{root.registry_number}"


def _as_values(values: Any) -> list[Any]:
    if values is None:
        return []
    if isinstance(values, str):
        return [line for line in values.splitlines() if line.strip()]
    if isinstance(values, (list, tuple, set)):
        return list(values)
    return [values]


def _first_value(item: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if value and key not in seen:
            out.append(value)
            seen.add(key)
    return out

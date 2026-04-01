from __future__ import annotations

import hashlib
import html
import json
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib import error, request

from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.mapping_low_confidence import MappingStore, normalize_mapping_label
from src.openai_api import extract_json_document
from src.services.pdf_enrichment import (
    PdfEnrichmentService,
    _has_meaningful_text,
    _ocr_pdf,
)

_GENERATED_WORKBOOK_NAME = "__evidence_enrichment__"
_HTML_SKIP_RE = re.compile(
    r"<(script|style|noscript|svg)[^>]*>.*?</\1>",
    flags=re.IGNORECASE | re.DOTALL,
)
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)
_WHITESPACE_RE = re.compile(r"\s+")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_MAX_SOURCE_CHARS = 48000
_FOCUS_KEYWORDS = (
    "we, the undersigned",
    "signatures",
    "signatories",
    "signed by",
    "affiliate",
    "affiliates",
    "partner",
    "partners",
    "partnered",
    "member organisations",
    "members",
    "supporters",
    "endorsed by",
)
_BAD_SUMMARY_PHRASES = (
    "this document chunk",
    "provided text chunk",
    "website navigation",
    "navigation menu",
    "navigation links",
    "main content sections",
    "branding for",
    "consists primarily of navigation",
    "without any substantive content",
    "boilerplate",
    "online gambling platform",
    "slot gacor",
    "togel asia",
)
_SIGNATORY_CONTEXT_KEYWORDS = (
    "open letter",
    "joint statement",
    "statement of solidarity",
    "signatory",
    "signatories",
    "signed by",
    "we, the undersigned",
    "undersigned",
    "petition",
    "statement",
)
_SIGNATORY_CHUNK_KEYWORDS = (
    "signatories",
    "signed by",
    "undersigned",
    "statement of solidarity",
    "open letter",
    "joint statement",
)
_SIGNATORY_ROLE_TOKENS = (
    "prof",
    "professor",
    "dr",
    "imam",
    "maulana",
    "chair",
    "director",
    "trustee",
    "secretary",
    "president",
    "ceo",
    "chief executive",
    "head imam",
    "vice president",
    "member",
)
_BAD_SIGNATORY_SUMMARY_PHRASES = (
    "does not contain a list",
    "does not contain the list",
    "introductory part",
    "does not contain a signatory list",
    "does not contain signatories",
)
_RELEVANCE_STOPWORDS = {
    "about",
    "after",
    "against",
    "among",
    "and",
    "article",
    "because",
    "before",
    "being",
    "between",
    "calling",
    "campaign",
    "document",
    "during",
    "from",
    "have",
    "http",
    "https",
    "into",
    "joined",
    "letter",
    "open",
    "published",
    "section",
    "signed",
    "signing",
    "students",
    "that",
    "their",
    "them",
    "they",
    "this",
    "through",
    "with",
    "were",
}


@dataclass(slots=True)
class MappingEvidenceDocument:
    url: str
    title: str
    source_type: str
    local_path: str = ""
    text_path: str = ""
    text: str = ""


@dataclass(slots=True)
class MappingDocumentContext:
    claim_texts: list[str]
    source_labels: list[str]
    target_labels: list[str]
    link_types: list[str]
    workbook_names: list[str]
    sheet_names: list[str]
    document_label: str
    document_kind: str


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        value = str(data or "").strip()
        if value:
            self.parts.append(value)

    def get_text(self) -> str:
        return "\n".join(self.parts)


def _clean_text(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _document_key(url: str) -> str:
    return hashlib.sha256(str(url).encode("utf-8")).hexdigest()[:16]


def _cache_path(base_dir: Path, key: str, suffix: str) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{key}{suffix}"


def _looks_like_pdf(url: str) -> bool:
    return bool(re.search(r"\.pdf($|[?#])", str(url or ""), flags=re.IGNORECASE))


def _safe_file_stem(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return cleaned or "document"


def _normalize_link_type(value: str) -> str:
    text = _clean_text(value).lower()
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_") or "document_link"


def _sanitize_text(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", _CONTROL_CHAR_RE.sub(" ", str(value or ""))).strip()


def _sanitize_block_text(value: str) -> str:
    text = _CONTROL_CHAR_RE.sub(" ", str(value or ""))
    lines = [
        re.sub(r"[ \t]+", " ", line).strip()
        for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    compacted: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line
        if is_blank:
            if not previous_blank:
                compacted.append("")
            previous_blank = True
            continue
        compacted.append(line)
        previous_blank = False
    return "\n".join(compacted).strip()


def _is_usable_document_summary(value: str) -> bool:
    text = _clean_text(value)
    if not text:
        return False
    lowered = text.lower()
    return not any(phrase in lowered for phrase in _BAD_SUMMARY_PHRASES)


def _claim_terms(claim_texts: list[str], *, limit: int = 24) -> list[str]:
    counts: dict[str, int] = {}
    for claim_text in claim_texts:
        normalized = re.sub(r"[^a-z0-9]+", " ", _clean_text(claim_text).lower())
        for token in normalized.split():
            if len(token) < 4 or token in _RELEVANCE_STOPWORDS:
                continue
            counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
    return [token for token, _ in ranked[:limit]]


def _relevance_score(text: str, claim_texts: list[str]) -> int:
    normalized = re.sub(r"[^a-z0-9]+", " ", _clean_text(text).lower())
    if not normalized:
        return 0
    score = 0
    for token in _claim_terms(claim_texts):
        if token in normalized:
            score += 1 + normalized.count(token)
    for phrase in _FOCUS_KEYWORDS:
        if phrase in normalized:
            score += 2
    return score


def _select_relevant_chunks(
    chunks: list[str],
    claim_texts: list[str],
    *,
    max_chunks: int,
) -> list[str]:
    if not chunks:
        return []
    capped_max_chunks = max(1, max_chunks)
    if not claim_texts:
        return chunks[:capped_max_chunks]
    scored = [
        (index, _relevance_score(chunk, claim_texts), chunk)
        for index, chunk in enumerate(chunks)
    ]
    scored.sort(key=lambda item: (-item[1], item[0]))
    selected = scored[:capped_max_chunks]
    if not any(score > 0 for _, score, _ in selected):
        return chunks[:capped_max_chunks]
    return [chunk for _, _, chunk in selected]


def _select_best_summary(candidate_summaries: list[str], claim_texts: list[str]) -> str:
    usable = [summary for summary in candidate_summaries if _is_usable_document_summary(summary)]
    if not usable:
        return ""
    ranked = sorted(
        usable,
        key=lambda summary: (_relevance_score(summary, claim_texts), len(summary)),
        reverse=True,
    )
    return ranked[0]


def _most_common_text(values: list[str]) -> str:
    cleaned = [_clean_text(value) for value in values if _clean_text(value)]
    if not cleaned:
        return ""
    return Counter(cleaned).most_common(1)[0][0]


def _classify_document_kind(
    *,
    claim_texts: list[str],
    target_labels: list[str],
    link_types: list[str],
    document_title: str,
) -> str:
    link_type_values = {_clean_text(value).lower() for value in link_types if _clean_text(value)}
    combined = " ".join(
        [
            *claim_texts,
            *target_labels,
            document_title,
            " ".join(sorted(link_type_values)),
        ]
    ).lower()
    signatory_hits = 0
    if "signatory" in link_type_values:
        signatory_hits += 3
    signatory_hits += sum(1 for token in _SIGNATORY_CONTEXT_KEYWORDS if token in combined)
    return "signatory_list" if signatory_hits >= 2 else "summary_only"


def _build_document_context(
    matching_rows: list[Any],
    *,
    document_title: str,
) -> MappingDocumentContext:
    claim_texts = [
        _clean_text(row["link_description"] or "")
        for row in matching_rows
        if _clean_text(row["link_description"] or "")
    ]
    source_labels = [
        _clean_text(row["from_label"] or "")
        for row in matching_rows
        if _clean_text(row["from_label"] or "")
    ]
    target_labels = [
        _clean_text(row["to_label"] or "")
        for row in matching_rows
        if _clean_text(row["to_label"] or "")
    ]
    link_types = [
        _clean_text(row["link_type"] or "")
        for row in matching_rows
        if _clean_text(row["link_type"] or "")
    ]
    workbook_names = [
        _clean_text(row["workbook_name"] or "")
        for row in matching_rows
        if _clean_text(row["workbook_name"] or "")
    ]
    sheet_names = [
        _clean_text(row["sheet_name"] or "")
        for row in matching_rows
        if _clean_text(row["sheet_name"] or "")
    ]
    document_label = _most_common_text(target_labels) or _clean_text(document_title) or "Evidence document"
    document_kind = _classify_document_kind(
        claim_texts=claim_texts,
        target_labels=target_labels,
        link_types=link_types,
        document_title=document_title,
    )
    return MappingDocumentContext(
        claim_texts=claim_texts,
        source_labels=source_labels,
        target_labels=target_labels,
        link_types=link_types,
        workbook_names=workbook_names,
        sheet_names=sheet_names,
        document_label=document_label,
        document_kind=document_kind,
    )


def _chunk_source_text(text: str, *, max_chars: int = 12000) -> list[str]:
    cleaned = _sanitize_block_text(text)
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]

    blocks = [block.strip() for block in re.split(r"\n\s*\n", cleaned) if block.strip()]
    if len(blocks) <= 1:
        blocks = [line.strip() for line in cleaned.splitlines() if line.strip()]

    chunks: list[str] = []
    current = ""
    for block in blocks:
        if len(block) > max_chars:
            words = block.split()
            piece = ""
            for word in words:
                candidate = f"{piece} {word}".strip()
                if piece and len(candidate) > max_chars:
                    chunks.append(piece)
                    piece = word
                else:
                    piece = candidate
            if piece:
                if current:
                    chunks.append(current)
                    current = ""
                chunks.append(piece)
            continue

        candidate = f"{current}\n\n{block}".strip() if current else block
        if current and len(candidate) > max_chars:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _signatory_chunk_score(text: str, claim_texts: list[str]) -> int:
    lowered = str(text or "").lower()
    score = _relevance_score(text, claim_texts)
    score += sum(3 for token in _SIGNATORY_CHUNK_KEYWORDS if token in lowered)
    score += min(lowered.count("\n"), 20)
    score += sum(1 for token in _SIGNATORY_ROLE_TOKENS if token in lowered)
    if re.search(r"\b[a-z][a-z.'’-]+\s+[a-z][a-z.'’-]+\b", lowered):
        score += 2
    return score


def _select_signatory_chunks(
    chunks: list[str],
    claim_texts: list[str],
    *,
    max_chunks: int,
) -> list[str]:
    if not chunks:
        return []
    capped_max_chunks = max(1, max_chunks)
    scored = [
        (index, _signatory_chunk_score(chunk, claim_texts), chunk)
        for index, chunk in enumerate(chunks)
    ]
    scored.sort(key=lambda item: (-item[1], item[0]))
    selected = scored[:capped_max_chunks]
    if not any(score > 0 for _, score, _ in selected):
        return chunks[:capped_max_chunks]
    return [chunk for _, _, chunk in selected]


def _focus_source_text(text: str, *, max_chars: int = 18000) -> str:
    cleaned = _sanitize_block_text(text)
    if len(cleaned) <= max_chars:
        return cleaned
    lowered = cleaned.lower()
    prefix = cleaned[:2000].strip()
    for keyword in _FOCUS_KEYWORDS:
        index = lowered.find(keyword)
        if index < 0:
            continue
        start = max(0, index - 1200)
        end = min(len(cleaned), index + max_chars - 2500)
        focused = cleaned[start:end].strip()
        if start > 2500 and prefix:
            return f"{prefix}\n\n...\n\n{focused}"[:max_chars].strip()
        return focused[:max_chars].strip()
    return cleaned[:max_chars].strip()


def _extract_title_from_html(html_text: str, fallback: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return fallback
    return _clean_text(html.unescape(re.sub(r"<[^>]+>", " ", match.group(1))))


def _html_to_text(html_text: str) -> str:
    without_comments = _HTML_COMMENT_RE.sub(" ", html_text)
    without_skipped = _HTML_SKIP_RE.sub(" ", without_comments)
    parser = _TextExtractor()
    parser.feed(without_skipped)
    text = parser.get_text()
    return _sanitize_block_text(html.unescape(text))


def _build_extraction_prompt(
    *,
    document_title: str,
    document_url: str,
    chunk_text: str,
    claim_context: str = "",
) -> str:
    return f"""\
Read this evidence document chunk and return JSON only with this shape:
{{
  "summary": "",
  "entities": [
    {{
      "name": "",
      "entity_type": "person" | "organisation" | "other",
      "organisation_type_hint": "",
      "description": "",
      "confidence": 0.0
    }}
  ],
  "links": [
    {{
      "from_name": "",
      "from_type": "person" | "organisation" | "other",
      "from_role_or_title": "",
      "to_name": "",
      "to_type": "person" | "organisation" | "other",
      "link_type": "",
      "description": "",
      "confidence": 0.0
    }}
  ]
}}

Rules:
- Extract only entities and links that the document explicitly states.
- Include both people and organisations.
- For organisations, set organisation_type_hint when the text indicates a type such as university, union, mosque, think tank, charity, company, school, council, campaign, party, or foundation.
- In signature or signatory lists, if a person's name is immediately followed by an organisation or institution on the same line, treat that as an explicit affiliation link.
- If a title or role is present near the person name, include it in from_role_or_title, e.g. Professor, Dr, Imam, MP, Director, Trustee.
- Prefer person -> organisation links such as signatory, trustee, director, spokesperson, member, employee, affiliate, supporter, advisor, auditor, or representative.
- Include organisation -> organisation links when the document clearly states sponsor, partner, parent, subsidiary, affiliate, member_of, coalition_with, hosted_by, or funded_by style relationships.
- Return only items explicitly named in this chunk.
- Do not return more than 20 entities or 20 links for a single chunk.
- Keep the summary to 1-2 short sentences about what the document says, not a paste of the document.
- Ignore page chrome, menus, comments widgets, donation prompts, and other site boilerplate.
- Prefer precision over recall.
- Use the existing claim context below to decide what is relevant. If this chunk does not help validate or explain those claims, return an empty summary and no links.
- Ignore unrelated bylines, photo credits, publishers, and article metadata unless they are directly part of the existing claims.

Document title: {document_title}
Document URL: {document_url}
Existing claim context:
{claim_context or "(none provided)"}

Text:
{chunk_text}"""


def _build_signatory_extraction_prompt(
    *,
    document_title: str,
    document_url: str,
    document_label: str,
    chunk_text: str,
    claim_context: str = "",
) -> str:
    return f"""\
Read this document chunk and return JSON only with this shape:
{{
  "summary": "",
  "signatories": [
    {{
      "signer_name": "",
      "signer_type": "person" | "organisation",
      "signer_role_or_title": "",
      "affiliation_name": "",
      "affiliation_type": "organisation" | "other" | "",
      "affiliation_role_or_type": "",
      "signatory_line": "",
      "confidence": 0.0
    }}
  ]
}}

Rules:
- This mode is only for signatory lists, letters, statements, petitions, or similar documents.
- Extract only explicit signatories named in this chunk.
- Include both person signatories and organisation signatories.
- Always treat the signer as signing `{document_label}`.
- If a signatory line explicitly includes an affiliation or organisation on the same line, include it.
- If a title or role is present next to the signer, include it in `signer_role_or_title`.
- Keep `signatory_line` as a short near-verbatim snippet from the chunk that justifies the extraction.
- Do not extract bylines, publishers, photo credits, quoted authors, article metadata, institutional hierarchies, or unrelated relationships.
- Do not infer affiliations from surrounding prose; only include same-line or clearly attached signatory-list details.
- Keep the summary to 1-2 short sentences about the document and the signatory list.
- If this chunk does not contain signatory-list content relevant to the claim context, return an empty summary and an empty signatories list.

Document title: {document_title}
Document label: {document_label}
Document URL: {document_url}
Existing claim context:
{claim_context or "(none provided)"}

Text:
{chunk_text}"""


def _parse_extraction_payload(payload: Any) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        return ("", [], [])
    summary = _clean_text(payload.get("summary", ""))
    entities: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    for row in payload.get("entities", []) if isinstance(payload.get("entities"), list) else []:
        if not isinstance(row, dict):
            continue
        name = _clean_text(row.get("name", ""))
        entity_type = _clean_text(row.get("entity_type", "")).lower()
        organisation_type_hint = _clean_text(row.get("organisation_type_hint", "")).lower()
        description = _clean_text(row.get("description", ""))
        try:
            confidence = float(row.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if not name or entity_type not in {"person", "organisation", "other"}:
            continue
        entities.append(
            {
                "name": name,
                "entity_type": entity_type,
                "organisation_type_hint": organisation_type_hint,
                "description": description,
                "confidence": max(0.0, min(confidence, 1.0)),
            }
        )
    for row in payload.get("links", []) if isinstance(payload.get("links"), list) else []:
        if not isinstance(row, dict):
            continue
        from_name = _clean_text(row.get("from_name", ""))
        to_name = _clean_text(row.get("to_name", ""))
        from_type = _clean_text(row.get("from_type", "")).lower()
        from_role_or_title = _clean_text(row.get("from_role_or_title", ""))
        to_type = _clean_text(row.get("to_type", "")).lower()
        link_type = _clean_text(row.get("link_type", ""))
        description = _clean_text(row.get("description", ""))
        try:
            confidence = float(row.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if not from_name or not to_name:
            continue
        if from_type not in {"person", "organisation", "other"}:
            from_type = "other"
        if to_type not in {"person", "organisation", "other"}:
            to_type = "other"
        links.append(
            {
                "from_name": from_name,
                "from_type": from_type,
                "from_role_or_title": from_role_or_title,
                "to_name": to_name,
                "to_type": to_type,
                "link_type": link_type or "document_link",
                "description": description,
                "confidence": max(0.0, min(confidence, 1.0)),
            }
        )
    return (summary, entities, links)


def _parse_signatory_payload(payload: Any) -> tuple[str, list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        return ("", [])
    summary = _clean_text(payload.get("summary", ""))
    signatories: list[dict[str, Any]] = []
    raw_signatories = payload.get("signatories", [])
    if not isinstance(raw_signatories, list):
        return (summary, [])
    for row in raw_signatories:
        if not isinstance(row, dict):
            continue
        signer_name = _clean_text(row.get("signer_name", ""))
        signer_type = _clean_text(row.get("signer_type", "")).lower()
        signer_role_or_title = _clean_text(row.get("signer_role_or_title", ""))
        affiliation_name = _clean_text(row.get("affiliation_name", ""))
        affiliation_type = _clean_text(row.get("affiliation_type", "")).lower()
        affiliation_role_or_type = _clean_text(row.get("affiliation_role_or_type", ""))
        signatory_line = _clean_text(row.get("signatory_line", ""))
        try:
            confidence = float(row.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if not signer_name or signer_type not in {"person", "organisation"}:
            continue
        if affiliation_type not in {"organisation", "other", ""}:
            affiliation_type = ""
        signatories.append(
            {
                "signer_name": signer_name,
                "signer_type": signer_type,
                "signer_role_or_title": signer_role_or_title,
                "affiliation_name": affiliation_name,
                "affiliation_type": affiliation_type,
                "affiliation_role_or_type": affiliation_role_or_type,
                "signatory_line": signatory_line,
                "confidence": max(0.0, min(confidence, 1.0)),
            }
        )
    return (summary, signatories)


def _signatory_description(
    *,
    signer_name: str,
    document_label: str,
    signer_role_or_title: str = "",
) -> str:
    if signer_role_or_title:
        return f"{signer_name} is listed as a signatory to {document_label} ({signer_role_or_title})."
    return f"{signer_name} is listed as a signatory to {document_label}."


def _affiliation_description(
    *,
    signer_name: str,
    affiliation_name: str,
    document_label: str,
    affiliation_role_or_type: str = "",
) -> str:
    if affiliation_role_or_type:
        return (
            f"{signer_name} is listed with {affiliation_name} as {affiliation_role_or_type} "
            f"in the signatory list for {document_label}."
        )
    return f"{signer_name} is listed with {affiliation_name} in the signatory list for {document_label}."


def _signatory_payload_to_entities_links(
    *,
    signatories: list[dict[str, Any]],
    document_label: str,
    document_summary: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    entities: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    seen_entities: set[tuple[str, str]] = set()

    for signatory in signatories:
        signer_name = str(signatory["signer_name"])
        signer_type = str(signatory["signer_type"])
        signer_role_or_title = str(signatory.get("signer_role_or_title", "") or "")
        affiliation_name = str(signatory.get("affiliation_name", "") or "")
        affiliation_type = str(signatory.get("affiliation_type", "") or "")
        affiliation_role_or_type = str(signatory.get("affiliation_role_or_type", "") or "")
        signatory_line = str(signatory.get("signatory_line", "") or "")
        confidence = float(signatory.get("confidence", 0.0) or 0.0)

        signer_entity_key = (signer_name.lower(), signer_type)
        if signer_entity_key not in seen_entities:
            seen_entities.add(signer_entity_key)
            signer_description = _signatory_description(
                signer_name=signer_name,
                document_label=document_label,
                signer_role_or_title=signer_role_or_title,
            )
            entities.append(
                {
                    "name": signer_name,
                    "entity_type": signer_type,
                    "organisation_type_hint": "",
                    "description": signer_description,
                    "confidence": confidence,
                }
            )

        links.append(
            {
                "from_name": signer_name,
                "from_type": signer_type,
                "from_role_or_title": signer_role_or_title,
                "to_name": document_label,
                "to_type": "organisation",
                "link_type": "signatory",
                "description": _signatory_description(
                    signer_name=signer_name,
                    document_label=document_label,
                    signer_role_or_title=signer_role_or_title,
                ),
                "evidence_snippet": signatory_line or document_summary,
                "confidence": confidence,
            }
        )

        if affiliation_name and affiliation_type == "organisation":
            affiliation_entity_key = (affiliation_name.lower(), "organisation")
            if affiliation_entity_key not in seen_entities:
                seen_entities.add(affiliation_entity_key)
                entities.append(
                    {
                        "name": affiliation_name,
                        "entity_type": "organisation",
                        "organisation_type_hint": "",
                        "description": _affiliation_description(
                            signer_name=signer_name,
                            affiliation_name=affiliation_name,
                            document_label=document_label,
                            affiliation_role_or_type=affiliation_role_or_type,
                        ),
                        "confidence": confidence,
                    }
                )
            links.append(
                {
                    "from_name": signer_name,
                    "from_type": signer_type,
                    "from_role_or_title": signer_role_or_title,
                    "to_name": affiliation_name,
                    "to_type": "organisation",
                    "link_type": "affiliate",
                    "description": _affiliation_description(
                        signer_name=signer_name,
                        affiliation_name=affiliation_name,
                        document_label=document_label,
                        affiliation_role_or_type=affiliation_role_or_type,
                    ),
                    "evidence_snippet": signatory_line or document_summary,
                    "confidence": confidence,
                }
            )

    return (entities, links)


def _fallback_signatory_summary(document_label: str) -> str:
    label = _clean_text(document_label) or "this document"
    return (
        f"This document contains a signatory list for {label}. "
        "Extracted links represent explicit signatories and same-line listed affiliations."
    )


def _is_usable_signatory_summary(value: str) -> bool:
    text = _clean_text(value)
    if not text:
        return False
    lowered = text.lower()
    if any(phrase in lowered for phrase in _BAD_SIGNATORY_SUMMARY_PHRASES):
        return False
    return True


def _entity_type_label(entity: dict[str, Any]) -> str:
    entity_type = _clean_text(entity.get("entity_type", "")).lower()
    if entity_type == "organisation":
        return _clean_text(entity.get("organisation_type_hint", "")) or "organisation"
    return entity_type or "other"


def _display_link_type(link: dict[str, Any]) -> str:
    base = _clean_text(link.get("link_type", "")) or "document_link"
    role = _clean_text(link.get("from_role_or_title", ""))
    if not role:
        return base
    if role.lower() in base.lower():
        return base
    return f"{base} ({role})"


class MappingEvidenceEnricher:
    def __init__(self, *, settings: Settings, database_path: Path) -> None:
        self.settings = settings
        self.database_path = Path(database_path)
        self.store = MappingStore(self.database_path)
        self.store.init_db()
        self.cache_dir = self.settings.cache_dir / "mapping_evidence"
        self.source_dir = self.cache_dir / "source"
        self.text_dir = self.cache_dir / "text"
        self.response_dir = self.cache_dir / "responses"
        if not self.settings.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is required for mapping evidence enrichment.")
        self.gemini = GeminiClient(
            api_key=self.settings.gemini_api_key,
            cache_dir=self.cache_dir / "gemini",
        )

    def enrich(
        self,
        *,
        limit: int | None = None,
        only_urls: list[str] | None = None,
        allow_generated_rows: bool = False,
    ) -> dict[str, Any]:
        with self.store.managed_connection() as connection:
            evidence_rows = connection.execute(
                """
                SELECT
                    mapping_evidence.id,
                    mapping_evidence.mapping_link_id,
                    mapping_evidence.ordinal,
                    mapping_evidence.title,
                    mapping_evidence.url,
                    mapping_links.from_label,
                    mapping_links.to_label,
                    mapping_links.link_type,
                    mapping_links.description AS link_description,
                    mapping_links.workbook_name,
                    mapping_links.sheet_name
                FROM mapping_evidence
                JOIN mapping_links
                    ON mapping_links.id = mapping_evidence.mapping_link_id
                WHERE trim(mapping_evidence.url) <> ''
                ORDER BY mapping_evidence.mapping_link_id, mapping_evidence.ordinal
                """
            ).fetchall()
        urls = []
        seen_urls: set[str] = set()
        allowed_urls = {str(value).strip() for value in (only_urls or []) if str(value).strip()}
        for row in evidence_rows:
            url = str(row["url"] or "").strip()
            if not url or url in seen_urls:
                continue
            if allowed_urls and url not in allowed_urls:
                continue
            seen_urls.add(url)
            urls.append(url)
        if limit is not None and limit >= 0:
            urls = urls[:limit]

        import_id = (
            self.store.create_import(Path("mapping_evidence_enrichment"))
            if allow_generated_rows
            else None
        )
        summary = {
            "document_count": 0,
            "generated_entity_count": 0,
            "generated_link_count": 0,
            "removed_generated_entity_count": 0,
            "removed_generated_link_count": 0,
            "updated_evidence_count": 0,
            "selected_url_count": len(urls),
            "processed_urls": [],
            "warnings": [],
            "allow_generated_rows": allow_generated_rows,
        }

        for url in urls:
            matching_rows = [row for row in evidence_rows if str(row["url"] or "").strip() == url]
            if not matching_rows:
                continue
            try:
                document = self._prepare_document(
                    url=url,
                    title=_clean_text(matching_rows[0]["title"] or "Evidence document"),
                )
                document_context = _build_document_context(
                    matching_rows,
                    document_title=document.title,
                )
                doc_summary, entities, links = self._extract_document(
                    document,
                    context=document_context,
                )
                doc_key = _document_key(url)
                self._update_evidence_summary(url=url, document_summary=doc_summary)
                if allow_generated_rows:
                    self._replace_generated_rows(
                        import_id=int(import_id),
                        doc_key=doc_key,
                        document=document,
                        document_summary=doc_summary,
                        entities=entities,
                        links=links,
                    )
                    summary["generated_entity_count"] += len(entities)
                    summary["generated_link_count"] += len(links)
                else:
                    removed_counts = self._clear_generated_rows(doc_key)
                    summary["removed_generated_entity_count"] += removed_counts["entity_count"]
                    summary["removed_generated_link_count"] += removed_counts["link_count"]
                summary["document_count"] += 1
                summary["updated_evidence_count"] += len(matching_rows)
                summary["processed_urls"].append(url)
            except RuntimeError as exc:
                summary["warnings"].append(f"{url}: {exc}")
        return summary

    def rebuild_graph(self) -> None:
        rebuild_script = self.settings.project_root / "scripts" / "rebuild_graph.py"
        subprocess.run(
            [sys.executable, str(rebuild_script)],
            check=True,
            cwd=self.settings.project_root,
        )

    def _prepare_document(self, *, url: str, title: str) -> MappingEvidenceDocument:
        key = _document_key(url)
        if _looks_like_pdf(url):
            pdf_path = self._download_binary(url=url, key=key, suffix=".pdf")
            text_path, text = self._pdf_to_text(pdf_path)
            return MappingEvidenceDocument(
                url=url,
                title=title,
                source_type="pdf",
                local_path=str(pdf_path),
                text_path=str(text_path),
                text=text[:_MAX_SOURCE_CHARS],
            )
        html_path = self._download_text(url=url, key=key, suffix=".html")
        html_text = html_path.read_text(encoding="utf-8", errors="replace")
        text = _html_to_text(html_text)[:_MAX_SOURCE_CHARS]
        text_path = _cache_path(self.text_dir, key, ".txt")
        text_path.write_text(text, encoding="utf-8")
        return MappingEvidenceDocument(
            url=url,
            title=_extract_title_from_html(html_text, title),
            source_type="html",
            local_path=str(html_path),
            text_path=str(text_path),
            text=text,
        )

    def _download_text(self, *, url: str, key: str, suffix: str) -> Path:
        cache_path = _cache_path(self.source_dir, key, suffix)
        if cache_path.exists():
            return cache_path
        req = request.Request(url, headers={"User-Agent": self.settings.user_agent}, method="GET")
        try:
            with request.urlopen(req, timeout=60) as response:
                body = response.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            raise RuntimeError(f"document fetch failed: {exc.code}") from exc
        except Exception as exc:
            raise RuntimeError(f"document fetch failed: {exc}") from exc
        cache_path.write_text(body, encoding="utf-8")
        return cache_path

    def _download_binary(self, *, url: str, key: str, suffix: str) -> Path:
        cache_path = _cache_path(self.source_dir, key, suffix)
        if cache_path.exists():
            return cache_path
        req = request.Request(url, headers={"User-Agent": self.settings.user_agent}, method="GET")
        try:
            with request.urlopen(req, timeout=60) as response:
                cache_path.write_bytes(response.read())
        except error.HTTPError as exc:
            raise RuntimeError(f"document download failed: {exc.code}") from exc
        except Exception as exc:
            raise RuntimeError(f"document download failed: {exc}") from exc
        return cache_path

    def _pdf_to_text(self, pdf_path: Path) -> tuple[Path, str]:
        text_path = _cache_path(self.text_dir, pdf_path.stem, ".md")
        if text_path.exists():
            return text_path, text_path.read_text(encoding="utf-8", errors="replace")
        text = PdfEnrichmentService._try_opendataloader(pdf_path)
        if not _has_meaningful_text(text):
            text = _ocr_pdf(pdf_path)
        text = _sanitize_block_text(text)
        text_path.write_text(text, encoding="utf-8")
        return text_path, text

    def _extract_document(
        self,
        document: MappingEvidenceDocument,
        *,
        context: MappingDocumentContext | None = None,
    ) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        context = context or MappingDocumentContext(
            claim_texts=[],
            source_labels=[],
            target_labels=[],
            link_types=[],
            workbook_names=[],
            sheet_names=[],
            document_label=_clean_text(document.title) or "Evidence document",
            document_kind="summary_only",
        )
        claim_texts = [str(value).strip() for value in (context.claim_texts or []) if str(value).strip()]
        base_text = (
            _sanitize_block_text(document.text)
            if claim_texts
            else _focus_source_text(document.text, max_chars=18000)
        )
        chunks = _chunk_source_text(base_text, max_chars=1200)
        if not chunks:
            raise RuntimeError("document text was empty after extraction")
        max_chunks = max(1, self.settings.pdf_enrichment_max_chunks)
        if context.document_kind == "signatory_list":
            selected_chunks = _select_signatory_chunks(chunks, claim_texts, max_chunks=max_chunks)
        else:
            selected_chunks = _select_relevant_chunks(chunks, claim_texts, max_chunks=max_chunks)
        candidate_summaries: list[str] = []
        entity_map: dict[tuple[str, str], dict[str, Any]] = {}
        link_map: dict[tuple[str, str, str], dict[str, Any]] = {}
        claim_context = "\n".join(f"- {text}" for text in claim_texts[:5])
        for index, chunk in enumerate(selected_chunks, start=1):
            if context.document_kind == "signatory_list":
                prompt = _build_signatory_extraction_prompt(
                    document_title=document.title,
                    document_url=document.url,
                    document_label=context.document_label,
                    chunk_text=chunk,
                    claim_context=claim_context,
                )
            else:
                prompt = _build_extraction_prompt(
                    document_title=document.title,
                    document_url=document.url,
                    chunk_text=chunk,
                    claim_context=claim_context,
                )
            response = self.gemini.generate(
                model=self.settings.pdf_enrichment_model,
                prompt=prompt,
                temperature=0.1,
            )
            response_text = extract_gemini_text(response)
            response_path = _cache_path(
                self.response_dir,
                hashlib.sha256(f"{document.url}:{index}".encode("utf-8")).hexdigest(),
                ".json",
            )
            response_path.write_text(
                json.dumps(
                    {
                        "document_url": document.url,
                        "chunk_index": index,
                        "response": response,
                        "response_text": response_text,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            try:
                parsed = extract_json_document(response_text)
            except (ValueError, KeyError) as exc:
                raise RuntimeError(f"Gemini returned invalid JSON for {document.url}: {exc}") from exc
            if context.document_kind == "signatory_list":
                chunk_summary, signatories = _parse_signatory_payload(parsed)
                entities, links = _signatory_payload_to_entities_links(
                    signatories=signatories,
                    document_label=context.document_label,
                    document_summary=chunk_summary,
                )
            else:
                chunk_summary, entities, links = _parse_extraction_payload(parsed)
            if chunk_summary:
                candidate_summaries.append(chunk_summary)
            if claim_texts:
                links = [
                    link
                    for link in links
                    if _relevance_score(
                        " ".join(
                            [
                                str(link.get("from_name", "")),
                                str(link.get("to_name", "")),
                                str(link.get("link_type", "")),
                                str(link.get("description", "")),
                            ]
                        ),
                        claim_texts,
                    )
                    > 0
                ]
            for entity in entities:
                key = (entity["name"].lower(), entity["entity_type"])
                existing = entity_map.get(key)
                if existing is None or float(entity["confidence"]) > float(existing["confidence"]):
                    entity_map[key] = entity
            for link in links:
                key = (
                    normalize_mapping_label(link["from_name"]),
                    normalize_mapping_label(link["to_name"]),
                    _normalize_link_type(link["link_type"]),
                )
                existing = link_map.get(key)
                if existing is None or float(link["confidence"]) > float(existing["confidence"]):
                    link_map[key] = link
        merged_summary = _select_best_summary(candidate_summaries, claim_texts)
        if context.document_kind == "signatory_list" and not _is_usable_signatory_summary(merged_summary):
            merged_summary = _fallback_signatory_summary(context.document_label)
        return (merged_summary, list(entity_map.values()), list(link_map.values()))

    def _update_evidence_summary(self, *, url: str, document_summary: str) -> None:
        cleaned_summary = _clean_text(document_summary)
        if not _is_usable_document_summary(cleaned_summary):
            cleaned_summary = ""
        with self.store.managed_connection() as connection:
            connection.execute(
                """
                UPDATE mapping_evidence
                SET document_summary = ?
                WHERE url = ?
                """,
                (cleaned_summary, url),
            )

    def _clear_generated_rows(self, doc_key: str) -> dict[str, int]:
        workbook_name = _GENERATED_WORKBOOK_NAME
        sheet_name = doc_key
        with self.store.managed_connection() as connection:
            generated_link_ids = [
                int(row["id"])
                for row in connection.execute(
                    """
                    SELECT id
                    FROM mapping_links
                    WHERE workbook_name = ? AND sheet_name = ?
                    """,
                    (workbook_name, sheet_name),
                ).fetchall()
            ]
            generated_entity_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM mapping_entities
                    WHERE workbook_name = ? AND sheet_name = ?
                    """,
                    (workbook_name, sheet_name),
                ).fetchone()[0]
            )
            if generated_link_ids:
                placeholders = ",".join("?" for _ in generated_link_ids)
                connection.execute(
                    f"DELETE FROM mapping_matches WHERE mapping_link_id IN ({placeholders})",
                    generated_link_ids,
                )
                connection.execute(
                    f"DELETE FROM mapping_evidence WHERE mapping_link_id IN ({placeholders})",
                    generated_link_ids,
                )
            generated_link_count = len(generated_link_ids)
            connection.execute(
                "DELETE FROM mapping_links WHERE workbook_name = ? AND sheet_name = ?",
                (workbook_name, sheet_name),
            )
            connection.execute(
                "DELETE FROM mapping_entities WHERE workbook_name = ? AND sheet_name = ?",
                (workbook_name, sheet_name),
            )
        return {
            "entity_count": generated_entity_count,
            "link_count": generated_link_count,
        }

    def _replace_generated_rows(
        self,
        *,
        import_id: int,
        doc_key: str,
        document: MappingEvidenceDocument,
        document_summary: str,
        entities: list[dict[str, Any]],
        links: list[dict[str, Any]],
    ) -> None:
        workbook_name = _GENERATED_WORKBOOK_NAME
        sheet_name = doc_key
        self._clear_generated_rows(doc_key)

        entity_names_in_links: set[str] = set()
        for link in links:
            entity_names_in_links.add(link["from_name"])
            entity_names_in_links.add(link["to_name"])

        ordered_entities = sorted(
            entities,
            key=lambda item: (item["entity_type"], item["name"].lower(), item["description"].lower()),
        )
        entity_rows = [
            entity
            for entity in ordered_entities
            if entity["name"] in entity_names_in_links
        ]
        seen_entity_labels: set[tuple[str, str]] = set()
        next_entity_row = 1
        for entity in entity_rows:
            entity_key = (entity["name"].lower(), entity["entity_type"])
            if entity_key in seen_entity_labels:
                continue
            seen_entity_labels.add(entity_key)
            self.store.insert_entity(
                import_id=import_id,
                workbook_name=workbook_name,
                sheet_name=sheet_name,
                row_number=next_entity_row,
                label=entity["name"],
                entity_type=_entity_type_label(entity),
                description=entity["description"] or document_summary,
                raw_row=[
                    entity["name"],
                    entity["entity_type"],
                    entity.get("organisation_type_hint", ""),
                    entity["description"],
                    document.url,
                ],
            )
            next_entity_row += 1

        seen_links: set[tuple[str, str, str]] = set()
        next_link_row = 1
        for link in sorted(
            links,
            key=lambda item: (
                item["from_name"].lower(),
                item["to_name"].lower(),
                _normalize_link_type(item["link_type"]),
            ),
        ):
            if link["from_type"] == "other" or link["to_type"] == "other":
                continue
            semantic_key = (
                normalize_mapping_label(link["from_name"]),
                normalize_mapping_label(link["to_name"]),
                _normalize_link_type(link["link_type"]),
            )
            if semantic_key in seen_links:
                continue
            seen_links.add(semantic_key)
            link_id = self.store.insert_link(
                import_id=import_id,
                workbook_name=workbook_name,
                sheet_name=sheet_name,
                row_number=next_link_row,
                from_label=link["from_name"],
                to_label=link["to_name"],
                link_type=_display_link_type(link),
                description=link["description"] or document_summary,
                raw_row=[
                    link["from_name"],
                    link["from_type"],
                    link.get("from_role_or_title", ""),
                    link["to_name"],
                    link["to_type"],
                    link["link_type"],
                    link["description"],
                    link.get("evidence_snippet", ""),
                    document.url,
                ],
            )
            self.store.insert_evidence(
                mapping_link_id=link_id,
                ordinal=1,
                evidence_kind=document.source_type,
                title=document.title or _safe_file_stem(doc_key),
                url=document.url,
                snippet=str(link.get("evidence_snippet") or link["description"] or document_summary),
                document_summary=document_summary,
            )
            next_link_row += 1


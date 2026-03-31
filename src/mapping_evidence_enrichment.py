from __future__ import annotations

import hashlib
import html
import json
import re
import subprocess
import sys
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


@dataclass(slots=True)
class MappingEvidenceDocument:
    url: str
    title: str
    source_type: str
    local_path: str = ""
    text_path: str = ""
    text: str = ""


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

Document title: {document_title}
Document URL: {document_url}

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
    ) -> dict[str, Any]:
        with self.store.connect() as connection:
            evidence_rows = connection.execute(
                """
                SELECT
                    mapping_evidence.id,
                    mapping_evidence.mapping_link_id,
                    mapping_evidence.ordinal,
                    mapping_evidence.title,
                    mapping_evidence.url,
                    mapping_links.description AS link_description
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

        import_id = self.store.create_import(Path("mapping_evidence_enrichment"))
        summary = {
            "document_count": 0,
            "generated_entity_count": 0,
            "generated_link_count": 0,
            "updated_evidence_count": 0,
            "selected_url_count": len(urls),
            "processed_urls": [],
            "warnings": [],
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
                doc_summary, entities, links = self._extract_document(document)
                doc_key = _document_key(url)
                self._update_evidence_summary(url=url, document_summary=doc_summary)
                self._replace_generated_rows(
                    import_id=import_id,
                    doc_key=doc_key,
                    document=document,
                    document_summary=doc_summary,
                    entities=entities,
                    links=links,
                )
                summary["document_count"] += 1
                summary["updated_evidence_count"] += len(matching_rows)
                summary["generated_entity_count"] += len(entities)
                summary["generated_link_count"] += len(links)
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
    ) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
        focused_text = _focus_source_text(document.text, max_chars=18000)
        chunks = _chunk_source_text(focused_text, max_chars=1200)
        if not chunks:
            raise RuntimeError("document text was empty after extraction")
        merged_summary = ""
        entity_map: dict[tuple[str, str], dict[str, Any]] = {}
        link_map: dict[tuple[str, str, str], dict[str, Any]] = {}
        max_chunks = max(self.settings.pdf_enrichment_max_chunks, 12)
        for index, chunk in enumerate(chunks[:max_chunks], start=1):
            prompt = _build_extraction_prompt(
                document_title=document.title,
                document_url=document.url,
                chunk_text=chunk,
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
            chunk_summary, entities, links = _parse_extraction_payload(parsed)
            if chunk_summary and not merged_summary:
                merged_summary = chunk_summary
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
        return (merged_summary, list(entity_map.values()), list(link_map.values()))

    def _update_evidence_summary(self, *, url: str, document_summary: str) -> None:
        with self.store.connect() as connection:
            connection.execute(
                """
                UPDATE mapping_evidence
                SET document_summary = ?
                WHERE url = ?
                """,
                (_clean_text(document_summary), url),
            )

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
        with self.store.connect() as connection:
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
            connection.execute(
                "DELETE FROM mapping_links WHERE workbook_name = ? AND sheet_name = ?",
                (workbook_name, sheet_name),
            )
            connection.execute(
                "DELETE FROM mapping_entities WHERE workbook_name = ? AND sheet_name = ?",
                (workbook_name, sheet_name),
            )

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
                    document.url,
                ],
            )
            self.store.insert_evidence(
                mapping_link_id=link_id,
                ordinal=1,
                evidence_kind=document.source_type,
                title=document.title or _safe_file_stem(doc_key),
                url=document.url,
                snippet=link["description"] or document_summary,
                document_summary=document_summary,
            )
            next_link_row += 1


from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib import error, request

from src.charity_commission.search import search_name_to_organisation
from src.companies_house.client import CompaniesHouseClient
from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.models import EvidenceItem, OrganisationRecord, PdfExtractedEntity, PdfSourceDocument
from src.openai_api import extract_json_document
from src.search.provider import WebDorkSearchProvider
from src.search.queries import normalize_name

log = logging.getLogger("istari.pdf_enrichment")

_ALLOWED_ROLE_CATEGORIES = {
    "person",
    "organisation",
    "accountant_or_auditor",
    "other_professional",
    "ignore",
}
_ALLOWED_ENTITY_TYPES = {"person", "organisation", "other"}
_MAX_CHARS_PER_CHUNK = 12000


def _clean_text(value: str) -> str:
    return " ".join(str(value).split()).strip()


def _normalize_for_match(value: str) -> str:
    return normalize_name(_clean_text(value))


def _json_cache_path(base_dir: Path, cache_key: str, suffix: str) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{cache_key}{suffix}"


def chunk_markdown(markdown_text: str, *, max_chars: int = _MAX_CHARS_PER_CHUNK) -> list[str]:
    cleaned = markdown_text.strip()
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]

    paragraphs = re.split(r"\n\s*\n", cleaned)
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        candidate = f"{current}\n\n{paragraph}".strip() if current else paragraph
        if current and len(candidate) > max_chars:
            chunks.append(current)
            current = paragraph
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def parse_pdf_entities_document(
    document: Any,
    *,
    organisation_name: str,
    source_document_url: str,
) -> list[PdfExtractedEntity]:
    if not isinstance(document, dict):
        return []
    rows = document.get("entities", [])
    if not isinstance(rows, list):
        return []

    entities: list[PdfExtractedEntity] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _clean_text(row.get("name", ""))
        if not name:
            continue
        entity_type = _clean_text(row.get("entity_type", "other")).lower()
        if entity_type not in _ALLOWED_ENTITY_TYPES:
            entity_type = "other"
        role_category = _clean_text(row.get("role_category", "ignore")).lower()
        if role_category not in _ALLOWED_ROLE_CATEGORIES:
            role_category = "ignore"
        role_label = _clean_text(row.get("role_label", ""))
        source_page_hint = _clean_text(row.get("source_page_hint", ""))
        registry_hint = _clean_text(row.get("registry_hint", ""))
        notes = _clean_text(row.get("notes", ""))
        try:
            confidence = float(row.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0

        entities.append(
            PdfExtractedEntity(
                name=name,
                entity_type=entity_type,
                role_category=role_category,
                role_label=role_label,
                organisation_name=_clean_text(row.get("organisation_name") or organisation_name),
                source_document_url=source_document_url,
                source_page_hint=source_page_hint,
                confidence=max(0.0, min(confidence, 1.0)),
                registry_hint=registry_hint,
                notes=notes,
            )
        )
    return entities


def role_mapping_for_entity(entity: PdfExtractedEntity) -> tuple[str, str, str, str, float] | None:
    label = entity.role_label or entity.role_category or entity.entity_type
    if entity.role_category == "ignore":
        return None
    if entity.role_category == "organisation":
        return None
    if entity.role_category == "accountant_or_auditor":
        return ("accountant_or_auditor", label, "professional_to", "is an accountant or auditor for", 0.6)
    if entity.role_category == "other_professional":
        return ("other_professional", label, "professional_to", "is professionally linked to", 0.55)
    return ("pdf_person_mention", label or "pdf_person_mention", "mentioned_for", "is mentioned in documents for", 0.45)


def _build_extraction_prompt(
    *,
    organisation_name: str,
    document_title: str,
    document_url: str,
    markdown_chunk: str,
) -> str:
    return f"""\
Extract named entities from this charity/company PDF markdown.
Return JSON only with this shape:
{{
  "entities": [
    {{
      "name": "",
      "entity_type": "person" | "organisation" | "other",
      "role_category": "person" | "organisation" | "accountant_or_auditor" | "other_professional" | "ignore",
      "role_label": "",
      "organisation_name": "",
      "source_page_hint": "",
      "confidence": 0.0,
      "registry_hint": "",
      "notes": ""
    }}
  ]
}}

Rules:
- Include people named in governance, accounts, annual report, trustees, directors, officers, auditors, accountants, secretaries, advisors, or related roles.
- Use role_category=organisation for named organisations mentioned as counterparties, linked entities, subsidiaries, parent entities, auditors, accountants, or service firms.
- Use role_category=accountant_or_auditor for people or firms acting as auditor, examiner, accountant, or accounting practice.
- Use role_category=ignore for incidental names that are not useful for registry/company pivoting.
- Prefer precision over recall.
- Do not invent registry numbers.
- Keep organisation_name as the scoped organisation unless the text clearly ties the entity to another organisation.

Scoped organisation: {organisation_name}
Document title: {document_title}
Document URL: {document_url}

Markdown:
{markdown_chunk}"""


class PdfOrganisationResolver:
    def __init__(
        self,
        *,
        settings: Settings,
        charity_client: Any,
        companies_house_client: CompaniesHouseClient,
    ) -> None:
        self.settings = settings
        self.charity_client = charity_client
        self.companies_house_client = companies_house_client

    def resolve(self, organisation_name: str) -> OrganisationRecord | None:
        cleaned = _clean_text(organisation_name)
        if not cleaned:
            return None

        company_match = self._resolve_company(cleaned)
        if company_match is not None:
            return company_match

        return search_name_to_organisation(self.charity_client, cleaned)

    def _resolve_company(self, organisation_name: str) -> OrganisationRecord | None:
        if not self.settings.companies_house_api_key:
            return None
        try:
            payload = self.companies_house_client.search_companies(organisation_name, items_per_page=5)
        except RuntimeError:
            return None
        items = payload.get("items", [])
        if not isinstance(items, list):
            return None

        target = _normalize_for_match(organisation_name)
        best: dict[str, Any] | None = None
        best_score = 0.0
        for item in items:
            if not isinstance(item, dict):
                continue
            title = _clean_text(item.get("title", ""))
            if not title:
                continue
            score = 1.0 if _normalize_for_match(title) == target else 0.0
            if not score:
                continue
            if score > best_score:
                best_score = score
                best = item
        if not best:
            return None

        company_number = _clean_text(best.get("company_number", ""))
        if not company_number:
            return None
        return OrganisationRecord(
            registry_type="company",
            registry_number=company_number,
            suffix=0,
            organisation_number=None,
            name=_clean_text(best.get("title") or organisation_name),
            status=_clean_text(best.get("company_status", "")) or None,
            metadata=best,
        )


class PdfEnrichmentService:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: Any,
        charity_client: Any,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.charity_client = charity_client
        self.cache_dir = self.settings.cache_dir / "pdf_enrichment"
        self.pdf_dir = self.cache_dir / "pdfs"
        self.markdown_dir = self.cache_dir / "markdown"
        self.response_dir = self.cache_dir / "responses"
        self.web_search = WebDorkSearchProvider(settings)
        self.companies_house_client = CompaniesHouseClient(settings)
        self.org_resolver = PdfOrganisationResolver(
            settings=settings,
            charity_client=charity_client,
            companies_house_client=self.companies_house_client,
        )
        self._gemini = (
            GeminiClient(
                api_key=settings.gemini_api_key,
                cache_dir=settings.cache_dir / "gemini_pdf_enrichment",
            )
            if settings.gemini_api_key
            else None
        )

    def enrich_run(self, *, run_id: int, organisations: list[Any]) -> dict[str, Any]:
        if not self.settings.pdf_enrichment_enabled:
            return {"run_id": run_id, "enabled": False, "processed_organisation_count": 0}
        if self._gemini is None:
            return {"run_id": run_id, "enabled": False, "processed_organisation_count": 0, "warning": "No Gemini API key configured"}

        summary = {
            "run_id": run_id,
            "enabled": True,
            "processed_organisation_count": 0,
            "document_count": 0,
            "entity_count": 0,
            "people_added": 0,
            "organisation_mentions_resolved": 0,
            "organisation_mentions_seen": 0,
            "warnings": [],
        }
        for organisation in organisations:
            summary["processed_organisation_count"] += 1
            try:
                org_summary = self._enrich_organisation(run_id=run_id, organisation=organisation)
            except RuntimeError as exc:
                summary["warnings"].append(str(exc))
                continue
            summary["document_count"] += int(org_summary["document_count"])
            summary["entity_count"] += int(org_summary["entity_count"])
            summary["people_added"] += int(org_summary["people_added"])
            summary["organisation_mentions_resolved"] += int(org_summary["organisation_mentions_resolved"])
            summary["organisation_mentions_seen"] += int(org_summary["organisation_mentions_seen"])
            summary["warnings"].extend(org_summary.get("warnings", []))
        return summary

    def _enrich_organisation(self, *, run_id: int, organisation: Any) -> dict[str, Any]:
        org_name = _clean_text(organisation["name"])
        org_id = int(organisation["id"])
        documents = self.find_documents_for_organisation(org_name)[: self.settings.pdf_enrichment_max_documents]
        summary = {
            "document_count": len(documents),
            "entity_count": 0,
            "people_added": 0,
            "organisation_mentions_resolved": 0,
            "organisation_mentions_seen": 0,
            "warnings": [],
        }
        for document in documents:
            try:
                hydrated = self._prepare_document(document)
                entities = self.extract_entities_from_document(
                    organisation_name=org_name,
                    document=hydrated,
                )
            except RuntimeError as exc:
                summary["warnings"].append(f"{org_name}: {exc}")
                continue

            for index, entity in enumerate(entities, start=1):
                evidence_id = self._store_entity_evidence(
                    run_id=run_id,
                    organisation_name=org_name,
                    document=hydrated,
                    entity=entity,
                    entity_index=index,
                )
                summary["entity_count"] += 1
                if entity.role_category == "organisation":
                    summary["organisation_mentions_seen"] += 1
                    if self._resolve_organisation_entity(run_id=run_id, parent_org=organisation, entity=entity, evidence_id=evidence_id):
                        summary["organisation_mentions_resolved"] += 1
                    continue

                role_mapping = role_mapping_for_entity(entity)
                if role_mapping is None:
                    continue
                person_id = self.repository.upsert_person(entity.name)
                role_type, role_label, relationship_kind, relationship_phrase, edge_weight = role_mapping
                self.repository.upsert_role(
                    person_id=person_id,
                    organisation_id=org_id,
                    role_type=role_type,
                    role_label=role_label,
                    relationship_kind=relationship_kind,
                    relationship_phrase=relationship_phrase,
                    source="pdf_gemini_extraction",
                    confidence_class="medium",
                    edge_weight=edge_weight,
                    provenance={
                        "pdf_entity": asdict(entity),
                        "document": {
                            "title": hydrated.title,
                            "url": hydrated.document_url,
                            "markdown_path": hydrated.markdown_path,
                        },
                        "evidence_id": evidence_id,
                    },
                )
                summary["people_added"] += 1
        return summary

    def find_documents_for_organisation(self, organisation_name: str) -> list[PdfSourceDocument]:
        query = f'"{organisation_name}" ("annual report" OR accounts OR "trustees report" OR "directors report") filetype:pdf'
        results = self.web_search._cached_search(query)
        documents: list[PdfSourceDocument] = []
        seen_urls: set[str] = set()
        for item in results:
            if not isinstance(item, dict):
                continue
            url = _clean_text(item.get("href") or item.get("url") or "")
            if not url or url in seen_urls or ".pdf" not in url.lower():
                continue
            seen_urls.add(url)
            documents.append(
                PdfSourceDocument(
                    organisation_name=organisation_name,
                    document_url=url,
                    title=_clean_text(item.get("title") or organisation_name),
                    source_provider="pdf_web_search",
                )
            )
        return documents

    def _prepare_document(self, document: PdfSourceDocument) -> PdfSourceDocument:
        pdf_path = self._download_pdf(document.document_url)
        markdown_path, markdown_text = self._convert_pdf_to_markdown(pdf_path)
        return PdfSourceDocument(
            organisation_name=document.organisation_name,
            document_url=document.document_url,
            title=document.title,
            source_provider=document.source_provider,
            local_pdf_path=str(pdf_path),
            markdown_path=str(markdown_path),
            markdown_text=markdown_text,
        )

    def _download_pdf(self, url: str) -> Path:
        cache_key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        pdf_path = _json_cache_path(self.pdf_dir, cache_key, ".pdf")
        if pdf_path.exists():
            return pdf_path
        req = request.Request(url, headers={"User-Agent": self.settings.user_agent}, method="GET")
        try:
            with request.urlopen(req) as response:
                pdf_path.write_bytes(response.read())
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"PDF download failed: {exc.code} {body}") from exc
        except Exception as exc:
            raise RuntimeError(f"PDF download failed: {exc}") from exc
        return pdf_path

    def _convert_pdf_to_markdown(self, pdf_path: Path) -> tuple[Path, str]:
        markdown_path = _json_cache_path(self.markdown_dir, pdf_path.stem, ".md")
        if markdown_path.exists():
            return markdown_path, markdown_path.read_text(encoding="utf-8")
        if shutil.which("java") is None:
            raise RuntimeError("OpenDataLoader PDF requires Java 11+ on PATH")
        try:
            import opendataloader_pdf
        except ImportError as exc:
            raise RuntimeError("opendataloader-pdf is not installed") from exc

        output_dir = self.markdown_dir / pdf_path.stem
        output_dir.mkdir(parents=True, exist_ok=True)
        opendataloader_pdf.convert(
            input_path=[str(pdf_path)],
            output_dir=str(output_dir),
            format="markdown",
            quiet=True,
        )
        produced = output_dir / f"{pdf_path.stem}.md"
        if not produced.exists():
            candidates = sorted(output_dir.glob("*.md"))
            if not candidates:
                raise RuntimeError("OpenDataLoader did not produce markdown output")
            produced = candidates[0]
        markdown_text = produced.read_text(encoding="utf-8", errors="replace")
        markdown_path.write_text(markdown_text, encoding="utf-8")
        return markdown_path, markdown_text

    def extract_entities_from_document(
        self,
        *,
        organisation_name: str,
        document: PdfSourceDocument,
    ) -> list[PdfExtractedEntity]:
        chunks = chunk_markdown(document.markdown_text)
        if not chunks:
            return []

        entities: list[PdfExtractedEntity] = []
        seen_keys: set[tuple[str, str, str]] = set()
        for index, chunk in enumerate(chunks[: self.settings.pdf_enrichment_max_chunks], start=1):
            prompt = _build_extraction_prompt(
                organisation_name=organisation_name,
                document_title=document.title,
                document_url=document.document_url,
                markdown_chunk=chunk,
            )
            response = self._gemini.generate(
                model=self.settings.pdf_enrichment_model,
                prompt=prompt,
            )
            response_text = extract_gemini_text(response)
            response_path = _json_cache_path(
                self.response_dir,
                hashlib.sha256(f"{document.document_url}:{index}".encode("utf-8")).hexdigest(),
                ".json",
            )
            response_path.write_text(
                json.dumps(
                    {
                        "document_url": document.document_url,
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
            except (ValueError, KeyError):
                parsed = {}
            for entity in parse_pdf_entities_document(
                parsed,
                organisation_name=organisation_name,
                source_document_url=document.document_url,
            ):
                key = (entity.name.lower(), entity.role_category, entity.organisation_name.lower())
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                entities.append(entity)
        return entities

    def _store_entity_evidence(
        self,
        *,
        run_id: int,
        organisation_name: str,
        document: PdfSourceDocument,
        entity: PdfExtractedEntity,
        entity_index: int,
    ) -> int:
        source_key = hashlib.sha256(
            f"{organisation_name}|{document.document_url}|{entity_index}|{entity.name}|{entity.role_category}".encode("utf-8")
        ).hexdigest()
        item = EvidenceItem(
            source="pdf_gemini_extraction",
            source_key=source_key,
            title=document.title or organisation_name,
            url=document.document_url,
            snippet=f"{entity.name} extracted from PDF for {organisation_name}",
            raw_payload={
                "organisation_name": organisation_name,
                "document": {
                    "title": document.title,
                    "url": document.document_url,
                    "source_provider": document.source_provider,
                    "local_pdf_path": document.local_pdf_path,
                    "markdown_path": document.markdown_path,
                },
                "entity": asdict(entity),
            },
        )
        return self.repository.insert_evidence_item(run_id, item)

    def _resolve_organisation_entity(
        self,
        *,
        run_id: int,
        parent_org: Any,
        entity: PdfExtractedEntity,
        evidence_id: int,
    ) -> bool:
        resolved = self.org_resolver.resolve(entity.name)
        if resolved is None:
            return False
        organisation_id = self.repository.upsert_organisation(resolved)
        self.repository.link_run_organisation(
            run_id,
            organisation_id,
            stage="step2_connected_org",
            source="pdf_org_mention",
            metadata={
                "parent_organisation_id": int(parent_org["id"]),
                "parent_organisation_name": _clean_text(parent_org["name"]),
                "document_url": entity.source_document_url,
                "entity_name": entity.name,
                "role_category": entity.role_category,
                "evidence_id": evidence_id,
            },
        )
        return True


def enrich_run_from_pdfs(
    *,
    repository: Any,
    settings: Settings,
    charity_client: Any,
    run_id: int,
    organisations: list[Any],
) -> dict[str, Any]:
    service = PdfEnrichmentService(
        settings=settings,
        repository=repository,
        charity_client=charity_client,
    )
    return service.enrich_run(run_id=run_id, organisations=organisations)


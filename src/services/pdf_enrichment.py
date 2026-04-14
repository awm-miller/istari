from __future__ import annotations

import base64
from contextlib import redirect_stderr
import hashlib
import html
import http.client
import io
import json
import logging
import re
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urljoin, urlparse

from src.charity_commission.search import search_name_to_organisation
from src.companies_house.client import CompaniesHouseClient
from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.models import EvidenceItem, OrganisationRecord, PdfExtractedEntity, PdfSourceDocument
from src.openai_api import extract_json_document
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
_CHARITY_COMMISSION_REGISTER_BASE_URL = "https://register-of-charities.charitycommission.gov.uk"
_OCR_PAGE_TIMEOUT_SECONDS = 20
_OCR_MAX_PAGES = 12


def _clean_text(value: str) -> str:
    return " ".join(str(value).split()).strip()


def _normalize_for_match(value: str) -> str:
    return normalize_name(_clean_text(value))


def _significant_tokens(name: str, *, min_len: int = 3) -> set[str]:
    """Extract meaningful tokens from an org name, dropping short/common words."""
    _STOP = {"ltd", "limited", "plc", "llp", "cic", "uk", "the", "of", "and", "for", "in", "a"}
    return {
        t for t in normalize_name(name).split()
        if len(t) >= min_len and t not in _STOP
    }


def _text_mentions_org(text: str, org_name: str, *, threshold: float = 0.6) -> bool:
    """Check whether enough significant org-name tokens appear in the text."""
    tokens = _significant_tokens(org_name)
    if not tokens:
        return True
    text_lower = text.lower()
    hits = sum(1 for t in tokens if t in text_lower)
    return hits / len(tokens) >= threshold


_TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


def _has_meaningful_text(markdown: str, *, min_chars: int = 80) -> bool:
    text_only = "\n".join(
        line for line in markdown.split("\n")
        if not line.strip().startswith("![")
    ).strip()
    return len(text_only) >= min_chars


def _ocr_pdf(pdf_path: Path) -> str:
    try:
        import fitz  # PyMuPDF
        import pytesseract
        from PIL import Image
    except ImportError:
        log.warning("OCR dependencies (pymupdf, pytesseract, Pillow) not installed")
        return ""

    tesseract_path = Path(_TESSERACT_CMD)
    if not tesseract_path.exists() and shutil.which("tesseract") is None:
        log.warning("Tesseract not found at %s", _TESSERACT_CMD)
        return ""
    if tesseract_path.exists():
        pytesseract.pytesseract.tesseract_cmd = str(tesseract_path)

    doc = fitz.open(str(pdf_path))
    pages: list[str] = []
    for page_num in range(min(doc.page_count, _OCR_MAX_PAGES)):
        try:
            pix = doc[page_num].get_pixmap(dpi=300)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            text = pytesseract.image_to_string(
                img,
                lang="eng",
                timeout=_OCR_PAGE_TIMEOUT_SECONDS,
            ).strip()
        except RuntimeError as exc:
            log.warning(
                "OCR timed out for %s on page %d: %s",
                pdf_path.name,
                page_num + 1,
                exc,
            )
            break
        except Exception as exc:
            log.warning(
                "OCR failed for %s on page %d: %s",
                pdf_path.name,
                page_num + 1,
                exc,
            )
            break
        if text:
            pages.append(text)
    doc.close()
    return "\n\n".join(pages)


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
        connection_phrase = _clean_text(row.get("connection_phrase", ""))
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
                connection_phrase=connection_phrase,
                source_page_hint=source_page_hint,
                confidence=max(0.0, min(confidence, 1.0)),
                registry_hint=registry_hint,
                notes=notes,
            )
        )
    return entities


def _is_notice_boilerplate_text(value: str) -> bool:
    text = _clean_text(value).lower()
    if not text:
        return False
    if "gives notice" in text:
        return True
    if "issuing authority" in text or "issuing the gazette notice" in text:
        return True
    if "regulatory body issuing a notice" in text:
        return True
    if ("registrar of companies" in text or "companies house" in text) and any(
        token in text for token in ("notice", "gazette", "strike off", "striking off")
    ):
        return True
    return False


def _is_notice_boilerplate_entity(entity: PdfExtractedEntity) -> bool:
    return any(
        _is_notice_boilerplate_text(value)
        for value in (
            entity.name,
            entity.role_label,
            entity.connection_phrase,
            entity.notes,
        )
    )


def role_mapping_for_entity(entity: PdfExtractedEntity) -> tuple[str, str, str, str, float] | None:
    label = entity.role_label or entity.role_category or entity.entity_type
    phrase = entity.connection_phrase.strip()
    if entity.role_category == "ignore":
        return None
    if entity.role_category == "organisation":
        return None
    if entity.role_category == "accountant_or_auditor":
        return (
            "accountant_or_auditor",
            label,
            "professional_to",
            phrase or "is an accountant or auditor for",
            0.6,
        )
    if entity.role_category == "other_professional":
        return (
            "other_professional",
            label,
            "professional_to",
            phrase or "is professionally linked to",
            0.55,
        )
    return (
        "pdf_person_mention",
        label or "pdf_person_mention",
        "mentioned_for",
        phrase or "is mentioned in documents for",
        0.45,
    )


def _build_extraction_prompt(
    *,
    organisation_name: str,
    document_title: str,
    document_url: str,
    markdown_chunk: str,
    filing_description: str = "",
) -> str:
    filing_line = f"\nFiling description: {filing_description}" if filing_description else ""
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
      "connection_phrase": "",
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
- Use role_category=ignore for boilerplate notice issuers like Companies House / Registrar of Companies when they are only giving or issuing a notice.
- Set connection_phrase to a short natural-language phrase that explains the relationship to the scoped organisation, e.g. "is listed as a director of", "is named as auditor for", "is identified as a subsidiary of", "is described as providing services to".
- Use notes to briefly explain how the document makes that link, quoting or paraphrasing the relevant context.
- Prefer precision over recall.
- Do not invent registry numbers.
- Keep organisation_name as the scoped organisation unless the text clearly ties the entity to another organisation.

Scoped organisation: {organisation_name}
Document title: {document_title}
Document URL: {document_url}{filing_line}

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

    def resolve_all(self, organisation_name: str) -> list[OrganisationRecord]:
        cleaned = _clean_text(organisation_name)
        if not cleaned:
            return []

        resolved: list[OrganisationRecord] = []
        company_match = self._resolve_company(cleaned)
        if company_match is not None:
            resolved.append(company_match)

        charity_match = search_name_to_organisation(self.charity_client, cleaned)
        if charity_match is not None:
            resolved.append(charity_match)
        return self._dedupe_records(resolved)

    @staticmethod
    def _dedupe_records(records: list[OrganisationRecord]) -> list[OrganisationRecord]:
        seen: set[tuple[str, str, int]] = set()
        deduped: list[OrganisationRecord] = []
        for record in records:
            key = (
                str(record.registry_type or "").strip(),
                str(record.registry_number or "").strip(),
                int(record.suffix or 0),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(record)
        return deduped

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
                timeout_seconds=30.0,
                attempts=2,
            )
            if settings.gemini_api_key
            else None
        )
        self._ch_auth: str | None = None
        if settings.companies_house_api_key:
            raw = f"{settings.companies_house_api_key}:".encode("utf-8")
            self._ch_auth = base64.b64encode(raw).decode("ascii")

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
        seen_org_ids: set[int] = set()
        deduped: list[Any] = []
        for org in organisations:
            oid = int(org["id"])
            if oid not in seen_org_ids:
                seen_org_ids.add(oid)
                deduped.append(org)
        organisations = deduped
        self._run_seen_urls: set[str] = set()
        log.info("PDF enrichment: starting for %d unique organisations (deduped from %d rows)", len(organisations), len(seen_org_ids))
        for org_index, organisation in enumerate(organisations, start=1):
            log.info("PDF enrichment: org %d/%d: %s", org_index, len(organisations), _clean_text(organisation["name"]))
            summary["processed_organisation_count"] += 1
            try:
                org_summary = self._enrich_organisation(run_id=run_id, organisation=organisation)
            except RuntimeError as exc:
                summary["warnings"].append(str(exc))
                continue
            except Exception as exc:
                log.warning("PDF enrichment: org failed unexpectedly for %s: %s", _clean_text(organisation["name"]), exc)
                summary["warnings"].append(f"{_clean_text(organisation['name'])}: {exc}")
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
        try:
            registry_type = _clean_text(organisation["registry_type"] or "")
        except (KeyError, IndexError):
            registry_type = ""
        try:
            registry_number = _clean_text(organisation["registry_number"] or "")
        except (KeyError, IndexError):
            registry_number = ""
        log.info("PDF enrichment: searching for documents for %s (type=%s, num=%s)", org_name, registry_type, registry_number)
        documents = self.find_documents_for_organisation(
            org_name, registry_type=registry_type, registry_number=registry_number,
        )[: self.settings.pdf_enrichment_max_documents]
        documents = [d for d in documents if d.document_url not in self._run_seen_urls]
        log.info("PDF enrichment: found %d new documents for %s", len(documents), org_name)
        summary = {
            "document_count": len(documents),
            "entity_count": 0,
            "people_added": 0,
            "organisation_mentions_resolved": 0,
            "organisation_mentions_seen": 0,
            "warnings": [],
        }
        for doc_index, document in enumerate(documents, start=1):
            log.info("PDF enrichment: [%s] processing doc %d/%d: %s", org_name, doc_index, len(documents), document.document_url[:120])
            self._run_seen_urls.add(document.document_url)
            try:
                hydrated = self._prepare_document(document)
                log.info("PDF enrichment: [%s] doc %d prepared, %d chars markdown", org_name, doc_index, len(hydrated.markdown_text))
                if document.source_provider not in {"companies_house_filing", "charity_commission_accounts_tar"} and not _text_mentions_org(hydrated.markdown_text, org_name):
                    log.info("PDF enrichment: [%s] doc %d skipped -- org name not found in markdown", org_name, doc_index)
                    continue
                entities = self.extract_entities_from_document(
                    organisation_name=org_name,
                    document=hydrated,
                )
            except RuntimeError as exc:
                log.warning("PDF enrichment: [%s] doc %d failed: %s", org_name, doc_index, exc)
                summary["warnings"].append(f"{org_name}: {exc}")
                continue
            except Exception as exc:
                log.warning("PDF enrichment: [%s] doc %d failed unexpectedly: %s", org_name, doc_index, exc)
                summary["warnings"].append(f"{org_name}: {exc}")
                continue

            log.info("PDF enrichment: [%s] doc %d extracted %d entities", org_name, doc_index, len(entities))
            for index, entity in enumerate(entities, start=1):
                if _is_notice_boilerplate_entity(entity):
                    log.info("PDF enrichment: [%s] doc %d skipping notice boilerplate entity: %s", org_name, doc_index, entity.name)
                    continue
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
                    if self._resolve_organisation_entity(
                        run_id=run_id,
                        parent_org=organisation,
                        document=hydrated,
                        entity=entity,
                        evidence_id=evidence_id,
                    ):
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
                            "local_pdf_path": hydrated.local_pdf_path,
                            "markdown_path": hydrated.markdown_path,
                            "filing_description": hydrated.filing_description,
                        },
                        "evidence_id": evidence_id,
                    },
                )
                summary["people_added"] += 1
        return summary

    def find_documents_for_organisation(
        self, organisation_name: str, *, registry_type: str = "", registry_number: str = "",
    ) -> list[PdfSourceDocument]:
        if registry_type == "company" and registry_number and self._ch_auth:
            return self._find_ch_filing_documents(organisation_name, registry_number)
        if registry_type == "charity" and registry_number:
            return self._find_cc_accounts_documents(organisation_name, registry_number)
        return []

    def _find_ch_filing_documents(
        self, organisation_name: str, company_number: str,
    ) -> list[PdfSourceDocument]:
        try:
            payload = self.companies_house_client.get_filing_history(company_number)
        except RuntimeError as exc:
            log.warning("CH filing history fetch failed for %s: %s", company_number, exc)
            return []
        items = payload.get("items", [])
        if not isinstance(items, list):
            return []

        documents: list[PdfSourceDocument] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            links = item.get("links", {})
            doc_meta_url = links.get("document_metadata")
            if not doc_meta_url:
                continue
            description = _clean_text(item.get("description", ""))
            date = _clean_text(item.get("date", ""))
            desc_values = item.get("description_values", {}) or {}
            made_up_date = _clean_text(desc_values.get("made_up_date", ""))
            filing_desc = f"{description} ({date})" if date else description
            if made_up_date:
                filing_desc += f" [accounts to {made_up_date}]"
            title = f"{organisation_name} - {filing_desc}"
            doc_url = doc_meta_url if doc_meta_url.startswith("http") else f"https://document-api.company-information.service.gov.uk{doc_meta_url}"

            documents.append(
                PdfSourceDocument(
                    organisation_name=organisation_name,
                    document_url=doc_url,
                    title=title,
                    source_provider="companies_house_filing",
                    filing_description=filing_desc,
                )
            )
        return documents

    def _find_cc_accounts_documents(
        self, organisation_name: str, charity_number: str,
    ) -> list[PdfSourceDocument]:
        accounts_url = (
            f"{_CHARITY_COMMISSION_REGISTER_BASE_URL}/en/charity-search/-/charity-details/"
            f"{charity_number}/accounts-and-annual-returns"
        )
        try:
            html_text = self._fetch_text(accounts_url)
        except RuntimeError as exc:
            log.warning("CC accounts page fetch failed for %s: %s", charity_number, exc)
            return []

        documents: list[PdfSourceDocument] = []
        seen_urls: set[str] = set()
        for row_html in re.findall(r"<tr[^>]*>.*?</tr>", html_text, flags=re.IGNORECASE | re.DOTALL):
            if "accounts-resource" not in row_html or "Accounts and TAR" not in row_html:
                continue
            href_match = re.search(r'href="([^"]+)"', row_html, flags=re.IGNORECASE)
            if not href_match:
                continue
            href = html.unescape(href_match.group(1))
            doc_url = urljoin(_CHARITY_COMMISSION_REGISTER_BASE_URL, href)
            if doc_url in seen_urls:
                continue
            seen_urls.add(doc_url)

            row_text = _clean_text(re.sub(r"<[^>]+>", " ", html.unescape(row_html)))
            year_match = re.search(r"\b(19|20)\d{2}\b", row_text)
            reporting_year = year_match.group(0) if year_match else ""
            filing_desc = f"Accounts and TAR ({reporting_year})" if reporting_year else "Accounts and TAR"
            documents.append(
                PdfSourceDocument(
                    organisation_name=organisation_name,
                    document_url=doc_url,
                    title=f"{organisation_name} - {filing_desc}",
                    source_provider="charity_commission_accounts_tar",
                    filing_description=filing_desc,
                )
            )
        return documents

    def _fetch_text(self, url: str) -> str:
        cache_path = _json_cache_path(
            self.cache_dir / "pages",
            hashlib.sha256(url.encode("utf-8")).hexdigest(),
            ".html",
        )
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8", errors="replace")

        req = request.Request(url, headers={"User-Agent": self.settings.user_agent}, method="GET")
        try:
            with request.urlopen(req, timeout=30) as response:
                text = response.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Page fetch failed: {exc.code} {body}") from exc
        except Exception as exc:
            raise RuntimeError(f"Page fetch failed: {exc}") from exc

        cache_path.write_text(text, encoding="utf-8")
        return text

    def _prepare_document(self, document: PdfSourceDocument) -> PdfSourceDocument:
        pdf_path = self._download_pdf(document)
        markdown_path, markdown_text = self._convert_pdf_to_markdown(pdf_path)
        return PdfSourceDocument(
            organisation_name=document.organisation_name,
            document_url=document.document_url,
            title=document.title,
            source_provider=document.source_provider,
            local_pdf_path=str(pdf_path),
            markdown_path=str(markdown_path),
            markdown_text=markdown_text,
            filing_description=document.filing_description,
        )

    def _download_pdf(self, document: PdfSourceDocument) -> Path:
        url = document.document_url
        cache_key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        pdf_path = _json_cache_path(self.pdf_dir, cache_key, ".pdf")
        if pdf_path.exists():
            return pdf_path
        log.info("PDF download: %s", url[:120])

        if document.source_provider == "companies_house_filing" and self._ch_auth:
            return self._download_ch_pdf(url, pdf_path)

        req = request.Request(url, headers={"User-Agent": self.settings.user_agent}, method="GET")
        try:
            with request.urlopen(req, timeout=30) as response:
                pdf_path.write_bytes(response.read())
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"PDF download failed: {exc.code} {body}") from exc
        except Exception as exc:
            raise RuntimeError(f"PDF download failed: {exc}") from exc
        return pdf_path

    def _download_ch_pdf(self, metadata_url: str, pdf_path: Path) -> Path:
        """Two-step CH download: get metadata -> follow /content redirect -> download from S3."""
        content_url = metadata_url.rstrip("/") + "/content"
        parsed = urlparse(content_url)
        conn = http.client.HTTPSConnection(parsed.hostname, timeout=30)
        try:
            headers = {
                "Authorization": f"Basic {self._ch_auth}",
                "Accept": "application/pdf",
                "User-Agent": self.settings.user_agent,
            }
            conn.request("GET", f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path, headers=headers)
            resp = conn.getresponse()
            resp.read()
            if resp.status not in (301, 302, 303):
                raise RuntimeError(f"CH document API returned {resp.status} (expected redirect)")
            s3_url = resp.getheader("Location")
            if not s3_url:
                raise RuntimeError("CH document API redirect had no Location header")
        finally:
            conn.close()

        req = request.Request(s3_url, headers={"User-Agent": self.settings.user_agent}, method="GET")
        try:
            with request.urlopen(req, timeout=60) as response:
                pdf_path.write_bytes(response.read())
        except Exception as exc:
            raise RuntimeError(f"CH PDF download from S3 failed: {exc}") from exc
        return pdf_path

    def _convert_pdf_to_markdown(self, pdf_path: Path) -> tuple[Path, str]:
        markdown_path = _json_cache_path(self.markdown_dir, pdf_path.stem, ".md")
        if markdown_path.exists():
            return markdown_path, markdown_path.read_text(encoding="utf-8")

        markdown_text = self._try_opendataloader(pdf_path)
        if not _has_meaningful_text(markdown_text):
            log.info("PDF->Markdown: text extraction empty, trying OCR for %s", pdf_path.name)
            markdown_text = _ocr_pdf(pdf_path)

        markdown_path.write_text(markdown_text, encoding="utf-8")
        return markdown_path, markdown_text

    @staticmethod
    def _try_opendataloader(pdf_path: Path) -> str:
        if shutil.which("java") is None:
            return ""
        try:
            import opendataloader_pdf
        except ImportError:
            return ""
        output_dir = pdf_path.parent / f"{pdf_path.stem}_odl"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with io.StringIO() as stderr_buffer, redirect_stderr(stderr_buffer):
                opendataloader_pdf.convert(
                    input_path=[str(pdf_path)],
                    output_dir=str(output_dir),
                    format="markdown",
                    quiet=True,
                )
        except subprocess.CalledProcessError as exc:
            output = " ".join(str(exc.output or "").split())
            if output:
                log.warning(
                    "PDF->Markdown: opendataloader failed for %s (exit %s): %s",
                    pdf_path.name,
                    exc.returncode,
                    output[:300],
                )
            else:
                log.warning(
                    "PDF->Markdown: opendataloader failed for %s (exit %s)",
                    pdf_path.name,
                    exc.returncode,
                )
            return ""
        except Exception as exc:
            log.warning(
                "PDF->Markdown: opendataloader wrapper failed for %s: %s",
                pdf_path.name,
                exc,
            )
            return ""
        produced = output_dir / f"{pdf_path.stem}.md"
        if not produced.exists():
            candidates = sorted(output_dir.glob("*.md"))
            if not candidates:
                return ""
            produced = candidates[0]
        return produced.read_text(encoding="utf-8", errors="replace")

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
                filing_description=document.filing_description,
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
            snippet=f"{entity.name} extracted from PDF for {organisation_name} ({document.filing_description})" if document.filing_description else f"{entity.name} extracted from PDF for {organisation_name}",
            raw_payload={
                "organisation_name": organisation_name,
                "document": {
                    "title": document.title,
                    "url": document.document_url,
                    "source_provider": document.source_provider,
                    "local_pdf_path": document.local_pdf_path,
                    "markdown_path": document.markdown_path,
                    "filing_description": document.filing_description,
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
        document: PdfSourceDocument,
        entity: PdfExtractedEntity,
        evidence_id: int,
    ) -> bool:
        resolved_records = self.org_resolver.resolve_all(entity.name)
        if not resolved_records:
            return False
        for resolved in resolved_records:
            organisation_id = self.repository.upsert_organisation(resolved)
            self.repository.link_run_organisation(
                run_id,
                organisation_id,
                stage="step2_connected_org",
                source="pdf_org_mention",
                metadata={
                    "parent_organisation_id": int(parent_org["id"]),
                    "parent_organisation_name": _clean_text(parent_org["name"]),
                    "document_title": document.title,
                    "document_url": entity.source_document_url,
                    "local_pdf_path": document.local_pdf_path,
                    "filing_description": document.filing_description,
                    "entity_name": entity.name,
                    "role_category": entity.role_category,
                    "connection_phrase": entity.connection_phrase,
                    "connection_detail": entity.notes,
                    "source_page_hint": entity.source_page_hint,
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


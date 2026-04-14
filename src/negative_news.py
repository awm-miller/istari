"""Standalone adverse-media pilot: Serper discovery + full-page text extraction + Gemini classification."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urlparse

from src.config import Settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.html_plain_text import extract_title_from_html, html_to_plain_text
from src.openai_api import extract_json_document
from src.search.queries import generate_name_variants
from src.storage.negative_news_store import (
    NegativeNewsStore,
    cluster_lookup_key,
    person_ids_fingerprint,
)
from src.storage.repository import Repository

log = logging.getLogger("istari.negative_news")

DEFAULT_MAX_EXTRACT_CHARS = 500_000
DEFAULT_PAGES = 10
DEFAULT_NUM_PER_PAGE = 10
_CLUSTER_SOURCE_CACHE_VERSION = 2
_FETCH_TIMEOUT = 60
_MAX_PDF_BYTES = 3_000_000
_SKIP_RESULT_DOMAINS = {
    "charitycommission.gov.uk",
    "checkfree.co.uk",
    "endole.co.uk",
    "facebook.com",
    "find-and-update.company-information.service.gov.uk",
    "instagram.com",
    "linkedin.com",
    "northdata.com",
    "register-of-charities.charitycommission.gov.uk",
    "company-information.service.gov.uk",
    "youtube.com",
    "youtu.be",
    "twitter.com",
    "x.com",
    "tiktok.com",
}
_SKIP_RESULT_HOST_FAMILIES = {
    "northdata.",
}


def _log_label(value: str, *, limit: int = 120) -> str:
    clean = " ".join(str(value or "").split()).strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 3] + "..."


def _url_fingerprint(url: str) -> str:
    return hashlib.sha256(str(url).encode("utf-8")).hexdigest()[:16]


def _cache_path(base: Path, key: str, suffix: str) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{key}{suffix}"


def _normalize_domain(url: str) -> str:
    host = urlparse(str(url or "")).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _should_skip_result_url(url: str) -> bool:
    host = _normalize_domain(url)
    if any(host == blocked or host.endswith(f".{blocked}") for blocked in _SKIP_RESULT_DOMAINS):
        return True
    return any(host.startswith(prefix) or f".{prefix}" in host for prefix in _SKIP_RESULT_HOST_FAMILIES)


def _normalize_match_text(value: str) -> str:
    return " ".join(str(value or "").lower().split()).strip()


def _required_term_match_locations(
    required_terms: list[str] | None,
    *,
    title: str,
    snippet: str,
    extracted_text: str,
) -> dict[str, list[str]]:
    matches: dict[str, list[str]] = {}
    fields = {
        "title": _normalize_match_text(title),
        "snippet": _normalize_match_text(snippet),
        "extracted_text": _normalize_match_text(extracted_text),
    }
    for term in required_terms or []:
        needle = _normalize_match_text(term)
        if not needle:
            continue
        locations = [field for field, haystack in fields.items() if needle in haystack]
        if locations:
            matches[term] = locations
    return matches


def serper_search(
    settings: Settings,
    *,
    query: str,
    page: int,
    num: int,
    cache_dir: Path,
) -> list[dict[str, Any]]:
    """POST to Serper /search; return organic rows as dicts with title, link, snippet."""
    if not settings.serper_api_key:
        raise RuntimeError("SERPER_API_KEY is required for negative-news search")
    payload_obj = {"q": query, "num": num, "page": page}
    key = hashlib.sha256(json.dumps(payload_obj, sort_keys=True).encode()).hexdigest()[:20]
    cache_file = _cache_path(cache_dir, f"serper_{key}", ".json")
    if cache_file.exists():
        rows = json.loads(cache_file.read_text(encoding="utf-8"))
        log.info(
            "Serper cache hit q=%s page=%s rows=%s",
            _log_label(query, limit=90),
            page,
            len(rows) if isinstance(rows, list) else 0,
        )
        return rows

    url = f"{settings.serper_base_url}/search"
    payload = json.dumps(payload_obj).encode("utf-8")
    log.info("Serper request q=%s page=%s num=%s", _log_label(query, limit=90), page, num)
    req = request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={
            "X-API-KEY": str(settings.serper_api_key),
            "Content-Type": "application/json",
            "User-Agent": settings.user_agent,
        },
    )
    try:
        with request.urlopen(req, timeout=_FETCH_TIMEOUT) as response:
            body = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Serper request failed: {exc.code} {message}") from exc

    organic = body.get("organic", [])
    rows: list[dict[str, Any]] = []
    if isinstance(organic, list):
        for item in organic:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "title": item.get("title", ""),
                    "link": item.get("link") or item.get("url") or "",
                    "snippet": item.get("snippet", ""),
                    "position": item.get("position"),
                }
            )
    log.info(
        "Serper response q=%s page=%s organic_rows=%s",
        _log_label(query, limit=90),
        page,
        len(rows),
    )
    cache_file.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def _looks_like_pdf(url: str) -> bool:
    return bool(re.search(r"\.pdf($|[?#])", str(url or ""), flags=re.IGNORECASE))


def _extract_pdf_text(pdf_path: Path) -> str:
    from src.services.pdf_enrichment import _ocr_pdf

    return _ocr_pdf(pdf_path)


@dataclass(slots=True)
class ExtractionReport:
    url: str
    final_url: str
    content_type: str
    http_status: int
    raw_bytes: int
    text_chars: int
    non_blank_lines: int
    truncated_by_cap: bool
    max_extract_chars: int
    title: str
    text: str
    first_preview: str
    last_preview: str
    error: str | None = None


@dataclass(slots=True)
class QuerySpec:
    query: str
    pages: int
    bucket: str
    language: str
    required_terms: list[str]


def extraction_report_summary(
    ex: ExtractionReport,
    *,
    include_full_text: bool = False,
) -> dict[str, Any]:
    """JSON-serializable diagnostics for QA (whole-article extraction checks)."""
    body = {
        "url": ex.url,
        "final_url": ex.final_url,
        "http_status": ex.http_status,
        "content_type": ex.content_type,
        "raw_bytes": ex.raw_bytes,
        "text_chars": ex.text_chars,
        "non_blank_lines": ex.non_blank_lines,
        "truncated_by_cap": ex.truncated_by_cap,
        "max_extract_chars": ex.max_extract_chars,
        "title": ex.title,
        "error": ex.error,
        "first_preview": ex.first_preview,
        "last_preview": ex.last_preview,
        "low_body_warning": bool(
            ex.error is None and ex.http_status == 200 and ex.text_chars < 400
        ),
        "truncation_risk": bool(ex.truncated_by_cap),
    }
    if include_full_text:
        body["full_text"] = ex.text
    return body


def _unique_nonempty(values: list[str] | tuple[str, ...] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        clean = " ".join(str(value).split()).strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _append_query_spec(unique_specs: list[QuerySpec], spec: QuerySpec) -> None:
    key = spec.query.strip()
    if not key:
        return
    for existing in unique_specs:
        if existing.query != key:
            continue
        existing.pages = max(existing.pages, spec.pages)
        for term in spec.required_terms:
            if term not in existing.required_terms:
                existing.required_terms.append(term)
        return
    unique_specs.append(spec)


def _negative_news_db_path(settings: Settings) -> Path:
    return settings.project_root / "data" / "negative_news.sqlite"


def _cluster_kind(cluster_id: str, raw_kind: Any = "") -> str:
    kind = str(raw_kind or "").strip().lower()
    if kind:
        return kind
    cluster_id = str(cluster_id or "").strip().lower()
    if cluster_id.startswith("merged_person:"):
        return "person"
    if cluster_id.startswith("identity:"):
        return "seed_alias"
    return ""


def _cluster_source_cache_path(repository: Repository) -> Path:
    return repository.database_path.parent / "negative_news_cluster_source_cache.json"


def _load_cluster_source_cache(repository: Repository, *, run_ids: list[int]) -> dict[str, Any] | None:
    cache_path = _cluster_source_cache_path(repository)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("version") or 0) != _CLUSTER_SOURCE_CACHE_VERSION:
        return None
    cached_run_ids = payload.get("run_ids")
    clusters = payload.get("clusters")
    total_available = payload.get("total_available")
    if cached_run_ids != run_ids or not isinstance(clusters, list) or not isinstance(total_available, int):
        return None
    return payload


def _write_cluster_source_cache(
    repository: Repository,
    *,
    run_ids: list[int],
    clusters: list[dict[str, Any]],
) -> None:
    cache_path = _cluster_source_cache_path(repository)
    cache_path.write_text(
        json.dumps(
            {
                "version": _CLUSTER_SOURCE_CACHE_VERSION,
                "run_ids": list(run_ids),
                "total_available": len(clusters),
                "clusters": clusters,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def fetch_and_extract_article(
    settings: Settings,
    url: str,
    *,
    cache_dir: Path,
    max_extract_chars: int = DEFAULT_MAX_EXTRACT_CHARS,
) -> ExtractionReport:
    """Download URL (HTML or PDF), convert to plain text up to max_extract_chars."""
    key = _url_fingerprint(url)
    html_cache = _cache_path(cache_dir, f"fetch_{key}", ".html")
    pdf_cache = _cache_path(cache_dir, f"fetch_{key}", ".pdf")
    text_cache = _cache_path(cache_dir, f"extract_{key}", ".txt")

    if text_cache.exists():
        text = text_cache.read_text(encoding="utf-8", errors="replace")
        truncated = len(text) >= max_extract_chars * 0.98
        title = ""
        if html_cache.exists():
            raw = html_cache.read_text(encoding="utf-8", errors="replace")
            title = extract_title_from_html(raw, "")
        log.info(
            "Article cache hit url=%s chars=%s",
            _log_label(url, limit=100),
            len(text),
        )
        return ExtractionReport(
            url=url,
            final_url=url,
            content_type="cached",
            http_status=200,
            raw_bytes=html_cache.stat().st_size if html_cache.exists() else 0,
            text_chars=len(text),
            non_blank_lines=len([ln for ln in text.splitlines() if ln.strip()]),
            truncated_by_cap=truncated,
            max_extract_chars=max_extract_chars,
            title=title,
            text=text[:max_extract_chars],
            first_preview=text[:400],
            last_preview=text[-400:] if len(text) > 400 else text,
        )

    req = request.Request(url, headers={"User-Agent": settings.user_agent}, method="GET")
    log.info("Article fetch start url=%s", _log_label(url, limit=100))
    try:
        with request.urlopen(req, timeout=_FETCH_TIMEOUT) as resp:
            final_url = str(resp.geturl() or url)
            status = int(getattr(resp, "status", 200) or 200)
            content_type = str(resp.headers.get("Content-Type", "") or "")
            body = resp.read()
    except error.HTTPError as exc:
        log.warning("Article fetch HTTP error url=%s status=%s", _log_label(url, limit=100), exc.code)
        return ExtractionReport(
            url=url,
            final_url=url,
            content_type="",
            http_status=int(exc.code),
            raw_bytes=0,
            text_chars=0,
            non_blank_lines=0,
            truncated_by_cap=False,
            max_extract_chars=max_extract_chars,
            title="",
            text="",
            first_preview="",
            last_preview="",
            error=f"HTTP {exc.code}",
        )
    except Exception as exc:
        log.warning("Article fetch failed url=%s error=%s", _log_label(url, limit=100), exc)
        return ExtractionReport(
            url=url,
            final_url=url,
            content_type="",
            http_status=0,
            raw_bytes=0,
            text_chars=0,
            non_blank_lines=0,
            truncated_by_cap=False,
            max_extract_chars=max_extract_chars,
            title="",
            text="",
            first_preview="",
            last_preview="",
            error=str(exc),
        )

    raw_bytes = len(body)
    is_pdf = _looks_like_pdf(final_url) or "pdf" in content_type.lower()
    log.info(
        "Article fetch done url=%s status=%s type=%s bytes=%s pdf=%s",
        _log_label(final_url, limit=100),
        status,
        _log_label(content_type, limit=50),
        raw_bytes,
        is_pdf,
    )
    if is_pdf:
        if raw_bytes > _MAX_PDF_BYTES:
            log.warning(
                "PDF skipped as too large url=%s bytes=%s limit=%s",
                _log_label(final_url, limit=100),
                raw_bytes,
                _MAX_PDF_BYTES,
            )
            return ExtractionReport(
                url=url,
                final_url=final_url,
                content_type=content_type,
                http_status=status,
                raw_bytes=raw_bytes,
                text_chars=0,
                non_blank_lines=0,
                truncated_by_cap=False,
                max_extract_chars=max_extract_chars,
                title="",
                text="",
                first_preview="",
                last_preview="",
                error=f"pdf skipped: too large ({raw_bytes} bytes)",
            )
        pdf_cache.write_bytes(body)
        try:
            plain = _extract_pdf_text(pdf_cache)
        except Exception as exc:
            log.warning("PDF extraction failed url=%s error=%s", _log_label(final_url, limit=100), exc)
            return ExtractionReport(
                url=url,
                final_url=final_url,
                content_type=content_type,
                http_status=status,
                raw_bytes=raw_bytes,
                text_chars=0,
                non_blank_lines=0,
                truncated_by_cap=False,
                max_extract_chars=max_extract_chars,
                title="",
                text="",
                first_preview="",
                last_preview="",
                error=f"pdf extract failed: {exc}",
            )
    else:
        html_cache.write_bytes(body)
        raw_html = body.decode("utf-8", errors="replace")
        plain = html_to_plain_text(raw_html)
        title = extract_title_from_html(raw_html, "")

    truncated = len(plain) > max_extract_chars
    text = plain[:max_extract_chars]
    text_cache.write_text(text, encoding="utf-8")
    log.info(
        "Article extract done url=%s chars=%s truncated=%s",
        _log_label(final_url, limit=100),
        len(text),
        truncated,
    )

    return ExtractionReport(
        url=url,
        final_url=final_url,
        content_type=content_type,
        http_status=status,
        raw_bytes=raw_bytes,
        text_chars=len(text),
        non_blank_lines=len([ln for ln in text.splitlines() if ln.strip()]),
        truncated_by_cap=truncated,
        max_extract_chars=max_extract_chars,
        title=title if not is_pdf else "",
        text=text,
        first_preview=text[:400],
        last_preview=text[-400:] if len(text) > 400 else text,
    )


def generate_arabic_name_variants(
    *,
    gemini: GeminiClient,
    model: str,
    english_name: str,
) -> list[str]:
    prompt = f"""You help transliterate person names for Arabic web search.
Given this Latin-script name: "{english_name}"

Return JSON only with this exact shape:
{{"arabic_names": ["..."]}}

Rules:
- Provide exactly 1 Arabic spelling: the single most plausible form used in Arabic news bylines and headlines.
- Use Arabic script only in the strings.
- Do not add commentary outside JSON.
"""
    response = gemini.generate(model=model, prompt=prompt, temperature=0.2)
    raw = extract_gemini_text(response)
    doc = extract_json_document(raw)
    if not isinstance(doc, dict):
        return []
    names = doc.get("arabic_names")
    if not isinstance(names, list):
        return []
    out: list[str] = []
    for item in names:
        s = str(item).strip()
        if s and s not in out:
            out.append(s)
    return out[:1]


def _build_english_query_names(primary_name: str, name_aliases: list[str] | None = None) -> list[str]:
    return _unique_nonempty([primary_name, *(name_aliases or [])])


def _build_gemini_client(settings: Settings) -> GeminiClient | None:
    if not settings.gemini_api_key:
        return None
    base = Path(settings.cache_dir) / "negative_news"
    return GeminiClient(
        api_key=settings.gemini_api_key,
        cache_dir=base / "gemini",
        timeout_seconds=20.0,
        attempts=2,
    )


def _generate_arabic_query_names(
    gemini: GeminiClient | None,
    *,
    model: str,
    english_query_names: list[str],
) -> list[str]:
    if not gemini:
        return []
    lead_name = _unique_nonempty(english_query_names[:1])
    if not lead_name:
        return []
    try:
        return generate_arabic_name_variants(
            gemini=gemini,
            model=model,
            english_name=lead_name[0],
        )
    except Exception as exc:
        log.warning("Arabic name generation failed for %s: %s", lead_name[0], exc)
        return []


def build_mb_queries(
    english_name: str,
    arabic_names: list[str],
    *,
    context_terms: list[str] | None = None,
) -> list[str]:
    """Name-first discovery queries, optionally narrowed with quoted org/context phrases."""
    q_en = f'"{english_name.strip()}"'
    cleaned_terms = [
        " ".join(str(term).split()).strip()
        for term in (context_terms or [])
        if " ".join(str(term).split()).strip()
    ]
    queries = [q_en]
    for term in cleaned_terms:
        queries.append(f'{q_en} "{term}"')
    for ar in arabic_names[:3]:
        qa = f'"{ar}"'
        queries.append(qa)
        for term in cleaned_terms:
            queries.append(f'{qa} "{term}"')
    seen: set[str] = set()
    unique: list[str] = []
    for q in queries:
        key = q.strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(key)
    return unique


def build_cluster_query_specs(
    english_aliases: list[str],
    arabic_aliases: list[str],
    *,
    context_terms: list[str] | None = None,
    broad_pages: int = DEFAULT_PAGES,
    org_pages: int = 2,
) -> list[QuerySpec]:
    specs: list[QuerySpec] = []
    cleaned_terms = _unique_nonempty(context_terms)
    for alias in _unique_nonempty(english_aliases):
        query = f'"{alias}"'
        _append_query_spec(
            specs,
            QuerySpec(
                query=query,
                pages=int(broad_pages),
                bucket="broad",
                language="english",
                required_terms=[],
            ),
        )
        for term in cleaned_terms:
            _append_query_spec(
                specs,
                QuerySpec(
                    query=f'{query} "{term}"',
                    pages=int(org_pages),
                    bucket="org",
                    language="english",
                    required_terms=[term],
                ),
            )
    for alias in _unique_nonempty(arabic_aliases):
        query = f'"{alias}"'
        _append_query_spec(
            specs,
            QuerySpec(
                query=query,
                pages=int(broad_pages),
                bucket="broad",
                language="arabic",
                required_terms=[],
            ),
        )
        for term in cleaned_terms:
            _append_query_spec(
                specs,
                QuerySpec(
                    query=f'{query} "{term}"',
                    pages=int(org_pages),
                    bucket="org",
                    language="arabic",
                    required_terms=[term],
                ),
            )
    return specs


def _collect_search_hits(
    settings: Settings,
    *,
    query_specs: list[QuerySpec],
    num_per_page: int,
    cache_dir: Path,
    max_articles: int | None,
    excluded_urls: set[str] | None = None,
    search_func: Any = serper_search,
) -> list[dict[str, Any]]:
    seen_urls: set[str] = set(excluded_urls or set())
    search_hits: list[dict[str, Any]] = []
    for spec in query_specs:
        log.info(
            "Search query start bucket=%s lang=%s pages=%s required_terms=%s q=%s",
            spec.bucket,
            spec.language,
            spec.pages,
            len(spec.required_terms),
            _log_label(spec.query, limit=100),
        )
        for page_idx in range(1, spec.pages + 1):
            try:
                rows = search_func(
                    settings,
                    query=spec.query,
                    page=page_idx,
                    num=num_per_page,
                    cache_dir=cache_dir,
                )
            except Exception as exc:
                log.warning("Serper failed q=%s page=%s: %s", spec.query[:80], page_idx, exc)
                break
            if not rows:
                log.info(
                    "Search query empty bucket=%s lang=%s page=%s q=%s",
                    spec.bucket,
                    spec.language,
                    page_idx,
                    _log_label(spec.query, limit=100),
                )
                break
            page_kept = 0
            for row in rows:
                link = str(row.get("link") or "").strip()
                if not link or link in seen_urls:
                    continue
                if _should_skip_result_url(link):
                    continue
                seen_urls.add(link)
                page_kept += 1
                search_hits.append(
                    {
                        "query": spec.query,
                        "page": page_idx,
                        "bucket": spec.bucket,
                        "language": spec.language,
                        "required_terms": list(spec.required_terms),
                        "title": row.get("title", ""),
                        "url": link,
                        "snippet": row.get("snippet", ""),
                    }
                )
                if max_articles is not None and len(search_hits) >= max_articles:
                    log.info(
                        "Search hit cap reached hits=%s cap=%s last_q=%s",
                        len(search_hits),
                        max_articles,
                        _log_label(spec.query, limit=100),
                    )
                    return search_hits
            log.info(
                "Search query page done bucket=%s lang=%s page=%s rows=%s kept=%s total_hits=%s q=%s",
                spec.bucket,
                spec.language,
                page_idx,
                len(rows),
                page_kept,
                len(search_hits),
                _log_label(spec.query, limit=100),
            )
    return search_hits


def _collect_cluster_search_hits(
    settings: Settings,
    *,
    query_specs: list[QuerySpec],
    num_per_page: int,
    cache_dir: Path,
    max_articles: int | None,
    search_func: Any = serper_search,
) -> list[dict[str, Any]]:
    broad_specs = [spec for spec in query_specs if spec.bucket == "broad"]
    org_specs = [spec for spec in query_specs if spec.bucket == "org"]
    if max_articles is None or not broad_specs or not org_specs:
        return _collect_search_hits(
            settings,
            query_specs=query_specs,
            num_per_page=num_per_page,
            cache_dir=cache_dir,
            max_articles=max_articles,
            search_func=search_func,
        )

    broad_budget = max(1, max_articles // 2)
    org_budget = max(1, max_articles - broad_budget)
    if broad_budget + org_budget > max_articles:
        broad_budget = max(0, max_articles - org_budget)

    broad_hits = _collect_search_hits(
        settings,
        query_specs=broad_specs,
        num_per_page=num_per_page,
        cache_dir=cache_dir,
        max_articles=broad_budget,
        search_func=search_func,
    )
    seen_urls = {str(hit.get("url") or "").strip() for hit in broad_hits if str(hit.get("url") or "").strip()}
    org_hits = _collect_search_hits(
        settings,
        query_specs=org_specs,
        num_per_page=num_per_page,
        cache_dir=cache_dir,
        max_articles=org_budget,
        excluded_urls=seen_urls,
        search_func=search_func,
    )
    collected = [*broad_hits, *org_hits]
    remaining = max_articles - len(collected)
    if remaining <= 0:
        return collected

    extra_hits = _collect_search_hits(
        settings,
        query_specs=query_specs,
        num_per_page=num_per_page,
        cache_dir=cache_dir,
        max_articles=remaining,
        excluded_urls={
            str(hit.get("url") or "").strip()
            for hit in collected
            if str(hit.get("url") or "").strip()
        },
        search_func=search_func,
    )
    return [*collected, *extra_hits]


def classify_article_mb(
    *,
    gemini: GeminiClient,
    model: str,
    person_name: str,
    article_title: str,
    article_url: str,
    article_text: str,
    max_chars_for_model: int = 120_000,
) -> dict[str, Any]:
    """Strict JSON classification using outlet plus article text."""
    text = article_text[:max_chars_for_model]
    tail_note = ""
    if len(article_text) > max_chars_for_model:
        tail = article_text[-20_000:]
        tail_note = f"\n\n[Article truncated for this prompt; end excerpt for context:]\n{tail}"
    outlet_domain = _normalize_domain(article_url)

    prompt = f"""You are screening news for links to the Muslim Brotherhood (MB) / الإخوان المسلمين and the wider Islamist ecosystem.

Person being screened: "{person_name}"
Article URL: {article_url}
Outlet domain: {outlet_domain or "(unknown)"}
Article title: {article_title}

Read the article text. Return JSON only with this shape:
{{
  "category": "explicit_mb_connection" | "writes_for_mb_outlet" | "other_mb_alignment" | "reject",
  "confidence": 0.0,
  "short_rationale": "",
  "evidence_quote": ""
}}

Category definitions (be strict; prefer "reject" when unsure):
- explicit_mb_connection: The article clearly ties THIS person to the Muslim Brotherhood, Ikhwan, or another clearly Islamist organization/network as membership, leadership, official role, or direct organizational link.
- writes_for_mb_outlet: The article states or clearly implies this person writes for, edits, or is a regular contributor to an outlet identified as MB-affiliated, Islamist, or clearly aligned with an Islamist movement/network (not generic Arab media).
- other_mb_alignment: Clear signal of alignment or wider Islamist connectedness (e.g. speaking for MB, described as an Islamist figure, recurring role in clearly Islamist organizations/events/media) short of the above, still about THIS person.
- reject: Name match only, different person, no MB link, or insufficient evidence.

Rules:
- Use BOTH the article text and the outlet/domain context.
- Do not treat a result as relevant just because the outlet is Arabic, Islamist, or politically charged.
- Do not treat a result as relevant just because the article discusses the Muslim Brotherhood, Islamism, or political Islam in general.
- The article must be about this specific person, or clearly identify them as a writer/contributor for the outlet.
- Prefer reject if the name is absent from the text or only appears in unrelated metadata/search noise.

If category is reject, set evidence_quote to "".

Article text:
{text}{tail_note}
"""
    response = gemini.generate(model=model, prompt=prompt, temperature=0.1)
    raw = extract_gemini_text(response)
    return extract_json_document(raw)  # type: ignore[return-value]


def load_negative_news_clusters(
    repository: Repository,
    *,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    run_ids = repository.get_latest_unique_run_ids()
    if not run_ids:
        return {"run_ids": [], "clusters": []}
    cached = _load_cluster_source_cache(repository, run_ids=run_ids)
    if cached is not None:
        clusters = list(cached.get("clusters") or [])
        start = max(0, int(offset))
        end = start + max(0, int(limit))
        log.info(
            "Cluster source cache hit total_available=%s offset=%s limit=%s",
            int(cached.get("total_available") or 0),
            offset,
            limit,
        )
        return {
            "run_ids": run_ids,
            "clusters": clusters[start:end],
            "total_available": int(cached.get("total_available") or 0),
        }
    from scripts.consolidate_and_graph import consolidate_multi_run

    log.info("Cluster source build start run_ids=%s", len(run_ids))
    with io.StringIO() as buffer, redirect_stdout(buffer):
        graph = consolidate_multi_run(run_ids)
    nodes = [
        node for node in graph.get("nodes", [])
        if (
            str(node.get("kind") or "") == "person"
            and str(node.get("id") or "").startswith("merged_person:")
        ) or str(node.get("kind") or "") == "seed_alias"
    ]
    nodes.sort(
        key=lambda node: (
            -float(node.get("score") or 0.0),
            -int(node.get("org_count") or 0),
            str(node.get("label") or ""),
        )
    )
    clusters: list[dict[str, Any]] = []
    for node in nodes:
        person_ids = sorted({int(person_id) for person_id in (node.get("person_ids") or [])})
        aliases = _unique_nonempty([str(node.get("label") or ""), *(node.get("aliases") or [])])
        identity_keys = _unique_nonempty([str(value) for value in (node.get("identity_keys") or [])])
        cluster = {
            "cluster_id": str(node.get("id") or ""),
            "cluster_kind": _cluster_kind(str(node.get("id") or ""), node.get("kind")),
            "label": str(node.get("label") or ""),
            "aliases": aliases,
            "identity_keys": identity_keys,
            "person_ids": person_ids,
            "org_count": int(node.get("org_count") or 0),
            "role_count": int(node.get("role_count") or 0),
            "score": float(node.get("score") or 0.0),
            "context_terms": repository.get_organisation_names_for_person_ids(person_ids),
        }
        cluster["cluster_lookup_key"] = cluster_lookup_key(cluster)
        clusters.append(
            cluster
        )
    _write_cluster_source_cache(repository, run_ids=run_ids, clusters=clusters)
    start = max(0, int(offset))
    end = start + max(0, int(limit))
    log.info("Cluster source build done total_available=%s", len(clusters))
    return {"run_ids": run_ids, "clusters": clusters[start:end], "total_available": len(clusters)}


def partition_negative_news_clusters_by_history(
    store: NegativeNewsStore,
    clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    historical_by_cluster_id = store.get_latest_completed_results_by_cluster_id()
    historical_by_cluster_lookup_key = store.get_latest_completed_results_by_cluster_lookup_key()
    historical_by_person_ids = store.get_latest_completed_results_by_person_ids()
    pending_clusters: list[dict[str, Any]] = []
    reused_clusters: list[dict[str, Any]] = []
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id") or "")
        cluster_kind = _cluster_kind(cluster_id, cluster.get("cluster_kind"))
        lookup_key = str(cluster.get("cluster_lookup_key") or cluster_lookup_key(cluster))
        fingerprint = person_ids_fingerprint(cluster.get("person_ids"))
        historical_match = historical_by_cluster_id.get(cluster_id)
        if historical_match is None and lookup_key:
            candidate = historical_by_cluster_lookup_key.get(lookup_key)
            if candidate is not None and _cluster_kind(
                str(candidate.get("cluster_id") or ""),
                (candidate.get("result") or {}).get("cluster_kind"),
            ) == cluster_kind:
                historical_match = candidate
        if historical_match is None and cluster_kind == "person" and fingerprint:
            candidate = historical_by_person_ids.get(fingerprint)
            if candidate is not None and _cluster_kind(
                str(candidate.get("cluster_id") or ""),
                (candidate.get("result") or {}).get("cluster_kind"),
            ) == "person":
                historical_match = candidate
        if historical_match:
            reused_clusters.append(
                {
                    **cluster,
                    "historical_cluster_id": str(historical_match.get("cluster_id") or ""),
                    "historical_label": str(historical_match.get("label") or ""),
                    "historical_cluster_kind": _cluster_kind(
                        str(historical_match.get("cluster_id") or ""),
                        (historical_match.get("result") or {}).get("cluster_kind"),
                    ),
                    "cluster_lookup_key": lookup_key,
                    "person_ids_fingerprint": fingerprint,
                }
            )
            continue
        pending_clusters.append(
            {
                **cluster,
                "cluster_lookup_key": lookup_key,
                "person_ids_fingerprint": fingerprint,
            }
        )
    return {
        "pending_clusters": pending_clusters,
        "reused_clusters": reused_clusters,
        "historical_match_count": len(reused_clusters),
    }


def run_negative_news_cluster_batch(
    settings: Settings,
    repository: Repository,
    *,
    offset: int = 0,
    limit: int = 50,
    broad_pages: int = DEFAULT_PAGES,
    org_pages: int = 2,
    num_per_page: int = DEFAULT_NUM_PER_PAGE,
    max_extract_chars: int = DEFAULT_MAX_EXTRACT_CHARS,
    max_articles_per_cluster: int | None = 100,
    classify: bool = True,
) -> dict[str, Any]:
    if not settings.serper_api_key:
        raise RuntimeError("SERPER_API_KEY is required")
    if classify and not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is required when classification is enabled")

    base = Path(settings.cache_dir) / "negative_news"
    serper_dir = base / "serper"
    fetch_dir = base / "fetch"
    model = settings.gemini_resolution_model
    gemini = _build_gemini_client(settings)
    store = NegativeNewsStore(
        _negative_news_db_path(settings),
        settings.project_root / "src" / "storage" / "negative_news_schema.sql",
    )
    store.init_db()

    log.info("Cluster batch load start offset=%s limit=%s", offset, limit)
    cluster_source = load_negative_news_clusters(repository, offset=offset, limit=limit)
    current_clusters = list(cluster_source.get("clusters") or [])
    screening_partition = partition_negative_news_clusters_by_history(store, current_clusters)
    pending_clusters = list(screening_partition.get("pending_clusters") or [])
    reused_clusters = list(screening_partition.get("reused_clusters") or [])
    cluster_ranks = {
        str(cluster.get("cluster_id") or ""): max(0, int(offset)) + index
        for index, cluster in enumerate(current_clusters, start=1)
    }
    log.info(
        "Cluster batch load done offset=%s limit=%s clusters=%s pending=%s reused=%s total_available=%s run_ids=%s",
        offset,
        limit,
        len(current_clusters),
        len(pending_clusters),
        len(reused_clusters),
        int(cluster_source.get("total_available") or 0),
        len(cluster_source.get("run_ids") or []),
    )
    if not pending_clusters:
        return {
            "meta": {
                "mode": "cluster_batch",
                "offset": int(offset),
                "cluster_limit": limit,
                "run_ids": cluster_source["run_ids"],
                "total_available": cluster_source["total_available"],
                "negative_news_db_path": str(_negative_news_db_path(settings)),
                "batch_run_id": None,
                "broad_pages": broad_pages,
                "org_pages": org_pages,
                "num_per_page": num_per_page,
                "max_articles_per_cluster": max_articles_per_cluster,
                "requested_cluster_count": len(current_clusters),
                "pending_cluster_count": 0,
                "historically_screened_count": len(reused_clusters),
            },
            "clusters": [],
            "category_counts": {},
            "interesting": [],
            "historically_screened_clusters": reused_clusters,
        }
    output_path = (
        settings.project_root
        / "data"
        / f"negative_news_clusters_offset{int(offset)}_limit{int(limit)}.json"
    )
    run_config = {
        "mode": "cluster_batch",
        "offset": int(offset),
        "limit": int(limit),
        "broad_pages": int(broad_pages),
        "org_pages": int(org_pages),
        "num_per_page": int(num_per_page),
        "max_extract_chars": int(max_extract_chars),
        "max_articles_per_cluster": None if max_articles_per_cluster is None else int(max_articles_per_cluster),
        "classify": bool(classify),
        "run_ids": cluster_source["run_ids"],
    }
    batch_run_id = store.get_or_create_batch_run(
        config=run_config,
        offset_value=int(offset),
        limit_value=int(limit),
        total_clusters=len(pending_clusters),
        output_path=str(output_path),
    )
    completed_cluster_ids = store.get_completed_cluster_ids(batch_run_id)
    out_clusters: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    interesting: list[dict[str, Any]] = []
    for row in store.get_cluster_results(batch_run_id):
        try:
            result = json.loads(str(row["result_json"] or "{}"))
        except json.JSONDecodeError:
            continue
        if isinstance(result, dict) and result:
            out_clusters.append(result)
        try:
            counts = json.loads(str(row["category_counts_json"] or "{}"))
        except json.JSONDecodeError:
            counts = {}
        if isinstance(counts, dict):
            for key, value in counts.items():
                category_counts[str(key)] = category_counts.get(str(key), 0) + int(value or 0)
        if not isinstance(result, dict):
            continue
        for article in result.get("articles", []):
            classification = article.get("classification") or {}
            category = str(classification.get("category") or "").strip()
            if not category or category == "reject":
                continue
            interesting.append(
                {
                    "cluster_id": result.get("cluster_id"),
                    "cluster_label": result.get("label"),
                    "category": category,
                    "confidence": classification.get("confidence"),
                    "short_rationale": classification.get("short_rationale"),
                    "evidence_quote": classification.get("evidence_quote"),
                    "url": article.get("search", {}).get("url"),
                    "title": article.get("search", {}).get("title"),
                    "required_term_matches": article.get("required_term_matches", {}),
                }
            )

    for cluster in pending_clusters:
        cluster_rank = cluster_ranks.get(str(cluster.get("cluster_id") or ""), max(0, int(offset)) + 1)
        if cluster["cluster_id"] in completed_cluster_ids:
            continue
        log.info(
            "Cluster start rank=%s label=%s person_ids=%s aliases=%s context_terms=%s",
            cluster_rank,
            _log_label(str(cluster.get("label") or ""), limit=100),
            len(cluster.get("person_ids") or []),
            len(cluster.get("aliases") or []),
            len(cluster.get("context_terms") or []),
        )
        english_aliases = _unique_nonempty(cluster.get("aliases") or [cluster.get("label") or ""])
        arabic_aliases = _generate_arabic_query_names(
            gemini,
            model=model,
            english_query_names=english_aliases,
        )
        log.info(
            "Cluster aliases rank=%s english=%s arabic=%s lead=%s arabic_aliases=%s",
            cluster_rank,
            len(english_aliases),
            len(arabic_aliases),
            _log_label(english_aliases[0] if english_aliases else "", limit=80),
            ", ".join(_log_label(alias, limit=40) for alias in arabic_aliases) or "(none)",
        )
        context_terms = _unique_nonempty(cluster.get("context_terms") or [])
        query_specs = build_cluster_query_specs(
            english_aliases,
            arabic_aliases,
            context_terms=context_terms,
            broad_pages=broad_pages,
            org_pages=org_pages,
        )
        log.info(
            "Cluster query plan rank=%s broad_queries=%s org_queries=%s",
            cluster_rank,
            len([spec for spec in query_specs if spec.bucket == "broad"]),
            len([spec for spec in query_specs if spec.bucket == "org"]),
        )
        search_hits = _collect_cluster_search_hits(
            settings,
            query_specs=query_specs,
            num_per_page=num_per_page,
            cache_dir=serper_dir,
            max_articles=max_articles_per_cluster,
        )
        log.info(
            "Cluster search complete rank=%s label=%s hits=%s",
            cluster_rank,
            _log_label(str(cluster.get("label") or ""), limit=100),
            len(search_hits),
        )
        articles_out: list[dict[str, Any]] = []
        cluster_error = ""
        try:
            for article_idx, hit in enumerate(search_hits, start=1):
                url = hit["url"]
                log.info(
                    "Cluster article start rank=%s item=%s/%s bucket=%s url=%s",
                    cluster_rank,
                    article_idx,
                    len(search_hits),
                    hit.get("bucket"),
                    _log_label(url, limit=100),
                )
                ex = fetch_and_extract_article(
                    settings,
                    url,
                    cache_dir=fetch_dir,
                    max_extract_chars=max_extract_chars,
                )
                entry: dict[str, Any] = {
                    "search": hit,
                    "extraction": extraction_report_summary(ex),
                }
                required_matches = _required_term_match_locations(
                    hit.get("required_terms"),
                    title=str(hit.get("title") or ""),
                    snippet=str(hit.get("snippet") or ""),
                    extracted_text=ex.text,
                )
                entry["required_term_matches"] = required_matches
                if hit.get("required_terms") and not required_matches:
                    log.info(
                        "Cluster article filtered rank=%s item=%s reason=required_org_term_absent url=%s",
                        cluster_rank,
                        article_idx,
                        _log_label(url, limit=100),
                    )
                    entry["classification"] = None
                    entry["filtered_out"] = "required_org_term_absent"
                    continue
                if ex.error or not ex.text.strip():
                    log.info(
                        "Cluster article extraction issue rank=%s item=%s error=%s chars=%s url=%s",
                        cluster_rank,
                        article_idx,
                        _log_label(str(ex.error or ""), limit=80) or "(empty_text)",
                        ex.text_chars,
                        _log_label(url, limit=100),
                    )
                    entry["classification"] = None
                    articles_out.append(entry)
                    continue

                if classify and gemini:
                    try:
                        log.info(
                            "Cluster article classify start rank=%s item=%s url=%s",
                            cluster_rank,
                            article_idx,
                            _log_label(ex.final_url, limit=100),
                        )
                        cls = classify_article_mb(
                            gemini=gemini,
                            model=model,
                            person_name=str(cluster.get("label") or ""),
                            article_title=ex.title or str(hit.get("title") or ""),
                            article_url=ex.final_url,
                            article_text=ex.text,
                        )
                        entry["classification"] = cls
                        log.info(
                            "Cluster article classify done rank=%s item=%s category=%s confidence=%s url=%s",
                            cluster_rank,
                            article_idx,
                            _log_label(str((cls or {}).get("category") or ""), limit=40),
                            (cls or {}).get("confidence"),
                            _log_label(ex.final_url, limit=100),
                        )
                    except Exception as exc:
                        log.warning(
                            "Cluster article classify failed rank=%s item=%s url=%s error=%s",
                            cluster_rank,
                            article_idx,
                            _log_label(ex.final_url, limit=100),
                            exc,
                        )
                        entry["classification"] = {"error": str(exc)}
                else:
                    entry["classification"] = None
                articles_out.append(entry)
        except Exception as exc:
            cluster_error = str(exc)
            log.exception(
                "Cluster failed rank=%s label=%s error=%s",
                cluster_rank,
                _log_label(str(cluster.get("label") or ""), limit=100),
                exc,
            )

        for article in articles_out:
            classification = article.get("classification") or {}
            category = str(classification.get("category") or "").strip()
            if not category or category == "reject":
                continue
            category_counts[category] = category_counts.get(category, 0) + 1
            interesting.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "cluster_label": cluster["label"],
                    "category": category,
                    "confidence": classification.get("confidence"),
                    "short_rationale": classification.get("short_rationale"),
                    "evidence_quote": classification.get("evidence_quote"),
                    "url": article.get("search", {}).get("url"),
                    "title": article.get("search", {}).get("title"),
                    "required_term_matches": article.get("required_term_matches", {}),
                }
            )

        cluster_result = {
            "cluster_id": cluster["cluster_id"],
            "cluster_kind": _cluster_kind(cluster["cluster_id"], cluster.get("cluster_kind")),
            "cluster_lookup_key": str(cluster.get("cluster_lookup_key") or ""),
            "label": cluster["label"],
            "aliases": english_aliases,
            "arabic_aliases": arabic_aliases,
            "identity_keys": list(cluster.get("identity_keys") or []),
            "person_ids": cluster["person_ids"],
            "context_terms": context_terms,
            "org_count": cluster["org_count"],
            "role_count": cluster["role_count"],
            "score": cluster["score"],
            "broad_queries": [spec.query for spec in query_specs if spec.bucket == "broad"],
            "org_queries": [spec.query for spec in query_specs if spec.bucket == "org"],
            "articles": articles_out,
        }
        out_clusters.append(cluster_result)
        local_category_counts: dict[str, int] = {}
        for article in articles_out:
            category = str((article.get("classification") or {}).get("category") or "").strip()
            if not category or category == "reject":
                continue
            local_category_counts[category] = local_category_counts.get(category, 0) + 1
        store.upsert_cluster_result(
            batch_run_id=batch_run_id,
            cluster_rank=cluster_rank,
            cluster_id=str(cluster["cluster_id"]),
            label=str(cluster["label"]),
            status="completed" if not cluster_error else "failed",
            interesting_count=sum(local_category_counts.values()),
            category_counts=local_category_counts,
            result=cluster_result,
            error_text=cluster_error,
        )
        log.info(
            "Cluster done rank=%s label=%s stored_articles=%s interesting=%s status=%s",
            cluster_rank,
            _log_label(str(cluster.get("label") or ""), limit=100),
            len(articles_out),
            sum(local_category_counts.values()),
            "failed" if cluster_error else "completed",
        )

    store.mark_batch_completed(batch_run_id)
    out_clusters.sort(key=lambda item: (-float(item.get("score") or 0.0), -int(item.get("org_count") or 0), str(item.get("label") or "")))
    return {
        "meta": {
            "mode": "cluster_batch",
            "offset": int(offset),
            "cluster_limit": limit,
            "run_ids": cluster_source["run_ids"],
            "total_available": cluster_source["total_available"],
            "negative_news_db_path": str(_negative_news_db_path(settings)),
            "batch_run_id": batch_run_id,
            "broad_pages": broad_pages,
            "org_pages": org_pages,
            "num_per_page": num_per_page,
            "max_articles_per_cluster": max_articles_per_cluster,
            "requested_cluster_count": len(current_clusters),
            "pending_cluster_count": len(pending_clusters),
            "historically_screened_count": len(reused_clusters),
        },
        "clusters": out_clusters,
        "category_counts": category_counts,
        "interesting": interesting,
        "historically_screened_clusters": reused_clusters,
    }


def run_negative_news_pilot(
    settings: Settings,
    *,
    names: list[str],
    context_terms: list[str] | None = None,
    name_aliases: list[str] | None = None,
    pages: int = DEFAULT_PAGES,
    num_per_page: int = DEFAULT_NUM_PER_PAGE,
    max_extract_chars: int = DEFAULT_MAX_EXTRACT_CHARS,
    max_articles_per_person: int | None = 40,
    classify: bool = True,
) -> dict[str, Any]:
    if not settings.serper_api_key:
        raise RuntimeError("SERPER_API_KEY is required")
    if classify and not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is required when classification is enabled")

    base = Path(settings.cache_dir) / "negative_news"
    serper_dir = base / "serper"
    fetch_dir = base / "fetch"

    model = settings.gemini_resolution_model
    gemini = _build_gemini_client(settings)

    out_people: list[dict[str, Any]] = []

    for raw_name in names:
        name = " ".join(str(raw_name).split()).strip()
        if not name:
            continue
        variants = generate_name_variants(name, creativity_level="balanced")
        primary_en = variants[0].name if variants else name
        english_query_names = _build_english_query_names(primary_en, name_aliases)
        arabic = _generate_arabic_query_names(
            gemini,
            model=model,
            english_query_names=english_query_names,
        )

        queries: list[str] = []
        for english_query_name in english_query_names:
            for query in build_mb_queries(
                english_query_name,
                arabic,
                context_terms=context_terms,
            ):
                if query not in queries:
                    queries.append(query)
        query_specs = [
            QuerySpec(
                query=query,
                pages=pages,
                bucket="mixed",
                language="mixed",
                required_terms=[term for term in (context_terms or []) if f'"{term}"' in query],
            )
            for query in queries
        ]
        search_hits = _collect_search_hits(
            settings,
            query_specs=query_specs,
            num_per_page=num_per_page,
            cache_dir=serper_dir,
            max_articles=max_articles_per_person,
        )

        articles_out: list[dict[str, Any]] = []
        for hit in search_hits:
            url = hit["url"]
            ex = fetch_and_extract_article(
                settings,
                url,
                cache_dir=fetch_dir,
                max_extract_chars=max_extract_chars,
            )
            entry: dict[str, Any] = {
                "search": hit,
                "extraction": extraction_report_summary(ex),
            }
            required_matches = _required_term_match_locations(
                hit.get("required_terms"),
                title=str(hit.get("title") or ""),
                snippet=str(hit.get("snippet") or ""),
                extracted_text=ex.text,
            )
            entry["required_term_matches"] = required_matches
            if hit.get("required_terms") and not required_matches:
                entry["classification"] = None
                entry["filtered_out"] = "required_org_term_absent"
                continue
            if ex.error or not ex.text.strip():
                entry["classification"] = None
                articles_out.append(entry)
                continue

            if classify and gemini:
                try:
                    cls = classify_article_mb(
                        gemini=gemini,
                        model=model,
                        person_name=primary_en,
                        article_title=ex.title or str(hit.get("title") or ""),
                        article_url=ex.final_url,
                        article_text=ex.text,
                    )
                    entry["classification"] = cls
                except Exception as exc:
                    entry["classification"] = {"error": str(exc)}
            else:
                entry["classification"] = None
            articles_out.append(entry)

        out_people.append(
            {
                "input_name": name,
                "primary_english": primary_en,
                "english_query_names": english_query_names,
                "arabic_name_variants": arabic,
                "queries": queries,
                "articles": articles_out,
            }
        )

    return {"people": out_people}

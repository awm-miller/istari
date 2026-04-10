"""Standalone adverse-media pilot: Serper discovery + full-page text extraction + Gemini classification."""

from __future__ import annotations

import hashlib
import json
import logging
import re
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

log = logging.getLogger("istari.negative_news")

DEFAULT_MAX_EXTRACT_CHARS = 500_000
DEFAULT_PAGES = 10
DEFAULT_NUM_PER_PAGE = 10
_FETCH_TIMEOUT = 60
_SKIP_RESULT_DOMAINS = {
    "charitycommission.gov.uk",
    "facebook.com",
    "find-and-update.company-information.service.gov.uk",
    "instagram.com",
    "linkedin.com",
    "register-of-charities.charitycommission.gov.uk",
    "company-information.service.gov.uk",
    "youtube.com",
    "youtu.be",
    "twitter.com",
    "x.com",
    "tiktok.com",
}


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
    return any(host == blocked or host.endswith(f".{blocked}") for blocked in _SKIP_RESULT_DOMAINS)


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
        return json.loads(cache_file.read_text(encoding="utf-8"))

    url = f"{settings.serper_base_url}/search"
    payload = json.dumps(payload_obj).encode("utf-8")
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
    cache_file.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def _looks_like_pdf(url: str) -> bool:
    return bool(re.search(r"\.pdf($|[?#])", str(url or ""), flags=re.IGNORECASE))


def _extract_pdf_text(data: bytes) -> str:
    import fitz  # PyMuPDF

    doc = fitz.open(stream=data, filetype="pdf")
    parts: list[str] = []
    try:
        for page in doc:
            parts.append(page.get_text())
    finally:
        doc.close()
    return "\n\n".join(p for p in parts if p and str(p).strip())


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
    try:
        with request.urlopen(req, timeout=_FETCH_TIMEOUT) as resp:
            final_url = str(resp.geturl() or url)
            status = int(getattr(resp, "status", 200) or 200)
            content_type = str(resp.headers.get("Content-Type", "") or "")
            body = resp.read()
    except error.HTTPError as exc:
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
    if is_pdf:
        pdf_cache.write_bytes(body)
        try:
            plain = _extract_pdf_text(body)
        except Exception as exc:
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
{{"arabic_names": ["...", "..."]}}

Rules:
- Provide 1 to 4 plausible Arabic spellings as used in Arabic news bylines and headlines.
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
    return out[:6]


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

    gemini: GeminiClient | None = None
    model = settings.gemini_resolution_model
    if classify and settings.gemini_api_key:
        gemini = GeminiClient(api_key=settings.gemini_api_key, cache_dir=base / "gemini")

    out_people: list[dict[str, Any]] = []

    for raw_name in names:
        name = " ".join(str(raw_name).split()).strip()
        if not name:
            continue
        variants = generate_name_variants(name, creativity_level="balanced")
        primary_en = variants[0].name if variants else name
        english_query_names: list[str] = []
        for candidate in [primary_en, *(name_aliases or [])]:
            clean = " ".join(str(candidate).split()).strip()
            if clean and clean not in english_query_names:
                english_query_names.append(clean)
        arabic: list[str] = []
        if gemini:
            try:
                for english_query_name in english_query_names:
                    generated = generate_arabic_name_variants(
                        gemini=gemini,
                        model=model,
                        english_name=english_query_name,
                    )
                    for item in generated:
                        if item not in arabic:
                            arabic.append(item)
            except Exception as exc:
                log.warning("Arabic name generation failed for %s: %s", name, exc)
                arabic = []

        queries: list[str] = []
        for english_query_name in english_query_names:
            for query in build_mb_queries(
                english_query_name,
                arabic,
                context_terms=context_terms,
            ):
                if query not in queries:
                    queries.append(query)
        seen_urls: set[str] = set()
        search_hits: list[dict[str, Any]] = []

        for query in queries:
            for page_idx in range(1, pages + 1):
                try:
                    rows = serper_search(
                        settings,
                        query=query,
                        page=page_idx,
                        num=num_per_page,
                        cache_dir=serper_dir,
                    )
                except Exception as exc:
                    log.warning("Serper failed q=%s page=%s: %s", query[:80], page_idx, exc)
                    break
                if not rows:
                    break
                for row in rows:
                    link = str(row.get("link") or "").strip()
                    if not link or link in seen_urls:
                        continue
                    if _should_skip_result_url(link):
                        continue
                    required_terms = [term for term in (context_terms or []) if f'"{term}"' in query]
                    seen_urls.add(link)
                    search_hits.append(
                        {
                            "query": query,
                            "page": page_idx,
                            "required_terms": required_terms,
                            "title": row.get("title", ""),
                            "url": link,
                            "snippet": row.get("snippet", ""),
                        }
                    )
                    if max_articles_per_person is not None and len(search_hits) >= max_articles_per_person:
                        break
                if max_articles_per_person is not None and len(search_hits) >= max_articles_per_person:
                    break
            if max_articles_per_person is not None and len(search_hits) >= max_articles_per_person:
                break

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

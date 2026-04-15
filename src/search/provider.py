from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urlparse

from src.charity_commission.identifiers import extract_charity_number_from_url
from src.companies_house.client import CompaniesHouseClient, extract_officer_id
from src.companies_house.relationships import (
    company_relationship_kind,
    company_relationship_phrase,
    company_role_type,
)
from src.config import Settings
from src.models import EvidenceItem, NameVariant
from src.search.queries import build_dork_queries, normalize_name

log = logging.getLogger("istari.search")

DORK_RESULTS_PER_QUERY = 8
DORK_DELAY_SECONDS = 1.0

_TRUSTED_DORK_HOST_SUFFIXES = (
    "charitycommission.gov.uk",
    "register-of-charities.charitycommission.gov.uk",
    "gov.uk",
    "landregistry.data.gov.uk",
    "find-and-update.company-information.service.gov.uk",
    "company-information.service.gov.uk",
    "companieshouse.gov.uk",
)

_ORG_SIGNAL_TERMS = (
    "charity",
    "charitable",
    "trust",
    "foundation",
    "registered charity",
    "charity number",
    "company number",
    "land registry",
    "title register",
)

_GOVERNANCE_SIGNAL_TERMS = (
    "trustee",
    "director",
    "secretary",
    "annual report",
    "accounts",
    "governance",
    "governing document",
    "examiner",
    "auditor",
    "accountant",
)

_NOISE_TERMS = (
    "project gutenberg",
    "doi.org",
    "journal",
    "corrosion",
    "nature-based solutions",
    "springer",
    "sciencedirect",
)


class SearchProvider:
    def search(self, variants: list[NameVariant]) -> list[EvidenceItem]:
        raise NotImplementedError


class NullSearchProvider(SearchProvider):
    def search(self, variants: list[NameVariant]) -> list[EvidenceItem]:
        return []


@dataclass(slots=True)
class WebDorkSearchProvider(SearchProvider):
    """Free web search using ddgs (DuckDuckGo) with dorking queries."""

    settings: Settings
    cache_dir: Path = field(init=False)
    metrics: dict[str, int] = field(init=False)

    def __post_init__(self) -> None:
        self.cache_dir = Path(self.settings.cache_dir) / "web_dork"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metrics = {
            "serper_live": 0,
            "serper_cache": 0,
            "ddgs_live": 0,
            "ddgs_cache": 0,
            "fallback_count": 0,
            "filtered_out": 0,
            "kept_after_filter": 0,
        }

    def search(self, variants: list[NameVariant]) -> list[EvidenceItem]:
        self.metrics = {
            "serper_live": 0,
            "serper_cache": 0,
            "ddgs_live": 0,
            "ddgs_cache": 0,
            "fallback_count": 0,
            "filtered_out": 0,
            "kept_after_filter": 0,
        }
        evidence: list[EvidenceItem] = []
        seen_urls: set[str] = set()
        total_queries = sum(len(build_dork_queries(v)) for v in variants)
        done = 0
        for variant in variants:
            queries = build_dork_queries(variant)
            for query in queries:
                done += 1
                for item in self._search_query(variant, query):
                    if item.url and item.url in seen_urls:
                        continue
                    if item.url:
                        seen_urls.add(item.url)
                    evidence.append(item)
                if done % 10 == 0 or done == total_queries:
                    log.info("  Dork search progress: %d/%d queries, %d results so far", done, total_queries, len(evidence))
        log.info("  Dork backend metrics: %s", self.metrics)
        return evidence

    def _search_query(self, variant: NameVariant, query: str) -> list[EvidenceItem]:
        results = self._cached_search(query)
        items: list[EvidenceItem] = []
        for index, result in enumerate(results, start=1):
            url = result.get("href") or result.get("url") or ""
            title = result.get("title", "")
            snippet = result.get("body", "")
            if not _is_relevant_dork_result(url=url, title=title, snippet=snippet):
                self.metrics["filtered_out"] += 1
                continue
            self.metrics["kept_after_filter"] += 1
            items.append(
                EvidenceItem(
                    source="web_dork_search",
                    source_key=f"{variant.name}:{index}:{url}",
                    title=title,
                    url=url or None,
                    snippet=snippet,
                    raw_payload={
                        "variant": variant.name,
                        "query": query,
                        "result": result,
                    },
                )
            )
        return items

    def _cached_search(self, query: str) -> list[dict[str, Any]]:
        cache_key = hashlib.sha256(query.encode()).hexdigest()[:16]

        if self.settings.serper_api_key:
            serper_cache = self.cache_dir / f"serper_{cache_key}.json"
            if serper_cache.exists():
                cached = json.loads(serper_cache.read_text(encoding="utf-8"))
                self.metrics["serper_cache"] += 1
                log.info("  Dork backend: serper (cache), %d results", len(cached))
                return cached
            try:
                results = self._search_serper(query)
                if results:
                    serper_cache.write_text(
                        json.dumps(results, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    self.metrics["serper_live"] += 1
                    log.info("  Dork backend: serper (live), %d results", len(results))
                    return results
                self.metrics["fallback_count"] += 1
                log.info("  Serper returned no rows, falling back to DDGS")
            except RuntimeError as exc:
                self.metrics["fallback_count"] += 1
                log.warning("  Serper search failed, falling back to DDGS: %s", exc)

        ddgs_cache = self.cache_dir / f"ddgs_{cache_key}.json"
        if ddgs_cache.exists():
            cached = json.loads(ddgs_cache.read_text(encoding="utf-8"))
            self.metrics["ddgs_cache"] += 1
            log.info("  Dork backend: ddgs (cache), %d results", len(cached))
            return cached

        from ddgs import DDGS
        time.sleep(DORK_DELAY_SECONDS)
        try:
            results = list(DDGS().text(query, max_results=DORK_RESULTS_PER_QUERY))
            self.metrics["ddgs_live"] += 1
            log.info("  Dork backend: ddgs (live), %d results", len(results))
        except Exception:
            log.warning("  Dork search failed for query: %s", query)
            results = []
        ddgs_cache.write_text(json.dumps(results, ensure_ascii=False), encoding="utf-8")
        return results

    def _search_serper(self, query: str) -> list[dict[str, Any]]:
        url = f"{self.settings.serper_base_url}/search"
        queries_to_try = [query]
        relaxed = _relax_query_for_serper(query)
        if relaxed and relaxed != query:
            queries_to_try.append(relaxed)

        for q in queries_to_try:
            payload = json.dumps({"q": q, "num": DORK_RESULTS_PER_QUERY}).encode("utf-8")
            req = request.Request(
                url=url,
                data=payload,
                method="POST",
                headers={
                    "X-API-KEY": str(self.settings.serper_api_key),
                    "Content-Type": "application/json",
                    "User-Agent": self.settings.user_agent,
                },
            )
            try:
                with request.urlopen(req) as response:
                    body = json.loads(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                message = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Serper request failed: {exc.code} {message}") from exc
            except Exception as exc:
                raise RuntimeError(f"Serper request failed: {exc}") from exc

            organic = body.get("organic", [])
            if not isinstance(organic, list) or not organic:
                continue
            rows: list[dict[str, Any]] = []
            for item in organic:
                if not isinstance(item, dict):
                    continue
                rows.append(
                    {
                        "title": item.get("title", ""),
                        "href": item.get("link") or item.get("url") or "",
                        "body": item.get("snippet", ""),
                    }
                )
            if rows:
                return rows
        return []


def _relax_query_for_serper(query: str) -> str:
    simplified = query
    simplified = simplified.replace(" OR ", " ")
    simplified = re.sub(r"\bsite:[^\s]+", " ", simplified, flags=re.IGNORECASE)
    simplified = re.sub(r'"', " ", simplified)
    simplified = re.sub(r"\s+", " ", simplified).strip()
    return simplified


@dataclass(slots=True)
class CompaniesHouseSearchProvider(SearchProvider):
    settings: Settings
    client: CompaniesHouseClient = field(init=False)

    def __post_init__(self) -> None:
        self.client = CompaniesHouseClient(self.settings)

    def search(self, variants: list[NameVariant]) -> list[EvidenceItem]:
        evidence: list[EvidenceItem] = []
        for vi, variant in enumerate(variants, 1):
            log.info("  CH officer search [%d/%d] '%s'", vi, len(variants), variant.name)
            try:
                officer_search = self.client.search_officers(variant.name)
            except RuntimeError as exc:
                log.warning("  CH officer search skipped for '%s': %s", variant.name, exc)
                continue
            for item in officer_search.get("items", []):
                officer_id = extract_officer_id(item)
                if not officer_id:
                    continue
                try:
                    appointments = self.client.get_officer_appointments(officer_id)
                except RuntimeError as exc:
                    log.warning("  CH appointments skipped for officer '%s': %s", officer_id, exc)
                    continue
                officer_name = item.get("title") or variant.name
                for appointment in appointments.get("items", []):
                    appointed_to = appointment.get("appointed_to", {})
                    company_number = appointed_to.get("company_number")
                    company_name = appointed_to.get("company_name")
                    if not company_number or not company_name:
                        continue

                    evidence.append(
                        EvidenceItem(
                            source="companies_house_officer_appointments",
                            source_key=f"{variant.name}:{officer_id}:{company_number}",
                            title=company_name,
                            url=_appointment_url(company_number),
                            snippet=(
                                f"{officer_name} linked to {company_name} via Companies House "
                                f"as {appointment.get('officer_role', 'officer')}"
                            ),
                            raw_payload={
                                "variant": variant.name,
                                "candidate_name": officer_name,
                                "matched_name": officer_name,
                                "organisation_name": company_name,
                                "registry_type": "company",
                                "registry_number": str(company_number),
                                "suffix": 0,
                                "role_type": company_role_type(appointment.get("officer_role")),
                                "role_label": appointment.get("officer_role") or "company_officer",
                                "relationship_kind": company_relationship_kind(
                                    appointment.get("officer_role")
                                ),
                                "relationship_phrase": company_relationship_phrase(
                                    appointment.get("officer_role")
                                ),
                                "officer_search_item": item,
                                "officer_id": officer_id,
                                "appointment": appointment,
                            },
                        )
                    )
        return evidence


@dataclass(slots=True)
class CharityCommissionSiteDorkProvider(SearchProvider):
    """Serper site-search scoped to the Charity Commission register,
    verified against the CC API.

    1. Dork finds charity pages mentioning the person's surname.
    2. Extracts unique charity numbers from the result URLs/titles.
    3. Calls the CC API to get the actual trustee list for each charity.
    4. Only creates evidence items for charities where a trustee name
       plausibly matches one of the seed variants.
    """

    settings: Settings
    cache_dir: Path = field(init=False)
    metrics: dict[str, int] = field(init=False)
    _cc_client: Any = field(init=False, default=None)

    _SITE = "register-of-charities.charitycommission.gov.uk"
    _TRUSTEE_MATCH_THRESHOLD = 0.45

    def __post_init__(self) -> None:
        self.cache_dir = Path(self.settings.cache_dir) / "cc_site_dork"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metrics = {"live": 0, "cache": 0, "evidence": 0, "charities_checked": 0, "trustee_matches": 0}
        if self.settings.charity_api_key:
            from src.charity_commission.client import CharityCommissionClient
            self._cc_client = CharityCommissionClient(self.settings)

    def _extract_surnames(self, variants: list[NameVariant]) -> set[str]:
        surnames: set[str] = set()
        for v in variants:
            tokens = normalize_name(v.name).split()
            if not tokens:
                continue
            surname = tokens[-1] if "," not in v.name else tokens[0]
            surnames.add(surname)
        return surnames

    def search(self, variants: list[NameVariant]) -> list[EvidenceItem]:
        from src.resolution.features import person_name_similarity

        self.metrics = {"live": 0, "cache": 0, "evidence": 0, "charities_checked": 0, "trustee_matches": 0}
        if not self.settings.serper_api_key:
            log.warning("  CC site dork skipped: no SERPER_API_KEY")
            return []

        surnames = self._extract_surnames(variants)
        variant_names = [v.name for v in variants]

        charity_numbers: dict[str, str] = {}
        for surname in surnames:
            query = f'site:{self._SITE} AND "{surname}"'
            results = self._cached_serper(query)
            log.info("  CC site dork for '%s': %d results", surname, len(results))
            for result in results:
                url = result.get("href") or result.get("url") or ""
                title = result.get("title", "")
                cn = _extract_charity_number_from_title(title)
                if not cn:
                    cn = extract_charity_number_from_url(url)
                if cn and cn not in charity_numbers:
                    org_name = title.split(" - ")[0].strip() if " - " in title else title
                    charity_numbers[cn] = org_name

        log.info("  CC site dork: %d unique charity numbers to verify via API", len(charity_numbers))

        if not self._cc_client:
            log.warning("  CC site dork: no CC API key, cannot verify trustees — skipping")
            return []

        evidence: list[EvidenceItem] = []
        for charity_number, org_name_hint in charity_numbers.items():
            self.metrics["charities_checked"] += 1
            try:
                trustees = self._cc_client.get_charity_trustee_information(int(charity_number))
            except Exception as exc:
                log.warning("  CC site dork: API call failed for charity %s: %s", charity_number, exc)
                continue

            if not isinstance(trustees, list):
                trustees = []

            trustee_names = [
                _clean_trustee_name(t.get("TrusteeName") or t.get("trustee_name") or t.get("name") or "")
                for t in trustees
            ]
            trustee_names = [n for n in trustee_names if n]

            best_trustee: str | None = None
            best_variant: str | None = None
            best_score = 0.0
            for tname in trustee_names:
                for vname in variant_names:
                    score = person_name_similarity(vname, tname)
                    if score > best_score:
                        best_score = score
                        best_trustee = tname
                        best_variant = vname

            if best_score < self._TRUSTEE_MATCH_THRESHOLD:
                log.info(
                    "  CC site dork: charity %s (%s) — no trustee matched (best %.2f)",
                    charity_number, org_name_hint, best_score,
                )
                continue

            self.metrics["trustee_matches"] += 1
            log.info(
                "  CC site dork: charity %s (%s) — matched trustee '%s' to variant '%s' (%.2f)",
                charity_number, org_name_hint, best_trustee, best_variant, best_score,
            )

            try:
                details = self._cc_client.get_all_charity_details(int(charity_number))
            except Exception:
                details = {}
            charity_name = (
                details.get("charity_name")
                or details.get("CharityName")
                or org_name_hint
            )

            evidence.append(
                EvidenceItem(
                    source="cc_site_dork",
                    source_key=f"cc_site:{charity_number}:{best_trustee}",
                    title=charity_name,
                    url=(
                        "https://register-of-charities.charitycommission.gov.uk/"
                        f"charity-details/?regid={charity_number}&subid=0"
                    ),
                    snippet=f"{best_trustee} is a trustee of {charity_name} (charity {charity_number})",
                    raw_payload={
                        "variant": best_variant,
                        "candidate_name": best_trustee,
                        "matched_name": best_trustee,
                        "organisation_name": charity_name,
                        "registry_type": "charity",
                        "registry_number": charity_number,
                        "suffix": 0,
                        "role_type": "trustee",
                        "role_label": "trustee",
                        "relationship_kind": "trustee_of",
                        "relationship_phrase": "is a trustee of",
                        "trustees": trustee_names,
                        "details": details,
                    },
                )
            )
            self.metrics["evidence"] += 1

        log.info("  CC site dork metrics: %s", self.metrics)
        return evidence

    def _cached_serper(self, query: str) -> list[dict[str, Any]]:
        cache_key = hashlib.sha256(query.encode()).hexdigest()[:16]
        cache_file = self.cache_dir / f"serper_{cache_key}.json"

        if cache_file.exists():
            self.metrics["cache"] += 1
            return json.loads(cache_file.read_text(encoding="utf-8"))

        url = f"{self.settings.serper_base_url}/search"
        payload = json.dumps({"q": query, "num": 20}).encode("utf-8")
        req = request.Request(
            url=url,
            data=payload,
            method="POST",
            headers={
                "X-API-KEY": str(self.settings.serper_api_key),
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(req) as response:
                body = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            log.warning("  CC site dork serper failed: %s %s", exc.code, message)
            return []
        except Exception as exc:
            log.warning("  CC site dork serper failed: %s", exc)
            return []

        rows: list[dict[str, Any]] = []
        for item in body.get("organic", []):
            if not isinstance(item, dict):
                continue
            rows.append({
                "title": item.get("title", ""),
                "href": item.get("link") or item.get("url") or "",
                "body": item.get("snippet", ""),
            })

        cache_file.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        self.metrics["live"] += 1
        return rows


_GENDER_SUFFIXES = re.compile(r"\s+(Male|Female)\s*$", re.IGNORECASE)


def _clean_trustee_name(name: str) -> str:
    return _GENDER_SUFFIXES.sub("", name).strip()


def _extract_charity_number_from_title(title: str) -> str | None:
    """Extract charity number from titles like 'CHARITY NAME - 259232 - Charity Commission'."""
    match = re.search(r"-\s*(\d{5,8})\s*-\s*Charity Commission\b", title, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def build_search_providers(
    settings: Settings,
    *,
    include_web_dork: bool = True,
) -> list[SearchProvider]:
    providers: list[SearchProvider] = []
    if settings.serper_api_key and settings.charity_api_key:
        providers.append(CharityCommissionSiteDorkProvider(settings))
    if include_web_dork:
        providers.append(WebDorkSearchProvider(settings))
    if settings.companies_house_api_key:
        providers.append(CompaniesHouseSearchProvider(settings))
    if not providers:
        providers.append(NullSearchProvider())
    return providers


def _appointment_url(company_number: str) -> str:
    return f"https://find-and-update.company-information.service.gov.uk/company/{company_number}"


def _is_relevant_dork_result(*, url: str, title: str, snippet: str) -> bool:
    host = _normalize_host(url)
    if host and any(host == suffix or host.endswith(f".{suffix}") for suffix in _TRUSTED_DORK_HOST_SUFFIXES):
        return True

    text = f"{title} {snippet} {url}".lower()
    org_signal = any(token in text for token in _ORG_SIGNAL_TERMS)
    governance_signal = any(token in text for token in _GOVERNANCE_SIGNAL_TERMS)
    has_registry_pattern = bool(
        re.search(
            r"(registered\s+charity\s+(number|no\.?)|charity\s+number|company\s+number|reg(istration)?\s+no\.?)",
            text,
            flags=re.IGNORECASE,
        )
    )
    is_pdf = ".pdf" in url.lower() if url else False
    noise_hit = any(token in text for token in _NOISE_TERMS)

    score = 0
    if org_signal:
        score += 1
    if governance_signal:
        score += 1
    if has_registry_pattern:
        score += 1
    if is_pdf and (org_signal or governance_signal):
        score += 1

    if noise_hit and score < 3:
        return False
    return score >= 2


def _normalize_host(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""

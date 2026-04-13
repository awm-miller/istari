from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib import error, request

from src.addresses import (
    NormalizedAddress,
    address_dork_query,
    extract_company_addresses,
    extract_charity_addresses,
    first_address_line,
)
from src.charity_commission.client import CharityCommissionClient
from src.charity_commission.expansion import build_charity_record
from src.charity_commission.identifiers import extract_charity_number_from_url
from src.companies_house.client import CompaniesHouseClient
from src.config import Settings
from src.models import OrganisationRecord

log = logging.getLogger("istari.address_pivot")

_COMPANY_SITE = "find-and-update.company-information.service.gov.uk/company"
_CHARITY_SITE = "register-of-charities.charitycommission.gov.uk"


class AddressPivotSearcher:
    def __init__(
        self,
        *,
        settings: Settings,
        charity_client: CharityCommissionClient,
        companies_house_client: CompaniesHouseClient,
    ) -> None:
        self.settings = settings
        self.charity_client = charity_client
        self.companies_house_client = companies_house_client
        self.cache_dir = Path(settings.cache_dir) / "address_pivot"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def find_related_organisations(
        self,
        *,
        address: NormalizedAddress,
        source_registry_type: str,
        source_registry_number: str,
        source_suffix: int,
    ) -> list[dict[str, Any]]:
        discovered: dict[tuple[str, str, int], dict[str, Any]] = {}
        for row in self._search_companies(address):
            key = ("company", str(row["registry_number"]), int(row.get("suffix", 0)))
            if key == (source_registry_type, source_registry_number, source_suffix):
                continue
            discovered[key] = row
        if self.settings.serper_api_key:
            for row in self._search_charities(address):
                key = ("charity", str(row["registry_number"]), int(row.get("suffix", 0)))
                if key == (source_registry_type, source_registry_number, source_suffix):
                    continue
                discovered[key] = row
        return list(discovered.values())

    def _search_companies(self, address: NormalizedAddress) -> list[dict[str, Any]]:
        search_terms = [term for term in [first_address_line(address.label), address.postcode or ""] if term]
        if not search_terms:
            return []
        query = " ".join(search_terms)
        rows = []
        seen_numbers: set[str] = set()
        try:
            search_results = self.companies_house_client.search_companies(query, items_per_page=20)
        except Exception as exc:
            log.warning("  Address pivot: company search failed for %s: %s", query, exc)
            return []
        for result in search_results.get("items", []):
            company_number = str(result.get("company_number") or "").upper().strip()
            if not company_number:
                continue
            if company_number in seen_numbers:
                continue
            seen_numbers.add(company_number)
            try:
                profile = self.companies_house_client.get_company_profile(company_number)
            except Exception as exc:
                log.warning("  Address pivot: company profile lookup failed for %s: %s", company_number, exc)
                continue
            if not _has_matching_address(address, extract_company_addresses(profile)):
                continue
            rows.append(
                {
                    "registry_type": "company",
                    "registry_number": company_number,
                    "suffix": 0,
                    "name": str(profile.get("company_name") or company_number).strip(),
                    "status": profile.get("company_status"),
                    "metadata": profile,
                    "source": "address_pivot_company",
                }
            )
        return rows

    def _search_charities(self, address: NormalizedAddress) -> list[dict[str, Any]]:
        query = address_dork_query(address, _CHARITY_SITE)
        if not query:
            return []
        rows = []
        seen_numbers: set[str] = set()
        for result in self._cached_serper(query):
            url = str(result.get("href") or result.get("url") or "")
            charity_number = extract_charity_number_from_url(url)
            if not charity_number or charity_number in seen_numbers:
                continue
            seen_numbers.add(charity_number)
            try:
                details = self.charity_client.get_all_charity_details(int(charity_number))
            except Exception as exc:
                log.warning("  Address pivot: charity detail lookup failed for %s: %s", charity_number, exc)
                continue
            if not _has_matching_address(address, extract_charity_addresses(details)):
                continue
            record = build_charity_record(details, charity_number=int(charity_number), suffix=0)
            rows.append(
                {
                    "registry_type": record.registry_type,
                    "registry_number": record.registry_number,
                    "suffix": record.suffix,
                    "name": record.name,
                    "status": record.status,
                    "metadata": record.metadata,
                    "source": "address_pivot_charity",
                }
            )
        return rows

    def _cached_serper(self, query: str) -> list[dict[str, Any]]:
        cache_key = hashlib.sha256(query.encode("utf-8")).hexdigest()[:16]
        cache_file = self.cache_dir / f"serper_{cache_key}.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text(encoding="utf-8"))

        req = request.Request(
            url=f"{self.settings.serper_base_url}/search",
            data=json.dumps({"q": query, "num": 10}).encode("utf-8"),
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
            log.warning("  Address pivot serper failed: %s %s", exc.code, message)
            return []
        except Exception as exc:
            log.warning("  Address pivot serper failed: %s", exc)
            return []

        rows: list[dict[str, Any]] = []
        for item in body.get("organic", []):
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "title": item.get("title", ""),
                    "href": item.get("link") or item.get("url") or "",
                    "body": item.get("snippet", ""),
                }
            )
        cache_file.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        return rows


def build_organisation_record(row: dict[str, Any]) -> OrganisationRecord:
    return OrganisationRecord(
        registry_type=str(row["registry_type"]),
        registry_number=str(row["registry_number"]),
        suffix=int(row.get("suffix", 0)),
        name=str(row.get("name") or "").strip(),
        status=str(row["status"]) if row.get("status") is not None else None,
        metadata=dict(row.get("metadata") or {}),
    )


def _has_matching_address(target: NormalizedAddress, candidates: list[NormalizedAddress]) -> bool:
    return any(candidate.normalized_key == target.normalized_key for candidate in candidates)


def _extract_charity_number_from_title(title: str) -> str | None:
    return None

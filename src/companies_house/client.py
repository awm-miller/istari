from __future__ import annotations

import base64
import json
import logging
import socket
import time
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from src.config import Settings

log = logging.getLogger("istari.companies_house")

_MIN_REQUEST_INTERVAL_SECONDS = 0.5
_TRANSIENT_ERROR_RETRY_DELAYS_SECONDS = (1.0, 2.0, 5.0)
_last_request_at = 0.0


@dataclass(slots=True)
class CompaniesHouseClient:
    settings: Settings
    cache_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        self.cache_dir = self.settings.cache_dir / "companies_house"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def search_officers(self, query: str, items_per_page: int = 10) -> dict[str, Any]:
        self._ensure_api_key()
        encoded_query = parse.quote(query)
        url = (
            f"{self.settings.companies_house_base_url}/search/officers"
            f"?q={encoded_query}&items_per_page={items_per_page}"
        )
        return self._get_json(url)

    def search_companies(self, query: str, items_per_page: int = 20) -> dict[str, Any]:
        self._ensure_api_key()
        encoded_query = parse.quote(query)
        url = (
            f"{self.settings.companies_house_base_url}/search/companies"
            f"?q={encoded_query}&items_per_page={items_per_page}"
        )
        payload = self._get_json(url)
        return payload if isinstance(payload, dict) else {}

    def get_officer_appointments(self, officer_id: str, items_per_page: int = 25) -> dict[str, Any]:
        self._ensure_api_key()
        encoded_officer_id = parse.quote(officer_id, safe="")
        url = (
            f"{self.settings.companies_house_base_url}/officers/{encoded_officer_id}/appointments"
            f"?items_per_page={items_per_page}"
        )
        return self._get_json(url)

    def get_company_profile(self, company_number: str) -> dict[str, Any]:
        self._ensure_api_key()
        encoded_company_number = parse.quote(str(company_number), safe="")
        url = f"{self.settings.companies_house_base_url}/company/{encoded_company_number}"
        payload = self._get_json(url)
        return payload if isinstance(payload, dict) else {}

    def get_company_officers(self, company_number: str, items_per_page: int = 100) -> dict[str, Any]:
        self._ensure_api_key()
        encoded_company_number = parse.quote(str(company_number), safe="")
        url = (
            f"{self.settings.companies_house_base_url}/company/{encoded_company_number}/officers"
            f"?items_per_page={items_per_page}"
        )
        payload = self._get_json(url)
        return payload if isinstance(payload, dict) else {}

    def get_filing_history(self, company_number: str, items_per_page: int = 100, start_index: int = 0) -> dict[str, Any]:
        self._ensure_api_key()
        encoded_company_number = parse.quote(str(company_number), safe="")
        url = (
            f"{self.settings.companies_house_base_url}/company/{encoded_company_number}/filing-history"
            f"?items_per_page={items_per_page}&start_index={start_index}"
        )
        payload = self._get_json(url)
        return payload if isinstance(payload, dict) else {}

    def _get_json(self, url: str) -> Any:
        global _last_request_at
        cache_key = sha256(url.encode("utf-8")).hexdigest()
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            log.debug("CH cache hit: %s", url.split("/")[-1])
            return json.loads(cache_path.read_text(encoding="utf-8"))

        log.debug("CH API call: %s", url)
        now = time.monotonic()
        wait = _MIN_REQUEST_INTERVAL_SECONDS - (now - _last_request_at)
        if wait > 0:
            time.sleep(wait)

        api_key_bytes = f"{self.settings.companies_house_api_key}:".encode("utf-8")
        auth_header = base64.b64encode(api_key_bytes).decode("ascii")
        req = request.Request(
            url=url,
            headers={
                "Authorization": f"Basic {auth_header}",
                "User-Agent": self.settings.user_agent,
            },
            method="GET",
        )

        for attempt, delay_seconds in enumerate((0.0, *_TRANSIENT_ERROR_RETRY_DELAYS_SECONDS), start=1):
            try:
                with request.urlopen(req) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                    _last_request_at = time.monotonic()
                    break
            except error.HTTPError as exc:
                _last_request_at = time.monotonic()
                body = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Companies House request failed: {exc.code} {body}") from exc
            except error.URLError as exc:
                _last_request_at = time.monotonic()
                if not self._is_transient_network_error(exc) or attempt > len(_TRANSIENT_ERROR_RETRY_DELAYS_SECONDS):
                    raise RuntimeError(f"Companies House request failed: {exc}") from exc
                log.warning(
                    "Companies House request transient failure (attempt %d/%d): %s",
                    attempt,
                    len(_TRANSIENT_ERROR_RETRY_DELAYS_SECONDS) + 1,
                    exc,
                )
                time.sleep(delay_seconds)
        else:
            raise RuntimeError("Companies House request failed after retries.")

        cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    @staticmethod
    def _is_transient_network_error(exc: error.URLError) -> bool:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, socket.gaierror):
            return True
        if isinstance(reason, TimeoutError):
            return True
        message = str(reason or exc).lower()
        transient_markers = (
            "getaddrinfo failed",
            "temporary failure in name resolution",
            "timed out",
            "timeout",
            "connection reset",
            "connection aborted",
            "connection refused",
            "network is unreachable",
        )
        return any(marker in message for marker in transient_markers)

    def _ensure_api_key(self) -> None:
        if not self.settings.companies_house_api_key:
            raise RuntimeError(
                "COMPANIES_HOUSE_API_KEY is required before calling the Companies House API."
            )


def extract_officer_id(search_item: dict[str, Any]) -> str | None:
    links = search_item.get("links", {})
    candidate = links.get("self") or links.get("officer", {}).get("appointments")
    if not candidate:
        return None

    parts = [part for part in str(candidate).split("/") if part]
    if "officers" in parts:
        officer_index = parts.index("officers")
        if officer_index + 1 < len(parts):
            return parts[officer_index + 1]
    return None

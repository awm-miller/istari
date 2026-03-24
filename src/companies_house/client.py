from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from src.config import Settings

log = logging.getLogger("istari.companies_house")


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

    def _get_json(self, url: str) -> Any:
        cache_key = sha256(url.encode("utf-8")).hexdigest()
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            log.debug("CH cache hit: %s", url.split("/")[-1])
            return json.loads(cache_path.read_text(encoding="utf-8"))

        log.debug("CH API call: %s", url)

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
        try:
            with request.urlopen(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Companies House request failed: {exc.code} {body}") from exc

        cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

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

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from src.config import Settings

log = logging.getLogger("istari.charity_commission")


@dataclass(slots=True)
class CharityCommissionClient:
    settings: Settings
    cache_dir: Path = field(init=False)
    base_path: str = field(init=False)
    _missing_api_key: bool = field(init=False)

    def __post_init__(self) -> None:
        self.cache_dir = self.settings.cache_dir / "charity_commission"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.base_path = self.settings.charity_api_base_url
        self._missing_api_key = not bool(self.settings.charity_api_key)

    def search_charities_by_name(self, charity_name: str) -> list[dict[str, Any]]:
        self._ensure_api_key()
        encoded = parse.quote(charity_name)
        return self._get_json(f"{self.base_path}/searchCharityName/{encoded}")

    def get_all_charity_details(self, charity_number: int, suffix: int = 0) -> dict[str, Any]:
        self._ensure_api_key()
        return self._get_json(
            f"{self.base_path}/allcharitydetailsV2/{charity_number}/{suffix}"
        )

    def get_charity_trustee_information(
        self, charity_number: int, suffix: int = 0
    ) -> list[dict[str, Any]]:
        self._ensure_api_key()
        return self._get_json(
            f"{self.base_path}/charitytrusteeinformationV2/{charity_number}/{suffix}"
        )

    def get_charity_trustee_names(self, charity_number: int, suffix: int = 0) -> list[str]:
        self._ensure_api_key()
        payload = self._get_json(
            f"{self.base_path}/charitytrusteenamesV2/{charity_number}/{suffix}"
        )
        if isinstance(payload, list):
            values: list[str] = []
            for item in payload:
                if isinstance(item, dict):
                    trustee_name = item.get("trustee_name") or item.get("TrusteeName")
                    if trustee_name:
                        values.append(str(trustee_name))
                        continue
                values.append(str(item))
            return values
        if isinstance(payload, dict):
            values = payload.get("trustees") or payload.get("names") or []
            return [str(item) for item in values]
        return []

    def get_charity_governance_information(
        self,
        charity_number: int,
        suffix: int = 0,
    ) -> dict[str, Any]:
        self._ensure_api_key()
        payload = self._get_optional_json(
            f"{self.base_path}/charitygovernanceinformation/{charity_number}/{suffix}",
            missing_message=f"CC governance info not available for charity {charity_number}/{suffix}",
        )
        return payload if isinstance(payload, dict) else {}

    def get_charity_governing_document(
        self,
        charity_number: int,
        suffix: int = 0,
    ) -> dict[str, Any]:
        self._ensure_api_key()
        payload = self._get_optional_json(
            f"{self.base_path}/charitygoverningdocument/{charity_number}/{suffix}",
            missing_message=f"CC governing document not available for charity {charity_number}/{suffix}",
        )
        return payload if isinstance(payload, dict) else {}

    def get_charity_account_ar_information(
        self,
        charity_number: int,
        suffix: int = 0,
    ) -> dict[str, Any]:
        self._ensure_api_key()
        payload = self._get_optional_json(
            f"{self.base_path}/charityaraccounts/{charity_number}/{suffix}",
            missing_message=f"CC account/ar info not available for charity {charity_number}/{suffix}",
        )
        return payload if isinstance(payload, dict) else {}

    def get_charity_linked_charities(
        self,
        charity_number: int,
        suffix: int = 0,
    ) -> Any:
        self._ensure_api_key()
        payload = self._get_optional_json(
            f"{self.base_path}/linkedcharities/{charity_number}/{suffix}",
            missing_message=f"CC linked charities not available for charity {charity_number}/{suffix}",
        )
        if isinstance(payload, (dict, list)):
            return payload
        return {}

    def get_charity_linked_charity(
        self,
        charity_number: int,
        suffix: int = 0,
    ) -> dict[str, Any]:
        self._ensure_api_key()
        payload = self._get_optional_json(
            f"{self.base_path}/linkedcharity/{charity_number}/{suffix}",
            missing_message=f"CC linked charity not available for charity {charity_number}/{suffix}",
        )
        return payload if isinstance(payload, dict) else {}

    def _get_json(self, url: str) -> Any:
        cache_key = sha256(url.encode("utf-8")).hexdigest()
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            log.debug("CC cache hit: %s", url.split("/")[-1])
            return json.loads(cache_path.read_text(encoding="utf-8"))

        log.debug("CC API call: %s", url)
        headers = {"User-Agent": self.settings.user_agent}
        if self.settings.charity_api_key:
            headers[self.settings.charity_api_key_header] = self.settings.charity_api_key

        req = request.Request(url=url, headers=headers, method="GET")
        try:
            with request.urlopen(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Charity Commission request failed: {exc.code} {body}") from exc

        cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    def _get_optional_json(self, url: str, missing_message: str | None = None) -> Any:
        try:
            return self._get_json(url)
        except RuntimeError as exc:
            if " 404 " in str(exc):
                log.info("%s", missing_message or f"CC resource not found: {url}")
                return {}
            raise

    def _ensure_api_key(self) -> None:
        if self._missing_api_key:
            raise RuntimeError(
                "CHARITY_COMMISSION_API_KEY or CCEW_API_KEY is required before calling the Charity Commission API."
            )

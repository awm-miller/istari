from __future__ import annotations

import re
from typing import Any


def looks_like_charity_url(url: str) -> bool:
    lowered = (url or "").lower()
    return "charitycommission.gov.uk" in lowered or "charity-details" in lowered


def extract_charity_number_from_url(url: str) -> str | None:
    match = re.search(r"charity-details/(\d+)", url or "", flags=re.IGNORECASE)
    if match:
        return match.group(1)
    parsed_match = re.search(r"[?&]regid=(\d+)", url or "", flags=re.IGNORECASE)
    if parsed_match:
        return parsed_match.group(1)
    return None


def extract_charity_number_from_payload(payload: dict[str, Any]) -> str | None:
    for key in ("charity_number", "registered_number", "reg_charity_number"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return str(int(value))
        except (TypeError, ValueError):
            continue
    return None

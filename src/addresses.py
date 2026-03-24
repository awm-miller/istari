from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


@dataclass(slots=True)
class NormalizedAddress:
    label: str
    normalized_key: str
    postcode: str | None = None
    country: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def extract_company_addresses(metadata: dict[str, Any]) -> list[NormalizedAddress]:
    address = metadata.get("registered_office_address")
    if not isinstance(address, dict):
        return []
    return _build_addresses(
        [
            address.get("premises"),
            address.get("address_line_1"),
            address.get("address_line_2"),
            address.get("locality"),
            address.get("region"),
        ],
        postcode=address.get("postal_code"),
        country=address.get("country"),
        source_kind="companies_house_registered_office",
        raw_payload=address,
    )


def extract_charity_addresses(metadata: dict[str, Any]) -> list[NormalizedAddress]:
    return _build_addresses(
        [
            metadata.get("address_line_one"),
            metadata.get("address_line_two"),
            metadata.get("address_line_three"),
            metadata.get("address_line_four"),
            metadata.get("address_line_five"),
        ],
        postcode=metadata.get("address_post_code"),
        country=metadata.get("country") or metadata.get("address_country"),
        source_kind="charity_commission_registered_address",
        raw_payload={
            "address_line_one": metadata.get("address_line_one"),
            "address_line_two": metadata.get("address_line_two"),
            "address_line_three": metadata.get("address_line_three"),
            "address_line_four": metadata.get("address_line_four"),
            "address_line_five": metadata.get("address_line_five"),
            "address_post_code": metadata.get("address_post_code"),
            "country": metadata.get("country") or metadata.get("address_country"),
        },
    )


def extract_addresses_for_organisation(
    registry_type: str,
    metadata: dict[str, Any],
) -> list[NormalizedAddress]:
    lowered = str(registry_type or "").strip().lower()
    if lowered == "company":
        return extract_company_addresses(metadata)
    if lowered == "charity":
        return extract_charity_addresses(metadata)
    return []


def address_dork_query(address: NormalizedAddress, site: str) -> str | None:
    line = first_address_line(address.label)
    postcode = address.postcode or ""
    if not line or not postcode:
        return None
    return f'site:{site} "{line}" "{postcode}"'


def first_address_line(label: str) -> str:
    return next((part.strip() for part in str(label).split(",") if part.strip()), "")


def _build_addresses(
    parts: list[Any],
    *,
    postcode: Any,
    country: Any,
    source_kind: str,
    raw_payload: dict[str, Any],
) -> list[NormalizedAddress]:
    clean_parts = [_clean_part(part) for part in parts]
    clean_parts = [part for part in clean_parts if part]
    clean_postcode = _normalize_postcode(postcode)
    clean_country = _clean_part(country)
    if not clean_parts and not clean_postcode:
        return []

    label_parts = list(clean_parts)
    if clean_postcode:
        label_parts.append(clean_postcode)
    if clean_country and clean_country.lower() not in {part.lower() for part in label_parts}:
        label_parts.append(clean_country)
    label = ", ".join(label_parts)

    key_parts = [_normalize_fragment(part) for part in clean_parts]
    if clean_postcode:
        key_parts.append(clean_postcode.replace(" ", ""))
    if clean_country:
        key_parts.append(_normalize_fragment(clean_country))
    normalized_key = "|".join(part for part in key_parts if part)
    if not normalized_key:
        return []

    return [
        NormalizedAddress(
            label=label,
            normalized_key=normalized_key,
            postcode=clean_postcode or None,
            country=clean_country or None,
            metadata={
                "source_kind": source_kind,
                "raw_payload": raw_payload,
            },
        )
    ]


def _clean_part(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ,")


def _normalize_postcode(value: Any) -> str:
    text = _clean_part(value).upper()
    if not text:
        return ""
    compact = re.sub(r"[^A-Z0-9]", "", text)
    if len(compact) > 3:
        return f"{compact[:-3]} {compact[-3:]}"
    return compact


def _normalize_fragment(value: str) -> str:
    text = re.sub(r"[^A-Z0-9]+", " ", str(value).upper()).strip()
    return re.sub(r"\s+", " ", text)

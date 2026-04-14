from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def default_address_coordinate_cache_path(project_root: Path) -> Path:
    return project_root / "data" / "address_coordinates_cache.json"


def build_address_coordinate_index(
    *,
    main_data: dict[str, Any],
    low_confidence_data: dict[str, Any],
    cache_path: Path,
    user_agent: str,
) -> dict[str, Any]:
    cache_path = Path(cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache = _load_cache(cache_path)

    address_nodes = _collect_address_nodes(main_data, low_confidence_data)
    lookup_by_key = {
        lookup["key"]: lookup
        for lookup in (_address_lookup(node) for node in address_nodes)
        if lookup["key"] and lookup["query"]
    }

    missing_keys = [key for key in lookup_by_key if key not in cache]
    for index, key in enumerate(missing_keys):
        lookup = lookup_by_key[key]
        cache[key] = _lookup_coordinates(lookup, user_agent=user_agent)
        if lookup["method"] == "nominatim" and index < len(missing_keys) - 1:
            time.sleep(1.0)

    coordinates = []
    for node in address_nodes:
        key = _address_lookup(node)["key"]
        if not key:
            continue
        point = cache.get(key)
        if not point:
            continue
        coordinates.append(
            {
                "node_id": str(node.get("id") or ""),
                "lat": point["lat"],
                "lon": point["lon"],
                "label": str(node.get("label") or point.get("label") or ""),
            }
        )

    cache_path.write_text(
        json.dumps({"queries": cache}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "coordinates": coordinates,
        "summary": {
            "cached_query_count": len(cache),
            "coordinate_count": len(coordinates),
            "address_node_count": len(address_nodes),
        },
    }


def _load_cache(cache_path: Path) -> dict[str, dict[str, Any]]:
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    queries = payload.get("queries")
    if not isinstance(queries, dict):
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for key, value in queries.items():
        if not isinstance(value, dict):
            continue
        try:
            lat = float(value["lat"])
            lon = float(value["lon"])
        except Exception:
            continue
        cache[str(key)] = {
            "lat": lat,
            "lon": lon,
            "label": str(value.get("label") or ""),
        }
    return cache


def _collect_address_nodes(main_data: dict[str, Any], low_confidence_data: dict[str, Any]) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for dataset in (main_data, low_confidence_data):
        for node in dataset.get("nodes") or []:
            if str(node.get("kind") or "") != "address":
                continue
            node_id = str(node.get("id") or "")
            if not node_id or node_id in seen_ids:
                continue
            seen_ids.add(node_id)
            nodes.append(node)
    return nodes


def _address_lookup(node: dict[str, Any]) -> dict[str, str]:
    label = str(node.get("label") or "").strip()
    postcode = str(node.get("postcode") or "").strip()
    country = str(node.get("country") or "").strip()
    label_query = _address_query(label=label, postcode=postcode, country=country)
    cleaned_label_query = _clean_address_query(label_query)
    if postcode:
        normalized_postcode = postcode.upper().replace(" ", "")
        return {
            "key": f"postcode:{normalized_postcode}",
            "query": postcode,
            "method": "postcode",
            "fallback_query": cleaned_label_query or label_query,
        }
    query = cleaned_label_query or label_query
    return {
        "key": f"query:{query.lower()}",
        "query": query,
        "method": "nominatim",
    }


def _lookup_coordinates(lookup: dict[str, str], *, user_agent: str) -> dict[str, Any] | None:
    if lookup["method"] == "postcode":
        point = _geocode_postcode(lookup["query"], user_agent=user_agent)
        if point:
            return point
        fallback_query = str(lookup.get("fallback_query") or "").strip()
        if fallback_query and fallback_query != str(lookup["query"]).strip():
            point = _geocode_query(fallback_query, user_agent=user_agent)
            if point:
                return point
    return _geocode_query(lookup["query"], user_agent=user_agent)


def _address_query(*, label: str, postcode: str, country: str) -> str:
    parts = [label] if label else []
    if postcode and postcode.lower() not in label.lower():
        parts.append(postcode)
    if country and country.lower() not in label.lower():
        parts.append(country)
    return ", ".join(part for part in parts if part).strip()


def _clean_address_query(query: str) -> str:
    cleaned = str(query or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(
        r"^\d+\s*-\s*COMPANIES HOUSE DEFAULT ADDRESS,?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    parts = [_clean_address_part(part) for part in cleaned.split(",")]
    return ", ".join(part for part in parts if part).strip(" ,")


def _clean_address_part(part: str) -> str:
    cleaned = str(part or "").strip()
    if not cleaned:
        return ""
    if _is_floor_or_level_text(cleaned):
        return ""
    cleaned = re.sub(
        r"\b(?:level\s+\d+[a-z]?|(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|ground|lower ground|upper ground|basement|mezzanine)\s+floor)\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip(" ,")


def _is_floor_or_level_text(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:level\s+\d+[a-z]?|(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|ground|lower ground|upper ground|basement|mezzanine)\s+floor)",
            str(value or "").strip(),
            flags=re.IGNORECASE,
        )
    )


def _geocode_postcode(postcode: str, *, user_agent: str) -> dict[str, Any] | None:
    normalized_postcode = str(postcode or "").strip().upper().replace(" ", "")
    if not normalized_postcode:
        return None
    url = f"https://api.postcodes.io/postcodes/{urllib.parse.quote(normalized_postcode)}"
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    try:
        lat = float(result["latitude"])
        lon = float(result["longitude"])
    except Exception:
        return None
    label_parts = [
        str(result.get("postcode") or "").strip(),
        str(result.get("admin_district") or "").strip(),
        str(result.get("country") or "").strip(),
    ]
    return {
        "lat": lat,
        "lon": lon,
        "label": ", ".join(part for part in label_parts if part),
    }


def _geocode_query(query: str, *, user_agent: str) -> dict[str, Any] | None:
    if not query:
        return None
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(
        {
            "format": "jsonv2",
            "limit": 1,
            "q": query,
        }
    )
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            rows = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None
    if not isinstance(rows, list) or not rows:
        return None
    row = rows[0]
    try:
        lat = float(row["lat"])
        lon = float(row["lon"])
    except Exception:
        return None
    return {
        "lat": lat,
        "lon": lon,
        "label": str(row.get("display_name") or query),
    }

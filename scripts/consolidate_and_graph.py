"""Post-processing script: consolidate alias people in a run and produce
a self-contained HTML network graph with 4-lane vertical layout.

Lanes:
  0  Seed name
  1  Consolidated seed-person aliases
  2  Organisations
  3  Expanded people (from org records)

Usage:
    python scripts/consolidate_and_graph.py <run_id> [--out graph.html]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import webbrowser
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.address_resolution import AddressMergeMatcher, address_bucket_keys, addresses_match
from src.config import load_settings
from src.graph.render import render_html as _render_html
from src.mapping_low_confidence import (
    build_low_confidence_overlay,
    rebuild_overlay_mapping_db,
)
from src.resolution.features import person_name_similarity
from src.search.queries import (
    _FUZZY_SWAPS,
    _SPELLING_CLUSTERS,
    normalize_name,
)
from src.storage.repository import Repository

# ---------------------------------------------------------------------------
# Alias detection
# ---------------------------------------------------------------------------

_CLUSTER_CANONICAL: dict[str, str] = {}
for _cluster in _SPELLING_CLUSTERS:
    canonical = sorted(_cluster)[0]
    for member in _cluster:
        _CLUSTER_CANONICAL[member] = canonical


def _canonical_token(token: str) -> str:
    return _CLUSTER_CANONICAL.get(token, token)


def _fuzzy_surname_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if _canonical_token(a) == _canonical_token(b):
        return True
    for old, new in _FUZZY_SWAPS:
        if a.replace(old, new) == b or b.replace(old, new) == a:
            return True
    if len(a) > 3 and len(b) > 3 and person_name_similarity(a, b) >= 0.75:
        return True
    return False


def _alias_tokens(name: str) -> list[str]:
    if "," in name:
        parts = name.split(",", 1)
        surname_raw = normalize_name(parts[0])
        given_raw = normalize_name(parts[1])
        tokens = given_raw.split() + surname_raw.split()
    else:
        tokens = normalize_name(name).split()
    return [_canonical_token(t) for t in tokens if t]


def are_aliases(name_a: str, name_b: str) -> bool:
    tokens_a = _alias_tokens(name_a)
    tokens_b = _alias_tokens(name_b)
    if not tokens_a or not tokens_b:
        return False
    surname_a = tokens_a[-1]
    surname_b = tokens_b[-1]
    if not _fuzzy_surname_match(surname_a, surname_b):
        return False
    given_a = set(tokens_a[:-1])
    given_b = set(tokens_b[:-1])
    if not given_a or not given_b:
        return bool(given_a) == bool(given_b)
    return given_a.issubset(given_b) or given_b.issubset(given_a)


def matches_seed_alias(seed_name: str, names: list[str]) -> bool:
    return any(
        are_aliases(seed_name, str(name))
        for name in names
        if str(name).strip()
    )


_DOB_RE = re.compile(r"(\d{4}-\d{2})$")


def _extract_birth_month(identity_key: str) -> str | None:
    """Return 'YYYY-MM' from a 'ch-name-dob:...:YYYY-MM' key, or None."""
    m = _DOB_RE.search(identity_key)
    return m.group(1) if m else None


def _birth_dates_conflict(key_a: str, key_b: str) -> bool:
    """True when both keys carry birth-month info and it differs."""
    if not key_a or not key_b:
        return False
    dob_a = _extract_birth_month(key_a)
    dob_b = _extract_birth_month(key_b)
    if dob_a and dob_b and dob_a != dob_b:
        return True
    return False


def _any_birth_date_conflict(left: dict, right: dict) -> bool:
    """Check identity_key (str) or identity_keys (list) for DOB conflicts."""
    left_keys = left.get("identity_keys") or []
    if not left_keys and left.get("identity_key"):
        left_keys = [left["identity_key"]]
    right_keys = right.get("identity_keys") or []
    if not right_keys and right.get("identity_key"):
        right_keys = [right["identity_key"]]
    for lk in left_keys:
        for rk in right_keys:
            if _birth_dates_conflict(lk, rk):
                return True
    return False


def names_match(left: dict, right: dict) -> bool:
    if _any_birth_date_conflict(left, right):
        return False
    left_names = [str(left["label"]), *(str(name) for name in left.get("aliases") or [])]
    right_names = [str(right["label"]), *(str(name) for name in right.get("aliases") or [])]
    for left_name in left_names:
        for right_name in right_names:
            if are_aliases(left_name, right_name):
                return True
    return False


class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        self._parent.setdefault(x, x)
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]
            x = self._parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _row_str(row, key: str) -> str:
    try:
        return str(row[key] or "")
    except (IndexError, KeyError):
        return ""


def _json_dict(raw_value: object) -> dict[str, object]:
    try:
        parsed = json.loads(str(raw_value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_parent_org_id(
    metadata: dict[str, object],
    org_registry_lookup: dict[tuple[str, str, int], int],
) -> int | None:
    try:
        parent_org_id = metadata.get("parent_organisation_id")
        if parent_org_id not in (None, ""):
            return int(parent_org_id)
    except (TypeError, ValueError):
        pass

    parent_registry_type = str(metadata.get("parent_registry_type") or "").strip()
    parent_registry_number = str(metadata.get("parent_registry_number") or "").strip()
    if not parent_registry_type or not parent_registry_number:
        return None
    try:
        parent_suffix = int(metadata.get("parent_suffix") or 0)
    except (TypeError, ValueError):
        parent_suffix = 0
    return org_registry_lookup.get((parent_registry_type, parent_registry_number, parent_suffix))


def _linked_org_phrase(source: str, metadata: dict[str, object]) -> str:
    custom_phrase = str(metadata.get("connection_phrase") or "").strip()
    if custom_phrase:
        return custom_phrase
    if source == "pdf_org_mention":
        return "is mentioned in filings for"
    if source == "charity_commission_linked_charities":
        return "is linked in Charity Commission records to"
    if source.startswith("address_pivot"):
        return "shares an address with"
    return "is linked to"


def _linked_org_detail(metadata: dict[str, object]) -> str:
    return str(metadata.get("connection_detail") or "").strip()


def _is_notice_boilerplate_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "gives notice" in text:
        return True
    if "issuing authority" in text or "issuing the gazette notice" in text:
        return True
    if "regulatory body issuing a notice" in text:
        return True
    if ("registrar of companies" in text or "companies house" in text) and any(
        token in text for token in ("notice", "gazette", "strike off", "striking off")
    ):
        return True
    return False


def _is_notice_org_mention(source: str, metadata: dict[str, object]) -> bool:
    if source != "pdf_org_mention":
        return False
    return any(
        _is_notice_boilerplate_text(str(metadata.get(key) or ""))
        for key in ("entity_name", "connection_phrase", "connection_detail")
    )


def _is_notice_role_edge(edge) -> bool:
    if _row_str(edge, "source") != "pdf_gemini_extraction":
        return False
    if _is_notice_boilerplate_text(_row_str(edge, "relationship_phrase")):
        return True
    if _is_notice_boilerplate_text(_row_str(edge, "role_label")):
        return True
    provenance = _json_dict(edge["provenance_json"])
    pdf_entity = provenance.get("pdf_entity", {}) if isinstance(provenance, dict) else {}
    return any(
        _is_notice_boilerplate_text(str(pdf_entity.get(key) or ""))
        for key in ("name", "role_label", "connection_phrase", "notes")
    )


def _pdf_role_detail(edge) -> str:
    if _row_str(edge, "source") != "pdf_gemini_extraction":
        return ""
    provenance = _json_dict(edge["provenance_json"])
    pdf_entity = provenance.get("pdf_entity", {}) if isinstance(provenance, dict) else {}
    return str(pdf_entity.get("notes") or "").strip()


def _parse_page_number(page_hint: str) -> int | None:
    match = re.search(r"(\d+)", str(page_hint or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _pdf_role_evidence(edge) -> dict[str, object] | None:
    if _row_str(edge, "source") != "pdf_gemini_extraction":
        return None
    provenance = _json_dict(edge["provenance_json"])
    if not isinstance(provenance, dict):
        return None
    pdf_entity = provenance.get("pdf_entity", {})
    document = provenance.get("document", {})
    if not isinstance(pdf_entity, dict) or not isinstance(document, dict):
        return None
    page_hint = str(pdf_entity.get("source_page_hint") or "").strip()
    return {
        "title": str(document.get("title") or "").strip(),
        "document_url": str(document.get("url") or "").strip(),
        "local_pdf_path": str(document.get("local_pdf_path") or "").strip(),
        "filing_description": str(document.get("filing_description") or "").strip(),
        "page_hint": page_hint,
        "page_number": _parse_page_number(page_hint),
        "notes": str(pdf_entity.get("notes") or "").strip(),
        "evidence_id": provenance.get("evidence_id"),
    }


def _pdf_org_evidence(metadata: dict[str, object]) -> dict[str, object] | None:
    if not metadata:
        return None
    page_hint = str(metadata.get("source_page_hint") or "").strip()
    return {
        "title": str(metadata.get("document_title") or "").strip(),
        "document_url": str(metadata.get("document_url") or "").strip(),
        "local_pdf_path": str(metadata.get("local_pdf_path") or "").strip(),
        "filing_description": str(metadata.get("filing_description") or "").strip(),
        "page_hint": page_hint,
        "page_number": _parse_page_number(page_hint),
        "notes": str(metadata.get("connection_detail") or "").strip(),
        "evidence_id": metadata.get("evidence_id"),
    }


def _companies_house_role_evidence(edge) -> dict[str, object] | None:
    source = _row_str(edge, "source")
    if source not in {"companies_house_company_officers", "companies_house_officer_appointments"}:
        return None
    provenance = _json_dict(edge["provenance_json"])
    if not isinstance(provenance, dict):
        return None

    web_root = "https://find-and-update.company-information.service.gov.uk"
    candidate_match = provenance.get("candidate_match", {})
    nested_evidence = candidate_match.get("evidence", {}) if isinstance(candidate_match, dict) else {}
    appointment = nested_evidence.get("appointment", {}) if isinstance(nested_evidence, dict) else {}
    officer_search_item = nested_evidence.get("officer_search_item", {}) if isinstance(nested_evidence, dict) else {}

    url = ""
    title = ""
    notes = ""

    if isinstance(officer_search_item, dict):
        officer_path = str((officer_search_item.get("links") or {}).get("self") or "").strip()
        if officer_path:
            url = officer_path if officer_path.startswith("http") else f"{web_root}{officer_path}"
        title = str(officer_search_item.get("title") or "").strip()
        notes = str(officer_search_item.get("description") or "").strip()

    if not url and isinstance(provenance.get("links"), dict):
        links = provenance.get("links") or {}
        officer_appointments = str(((links.get("officer") or {}).get("appointments")) or "").strip()
        self_path = str(links.get("self") or "").strip()
        next_path = officer_appointments or self_path
        if next_path:
            url = next_path if next_path.startswith("http") else f"{web_root}{next_path}"
        title = title or str(provenance.get("name") or "").strip()
        notes = notes or str(provenance.get("officer_role") or "").strip()

    if not url and isinstance(appointment, dict):
        company_number = str((appointment.get("appointed_to") or {}).get("company_number") or "").strip()
        if company_number:
            url = f"{web_root}/company/{company_number}"

    if not url:
        return None

    appointed_on = str(appointment.get("appointed_on") or provenance.get("appointed_on") or "").strip()
    resigned_on = str(appointment.get("resigned_on") or provenance.get("resigned_on") or "").strip()
    date_bits = [f"Appointed: {appointed_on}" if appointed_on else "", f"Resigned: {resigned_on}" if resigned_on else ""]
    date_note = "; ".join(bit for bit in date_bits if bit)
    if date_note:
        notes = f"{notes}. {date_note}".strip(". ").strip()

    role_label = _row_str(edge, "role_label") or _row_str(edge, "role_type") or "officer role"
    return {
        "title": title or f"Companies House {role_label}",
        "document_url": url,
        "page_hint": "",
        "page_number": None,
        "notes": notes,
    }


def _charity_commission_role_evidence(edge) -> dict[str, object] | None:
    source = _row_str(edge, "source").strip().lower()
    registry_type = _row_str(edge, "registry_type").strip().lower()
    registry_number = _row_str(edge, "registry_number").strip()
    if not source.startswith("charity_commission") or registry_type != "charity" or not registry_number:
        return None

    provenance = _json_dict(edge["provenance_json"])
    notes_bits = [
        _row_str(edge, "role_label") or _row_str(edge, "role_type"),
        str(provenance.get("Role") or provenance.get("role") or "").strip(),
    ]
    start_date = str(provenance.get("StartDate") or provenance.get("start_date") or "").strip()
    end_date = str(provenance.get("EndDate") or provenance.get("end_date") or "").strip()
    if start_date:
        notes_bits.append(f"Appointed: {start_date}")
    if end_date:
        notes_bits.append(f"Resigned: {end_date}")
    notes = "; ".join(bit for bit in notes_bits if bit)
    return {
        "title": "Charity Commission charity page",
        "document_url": (
            "https://register-of-charities.charitycommission.gov.uk/charity-search/-/charity-details/"
            f"{registry_number}"
        ),
        "page_hint": "",
        "page_number": None,
        "notes": notes,
    }


def _edge_evidence(edge) -> dict[str, object] | None:
    return (
        _pdf_role_evidence(edge)
        or _companies_house_role_evidence(edge)
        or _charity_commission_role_evidence(edge)
    )


def _canonical_role_phrase(text: str) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return ""
    if "trustee" in lowered:
        return "is a trustee of"
    if "director" in lowered:
        return "is a director of"
    if "secretary" in lowered:
        return "is a secretary of"
    if "accountant" in lowered or "examiner" in lowered or "auditor" in lowered:
        return "is listed in governance/finance docs for"
    return ""


def _role_phrase(edge) -> str:
    phrase = _row_str(edge, "relationship_phrase").strip()
    canonical = _canonical_role_phrase(phrase)
    if canonical:
        return canonical
    rt = _row_str(edge, "role_type")
    canonical = _canonical_role_phrase(rt)
    if canonical:
        return canonical
    role_label = _row_str(edge, "role_label")
    canonical = _canonical_role_phrase(role_label)
    if canonical:
        return canonical
    if phrase:
        return phrase
    return "is linked to"


def _role_key(edge) -> tuple[int, str]:
    return (int(edge["organisation_id"]), _role_phrase(edge))


def _sanction_warning(sanction: dict[str, object]) -> str:
    matches = sanction.get("matches") or []
    sources = sorted(
        {
            str(source).strip()
            for match in matches
            for source in (match.get("sources") or [match.get("source")])
            if str(source).strip()
        }
    )
    if not sources:
        return "\u26a0\ufe0f <strong>SANCTIONED (SANCTIONS LIST)</strong>"
    return (
        "\u26a0\ufe0f <strong>SANCTIONED</strong>: "
        + ", ".join(sources)
    )


def _tag_sanctioned_nodes(nodes: list[dict], sanctions_by_person_id: dict[int, dict[str, object]]) -> None:
    for node in nodes:
        person_ids_raw = node.get("person_ids") or []
        if not person_ids_raw:
            continue
        person_ids = [int(person_id) for person_id in person_ids_raw]
        matched = [
            sanctions_by_person_id[person_id]
            for person_id in person_ids
            if sanctions_by_person_id.get(person_id, {}).get("is_sanctioned")
        ]
        if not matched:
            continue
        node["sanctioned"] = True
        warning = _sanction_warning(matched[0])
        tooltip_lines = list(node.get("tooltip_lines") or [])
        if not tooltip_lines or tooltip_lines[0] != warning:
            node["tooltip_lines"] = [warning, *tooltip_lines]


def _address_edge_evidence(row) -> dict[str, object] | None:
    registry_type = str(row["registry_type"] or "").strip().lower()
    registry_number = str(row["registry_number"] or "").strip()
    if not registry_type or not registry_number:
        return None
    if registry_type == "company":
        return {
            "title": "Companies House company profile",
            "document_url": (
                "https://find-and-update.company-information.service.gov.uk/company/"
                f"{registry_number}"
            ),
        }
    if registry_type == "charity":
        return {
            "title": "Charity Commission charity page",
            "document_url": (
                "https://register-of-charities.charitycommission.gov.uk/charity-search/-/charity-details/"
                f"{registry_number}"
            ),
        }
    return None


def consolidate_run(run_id: int) -> dict:
    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()

    ranked = repository.get_ranked_people_for_run(run_id, limit=500)
    raw_edges = repository.get_run_network_edges(run_id)
    scoped_org_rows = repository.get_run_scoped_organisations(run_id)
    run_org_rows = repository.get_run_organisations(run_id)
    address_rows = repository.get_run_address_edges(run_id)
    run_row = repository.get_run(run_id)
    seed_name = str(run_row["seed_name"]) if run_row else "Seed"
    raw_edges = [edge for edge in raw_edges if not _is_notice_role_edge(edge)]
    run_org_rows = [
        row for row in run_org_rows
        if not _is_notice_org_mention(str(row["source"] or ""), _json_dict(row["run_metadata_json"]))
    ]

    people = [
        {
            "person_id": int(r["id"]),
            "name": str(r["canonical_name"]),
            "identity_key": str(r["identity_key"] or ""),
            "org_count": int(r["organisation_count"]),
            "role_count": int(r["role_count"]),
            "score": float(r["weighted_organisation_score"]),
        }
        for r in ranked
    ]
    sanctions_by_person_id = repository.get_person_sanctions([p["person_id"] for p in people])

    # --- alias grouping (DOB-aware to prevent transitive bridging) ---
    uf = UnionFind()
    person_dobs = [_extract_birth_month(p["identity_key"]) for p in people]
    group_dobs: dict[int, set[str]] = {}
    for i, dob in enumerate(person_dobs):
        if dob:
            group_dobs.setdefault(uf.find(i), set()).add(dob)

    for i, a in enumerate(people):
        for j in range(i + 1, len(people)):
            if not are_aliases(a["name"], people[j]["name"]):
                continue
            root_i, root_j = uf.find(i), uf.find(j)
            if root_i == root_j:
                continue
            combined = group_dobs.get(root_i, set()) | group_dobs.get(root_j, set())
            if len(combined) > 1:
                continue
            uf.union(i, j)
            new_root = uf.find(i)
            group_dobs[new_root] = combined

    groups: dict[int, list[int]] = {}
    for i in range(len(people)):
        groups.setdefault(uf.find(i), []).append(i)

    person_id_to_group_id: dict[int, str] = {}
    consolidated: list[dict] = []
    for _root, members in groups.items():
        entries = [people[i] for i in members]
        all_names = sorted(set(e["name"] for e in entries), key=lambda n: len(n))
        label = max(all_names, key=len)
        pid_set = {e["person_id"] for e in entries}
        group_id = f"person:{min(pid_set)}"

        org_ids: set[int] = set()
        role_keys: set[tuple[int, str]] = set()
        total_weight = 0.0
        for edge in raw_edges:
            if int(edge["person_id"]) in pid_set:
                org_ids.add(int(edge["organisation_id"]))
                role_keys.add(_role_key(edge))
                total_weight += float(edge["edge_weight"] or 0)

        identity_keys = sorted(set(
            e["identity_key"] for e in entries if e.get("identity_key")
        ))
        consolidated.append({
            "group_id": group_id,
            "label": label,
            "aliases": sorted(set(e["name"] for e in entries)),
            "identity_keys": identity_keys,
            "person_ids": sorted(pid_set),
            "org_count": len(org_ids),
            "role_count": len(role_keys),
            "score": round(total_weight, 4),
            "is_seed_alias": matches_seed_alias(seed_name, all_names),
        })
        for pid in pid_set:
            person_id_to_group_id[pid] = group_id

    consolidated.sort(key=lambda c: (-c["score"], -c["org_count"], c["label"]))

    # Determine the seed's birth month from a ch-name-dob key whose name
    # tokens match the seed name tokens (order-independent, since CH uses
    # surname-first while our normalize_name keeps given-names-first).
    seed_tokens = set(normalize_name(seed_name).split())
    seed_dob: str | None = None
    for c in consolidated:
        if not c["is_seed_alias"]:
            continue
        for key in c.get("identity_keys", []):
            if not key.startswith("ch-name-dob:"):
                continue
            parts = key.rsplit(":", 2)
            if len(parts) == 3 and set(parts[1].split()) == seed_tokens:
                seed_dob = parts[2]
                break
        if seed_dob:
            break

    if seed_dob:
        for c in consolidated:
            if not c["is_seed_alias"]:
                continue
            group_dobs = {
                _extract_birth_month(k)
                for k in c.get("identity_keys", [])
                if _extract_birth_month(k)
            }
            if group_dobs and seed_dob not in group_dobs:
                c["is_seed_alias"] = False

    seed_aliases = [c for c in consolidated if c["is_seed_alias"]]
    expanded_people = [c for c in consolidated if not c["is_seed_alias"]]

    # --- build edges with full metadata ---
    org_map: dict[int, dict] = {}
    org_registry_lookup: dict[tuple[str, str, int], int] = {}
    for row in scoped_org_rows:
        oid = int(row["id"])
        org_map[oid] = {
            "id": f"org:{oid}",
            "label": str(row["name"] or ""),
            "registry_type": str(row["registry_type"] or ""),
            "registry_number": str(row["registry_number"] or ""),
        }
        org_registry_lookup[
            (
                str(row["registry_type"] or ""),
                str(row["registry_number"] or ""),
                int(row["suffix"] or 0),
            )
        ] = oid

    org_org_edges: list[dict] = []
    seen_oo: set[tuple[str, str, str]] = set()
    for row in run_org_rows:
        child_org_id = int(row["id"])
        if child_org_id not in org_map:
            continue
        metadata = _json_dict(row["run_metadata_json"])
        parent_org_id = _resolve_parent_org_id(metadata, org_registry_lookup)
        if parent_org_id is None or parent_org_id == child_org_id:
            continue
        if parent_org_id not in org_map or child_org_id not in org_map:
            continue
        phrase = _linked_org_phrase(str(row["source"] or ""), metadata)
        detail = _linked_org_detail(metadata)
        source_id = f"org:{parent_org_id}"
        target_id = f"org:{child_org_id}"
        key = (source_id, target_id, phrase)
        if key in seen_oo:
            continue
        seen_oo.add(key)
        source_label = org_map[parent_org_id]["label"]
        target_label = org_map[child_org_id]["label"]
        org_org_edges.append({
            "source": source_id,
            "target": target_id,
            "kind": "org_link",
            "role_type": "organisation_link",
            "role_label": str(row["source"] or "organisation_link"),
            "phrase": phrase,
            "detail": detail,
            "source_provider": str(row["source"] or ""),
            "confidence": "medium",
            "weight": 0.55,
            "tooltip": f"{target_label} {phrase} {source_label}",
            "evidence": _pdf_org_evidence(metadata),
        })

    address_map: dict[int, dict] = {}
    org_addresses: dict[str, list[dict]] = defaultdict(list)
    for row in address_rows:
        aid = int(row["address_id"])
        if aid not in address_map:
            address_map[aid] = {
                "id": f"addr:{aid}",
                "label": str(row["address_label"] or ""),
                "normalized_key": str(row["normalized_key"] or ""),
                "postcode": str(row["postcode"] or ""),
                "country": str(row["country"] or ""),
            }
        org_addresses[f"org:{int(row['organisation_id'])}"].append(
            {
                "address_id": aid,
                "label": str(row["address_label"] or ""),
                "phrase": str(row["relationship_phrase"] or "is registered at"),
                "source": str(row["source"] or ""),
            }
        )

    # person→org edges with role detail
    person_org_edges: list[dict] = []
    seen_po: set[tuple[str, str, str]] = set()
    # also track per-person their roles for tooltip
    person_roles: dict[str, list[dict]] = defaultdict(list)
    # track per-org their people for tooltip
    org_people: dict[str, list[dict]] = defaultdict(list)

    for edge in raw_edges:
        pid = int(edge["person_id"])
        gid = person_id_to_group_id.get(pid)
        if not gid:
            continue
        org_id = f"org:{int(edge['organisation_id'])}"
        role_type = str(edge["role_type"] or "link")
        role_label = str(edge["role_label"] or "")
        phrase = _role_phrase(edge)
        detail = _pdf_role_detail(edge)
        evidence = _edge_evidence(edge)
        key = (gid, org_id, phrase)
        if key not in seen_po:
            seen_po.add(key)
            person_org_edges.append({
                "source": gid,
                "target": org_id,
                "role_type": role_type,
                "role_label": role_label,
                "phrase": phrase,
                "detail": detail,
                "source_provider": str(edge["source"] or ""),
                "confidence": str(edge["confidence_class"] or ""),
                "weight": float(edge["edge_weight"] or 0.35),
                "evidence": evidence,
            })
        elif evidence:
            for existing in person_org_edges:
                if existing["source"] == gid and existing["target"] == org_id and existing["phrase"] == phrase:
                    if not existing.get("evidence"):
                        existing["evidence"] = evidence
                    break
        group_entry = next((c for c in consolidated if c["group_id"] == gid), None)
        person_label = group_entry["label"] if group_entry else ""
        org_label = org_map[int(edge["organisation_id"])]["label"] if int(edge["organisation_id"]) in org_map else ""
        person_roles[gid].append({
            "phrase": phrase,
            "detail": detail,
            "org": org_label,
            "role_type": role_type,
            "role_label": role_label,
        })
        org_people[org_id].append({
            "person": person_label,
            "phrase": phrase,
            "detail": detail,
            "role_type": role_type,
        })

    # --- person↔person shared-org connections ---
    org_to_group_ids: dict[str, set[str]] = defaultdict(set)
    for edge in raw_edges:
        pid = int(edge["person_id"])
        gid = person_id_to_group_id.get(pid)
        if gid:
            org_to_group_ids[f"org:{int(edge['organisation_id'])}"].add(gid)

    shared_org_edges: list[dict] = []
    seen_pp: set[tuple[str, str]] = set()
    for org_id, group_ids in org_to_group_ids.items():
        glist = sorted(group_ids)
        for i, ga in enumerate(glist):
            for gb in glist[i + 1:]:
                pair = (min(ga, gb), max(ga, gb))
                if pair in seen_pp:
                    continue
                seen_pp.add(pair)
                org_label = org_map.get(int(org_id.split(":")[1]), {}).get("label", org_id)
                shared_org_edges.append({
                    "source": ga,
                    "target": gb,
                    "shared_org": org_label,
                    "shared_org_id": org_id,
                })

    # Deduplicate person roles for tooltips
    for gid in person_roles:
        seen = set()
        deduped = []
        for r in person_roles[gid]:
            key = (r["phrase"], r["org"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        person_roles[gid] = deduped

    for oid in org_people:
        seen = set()
        deduped = []
        for r in org_people[oid]:
            key = (r["person"], r["phrase"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        org_people[oid] = deduped

    for oid in org_addresses:
        seen = set()
        deduped = []
        for r in org_addresses[oid]:
            key = (r["label"], r["phrase"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        org_addresses[oid] = deduped

    # --- assemble nodes ---
    nodes: list[dict] = []

    nodes.append({
        "id": "seed",
        "label": seed_name,
        "kind": "seed",
        "lane": 0,
        "tooltip_lines": [f"Seed: {seed_name}"],
    })

    for entry in seed_aliases:
        roles = person_roles.get(entry["group_id"], [])
        tooltip = [f"<strong>{entry['label']}</strong>"]
        if len(entry["aliases"]) > 1:
            tooltip.append(f"Aliases: {', '.join(entry['aliases'])}")
        tooltip.append(f"{entry['org_count']} orgs, {entry['role_count']} roles, score {entry['score']}")
        for r in roles[:15]:
            tooltip.append(f"  {r['phrase']} <em>{r['org']}</em>")
        nodes.append({
            "id": entry["group_id"],
            "label": entry["label"],
            "kind": "seed_alias",
            "lane": 1,
            "aliases": entry["aliases"],
            "person_ids": entry.get("person_ids", []),
            "identity_keys": entry.get("identity_keys", []),
            "org_count": entry["org_count"],
            "role_count": entry["role_count"],
            "score": entry["score"],
            "tooltip_lines": tooltip,
        })

    for oid, info in org_map.items():
        people_list = org_people.get(info["id"], [])
        tooltip = [f"<strong>{info['label']}</strong>"]
        tooltip.append(f"{info['registry_type']} {info['registry_number']}")
        addresses = org_addresses.get(info["id"], [])
        if addresses:
            tooltip.append(f"{len(addresses)} linked addresses:")
            for addr in addresses[:5]:
                tooltip.append(f"  {addr['phrase']} <em>{addr['label']}</em>")
        tooltip.append(f"{len(people_list)} linked people:")
        for p in people_list[:20]:
            tooltip.append(f"  {p['person']} {p['phrase']}")
        nodes.append({
            "id": info["id"],
            "label": info["label"],
            "kind": "organisation",
            "lane": 2,
            "registry_type": info["registry_type"],
            "registry_number": info["registry_number"],
            "seed_names": [seed_name],
            "people_count": len(people_list),
            "tooltip_lines": tooltip,
        })

    for aid, info in address_map.items():
        tooltip = [f"<strong>{info['label']}</strong>"]
        if info["postcode"]:
            tooltip.append(f"Postcode: {info['postcode']}")
        if info["country"]:
            tooltip.append(f"Country: {info['country']}")
        nodes.append(
            {
                "id": info["id"],
                "label": info["label"],
                "kind": "address",
                "lane": 3,
                "normalized_key": info["normalized_key"],
                "postcode": info["postcode"],
                "country": info["country"],
                "tooltip_lines": tooltip,
            }
        )

    for entry in expanded_people:
        roles = person_roles.get(entry["group_id"], [])
        tooltip = [f"<strong>{entry['label']}</strong>"]
        if len(entry["aliases"]) > 1:
            tooltip.append(f"Aliases: {', '.join(entry['aliases'])}")
        tooltip.append(f"{entry['org_count']} orgs, {entry['role_count']} roles, score {entry['score']}")
        for r in roles[:15]:
            tooltip.append(f"  {r['phrase']} <em>{r['org']}</em>")
        nodes.append({
            "id": entry["group_id"],
            "label": entry["label"],
            "kind": "person",
            "lane": 4,
            "aliases": entry.get("aliases", []),
            "person_ids": entry.get("person_ids", []),
            "identity_keys": entry.get("identity_keys", []),
            "org_count": entry["org_count"],
            "role_count": entry["role_count"],
            "score": entry["score"],
            "tooltip_lines": tooltip,
        })

    # --- assemble edges ---
    graph_edges: list[dict] = []

    for entry in seed_aliases:
        graph_edges.append({
            "source": "seed",
            "target": entry["group_id"],
            "kind": "alias",
            "tooltip": f"{seed_name} = {entry['label']}",
        })

    for pe in person_org_edges:
        person_node = next((n for n in nodes if n["id"] == pe["source"]), None)
        org_node = next((n for n in nodes if n["id"] == pe["target"]), None)
        p_label = person_node["label"] if person_node else pe["source"]
        o_label = org_node["label"] if org_node else pe["target"]
        graph_edges.append({
            "source": pe["source"],
            "target": pe["target"],
            "kind": "role",
            "role_type": pe["role_type"],
            "role_label": pe["role_label"],
            "phrase": pe["phrase"],
            "source_provider": pe["source_provider"],
            "confidence": pe["confidence"],
            "weight": pe["weight"],
            "tooltip": f"{p_label} {pe['phrase']} {o_label}",
            "evidence": pe.get("evidence"),
        })

    graph_edges.extend(org_org_edges)

    for row in address_rows:
        graph_edges.append(
            {
                "source": f"org:{int(row['organisation_id'])}",
                "target": f"addr:{int(row['address_id'])}",
                "kind": "address_link",
                "role_type": "organisation_address",
                "role_label": "registered_address",
                "phrase": str(row["relationship_phrase"] or "is registered at"),
                "source_provider": str(row["source"] or ""),
                "confidence": "high",
                "weight": 0.8,
                "tooltip": (
                    f"{str(row['organisation_name'] or '')} "
                    f"{str(row['relationship_phrase'] or 'is registered at')} "
                    f"{str(row['address_label'] or '')}"
                ).replace("  ", " "),
                "evidence": _address_edge_evidence(row),
            }
        )

    for se in shared_org_edges:
        a_node = next((n for n in nodes if n["id"] == se["source"]), None)
        b_node = next((n for n in nodes if n["id"] == se["target"]), None)
        a_label = a_node["label"] if a_node else se["source"]
        b_label = b_node["label"] if b_node else se["target"]
        graph_edges.append({
            "source": se["source"],
            "target": se["target"],
            "kind": "shared_org",
            "shared_org": se["shared_org"],
            "tooltip": f"{a_label} & {b_label} share org: {se['shared_org']}",
        })

    _tag_sanctioned_nodes(nodes, sanctions_by_person_id)
    return {
        "seed_name": seed_name,
        "run_id": run_id,
        "consolidated": consolidated,
        "nodes": nodes,
        "edges": graph_edges,
    }


# ---------------------------------------------------------------------------
# Multi-seed merge
# ---------------------------------------------------------------------------

def consolidate_multi_run(run_ids: list[int]) -> dict:
    """Merge multiple runs into one graph while keeping per-seed identities.

    Layout:
      lane 0: seeds (one per run)
      lane 1: identity nodes / seed aliases (kept per run)
      lane 2: deduplicated organisations
      lane 3: deduplicated expanded people (merged only by alias matching)
    """
    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    runs = []
    total_runs = len(run_ids)
    for index, rid in enumerate(run_ids, start=1):
        print(f"[graph] Loading run {index}/{total_runs}: {rid}", flush=True)
        run = consolidate_run(rid)
        print(
            f"[graph] Loaded run {rid}: {len(run['nodes'])} nodes, {len(run['edges'])} edges",
            flush=True,
        )
        runs.append(run)
    seed_names = [r["seed_name"] for r in runs]

    def _dedupe_edges(items: list[dict], key_fields: tuple[str, ...]) -> list[dict]:
        seen: dict[tuple, dict] = {}
        out: list[dict] = []
        for item in items:
            key = tuple(item.get(field) for field in key_fields)
            existing = seen.get(key)
            if existing is not None:
                if item.get("evidence") and not existing.get("evidence"):
                    existing["evidence"] = item.get("evidence")
                if item.get("evidence_items") and not existing.get("evidence_items"):
                    existing["evidence_items"] = item.get("evidence_items")
                continue
            seen[key] = item
            out.append(item)
        return out

    def _surname_keys(entry: dict) -> set[str]:
        keys: set[str] = set()
        names = [str(entry["label"]), *(str(name) for name in entry.get("aliases") or [])]
        for name in names:
            tokens = _alias_tokens(name)
            if not tokens:
                continue
            surname = tokens[-1]
            keys.add(surname)
            for old, new in _FUZZY_SWAPS:
                if old in surname:
                    keys.add(surname.replace(old, new))
                if new in surname:
                    keys.add(surname.replace(new, old))
        return keys

    def _collect_entry_dobs(entries: list[dict]) -> dict[int, set[str]]:
        """Extract known birth months from each entry's identity_keys."""
        group_dobs: dict[int, set[str]] = {}
        for i, entry in enumerate(entries):
            for key in entry.get("identity_keys") or []:
                dob = _extract_birth_month(key)
                if dob:
                    group_dobs.setdefault(i, set()).add(dob)
        return group_dobs

    def _union_matching_entries(entries: list[dict], union_find: UnionFind) -> int:
        buckets: dict[str, list[int]] = defaultdict(list)
        for index, entry in enumerate(entries):
            for key in _surname_keys(entry):
                buckets[key].append(index)
        compared_pairs: set[tuple[int, int]] = set()
        comparisons = 0
        dob_groups = _collect_entry_dobs(entries)
        for bucket in buckets.values():
            if len(bucket) < 2:
                continue
            for offset, left_index in enumerate(bucket):
                for right_index in bucket[offset + 1:]:
                    pair = (left_index, right_index) if left_index < right_index else (right_index, left_index)
                    if pair in compared_pairs:
                        continue
                    compared_pairs.add(pair)
                    comparisons += 1
                    if not names_match(entries[left_index], entries[right_index]):
                        continue
                    root_l, root_r = union_find.find(left_index), union_find.find(right_index)
                    if root_l == root_r:
                        continue
                    combined = dob_groups.get(root_l, set()) | dob_groups.get(root_r, set())
                    if len(combined) > 1:
                        continue
                    union_find.union(left_index, right_index)
                    new_root = union_find.find(left_index)
                    dob_groups[new_root] = combined
        return comparisons

    def _union_matching_addresses(entries: list[dict], union_find: UnionFind) -> int:
        buckets: dict[str, list[int]] = defaultdict(list)
        for index, entry in enumerate(entries):
            for key in address_bucket_keys(entry):
                buckets[key].append(index)
        compared_pairs: set[tuple[int, int]] = set()
        comparisons = 0
        matcher = AddressMergeMatcher(load_settings())
        for bucket in buckets.values():
            if len(bucket) < 2:
                continue
            for offset, left_index in enumerate(bucket):
                for right_index in bucket[offset + 1:]:
                    pair = (left_index, right_index) if left_index < right_index else (right_index, left_index)
                    if pair in compared_pairs:
                        continue
                    compared_pairs.add(pair)
                    comparisons += 1
                    if addresses_match(entries[left_index], entries[right_index], matcher=matcher):
                        union_find.union(left_index, right_index)
        return comparisons

    print(f"[graph] Building merge context for {len(runs)} runs", flush=True)

    # Build per-run maps and gather entities for cross-run merging.
    run_contexts: list[dict] = []
    identity_entries: list[dict] = []
    person_entries: list[dict] = []
    address_nodes: dict[str, dict] = {}
    org_address_edges: list[dict] = []
    org_org_edges: list[dict] = []
    for seed_index, run in enumerate(runs):
        node_map: dict[str, dict] = {str(node["id"]): node for node in run["nodes"]}
        identity_ids = {str(node["id"]) for node in run["nodes"] if node["kind"] == "seed_alias"}
        person_ids = {str(node["id"]) for node in run["nodes"] if node["kind"] == "person"}
        for node in run["nodes"]:
            if node["kind"] == "address":
                address_id = str(node["id"])
                if address_id not in address_nodes:
                    address_nodes[address_id] = {
                        **node,
                        "id": address_id,
                        "lane": 3,
                    }
            if node["kind"] == "seed_alias":
                identity_entries.append({
                    "run_id": int(run["run_id"]),
                    "orig_id": str(node["id"]),
                    "label": str(node["label"]),
                    "aliases": list(node.get("aliases") or []),
                    "identity_keys": list(node.get("identity_keys") or []),
                })
            if node["kind"] != "person":
                continue
            person_entries.append({
                "run_id": int(run["run_id"]),
                "seed_index": seed_index,
                "seed_name": run["seed_name"],
                "orig_id": str(node["id"]),
                "label": str(node["label"]),
                "aliases": list(node.get("aliases") or []),
                "identity_keys": list(node.get("identity_keys") or []),
                "person_ids": [int(person_id) for person_id in (node.get("person_ids") or [])],
                "tooltip_lines": list(node.get("tooltip_lines") or []),
            })
        run_contexts.append({
            "run": run,
            "seed_index": seed_index,
            "node_map": node_map,
            "identity_ids": identity_ids,
            "person_ids": person_ids,
        })

    address_entries = list(address_nodes.values())
    print(
        f"[graph] Collected {len(person_entries)} people, {len(identity_entries)} identities, "
        f"and {len(address_entries)} addresses",
        flush=True,
    )
    print("[graph] Matching addresses across runs", flush=True)

    address_uf = UnionFind()
    address_comparisons = _union_matching_addresses(address_entries, address_uf)
    print(f"[graph] Compared {address_comparisons} candidate address pairs", flush=True)

    address_groups: dict[int, list[dict]] = {}
    for i, entry in enumerate(address_entries):
        address_groups.setdefault(address_uf.find(i), []).append(entry)
    print(f"[graph] Built {len(address_groups)} merged address groups", flush=True)

    address_entry_to_merged_id: dict[str, str] = {}
    merged_address_nodes: dict[str, dict] = {}
    for group_index, entries in enumerate(address_groups.values(), start=1):
        merged_id = f"merged_address:{group_index}"
        labels = sorted({str(entry["label"]).strip() for entry in entries if str(entry["label"]).strip()})
        postcodes = sorted({str(entry.get("postcode") or "").strip() for entry in entries if str(entry.get("postcode") or "").strip()})
        countries = sorted({str(entry.get("country") or "").strip() for entry in entries if str(entry.get("country") or "").strip()})
        normalized_keys = sorted({
            str(entry.get("normalized_key") or "").strip()
            for entry in entries
            if str(entry.get("normalized_key") or "").strip()
        })
        label = max(labels, key=len) if labels else str(entries[0].get("label") or "Unknown address")
        for entry in entries:
            address_entry_to_merged_id[str(entry["id"])] = merged_id
        tooltip = [f"<strong>{label}</strong>"]
        if len(labels) > 1:
            tooltip.append(f"Address variants: {', '.join(labels[:6])}")
        if postcodes:
            tooltip.append(f"Postcode: {postcodes[0]}")
        if countries:
            tooltip.append(f"Country: {countries[0]}")
        merged_address_nodes[merged_id] = {
            "id": merged_id,
            "label": label,
            "kind": "address",
            "lane": 3,
            "aliases": labels,
            "normalized_keys": normalized_keys,
            "postcode": postcodes[0] if postcodes else "",
            "country": countries[0] if countries else "",
            "tooltip_lines": tooltip,
            "shared": len(entries) > 1,
        }

    print("[graph] Matching people across runs", flush=True)

    # Merge stage-4 people across runs using the existing alias matcher.
    person_uf = UnionFind()
    person_comparisons = _union_matching_entries(person_entries, person_uf)
    print(f"[graph] Compared {person_comparisons} candidate person pairs", flush=True)

    merged_person_groups: dict[int, list[dict]] = {}
    for i, entry in enumerate(person_entries):
        merged_person_groups.setdefault(person_uf.find(i), []).append(entry)

    print(f"[graph] Built {len(merged_person_groups)} merged person groups", flush=True)

    person_entry_to_merged_id: dict[tuple[int, str], str] = {}
    merged_person_nodes: dict[str, dict] = {}
    merged_person_orgs: dict[str, set[str]] = defaultdict(set)
    merged_person_role_keys: dict[str, set[tuple[str, str]]] = defaultdict(set)
    merged_person_score: dict[str, float] = defaultdict(float)
    merged_person_roles: dict[str, list[dict]] = defaultdict(list)
    merged_person_seeds: dict[str, set[str]] = defaultdict(set)

    for group_index, entries in enumerate(merged_person_groups.values(), start=1):
        merged_id = f"merged_person:{group_index}"
        all_aliases = sorted({
            str(name)
            for entry in entries
            for name in [entry["label"], *entry.get("aliases", [])]
            if str(name).strip()
        })
        label = max(all_aliases, key=len) if all_aliases else entries[0]["label"]
        for entry in entries:
            person_entry_to_merged_id[(int(entry["run_id"]), str(entry["orig_id"]))] = merged_id
            merged_person_seeds[merged_id].add(str(entry["seed_name"]))
        merged_person_nodes[merged_id] = {
            "id": merged_id,
            "individual_key": merged_id,
            "label": label,
            "kind": "person",
            "lane": 4,
            "aliases": all_aliases,
            "person_ids": sorted(
                {
                    int(person_id)
                    for entry in entries
                    for person_id in (entry.get("person_ids") or [])
                }
            ),
            "org_count": 0,
            "role_count": 0,
            "score": 0.0,
            "shared": len({entry["seed_name"] for entry in entries}) > 1,
            "tooltip_lines": [],
        }

    all_identity_names: set[str] = set()
    for run in runs:
        all_identity_names.add(run["seed_name"])
        for node in run["nodes"]:
            if node["kind"] == "seed_alias":
                all_identity_names.add(str(node["label"]))
                for alias in node.get("aliases", []):
                    all_identity_names.add(str(alias))

    pruned_ids: set[str] = set()
    for mid, mnode in list(merged_person_nodes.items()):
        names = [mnode["label"], *(mnode.get("aliases") or [])]
        if any(matches_seed_alias(str(identity_name), names) for identity_name in all_identity_names):
            pruned_ids.add(mid)
    for mid in pruned_ids:
        del merged_person_nodes[mid]

    print("[graph] Matching identities across runs", flush=True)
    identity_uf = UnionFind()
    identity_comparisons = _union_matching_entries(identity_entries, identity_uf)
    print(f"[graph] Compared {identity_comparisons} candidate identity pairs", flush=True)

    identity_cluster_keys: dict[tuple[int, str], str] = {}
    identity_groups: dict[int, list[dict]] = {}
    for i, entry in enumerate(identity_entries):
        identity_groups.setdefault(identity_uf.find(i), []).append(entry)
    print(f"[graph] Built {len(identity_groups)} identity groups", flush=True)
    for group_index, entries in enumerate(identity_groups.values(), start=1):
        cluster_id = f"identity_cluster:{group_index}"
        for entry in entries:
            identity_cluster_keys[(int(entry["run_id"]), str(entry["orig_id"]))] = cluster_id

    nodes: list[dict] = []
    edges: list[dict] = []
    org_nodes: dict[str, dict] = {}
    org_people: dict[str, list[dict]] = defaultdict(list)
    org_identities: dict[str, list[dict]] = defaultdict(list)
    org_seed_names: dict[str, set[str]] = defaultdict(set)
    org_identity_seed_names: dict[str, set[str]] = defaultdict(set)
    identity_meta: dict[str, dict] = {}

    print("[graph] Building final merged nodes and edges", flush=True)

    # Keep seeds and identities separate per run.
    for context in run_contexts:
        run = context["run"]
        seed_index = context["seed_index"]
        seed_id = f"seed:{run['run_id']}"
        nodes.append({
            "id": seed_id,
            "label": run["seed_name"],
            "kind": "seed",
            "lane": 0,
            "seed_index": seed_index,
            "seed_name": run["seed_name"],
            "tooltip_lines": [f"Seed: {run['seed_name']}"],
        })

        for node in run["nodes"]:
            if node["kind"] != "seed_alias":
                continue
            identity_id = f"identity:{run['run_id']}:{node['id']}"
            identity_label = str(node["label"])
            identity_node = {
                **node,
                "id": identity_id,
                "individual_key": identity_cluster_keys.get((int(run["run_id"]), str(node["id"])), identity_id),
                "lane": 1,
                "seed_index": seed_index,
                "seed_name": run["seed_name"],
                "shared": False,
            }
            nodes.append(identity_node)
            identity_meta[identity_id] = identity_node
            edges.append({
                "source": seed_id,
                "target": identity_id,
                "kind": "alias",
                "tooltip": f"{run['seed_name']} = {identity_label}",
            })

    # Shared organisations plus merged org/people edges.
    identity_org_edges: list[dict] = []
    org_person_edges: list[dict] = []
    for context in run_contexts:
        run = context["run"]
        node_map = context["node_map"]
        identity_ids = context["identity_ids"]
        person_ids = context["person_ids"]

        for node in run["nodes"]:
            if node["kind"] != "organisation":
                continue
            org_id = str(node["id"])
            org_seed_names[org_id].add(str(run["seed_name"]))
            if org_id not in org_nodes:
                org_nodes[org_id] = {
                    **node,
                    "id": org_id,
                    "lane": 2,
                    "shared": False,
                    "seed_names": [],
                }

        for edge in run["edges"]:
            if edge.get("kind") == "org_link":
                source_id = str(edge["source"])
                target_id = str(edge["target"])
                if source_id in org_nodes and target_id in org_nodes:
                    org_org_edges.append(
                        {
                            "source": source_id,
                            "target": target_id,
                            "kind": "org_link",
                            "role_type": edge.get("role_type", "organisation_link"),
                            "role_label": edge.get("role_label", "organisation_link"),
                            "phrase": edge.get("phrase", ""),
                            "source_provider": edge.get("source_provider", ""),
                            "confidence": edge.get("confidence", ""),
                            "weight": float(edge.get("weight") or 0.55),
                            "tooltip": edge.get("tooltip", ""),
                            "evidence": edge.get("evidence"),
                        }
                    )
                continue
            if edge.get("kind") == "address_link":
                org_id = str(edge["source"])
                address_id = str(edge["target"])
                if org_id in org_nodes and address_id in address_nodes:
                    org_address_edges.append(
                        {
                            "source": org_id,
                            "target": address_entry_to_merged_id.get(address_id, address_id),
                            "kind": "address_link",
                            "role_type": "organisation_address",
                            "role_label": "registered_address",
                            "phrase": edge.get("phrase", ""),
                            "source_provider": edge.get("source_provider", ""),
                            "confidence": edge.get("confidence", ""),
                            "weight": float(edge.get("weight") or 0.8),
                            "tooltip": edge.get("tooltip", ""),
                            "evidence": edge.get("evidence"),
                        }
                    )
                continue
            if edge.get("kind") != "role":
                continue
            source_id = str(edge["source"])
            target_id = str(edge["target"])
            source_node = node_map.get(source_id)
            target_node = node_map.get(target_id)
            if source_node is None or target_node is None:
                continue

            org_node = source_node if source_node.get("kind") == "organisation" else target_node if target_node.get("kind") == "organisation" else None
            person_node = source_node if source_id in identity_ids or source_id in person_ids else target_node if target_id in identity_ids or target_id in person_ids else None
            person_orig_id = source_id if person_node is source_node else target_id if person_node is target_node else None
            if org_node is None or person_node is None or person_orig_id is None:
                continue

            org_id = str(org_node["id"])
            org_seed_names[org_id].add(str(run["seed_name"]))

            if person_orig_id in identity_ids:
                org_identity_seed_names[org_id].add(str(run["seed_name"]))
                identity_id = f"identity:{run['run_id']}:{person_orig_id}"
                identity_org_edges.append({
                    "source": identity_id,
                    "target": org_id,
                    "kind": "role",
                    "role_type": edge.get("role_type", ""),
                    "role_label": edge.get("role_label", ""),
                    "phrase": edge.get("phrase", ""),
                    "source_provider": edge.get("source_provider", ""),
                    "confidence": edge.get("confidence", ""),
                    "weight": float(edge.get("weight") or 0.35),
                    "tooltip": edge.get("tooltip", ""),
                    "evidence": edge.get("evidence"),
                })
                org_identities[org_id].append({
                    "identity": identity_meta[identity_id]["label"],
                    "seed": run["seed_name"],
                    "phrase": edge.get("phrase", ""),
                })
                continue

            if person_orig_id in person_ids:
                merged_person_id = person_entry_to_merged_id.get((int(run["run_id"]), person_orig_id))
                if not merged_person_id or merged_person_id not in merged_person_nodes:
                    continue
                phrase = str(edge.get("phrase", "") or "")
                merged_person_orgs[merged_person_id].add(org_id)
                merged_person_role_keys[merged_person_id].add((org_id, phrase))
                merged_person_score[merged_person_id] += float(edge.get("weight") or 0.35)
                org_person_edges.append({
                    "source": org_id,
                    "target": merged_person_id,
                    "kind": "role",
                    "role_type": edge.get("role_type", ""),
                    "role_label": edge.get("role_label", ""),
                    "phrase": phrase,
                    "source_provider": edge.get("source_provider", ""),
                    "confidence": edge.get("confidence", ""),
                    "weight": float(edge.get("weight") or 0.35),
                    "tooltip": edge.get("tooltip", ""),
                    "evidence": edge.get("evidence"),
                })
                person_label = merged_person_nodes[merged_person_id]["label"]
                org_people[org_id].append({
                    "person": person_label,
                    "phrase": phrase,
                })
                merged_person_roles[merged_person_id].append({
                    "phrase": phrase,
                    "org": org_nodes[org_id]["label"],
                })

    identity_org_edges = _dedupe_edges(identity_org_edges, ("source", "target", "phrase"))
    org_person_edges = _dedupe_edges(org_person_edges, ("source", "target", "phrase"))
    org_org_edges = _dedupe_edges(org_org_edges, ("source", "target", "phrase"))
    org_address_edges = _dedupe_edges(org_address_edges, ("source", "target", "phrase"))

    for org_id, node in org_nodes.items():
        node["shared"] = len(org_identity_seed_names.get(org_id, set())) > 1
        node["seed_names"] = sorted(org_seed_names.get(org_id, set()))
        identity_seen: set[tuple[str, str, str]] = set()
        identities = []
        for row in org_identities.get(org_id, []):
            key = (str(row["seed"]), str(row["identity"]), str(row["phrase"]))
            if key in identity_seen:
                continue
            identity_seen.add(key)
            identities.append(row)
        people_seen: set[tuple[str, str]] = set()
        people = []
        for row in org_people.get(org_id, []):
            key = (str(row["person"]), str(row["phrase"]))
            if key in people_seen:
                continue
            people_seen.add(key)
            people.append(row)
        tooltip = [f"<strong>{node['label']}</strong>"]
        if node.get("registry_type") or node.get("registry_number"):
            tooltip.append(f"{node.get('registry_type', '')} {node.get('registry_number', '')}".strip())
        if len(org_identity_seed_names.get(org_id, set())) > 1:
            tooltip.append(f"Shared by: {', '.join(sorted(org_identity_seed_names[org_id]))}")
        if identities:
            tooltip.append(f"{len(identities)} linked identities:")
            for row in identities[:10]:
                line = f"  {row['seed']}: {row['identity']} {row['phrase']}"
                if row.get("detail"):
                    line += f" ({row['detail']})"
                tooltip.append(line)
        if people:
            tooltip.append(f"{len(people)} linked people:")
            for row in people[:12]:
                line = f"  {row['person']} {row['phrase']}"
                if row.get("detail"):
                    line += f" ({row['detail']})"
                tooltip.append(line)
        node["tooltip_lines"] = tooltip
        nodes.append(node)

    address_org_ids: dict[str, set[str]] = defaultdict(set)
    for edge in org_address_edges:
        address_org_ids[str(edge["target"])].add(str(edge["source"]))

    for address_id, node in merged_address_nodes.items():
        linked_org_ids = sorted(address_org_ids.get(address_id, set()))
        seed_names = sorted({
            seed_name
            for org_id in linked_org_ids
            for seed_name in org_seed_names.get(org_id, set())
        })
        identity_refs = []
        identity_seen: set[tuple[str, str]] = set()
        for org_id in linked_org_ids:
            for row in org_identities.get(org_id, []):
                key = (str(row["seed"]), str(row["identity"]))
                if key in identity_seen:
                    continue
                identity_seen.add(key)
                identity_refs.append({
                    "seed": str(row["seed"]),
                    "identity": str(row["identity"]),
                })
        node["seed_names"] = seed_names
        node["appears_under_identities"] = identity_refs
        tooltip = list(node.get("tooltip_lines") or [f"<strong>{node['label']}</strong>"])
        if len(seed_names) > 1 and not identity_refs:
            tooltip.append(f"Appears under: {', '.join(seed_names)}")
        if identity_refs:
            tooltip.append("Appears under identities:")
            for row in identity_refs[:10]:
                tooltip.append(f"  {row['seed']}: {row['identity']}")
        node["tooltip_lines"] = tooltip
        nodes.append(node)

    merged_people_consolidated: list[dict] = []
    for merged_id, node in merged_person_nodes.items():
        node["org_count"] = len(merged_person_orgs.get(merged_id, set()))
        node["role_count"] = len(merged_person_role_keys.get(merged_id, set()))
        node["score"] = round(float(merged_person_score.get(merged_id, 0.0)), 4)
        identity_refs = []
        identity_seen: set[tuple[str, str]] = set()
        for org_id in sorted(merged_person_orgs.get(merged_id, set())):
            for row in org_identities.get(org_id, []):
                key = (str(row["seed"]), str(row["identity"]))
                if key in identity_seen:
                    continue
                identity_seen.add(key)
                identity_refs.append({
                    "seed": str(row["seed"]),
                    "identity": str(row["identity"]),
                })
        node["appears_under_identities"] = identity_refs
        tooltip = [f"<strong>{node['label']}</strong>"]
        if len(node.get("aliases") or []) > 1:
            tooltip.append(f"Aliases: {', '.join(node['aliases'])}")
        tooltip.append(f"{node['org_count']} orgs, {node['role_count']} roles, score {node['score']}")
        if len(merged_person_seeds.get(merged_id, set())) > 1 and not identity_refs:
            tooltip.append(f"Appears under: {', '.join(sorted(merged_person_seeds[merged_id]))}")
        if identity_refs:
            tooltip.append("Appears under identities:")
            for row in identity_refs[:10]:
                tooltip.append(f"  {row['seed']}: {row['identity']}")
        seen_lines: set[tuple[str, str]] = set()
        for row in merged_person_roles.get(merged_id, []):
            key = (str(row["phrase"]), str(row["org"]))
            if key in seen_lines:
                continue
            seen_lines.add(key)
            line = f"  {row['phrase']} <em>{row['org']}</em>"
            if row.get("detail"):
                line += f" ({row['detail']})"
            tooltip.append(line)
            if len(seen_lines) >= 15:
                break
        node["tooltip_lines"] = tooltip
        nodes.append(node)
        merged_people_consolidated.append({
            "group_id": merged_id,
            "label": node["label"],
            "aliases": list(node.get("aliases") or []),
            "person_ids": list(node.get("person_ids") or []),
            "org_count": node["org_count"],
            "role_count": node["role_count"],
            "score": node["score"],
            "is_seed_alias": False,
        })

    edges.extend(identity_org_edges)
    edges.extend(org_org_edges)
    edges.extend(org_address_edges)
    edges.extend(org_person_edges)

    consolidated: list[dict] = []
    for context in run_contexts:
        run = context["run"]
        for entry in run["consolidated"]:
            if not entry["is_seed_alias"]:
                continue
            consolidated.append({
                **entry,
                "group_id": f"identity:{run['run_id']}:{entry['group_id']}",
                "seed_name": run["seed_name"],
            })
    consolidated.extend(merged_people_consolidated)
    consolidated.sort(key=lambda c: (-float(c["score"]), -int(c["org_count"]), c["label"]))

    sanctions_by_person_id = repository.get_person_sanctions(
        [
            int(person_id)
            for node in nodes
            for person_id in (node.get("person_ids") or [])
        ]
    )
    _tag_sanctioned_nodes(nodes, sanctions_by_person_id)
    print(f"[graph] Final merged graph: {len(nodes)} nodes, {len(edges)} edges", flush=True)
    return {
        "seed_name": "Istari",
        "run_id": "+".join(str(r) for r in run_ids),
        "seed_names": seed_names,
        "runs": runs,
        "consolidated": consolidated,
        "nodes": nodes,
        "edges": edges,
    }


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def render_html(data: dict) -> str:
    nodes_json = json.dumps(data["nodes"], ensure_ascii=False).replace("</", "<\\/")
    edges_json = json.dumps(data["edges"], ensure_ascii=False).replace("</", "<\\/")
    seed_name = html.escape(data["seed_name"])
    run_id = data["run_id"]
    is_multi_seed = sum(1 for n in data["nodes"] if n["kind"] == "seed") > 1
    identity_dropdown_html = (
        '  <div class="org-dropdown" id="identity-dropdown">\n'
        '    <button class="org-dropdown-btn" id="identity-dropdown-btn">Identities &#9662;</button>\n'
        '    <div class="org-dropdown-menu" id="identity-dropdown-menu"></div>\n'
        "  </div>\n"
        if is_multi_seed else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Istari</title>
<style>
:root {{
  --bg: #0c0e14; --surface: #141820; --border: #1e2430;
  --text: #d0d4dc; --text-dim: #6b7385; --text-bright: #f0f2f5;
  --red: #e55561; --amber: #d4a017; --blue: #58a6ff; --green: #3fb950;
  --purple: #b382f0; --shared: #445577;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); overflow: hidden; height: 100vh; }}
.topbar {{
  display: flex; align-items: center; gap: 16px; padding: 12px 20px;
  background: var(--surface); border-bottom: 1px solid var(--border); z-index: 20; position: relative;
}}
.topbar h1 {{ font-size: 16px; font-weight: 700; color: var(--text-bright); white-space: nowrap; }}
.topbar .stats {{ font-size: 12px; color: var(--text-dim); white-space: nowrap; }}
.search-box {{
  margin-left: auto; display: flex; align-items: center; gap: 6px;
}}
.search-box input {{
  width: 220px; padding: 6px 12px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--bg); color: var(--text); font-size: 13px; outline: none;
}}
.search-box input:focus {{ border-color: var(--blue); }}
.toggle {{
  display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-dim);
  cursor: pointer; white-space: nowrap; user-select: none;
}}
.toggle input {{ accent-color: var(--blue); cursor: pointer; }}
.toggle input:checked + span {{ color: var(--blue); }}
.org-dropdown {{ position: relative; margin-left: 8px; }}
.org-dropdown-btn {{
  background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
  color: var(--text); font-size: 12px; padding: 5px 12px; cursor: pointer; white-space: nowrap;
}}
.org-dropdown-btn:hover {{ border-color: var(--blue); }}
.org-dropdown-menu {{
  display: none; position: absolute; top: 100%; left: 0; margin-top: 4px;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 8px 0; min-width: 280px; max-height: 360px; overflow-y: auto; z-index: 50;
  box-shadow: 0 8px 24px rgba(0,0,0,0.5);
}}
.org-dropdown-menu.open {{ display: block; }}
.org-dropdown-menu label {{
  display: flex; align-items: center; gap: 8px; padding: 4px 14px; font-size: 11.5px;
  color: var(--text); cursor: pointer; white-space: nowrap;
}}
.org-dropdown-menu label:hover {{ background: rgba(255,255,255,0.04); }}
.org-dropdown-menu input {{ accent-color: var(--green); cursor: pointer; }}
.search-box .clear-btn {{
  background: none; border: none; color: var(--text-dim); cursor: pointer; font-size: 16px; padding: 2px 6px;
}}
.legend {{
  position: absolute; top: 56px; right: 12px; z-index: 15;
  display: flex; flex-direction: column; gap: 5px; background: var(--surface);
  border: 1px solid var(--border); border-radius: 8px; padding: 10px 14px; font-size: 11px;
}}
.legend .row {{ display: flex; align-items: center; gap: 6px; }}
.legend .dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
.legend .icon-chip {{
  width: 18px; height: 18px; border-radius: 999px; display: inline-flex;
  align-items: center; justify-content: center; flex-shrink: 0;
  border: 1px solid rgba(255,255,255,0.18);
}}
.legend .icon-chip svg {{ width: 12px; height: 12px; overflow: visible; }}
.legend .icon-chip path {{
  fill: none; stroke: currentColor; stroke-width: 1.8;
  stroke-linecap: round; stroke-linejoin: round;
}}
#graph {{ width: 100vw; height: calc(100vh - 48px); overflow: hidden; }}
.tooltip {{
  position: fixed; background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 12px 16px; font-size: 12px; line-height: 1.6; pointer-events: none; z-index: 100;
  max-width: 380px; box-shadow: 0 8px 24px rgba(0,0,0,0.6); display: none;
}}
.tooltip strong {{ color: var(--text-bright); }}
.tooltip em {{ color: var(--green); font-style: normal; }}
.tooltip .dim {{ color: var(--text-dim); }}
.focus-panel {{
  position: fixed; top: 60px; right: 12px; z-index: 200;
  background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
  padding: 18px 22px; font-size: 12.5px; line-height: 1.7;
  max-width: 420px; max-height: calc(100vh - 100px); overflow-y: auto;
  box-shadow: 0 12px 40px rgba(0,0,0,0.7); display: none;
}}
.focus-panel h3 {{ margin: 0 0 8px; font-size: 15px; color: var(--text-bright); }}
.focus-panel .close-btn {{
  position: absolute; top: 8px; right: 12px; background: none; border: none;
  color: var(--text-dim); font-size: 20px; cursor: pointer; line-height: 1;
}}
.focus-panel .close-btn:hover {{ color: var(--text-bright); }}
.focus-panel .section {{ margin-top: 10px; }}
.focus-panel .section-title {{ font-weight: 700; color: var(--blue); font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
.focus-panel .conn {{ padding: 3px 0; color: var(--text); }}
.focus-panel .conn em {{ color: var(--green); font-style: normal; }}
.focus-panel .conn .dim {{ color: var(--text-dim); }}
svg text {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }}
</style>
</head>
<body>
<div class="topbar">
  <h1>Istari</h1>
{identity_dropdown_html}
  <div class="org-dropdown" id="type-dropdown">
    <button class="org-dropdown-btn" id="type-dropdown-btn">Types &#9662;</button>
    <div class="org-dropdown-menu" id="type-dropdown-menu"></div>
  </div>
  <label class="toggle">
    <input id="stage3-multi-org-only" type="checkbox" />
    <span>only show individuals connected to 2+ organisations</span>
  </label>
  <label class="toggle">
    <input id="org-multi-person-only" type="checkbox" />
    <span>only show organisations connected to 2+ individuals</span>
  </label>
  <label class="toggle">
    <input id="overlaps-only" type="checkbox" />
    <span>show overlaps only</span>
  </label>
  <label class="toggle">
    <input id="indirect-orgs" type="checkbox" />
    <span>reveal indirectly connected orgs</span>
  </label>
  <div class="search-box">
    <input id="search" type="search" placeholder="Filter by name..." autocomplete="off" />
    <button class="clear-btn" id="clear-search">&times;</button>
  </div>
</div>
<div id="graph"></div>
<div class="legend" id="legend"></div>
<div class="tooltip" id="tooltip"></div>
<div class="focus-panel" id="focus-panel"><button class="close-btn" id="focus-close">&times;</button><div id="focus-content"></div></div>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<script>
const allNodes = {nodes_json};
const allEdges = {edges_json}.filter(e => e.kind !== "shared_org" && e.kind !== "cross_seed");
const multiSeed = allNodes.filter(n => n.kind === "seed").length > 1;

const container = document.getElementById("graph");
const tooltipEl = document.getElementById("tooltip");
const searchInput = document.getElementById("search");
const clearBtn = document.getElementById("clear-search");
const stage3MultiOrgToggle = document.getElementById("stage3-multi-org-only");
const orgMultiPersonToggle = document.getElementById("org-multi-person-only");
const overlapsOnlyToggle = document.getElementById("overlaps-only");
const indirectOrgsToggle = document.getElementById("indirect-orgs");

const W = container.clientWidth;
const H = container.clientHeight;

const LANE_Y = {{ 0: 80, 1: 260, 2: 520, 3: 760, 4: 980 }};
const LANE_NAMES = {{ 1: "Identity", 2: "Organisations", 3: "Addresses", 4: "People" }};

const svg = d3.select(container).append("svg")
  .attr("width", "100%").attr("height", "100%")
  .style("display", "block");
const gRoot = svg.append("g");
const zoom = d3.zoom().scaleExtent([0.05, 6]).on("zoom", (e) => gRoot.attr("transform", e.transform));
svg.call(zoom);
svg.on("dblclick.zoom", null);

const nodeById = new Map(allNodes.map(n => [n.id, n]));
const edgesByNodeId = new Map();
allEdges.forEach(e => {{
  if (!edgesByNodeId.has(e.source)) edgesByNodeId.set(e.source, []);
  if (!edgesByNodeId.has(e.target)) edgesByNodeId.set(e.target, []);
  edgesByNodeId.get(e.source).push(e);
  edgesByNodeId.get(e.target).push(e);
}});
const directEdgePairs = new Set(
  allEdges.map(e => {{
    const [a, b] = [e.source, e.target].sort();
    return `${{a}}||${{b}}`;
  }})
);

// -- build identity and people dropdowns --
const identityNodes = allNodes
  .filter(n => n.lane === 1)
  .sort((a, b) => (a.seed_index - b.seed_index) || a.label.localeCompare(b.label));
const selectedIdentities = new Set(identityNodes.map(n => n.id));

const peopleNodes = allNodes.filter(n => n.lane === 4).sort((a, b) => a.label.localeCompare(b.label));
const selectedPeople = new Set(peopleNodes.map(n => n.id));

const identityDropdownBtn = document.getElementById("identity-dropdown-btn");
const identityDropdownMenu = document.getElementById("identity-dropdown-menu");
const peopleDropdownBtn = null;
const peopleDropdownMenu = null;
const typeDropdownBtn = document.getElementById("type-dropdown-btn");
const typeDropdownMenu = document.getElementById("type-dropdown-menu");
const legendEl = document.getElementById("legend");
const nodeTypeOptions = [
  {{ key: "identity", label: "Identity" }},
  {{ key: "charity", label: "Charity" }},
  {{ key: "company", label: "Company" }},
  {{ key: "organisation", label: "Other organisation" }},
  {{ key: "address", label: "Address" }},
  {{ key: "person", label: "Person" }},
];
const selectedNodeTypes = new Set(nodeTypeOptions.map(opt => opt.key));

function nodeTypeKey(node) {{
  if (node.kind === "seed") return "seed";
  if (node.lane === 1) return "identity";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "charity") return "charity";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "company") return "company";
  if (node.kind === "organisation") return "organisation";
  if (node.kind === "address") return "address";
  return "person";
}}

// per-person org count for multi-org filter
const personOrgIds = new Map();
allEdges.filter(e => e.kind === "role").forEach(e => {{
  const personId = nodeById.get(e.source)?.kind === "organisation" ? e.target : e.source;
  const orgId = nodeById.get(e.source)?.kind === "organisation" ? e.source : e.target;
  const personNode = nodeById.get(personId);
  const orgNode = nodeById.get(orgId);
  if (personNode?.lane !== 4 || orgNode?.kind !== "organisation") return;
  if (!personOrgIds.has(personId)) personOrgIds.set(personId, new Set());
  personOrgIds.get(personId).add(orgId);
}});

// per-organisation individual count for org filter
const orgPersonIds = new Map();
allEdges.filter(e => e.kind === "role").forEach(e => {{
  const sourceNode = nodeById.get(e.source);
  const targetNode = nodeById.get(e.target);
  const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
  const personNode = sourceNode?.kind === "organisation" ? targetNode : sourceNode;
  if (!orgNode || !personNode) return;
  if (personNode.lane !== 1 && personNode.lane !== 4) return;
  if (!orgPersonIds.has(orgNode.id)) orgPersonIds.set(orgNode.id, new Set());
  orgPersonIds.get(orgNode.id).add(personNode.individual_key || personNode.id);
}});

const orgLinkIds = new Map();
allEdges.filter(e => e.kind === "org_link").forEach(e => {{
  if (!orgLinkIds.has(e.source)) orgLinkIds.set(e.source, new Set());
  if (!orgLinkIds.has(e.target)) orgLinkIds.set(e.target, new Set());
  orgLinkIds.get(e.source).add(e.target);
  orgLinkIds.get(e.target).add(e.source);
}});

const orgAddressIds = new Map();
const addressOrgIds = new Map();
allEdges.filter(e => e.kind === "address_link").forEach(e => {{
  const sourceNode = nodeById.get(e.source);
  const targetNode = nodeById.get(e.target);
  const orgId = sourceNode?.kind === "organisation" ? e.source : targetNode?.kind === "organisation" ? e.target : null;
  const addrId = sourceNode?.kind === "address" ? e.source : targetNode?.kind === "address" ? e.target : null;
  if (!orgId || !addrId) return;
  if (!orgAddressIds.has(orgId)) orgAddressIds.set(orgId, new Set());
  orgAddressIds.get(orgId).add(addrId);
  if (!addressOrgIds.has(addrId)) addressOrgIds.set(addrId, new Set());
  addressOrgIds.get(addrId).add(orgId);
}});

const indirectOrgIndividuals = new Map();
allNodes.filter(n => n.lane === 1 || n.lane === 4).forEach(individual => {{
  const directOrgs = new Set();
  (edgesByNodeId.get(individual.id) || []).forEach(e => {{
    if (e.kind !== "role") return;
    const otherId = e.source === individual.id ? e.target : e.source;
    const other = nodeById.get(otherId);
    if (other?.kind === "organisation") directOrgs.add(otherId);
  }});
  if (!directOrgs.size) return;
  const reachableOrgs = new Set();
  directOrgs.forEach(orgId => {{
    (orgLinkIds.get(orgId) || new Set()).forEach(id => reachableOrgs.add(id));
    (orgAddressIds.get(orgId) || new Set()).forEach(addrId => {{
      (addressOrgIds.get(addrId) || new Set()).forEach(id => reachableOrgs.add(id));
    }});
  }});
  directOrgs.forEach(id => reachableOrgs.delete(id));
  reachableOrgs.forEach(orgId => {{
    if (!indirectOrgIndividuals.has(orgId)) indirectOrgIndividuals.set(orgId, new Set());
    indirectOrgIndividuals.get(orgId).add(individual.id);
  }});
}});

function expandOrgIdsThroughOrgLinks(startOrgIds) {{
  const expanded = new Set(startOrgIds);
  const queue = [...startOrgIds];
  while (queue.length) {{
    const orgId = queue.shift();
    const linked = orgLinkIds.get(orgId) || new Set();
    linked.forEach(otherId => {{
      if (expanded.has(otherId)) return;
      expanded.add(otherId);
      queue.push(otherId);
    }});
  }}
  return expanded;
}}

function activeIdentityIds() {{
  if (!multiSeed) return new Set(identityNodes.map(n => n.id));
  return new Set(identityNodes.filter(n => selectedIdentities.has(n.id)).map(n => n.id));
}}

function candidateOrgIdsForSelectedIdentities() {{
  if (!multiSeed) return new Set(allNodes.filter(n => n.kind === "organisation").map(n => n.id));
  const activeSeedNames = new Set(
    identityNodes
      .filter(n => selectedIdentities.has(n.id))
      .map(n => n.seed_name)
  );
  return new Set(
    allNodes
      .filter(n => n.kind === "organisation")
      .filter(n => (n.seed_names || []).some(seedName => activeSeedNames.has(seedName)))
      .map(n => n.id)
  );
}}

function filteredOrgIdsForCurrentFilters() {{
  const candidateOrgIds = candidateOrgIdsForSelectedIdentities();
  return new Set(
    [...candidateOrgIds].filter(orgId => {{
      if (overlapsOnlyToggle?.checked) {{
        const node = nodeById.get(orgId);
        if ((node?.seed_names || []).length < 2) return false;
      }}
      if (orgMultiPersonToggle?.checked && (orgPersonIds.get(orgId)?.size || 0) < 2) return false;
      return true;
    }})
  );
}}

function visiblePeopleList() {{
  const visibleOrgIds = filteredOrgIdsForCurrentFilters();
  return peopleNodes.filter(n => {{
    if (stage3MultiOrgToggle?.checked && (n.org_count || 0) < 2) return false;
    const myOrgs = personOrgIds.get(n.id) || new Set();
    return [...myOrgs].some(orgId => visibleOrgIds.has(orgId));
  }});
}}

function visibleAddressIdsForVisibleOrgs(visibleOrgs) {{
  const visibleAddresses = new Set();
  allEdges.forEach(e => {{
    if (e.kind !== "address_link") return;
    const sourceNode = nodeById.get(e.source);
    const targetNode = nodeById.get(e.target);
    const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
    const addressNode = sourceNode?.kind === "address" ? sourceNode : targetNode?.kind === "address" ? targetNode : null;
    if (!orgNode || !addressNode) return;
    if (!visibleOrgs.has(orgNode.id)) return;
    visibleAddresses.add(addressNode.id);
  }});
  return visibleAddresses;
}}

function orgIdsForVisiblePeople(visiblePeople) {{
  const orgIds = new Set();
  visiblePeople.forEach(personId => {{
    const myOrgs = personOrgIds.get(personId) || new Set();
    [...myOrgs].forEach(orgId => orgIds.add(orgId));
  }});
  return orgIds;
}}

function buildIdentityDropdown() {{
  if (!identityDropdownMenu || !identityDropdownBtn) return;
  identityDropdownMenu.innerHTML = "";

  const allRow = document.createElement("label");
  allRow.style.fontWeight = "600";
  allRow.style.borderBottom = "1px solid var(--border)";
  allRow.style.paddingBottom = "6px";
  allRow.style.marginBottom = "4px";
  const allCb = document.createElement("input");
  allCb.type = "checkbox";
  allCb.checked = identityNodes.every(n => selectedIdentities.has(n.id));
  allCb.addEventListener("change", () => {{
    if (allCb.checked) identityNodes.forEach(n => selectedIdentities.add(n.id));
    else selectedIdentities.clear();
    buildIdentityDropdown();
    buildPeopleDropdown();
    applyFilter();
  }});
  allRow.appendChild(allCb);
  allRow.appendChild(document.createTextNode(" All identities"));
  identityDropdownMenu.appendChild(allRow);

  identityNodes.forEach(identity => {{
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selectedIdentities.has(identity.id);
    cb.addEventListener("change", () => {{
      if (cb.checked) selectedIdentities.add(identity.id);
      else selectedIdentities.delete(identity.id);
      buildPeopleDropdown();
      applyFilter();
    }});
    label.appendChild(cb);
    label.appendChild(document.createTextNode(` ${{identity.seed_name}}: ${{identity.label}}`));
    identityDropdownMenu.appendChild(label);
  }});
}}

function buildPeopleDropdown() {{
  if (!peopleDropdownMenu) return;
  peopleDropdownMenu.innerHTML = "";
  const visList = visiblePeopleList();

  const allRow = document.createElement("label");
  allRow.style.fontWeight = "600";
  allRow.style.borderBottom = "1px solid var(--border)";
  allRow.style.paddingBottom = "6px";
  allRow.style.marginBottom = "4px";
  const allCb = document.createElement("input");
  allCb.type = "checkbox";
  allCb.checked = visList.every(n => selectedPeople.has(n.id));
  allCb.addEventListener("change", () => {{
    if (allCb.checked) visList.forEach(n => selectedPeople.add(n.id));
    else visList.forEach(n => selectedPeople.delete(n.id));
    buildPeopleDropdown();
    applyFilter();
  }});
  allRow.appendChild(allCb);
  allRow.appendChild(document.createTextNode(" All people"));
  peopleDropdownMenu.appendChild(allRow);

  visList.forEach(person => {{
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selectedPeople.has(person.id);
    cb.addEventListener("change", () => {{
      if (cb.checked) selectedPeople.add(person.id);
      else selectedPeople.delete(person.id);
      applyFilter();
    }});
    label.appendChild(cb);
    label.appendChild(document.createTextNode(" " + person.label));
    peopleDropdownMenu.appendChild(label);
  }});
}}
buildIdentityDropdown();
buildPeopleDropdown();
function buildTypeDropdown() {{
  if (!typeDropdownMenu || !typeDropdownBtn) return;
  typeDropdownMenu.innerHTML = "";

  const allRow = document.createElement("label");
  allRow.style.fontWeight = "600";
  allRow.style.borderBottom = "1px solid var(--border)";
  allRow.style.paddingBottom = "6px";
  allRow.style.marginBottom = "4px";
  const allCb = document.createElement("input");
  allCb.type = "checkbox";
  allCb.checked = nodeTypeOptions.every(opt => selectedNodeTypes.has(opt.key));
  allCb.addEventListener("change", () => {{
    if (allCb.checked) nodeTypeOptions.forEach(opt => selectedNodeTypes.add(opt.key));
    else selectedNodeTypes.clear();
    buildTypeDropdown();
    applyFilter();
  }});
  allRow.appendChild(allCb);
  allRow.appendChild(document.createTextNode(" All types"));
  typeDropdownMenu.appendChild(allRow);

  nodeTypeOptions.forEach(option => {{
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selectedNodeTypes.has(option.key);
    cb.addEventListener("change", () => {{
      if (cb.checked) selectedNodeTypes.add(option.key);
      else selectedNodeTypes.delete(option.key);
      buildTypeDropdown();
      applyFilter();
    }});
    label.appendChild(cb);
    label.appendChild(document.createTextNode(` ${{option.label}}`));
    typeDropdownMenu.appendChild(label);
  }});

  const selectedCount = nodeTypeOptions.filter(opt => selectedNodeTypes.has(opt.key)).length;
  typeDropdownBtn.textContent = selectedCount === nodeTypeOptions.length
    ? "Types ▾"
    : `${{selectedCount}}/${{nodeTypeOptions.length}} types ▾`;
}}
buildTypeDropdown();

identityDropdownBtn?.addEventListener("click", (e) => {{
  e.stopPropagation();
  identityDropdownMenu.classList.toggle("open");
}});
peopleDropdownBtn?.addEventListener("click", (e) => {{
  e.stopPropagation();
  peopleDropdownMenu.classList.toggle("open");
}});
typeDropdownBtn?.addEventListener("click", (e) => {{
  e.stopPropagation();
  typeDropdownMenu?.classList.toggle("open");
}});
document.addEventListener("click", () => {{
  identityDropdownMenu?.classList.remove("open");
  peopleDropdownMenu?.classList.remove("open");
  typeDropdownMenu?.classList.remove("open");
}});
identityDropdownMenu?.addEventListener("click", (e) => e.stopPropagation());
peopleDropdownMenu?.addEventListener("click", (e) => e.stopPropagation());
typeDropdownMenu?.addEventListener("click", (e) => e.stopPropagation());

// -- measure text widths via hidden canvas --
const measureCtx = document.createElement("canvas").getContext("2d");
function textWidth(text, fs) {{
  measureCtx.font = `${{fs}}px 'Segoe UI', system-ui, sans-serif`;
  return measureCtx.measureText(text).width;
}}

function nodeLabel(d) {{
  return d.label;
}}
function iconPath(kind) {{
  if (kind === "identity") return "M12 12a3 3 0 1 0 0-6a3 3 0 0 0 0 6ZM6.5 18a5.5 5.5 0 0 1 11 0";
  if (kind === "address") return "M12 21s-5-4.35-5-8.5a5 5 0 1 1 10 0C17 16.65 12 21 12 21Zm0-7a1.8 1.8 0 1 0 0-3.6A1.8 1.8 0 0 0 12 14Z";
  if (kind === "search") return "M11 18a7 7 0 1 1 4.95-2.05 M16 16l4 4";
  if (kind === "accountancy") return "M8 3h8a2 2 0 0 1 2 2v14H6V5a2 2 0 0 1 2-2Zm1 4h6M9 11h2M13 11h2M9 15h2M13 15h2";
  if (kind === "charity") return "M12 20s-6-3.9-6-8.2A3.8 3.8 0 0 1 12 9a3.8 3.8 0 0 1 6 2.8C18 16.1 12 20 12 20Z";
  if (kind === "company") return "M4 20h16M6 20V9l4-3v14M14 20V5h4v15M8 11h.01M8 14h.01M8 17h.01M16 9h.01M16 12h.01M16 15h.01";
  if (kind === "organisation") return "M12 5v3M7 18h10M8 18l1.5-6h5L16 18M6 10h12";
  return "M12 12a3 3 0 1 0 0-6a3 3 0 0 0 0 6Zm-5.5 7a5.5 5.5 0 0 1 11 0";
}}
function iconSpec(kind) {{
  if (kind === "identity") return {{ fill: "var(--amber)", color: "#0f172a", path: iconPath("identity") }};
  if (kind === "address") return {{ fill: "var(--purple)", color: "#ffffff", path: iconPath("address") }};
  if (kind === "accountancy") return {{ fill: "#8b5cf6", color: "#ffffff", path: iconPath("accountancy") }};
  if (kind === "charity") return {{ fill: "var(--green)", color: "#ffffff", path: iconPath("charity") }};
  if (kind === "company") return {{ fill: "#0ea5e9", color: "#ffffff", path: iconPath("company") }};
  if (kind === "organisation") return {{ fill: "#475569", color: "#ffffff", path: iconPath("organisation") }};
  return {{ fill: "var(--blue)", color: "#ffffff", path: iconPath("person") }};
}}
function badgeSpec(d) {{
  const label = (d.label || "").toLowerCase();
  const looksLikeAccountancy = /(account|audit|auditor|accountant|accounting|chartered)/.test(label);
  if (d.kind === "seed") return null;
  if (d.kind === "seed_alias") return iconSpec("identity");
  if (d.kind === "address") return iconSpec("address");
  if (looksLikeAccountancy) return iconSpec("accountancy");
  if (d.kind === "organisation" && (d.registry_type || "").toLowerCase() === "charity") return iconSpec("charity");
  if (d.kind === "organisation" && (d.registry_type || "").toLowerCase() === "company") return iconSpec("company");
  if (d.kind === "organisation") return iconSpec("organisation");
  return iconSpec("person");
}}
function badgeWidth(d) {{ return badgeSpec(d) ? 18 : 0; }}
function badgeHeight(d) {{ return Math.max(14, pillHeight(d) - 6); }}
function badgeTextInset(d) {{ return badgeSpec(d) ? 34 : 16; }}
function iconSvgMarkup(spec) {{
  return `<span class="icon-chip" style="background:${{spec.fill}};color:${{spec.color}}">
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="${{spec.path}}"></path></svg>
  </span>`;
}}
function renderLegend() {{
  if (!legendEl) return;
  const rows = [
    `<div class="row">${{iconSvgMarkup(iconSpec("identity"))}} Identity</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("charity"))}} Charity</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("company"))}} Company</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("accountancy"))}} Accountancy firm</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("organisation"))}} Other organisation</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("address"))}} Address</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("person"))}} Person</div>`,
    `<div class="row"><span class="dot" style="background:#ff2222;border:2px solid #ff2222"></span> Sanctioned</div>`,
  ];
  legendEl.innerHTML = rows.join("");
}}
renderLegend();
function nodeMatchesQuery(d, q) {{
  if (!q) return true;
  const labelMatch = (d.label || "").toLowerCase().includes(q);
  const aliasMatch = (d.aliases || []).some(a => (a || "").toLowerCase().includes(q));
  return labelMatch || aliasMatch;
}}
let searchOrFocusMode = false;
let stage3FocusNodeId = null;
function canInspectUpstream(d) {{ return d.kind === "address" || d.lane === 4; }}
function fontSize(d) {{
  if (d.kind === "seed") return 13;
  if (d.kind === "seed_alias") return 12;
  return 10.5;
}}
function pillHeight(d) {{ return fontSize(d) + 12; }}
function focusButtonWidth(d) {{ return canInspectUpstream(d) ? 24 : 0; }}
function pillWidth(d) {{ return badgeWidth(d) + textWidth(nodeLabel(d), fontSize(d)) + 32 + focusButtonWidth(d); }}

function nodeColor(d) {{
  if (d.sanctioned) return "#ff2222";
  if (d.kind === "seed") return "var(--red)";
  if (d.kind === "seed_alias") return "var(--amber)";
  if (d.kind === "organisation") return "var(--green)";
  if (d.kind === "address") return "var(--purple)";
  return "var(--blue)";
}}
function edgeStroke(d) {{
  if (d.kind === "hidden_connection") return "#94a3b8";
  if (d.kind === "alias") return "var(--amber)";
  if (d.kind === "org_link") return "var(--green)";
  if (d.kind === "address_link") return "var(--purple)";
  const rt = (d.role_type || "").toLowerCase();
  if (rt.includes("trustee")) return "var(--blue)";
  if (rt.includes("director")) return "var(--purple)";
  if (rt.includes("secretary")) return "#0ea5e9";
  return "#2a3040";
}}

// -- position nodes --
function layoutRow(list, yTop, xMin, xMax) {{
  if (list.length === 0) return 0;
  const spacing = 16;
  const rowGap = 18;
  const pad = 18;
  const usableMin = xMin + pad;
  const usableMax = xMax - pad;
  const maxRowW = Math.max(120, usableMax - usableMin);

  const rows = [];
  let currentRow = [];
  let currentWidth = 0;
  list.forEach(n => {{
    const pw = pillWidth(n);
    const nextWidth = currentRow.length ? currentWidth + spacing + pw : pw;
    if (currentRow.length && nextWidth > maxRowW) {{
      rows.push(currentRow);
      currentRow = [n];
      currentWidth = pw;
    }} else {{
      currentRow.push(n);
      currentWidth = nextWidth;
    }}
  }});
  if (currentRow.length) rows.push(currentRow);

  const maxPillH = Math.max(...rows.flat().map(pillHeight));
  const rowStep = maxPillH + rowGap;

  rows.forEach((row, rowIndex) => {{
    const rowW = row.reduce((sum, n) => sum + pillWidth(n), 0) + spacing * (row.length - 1);
    let cx = usableMin + Math.max(0, (maxRowW - rowW) / 2);
    const rowY = yTop + rowIndex * rowStep;
    row.forEach(n => {{
      const pw = pillWidth(n);
      n.x = cx + pw / 2;
      n.y = rowY;
      cx += pw + spacing;
    }});
  }});
  return rows.length * rowStep;
}}

const LANE_GAP = 50;

function sortByNeighborX(list) {{
  return list.sort((a, b) => {{
    function avgNeighborX(node) {{
      const xs = [];
      (edgesByNodeId.get(node.id) || []).forEach(e => {{
        const otherId = e.source === node.id ? e.target : e.source;
        const other = nodeById.get(otherId);
        if (other && other._visible && other.x != null && other.lane !== node.lane) xs.push(other.x);
      }});
      return xs.length ? xs.reduce((s, x) => s + x, 0) / xs.length : W / 2;
    }}
    return avgNeighborX(a) - avgNeighborX(b);
  }});
}}

function positionNodes() {{
  const visible = allNodes.filter(n => n._visible !== false);
  let curY = 60;
  if (!multiSeed) {{
    const laneKeys = [1, 2, 3, 4];
    laneKeys.forEach(lane => {{
      const list = visible.filter(n => n.lane === lane);
      if (lane > 1) sortByNeighborX(list);
      LANE_Y[lane] = curY;
      const h = layoutRow(list, curY, 0, W);
      curY += Math.max(h, 30) + LANE_GAP;
    }});
  }} else {{
    LANE_Y[1] = curY;
    const idH = layoutRow(visible.filter(n => n.lane === 1), curY, 0, W);
    curY += Math.max(idH, 30) + LANE_GAP;

    LANE_Y[2] = curY;
    const orgs = visible.filter(n => n.kind === "organisation");
    sortByNeighborX(orgs);
    const orgH = layoutRow(orgs, curY, 0, W);
    curY += Math.max(orgH, 30) + LANE_GAP;

    LANE_Y[3] = curY;
    const addrs = visible.filter(n => n.kind === "address");
    sortByNeighborX(addrs);
    const addrH = layoutRow(addrs, curY, 0, W);
    curY += Math.max(addrH, 30) + LANE_GAP;

    LANE_Y[4] = curY;
    const people = visible.filter(n => n.lane === 4);
    sortByNeighborX(people);
    layoutRow(people, curY, 0, W);
  }}
}}
positionNodes();

// -- edges layer --
const edgeGroup = gRoot.append("g");
let roleLine = edgeGroup.selectAll("line.role-edge");

// -- nodes layer: pill groups --
const nodeGroup = gRoot.append("g");
const pills = nodeGroup.selectAll("g.pill").data(allNodes).join("g")
  .attr("class", "pill")
  .style("cursor", "pointer");

const pillRects = pills.append("rect")
  .attr("rx", d => pillHeight(d) / 2)
  .attr("ry", d => pillHeight(d) / 2)
  .attr("width", pillWidth)
  .attr("height", pillHeight)
  .attr("fill", nodeColor)
  .attr("fill-opacity", d => d.sanctioned ? 0.35 : 0.18)
  .attr("stroke", nodeColor)
  .attr("stroke-width", d => d.sanctioned ? 2.5 : 1.2)
  .attr("stroke-opacity", d => d.sanctioned ? 1.0 : 0.7);

pills.append("text")
  .text(nodeLabel)
  .attr("font-size", fontSize)
  .attr("font-weight", d => (d.kind === "seed" || d.kind === "seed_alias") ? 600 : 400)
  .attr("fill", d => (d.kind === "seed" || d.kind === "seed_alias") ? "var(--text-bright)" : "var(--text)")
  .attr("text-anchor", "start")
  .attr("dominant-baseline", "central")
  .attr("x", badgeTextInset)
  .attr("y", d => pillHeight(d) / 2)
  .style("pointer-events", "none");

const badgeGroups = pills.append("g")
  .style("display", d => badgeSpec(d) ? null : "none");

badgeGroups.append("rect")
  .attr("rx", d => badgeHeight(d) / 2)
  .attr("ry", d => badgeHeight(d) / 2)
  .attr("x", 8)
  .attr("y", d => (pillHeight(d) - badgeHeight(d)) / 2)
  .attr("width", badgeWidth)
  .attr("height", badgeHeight)
  .attr("fill", d => badgeSpec(d)?.fill || "transparent")
  .attr("stroke", "rgba(255,255,255,0.18)")
  .attr("stroke-width", 0.8);

badgeGroups.append("path")
  .attr("d", d => badgeSpec(d)?.path || "")
  .attr("transform", d => {{
    const size = 24 * 0.5;
    const x = 8 + (badgeWidth(d) - size) / 2;
    const y = (pillHeight(d) - size) / 2;
    return `translate(${{x}},${{y}}) scale(0.5)`;
  }})
  .attr("fill", "none")
  .attr("stroke", d => badgeSpec(d)?.color || "transparent")
  .attr("stroke-width", 1.8)
  .attr("stroke-linecap", "round")
  .attr("stroke-linejoin", "round")
  .style("pointer-events", "none");

const addressFocusButtons = pills.append("g")
  .attr("class", "address-focus-btn")
  .style("display", d => canInspectUpstream(d) ? null : "none")
  .style("cursor", "pointer");

addressFocusButtons.append("circle")
  .attr("class", "focus-btn")
  .attr("cx", d => pillWidth(d) - 14)
  .attr("cy", d => pillHeight(d) / 2)
  .attr("r", 8)
  .attr("fill", "rgba(255,255,255,0.08)")
  .attr("stroke", "rgba(255,255,255,0.28)")
  .attr("stroke-width", 1);

addressFocusButtons.append("path")
  .attr("class", "focus-btn")
  .attr("d", iconPath("search"))
  .attr("transform", d => `translate(${{pillWidth(d) - 20}},${{pillHeight(d) / 2 - 6}}) scale(0.5)`)
  .attr("fill", "none")
  .attr("stroke", "#ffffff")
  .attr("stroke-width", 1.8)
  .attr("stroke-linecap", "round")
  .attr("stroke-linejoin", "round");

addressFocusButtons
  .on("mouseover", (event, d) => showTooltip(event, [`Inspect upstream connections for ${{d.label}}`]))
  .on("mousemove", (event) => positionTooltip(event))
  .on("mouseout", hideTooltip)
  .on("click", (event, d) => {{
    event.stopPropagation();
    stage3FocusNodeId = stage3FocusNodeId === d.id ? null : d.id;
    applyFilter();
  }});

function updatePositions() {{
  edgeGroup.selectAll("line")
    .attr("x1", d => nodeById.get(d.source)?.x ?? 0)
    .attr("y1", d => nodeById.get(d.source)?.y ?? 0)
    .attr("x2", d => nodeById.get(d.target)?.x ?? 0)
    .attr("y2", d => nodeById.get(d.target)?.y ?? 0);
  pills.attr("transform", d => `translate(${{d.x - pillWidth(d) / 2}},${{d.y - pillHeight(d) / 2}})`);
}}
renderEdges();
updatePositions();

// -- tooltip --
function showTooltip(event, lines) {{
  tooltipEl.innerHTML = lines.join("<br>");
  tooltipEl.style.display = "block";
  positionTooltip(event);
}}
function positionTooltip(event) {{
  const pad = 14;
  let x = event.clientX + pad;
  let y = event.clientY - 10;
  const rect = tooltipEl.getBoundingClientRect();
  if (x + rect.width > window.innerWidth - 10) x = event.clientX - rect.width - pad;
  if (y + rect.height > window.innerHeight - 10) y = window.innerHeight - rect.height - 10;
  tooltipEl.style.left = x + "px";
  tooltipEl.style.top = y + "px";
}}
function hideTooltip() {{ tooltipEl.style.display = "none"; }}

pills
  .on("mouseover", (event, d) => showTooltip(event, d.tooltip_lines || [d.label]))
  .on("mousemove", (event) => positionTooltip(event))
  .on("mouseout", hideTooltip);

// -- drag --
const drag = d3.drag()
  .filter((event) => !(event.target.classList && event.target.classList.contains("focus-btn")))
  .on("start", (event, d) => {{ d._dragging = true; }})
  .on("drag", (event, d) => {{
    d.x = event.x; d.y = event.y;
    updatePositions();
  }})
  .on("end", (event, d) => {{ d._dragging = false; }});
pills.call(drag);
svg.on("dblclick.focus", () => {{
  if (!stage3FocusNodeId) return;
  stage3FocusNodeId = null;
  applyFilter();
}});

// -- focus panel (double-click) --

function isNodeDisplayed(node) {{
  return !!node && !!node._visible;
}}

function isEdgeDisplayed(edge) {{
  const sourceNode = nodeById.get(edge.source);
  const targetNode = nodeById.get(edge.target);
  return isNodeDisplayed(sourceNode) && isNodeDisplayed(targetNode);
}}

function edgePairKey(a, b) {{
  return a < b ? `${{a}}||${{b}}` : `${{b}}||${{a}}`;
}}

function hiddenNodeTypeLabel(node) {{
  if (!node) return "node";
  if (node.kind === "seed") return "seed";
  if (node.lane === 1) return "identity";
  if (node.kind === "address") return "address";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "charity") return "charity";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "company") return "company";
  if (node.kind === "organisation") return "organisation";
  return "person";
}}

function hiddenConnectionStepLine(edge) {{
  if (edge.tooltip) return edge.tooltip;
  const source = nodeById.get(edge.source);
  const target = nodeById.get(edge.target);
  return `${{source?.label || edge.source}} is linked to ${{target?.label || edge.target}}`;
}}

function hiddenConnectionTooltipLines(sourceId, targetId, hiddenNodeIds, pathEdges) {{
  const source = nodeById.get(sourceId);
  const target = nodeById.get(targetId);
  const hiddenNodes = hiddenNodeIds.map(id => nodeById.get(id)).filter(Boolean);
  const viaText = hiddenNodes.length === 1 ? "1 hidden node" : `${{hiddenNodes.length}} hidden nodes`;
  const lines = [
    `<strong>${{source?.label || sourceId}}</strong> connects to <strong>${{target?.label || targetId}}</strong> through ${{viaText}}.`,
  ];
  if (hiddenNodes.length) {{
    lines.push(`Hidden path: ${{hiddenNodes.map(n => `${{n.label}} <span class="dim">(${{hiddenNodeTypeLabel(n)}})</span>`).join(" <span class=\\"dim\\">→</span> ")}}`);
  }}
  if (pathEdges.length) {{
    lines.push("<strong>How the connection works:</strong>");
    pathEdges.forEach(edge => lines.push(hiddenConnectionStepLine(edge)));
  }}
  return lines;
}}

function displayedNodeIds() {{
  return new Set(allNodes.filter(isNodeDisplayed).map(n => n.id));
}}

function isBridgeStartNode(node) {{
  if (!node) return false;
  return node.kind === "organisation";
}}

function isBridgeTargetNode(node) {{
  if (!node) return false;
  return node.lane === 1;
}}

function applyNodeTypeFilter() {{
  allNodes.forEach(n => {{
    if (!n._visible || n.kind === "seed") return;
    if (!selectedNodeTypes.has(nodeTypeKey(n))) n._visible = false;
  }});
  let changed = true;
  while (changed) {{
    changed = false;
    allNodes.forEach(n => {{
      if (!n._visible) return;
      if (n.kind === "organisation" || n.kind === "seed") return;
      if (searchOrFocusMode && n.lane === 1) return;
      const hasVisibleOrg = (edgesByNodeId.get(n.id) || []).some(e => {{
        const otherId = e.source === n.id ? e.target : e.source;
        const otherNode = nodeById.get(otherId);
        return !!otherNode?._visible && otherNode.kind === "organisation";
      }});
      if (!hasVisibleOrg) {{
        n._visible = false;
        changed = true;
      }}
    }});
  }}
}}

function findBridgeConnections(startId) {{
  const startNode = nodeById.get(startId);
  if (!isBridgeStartNode(startNode)) return [];
  const connections = new Map();
  const hiddenQueue = [];
  const visited = new Set([startId]);
  (edgesByNodeId.get(startId) || []).forEach(edge => {{
    const nextId = edge.source === startId ? edge.target : edge.source;
    if (visited.has(nextId)) return;
    visited.add(nextId);
    const nextNode = nodeById.get(nextId);
    if (nextNode && isBridgeTargetNode(nextNode)) {{
      if (!directEdgePairs.has(edgePairKey(startId, nextId))) {{
        connections.set(nextId, {{
          source: startId,
          target: nextId,
          kind: "hidden_connection",
          hops: 1,
          hiddenNodeIds: [nextId],
          pathEdges: [edge],
          tooltip_lines: hiddenConnectionTooltipLines(startId, nextId, [nextId], [edge]),
        }});
      }}
      return;
    }}
    if (!isBridgeStartNode(nextNode)) return;
    hiddenQueue.push({{ id: nextId, hops: 1, hiddenNodeIds: [nextId], pathEdges: [edge] }});
  }});
  while (hiddenQueue.length) {{
    const current = hiddenQueue.shift();
    (edgesByNodeId.get(current.id) || []).forEach(edge => {{
      const nextId = edge.source === current.id ? edge.target : edge.source;
      if (visited.has(nextId)) return;
      visited.add(nextId);
      const nextNode = nodeById.get(nextId);
      if (nextNode && isBridgeTargetNode(nextNode)) {{
        const existing = connections.get(nextId);
        if (!existing || current.hops + 1 < existing.hops) {{
          if (!directEdgePairs.has(edgePairKey(startId, nextId))) {{
            connections.set(nextId, {{
              source: startId,
              target: nextId,
              kind: "hidden_connection",
              hops: current.hops + 1,
              hiddenNodeIds: [...current.hiddenNodeIds, nextId],
              pathEdges: [...current.pathEdges, edge],
              tooltip_lines: hiddenConnectionTooltipLines(startId, nextId, current.hiddenNodeIds, [...current.pathEdges, edge]),
            }});
          }}
        }}
        return;
      }}
      if (!isBridgeStartNode(nextNode)) return;
      hiddenQueue.push({{
        id: nextId,
        hops: current.hops + 1,
        hiddenNodeIds: [...current.hiddenNodeIds, nextId],
        pathEdges: [...current.pathEdges, edge],
      }});
    }});
  }}
  return [...connections.values()];
}}

function deriveHiddenConnectionEdges() {{
  const displayedIds = displayedNodeIds();
  const hiddenConnections = new Map();
  displayedIds.forEach(startId => {{
    const startNode = nodeById.get(startId);
    if (!isBridgeStartNode(startNode)) return;
    findBridgeConnections(startId).forEach(connection => {{
      const targetNode = nodeById.get(connection.target);
      if (!targetNode || !targetNode._visible) return;
      const pairKey = edgePairKey(connection.source, connection.target);
      const existing = hiddenConnections.get(pairKey);
      if (!existing || connection.hops < existing.hops) {{
        hiddenConnections.set(pairKey, connection);
      }}
    }});
  }});
  return [...hiddenConnections.values()];
}}

function hiddenConnectionSectionKey(focusNode, otherNode) {{
  if (focusNode.kind !== "organisation") return "hidden";
  if (otherNode?.kind === "organisation") return "peer";
  if ((otherNode?.lane ?? focusNode.lane) <= focusNode.lane) return "upstream";
  return "downstream";
}}

function hiddenConnectionSectionTitle(sectionKey) {{
  if (sectionKey === "upstream") return "Upstream connections";
  if (sectionKey === "downstream") return "Downstream connections";
  if (sectionKey === "peer") return "Related organisations";
  return "Hidden connections";
}}

function hiddenConnectionSectionsForNode(nodeId) {{
  const focusNode = nodeById.get(nodeId);
  const sections = new Map();
  findBridgeConnections(nodeId).forEach(connection => {{
    const otherNode = nodeById.get(connection.target);
    if (!otherNode) return;
    const sectionKey = hiddenConnectionSectionKey(focusNode, otherNode);
    if (!sections.has(sectionKey)) sections.set(sectionKey, []);
    sections.get(sectionKey).push({{
      otherNode,
      tooltip_lines: connection.tooltip_lines,
      hops: connection.hops,
    }});
  }});
  sections.forEach(items => items.sort((a, b) =>
    (a.otherNode.lane - b.otherNode.lane) ||
    (a.hops - b.hops) ||
    a.otherNode.label.localeCompare(b.otherNode.label)
  ));
  return sections;
}}

function renderedEdges() {{
  const visible = allEdges.filter(isEdgeDisplayed);
  if (searchOrFocusMode) return visible.concat(deriveHiddenConnectionEdges());
  return visible;
}}

function bindRoleLines(selection) {{
  return selection
    .attr("stroke", edgeStroke)
    .attr("stroke-width", d => d.kind === "alias" ? 2.5 : d.kind === "hidden_connection" ? 1.6 : 1.4 + (d.weight || 0) * 1.5)
    .attr("stroke-opacity", d => d.kind === "alias" ? 0.8 : d.kind === "hidden_connection" ? 0.65 : d.kind === "address_link" ? 0.75 : 0.45)
    .attr("stroke-dasharray", d => d.kind === "hidden_connection" ? "5 4" : null)
    .style("pointer-events", "none");
}}

function renderEdges() {{
  const edgeData = renderedEdges();
  const key = d => `${{d.kind}}:${{d.source}}:${{d.target}}:${{d.hops || 0}}:${{(d.tooltip_lines || [d.tooltip || ""]).join("|")}}`;
  const groups = edgeGroup.selectAll("g.edge-group")
    .data(edgeData, key)
    .join(
      enter => {{
        const g = enter.append("g").attr("class", "edge-group");
        g.append("line").attr("class", "role-edge-hit")
          .attr("stroke", "transparent").attr("stroke-width", 12)
          .style("pointer-events", "stroke");
        g.append("line").attr("class", "role-edge")
          .style("pointer-events", "none");
        return g;
      }},
      update => update,
      exit => exit.remove()
    );
  roleLine = bindRoleLines(groups.select("line.role-edge"));
  groups.select("line.role-edge-hit")
    .on("mouseover", (event, d) => showTooltip(event, d.tooltip_lines || [d.tooltip || "link"]))
    .on("mousemove", positionTooltip)
    .on("mouseout", hideTooltip);
}}


// -- search / filter --
let searchTerm = "";

function applyFilter() {{
  const q = searchTerm.toLowerCase();

  allNodes.forEach(n => {{ n._visible = false; }});

  const activeIdentities = activeIdentityIds();
  const visibleOrgs = expandOrgIdsThroughOrgLinks(filteredOrgIdsForCurrentFilters());
  const visiblePeople = new Set();

  allNodes.filter(n => n.lane === 4).forEach(n => {{
    let visible = selectedPeople.has(n.id);
    if (visible && stage3MultiOrgToggle?.checked) visible = (n.org_count || 0) >= 2;
    const myOrgs = personOrgIds.get(n.id) || new Set();
    if (visible) visible = [...myOrgs].some(orgId => visibleOrgs.has(orgId));
    n._visible = visible;
    if (visible) visiblePeople.add(n.id);
  }});

  let addressFilterOrgIds = visibleOrgs;
  if (stage3MultiOrgToggle?.checked) {{
    addressFilterOrgIds = new Set(
      [...orgIdsForVisiblePeople(visiblePeople)].filter(orgId => visibleOrgs.has(orgId))
    );
  }}
  const visibleAddresses = visibleAddressIdsForVisibleOrgs(addressFilterOrgIds);

  allNodes.filter(n => n.kind === "organisation").forEach(n => {{ n._visible = visibleOrgs.has(n.id); }});
  allNodes.filter(n => n.kind === "address").forEach(n => {{ n._visible = visibleAddresses.has(n.id); }});

  const visibleAliases = new Set();
  allEdges.forEach(e => {{
    if (e.kind !== "role") return;
    const sourceNode = nodeById.get(e.source);
    const targetNode = nodeById.get(e.target);
    const aliasNode = sourceNode?.lane === 1 ? sourceNode : targetNode?.lane === 1 ? targetNode : null;
    const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
    if (!aliasNode || !orgNode) return;
    if (!activeIdentities.has(aliasNode.id)) return;
    if (!visibleOrgs.has(orgNode.id)) return;
    visibleAliases.add(aliasNode.id);
  }});
  allNodes.filter(n => n.lane === 1).forEach(n => {{ n._visible = visibleAliases.has(n.id); }});

  allNodes.filter(n => n.kind === "seed").forEach(n => {{ n._visible = false; }});

  const indirectOrgsActive = !q && indirectOrgsToggle?.checked;
  const stage3FocusActive = !indirectOrgsActive && !!stage3FocusNodeId;
  if (q || indirectOrgsActive || stage3FocusActive) {{
    searchOrFocusMode = true;
    allNodes.forEach(n => {{ n._visible = false; }});
    let matchedNodeIds;
    if (stage3FocusActive) {{
      matchedNodeIds = new Set([stage3FocusNodeId]);
    }} else if (q) {{
      matchedNodeIds = new Set(
        allNodes
          .filter(n => nodeMatchesQuery(n, q))
          .map(n => n.id)
      );
    }} else if (indirectOrgsActive) {{
      matchedNodeIds = new Set();
      indirectOrgIndividuals.forEach((individuals, orgId) => {{
        let count = 0;
        individuals.forEach(id => {{
          const node = nodeById.get(id);
          if (!node) return;
          if (node.lane === 1 && activeIdentities.has(id)) count++;
          else if (node.lane === 4 && selectedPeople.has(id)) count++;
        }});
        if (count >= 2) matchedNodeIds.add(orgId);
      }});
    }}
    matchedNodeIds.forEach(id => {{
      const n = nodeById.get(id);
      if (n) n._visible = true;
    }});
    const peopleOnlySearch = q && matchedNodeIds.size > 0 && [...matchedNodeIds].every(id => nodeById.get(id)?.lane === 4);

    function walkLane(nodeId, visited, directionFn) {{
      if (visited.has(nodeId)) return;
      visited.add(nodeId);
      const node = nodeById.get(nodeId);
      if (!node) return;
      node._visible = true;
      const nodeLane = node.lane ?? 0;
      (edgesByNodeId.get(nodeId) || []).forEach(e => {{
        const otherId = e.source === nodeId ? e.target : e.source;
        const otherNode = nodeById.get(otherId);
        if (!otherNode) return;
        const otherLane = otherNode.lane ?? 0;
        if (directionFn(otherLane, nodeLane)) walkLane(otherId, visited, directionFn);
      }});
    }}
    const upstreamVisited = new Set();
    const stage3FocusOrgIds = new Set();
    const useInspectUpstream = stage3FocusActive || peopleOnlySearch;
    if (useInspectUpstream) {{
      matchedNodeIds.forEach(id => {{
        const node = nodeById.get(id);
        if (!node) return;
        (edgesByNodeId.get(id) || []).forEach(e => {{
          const otherId = e.source === id ? e.target : e.source;
          const otherNode = nodeById.get(otherId);
          if (!otherNode || otherNode.kind !== "organisation") return;
          stage3FocusOrgIds.add(otherId);
          otherNode._visible = true;
          (edgesByNodeId.get(otherId) || []).forEach(e2 => {{
            if (e2.kind !== "role") return;
            const nextId = e2.source === otherId ? e2.target : e2.source;
            const nextNode = nodeById.get(nextId);
            if (nextNode?.lane === 1) nextNode._visible = true;
          }});
        }});
      }});
    }} else {{
      matchedNodeIds.forEach(id => walkLane(id, upstreamVisited, (other, self) => other < self));
    }}
    const bridgeStartIds = useInspectUpstream ? [...stage3FocusOrgIds] : [...matchedNodeIds];
    bridgeStartIds.forEach(startId => {{
      findBridgeConnections(startId).forEach(connection => {{
        const n = nodeById.get(connection.target);
        if (!n) return;
        if ((indirectOrgsActive || stage3FocusActive) && n.lane !== 1) return;
        if (peopleOnlySearch && n.lane === 4) return;
        n._visible = true;
      }});
    }});
    if (!indirectOrgsActive && !stage3FocusActive) {{
      const downstreamVisited = new Set();
      matchedNodeIds.forEach(id => walkLane(id, downstreamVisited, (other, self) => other > self));
      allNodes.filter(n => n._visible && n.kind === "organisation").forEach(n => {{
        if (!peopleOnlySearch) {{
          walkLane(n.id, downstreamVisited, (other, self) => other > self);
          return;
        }}
        (edgesByNodeId.get(n.id) || []).forEach(e => {{
          const otherId = e.source === n.id ? e.target : e.source;
          const otherNode = nodeById.get(otherId);
          if (otherNode?.kind === "address") otherNode._visible = true;
        }});
      }});
    }}
    allNodes.filter(n => n.kind === "seed").forEach(n => {{ n._visible = false; }});
  }} else {{
    searchOrFocusMode = false;
  }}

  applyNodeTypeFilter();

  positionNodes();
  updatePositions();
  syncVisibility();
  zoomToVisible();

  if (identityDropdownBtn && identityNodes.length) {{
    const selectedCount = identityNodes.filter(n => selectedIdentities.has(n.id)).length;
    const totalCount = identityNodes.length;
    identityDropdownBtn.textContent = selectedCount === totalCount
      ? "Identities \u25BE"
      : `${{selectedCount}}/${{totalCount}} identities \u25BE`;
  }}
}}

searchInput.addEventListener("input", () => {{
  searchTerm = searchInput.value.trim();
  applyFilter();
}});
stage3MultiOrgToggle?.addEventListener("change", () => {{
  buildPeopleDropdown();
  applyFilter();
}});
orgMultiPersonToggle?.addEventListener("change", () => {{
  buildPeopleDropdown();
  applyFilter();
}});
overlapsOnlyToggle?.addEventListener("change", () => {{
  buildPeopleDropdown();
  applyFilter();
}});
indirectOrgsToggle?.addEventListener("change", () => {{
  buildPeopleDropdown();
  applyFilter();
}});
clearBtn.addEventListener("click", () => {{
  searchInput.value = "";
  searchTerm = "";
  applyFilter();
  searchInput.focus();
}});

applyFilter();

function syncVisibility() {{
  pills
    .attr("display", d => isNodeDisplayed(d) ? null : "none")
    .attr("opacity", d => isNodeDisplayed(d) ? 1 : 0);
  renderEdges();
  updatePositions();
}}

function zoomToVisible() {{
  const visibleNodes = allNodes.filter(isNodeDisplayed);
  if (!visibleNodes.length) return;
  const allX = visibleNodes.map(n => n.x);
  const allY = visibleNodes.map(n => n.y);
  const bounds = {{
    x0: Math.min(...allX) - 60, x1: Math.max(...allX) + 60,
    y0: Math.min(...allY) - 40, y1: Math.max(...allY) + 40,
  }};
  const bw = Math.max(1, bounds.x1 - bounds.x0);
  const bh = Math.max(1, bounds.y1 - bounds.y0);
  const scale = Math.min(W / bw, H / bh, 1.5) * 0.85;
  const tx = (W - bw * scale) / 2 - bounds.x0 * scale;
  const ty = (H - bh * scale) / 2 - bounds.y0 * scale;
  svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
}}
</script>
</body>
</html>"""


render_html = _render_html


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consolidate aliases and generate network graph.",
    )
    parser.add_argument("run_ids", type=int, nargs="+", help="One or more run IDs")
    parser.add_argument(
        "--out",
        default=None,
        help="Output HTML path (default: output/run_<ids>_graph.html)",
    )
    args = parser.parse_args()

    if len(args.run_ids) == 1:
        data = consolidate_run(args.run_ids[0])
    else:
        data = consolidate_multi_run(args.run_ids)

    print(f"\n{'='*70}")
    print(f"  Consolidated ranking (run {data['run_id']})")
    print(f"{'='*70}\n")
    for rank, entry in enumerate(data["consolidated"], 1):
        aliases = ", ".join(entry["aliases"])
        print(f"  {rank:>3}.  {entry['label']}")
        if len(entry["aliases"]) > 1:
            print(f"        aliases: {aliases}")
        print(f"        orgs={entry['org_count']}  roles={entry['role_count']}  score={entry['score']}")
        print()

    project_root = Path(__file__).resolve().parents[1]
    mapping_db_path = rebuild_overlay_mapping_db(project_root)
    low_confidence_data = {"nodes": [], "edges": [], "summary": {"run_key": str(data["run_id"])}}
    if mapping_db_path.exists():
        try:
            low_confidence_data = build_low_confidence_overlay(
                main_data=data,
                database_path=mapping_db_path,
                run_key=str(data["run_id"]),
                include_unmatched=True,
                include_generated_links=True,
                enable_ai_org_matching=True,
                settings=load_settings(),
            )
            print(
                "Loaded low-confidence overlay: "
                f"{len(low_confidence_data.get('nodes') or [])} nodes, "
                f"{len(low_confidence_data.get('edges') or [])} edges"
            )
        except Exception as error:
            print(f"Warning: failed to build low-confidence overlay: {error}")

    id_slug = "+".join(str(r) for r in args.run_ids)
    out_path = args.out or f"output/run_{id_slug}_graph.html"
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    render_payload = dict(data)
    render_payload["low_confidence"] = low_confidence_data
    html_content = render_html(render_payload)
    out_file.write_text(html_content, encoding="utf-8")
    print(f"Graph written to {out_path}")

    graph_json = json.dumps(data, indent=2, ensure_ascii=False)
    graph_json_path = out_file.parent / "graph-data.json"
    graph_json_path.write_text(graph_json, encoding="utf-8")
    print(f"Graph JSON written to {graph_json_path}")

    low_conf_json = json.dumps(low_confidence_data, indent=2, ensure_ascii=False)
    low_conf_json_path = out_file.parent / "graph-data-low-confidence.json"
    low_conf_json_path.write_text(low_conf_json, encoding="utf-8")
    print(f"Low-confidence JSON written to {low_conf_json_path}")

    netlify_path = Path("netlify_graph_viewer/index.html")
    if netlify_path.parent.exists():
        netlify_path.write_text(html_content, encoding="utf-8")
        print(f"Netlify viewer updated at {netlify_path}")
        (netlify_path.parent / "graph-data.json").write_text(graph_json, encoding="utf-8")
        print(f"Netlify graph JSON updated at {netlify_path.parent / 'graph-data.json'}")
        (netlify_path.parent / "graph-data-low-confidence.json").write_text(
            low_conf_json,
            encoding="utf-8",
        )
        print(
            "Netlify low-confidence JSON updated at "
            f"{netlify_path.parent / 'graph-data-low-confidence.json'}"
        )

    try:
        webbrowser.open(Path(out_path).resolve().as_uri())
    except Exception:
        pass


if __name__ == "__main__":
    main()

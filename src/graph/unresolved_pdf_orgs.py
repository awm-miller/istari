from __future__ import annotations

import json
from dataclasses import asdict
from hashlib import sha256
from typing import Any

from src.models import PdfExtractedEntity
from src.search.queries import normalize_name
from src.services.pdf_enrichment import _is_notice_boilerplate_entity
from src.storage.repository import Repository


def _json_dict(raw_value: object) -> dict[str, Any]:
    try:
        parsed = json.loads(str(raw_value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalized_label(value: object) -> str:
    return normalize_name(_clean_text(value))


def _stable_unresolved_org_key(
    *,
    parent_node_id: str,
    entity_name: str,
    document_url: str,
    source_page_hint: str,
    connection_phrase: str,
) -> str:
    digest = sha256(
        "\n".join(
            [
                _clean_text(parent_node_id),
                _normalized_label(entity_name),
                _clean_text(document_url),
                _clean_text(source_page_hint),
                _clean_text(connection_phrase),
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"unresolved-pdf-org:{digest}"


def _build_main_org_indexes(main_data: dict[str, Any]) -> tuple[dict[tuple[str, str], str], dict[str, list[str]]]:
    registry_lookup: dict[tuple[str, str], str] = {}
    name_lookup: dict[str, list[str]] = {}
    for node in main_data.get("nodes") or []:
        if str(node.get("kind") or "") != "organisation":
            continue
        node_id = str(node.get("id") or "").strip()
        if not node_id:
            continue
        registry_type = str(node.get("registry_type") or "").strip().lower()
        registry_number = str(node.get("registry_number") or "").strip()
        if registry_type and registry_number:
            registry_lookup.setdefault((registry_type, registry_number), node_id)
        for text in [node.get("label"), *(node.get("aliases") or [])]:
            normalized = _normalized_label(text)
            if not normalized:
                continue
            bucket = name_lookup.setdefault(normalized, [])
            if node_id not in bucket:
                bucket.append(node_id)
    return registry_lookup, name_lookup


def _resolve_parent_node_id(
    payload: dict[str, Any],
    *,
    registry_lookup: dict[tuple[str, str], str],
    name_lookup: dict[str, list[str]],
    main_node_by_id: dict[str, dict[str, Any]],
) -> str:
    parent_org_id = str(payload.get("parent_organisation_id") or "").strip()
    if parent_org_id:
        return f"org:{parent_org_id}"
    parent_registry_type = str(payload.get("parent_registry_type") or "").strip().lower()
    parent_registry_number = str(payload.get("parent_registry_number") or "").strip()
    if parent_registry_type and parent_registry_number:
        resolved = registry_lookup.get((parent_registry_type, parent_registry_number))
        if resolved:
            return resolved
    normalized_name = _normalized_label(payload.get("organisation_name") or payload.get("parent_organisation_name"))
    matches = name_lookup.get(normalized_name, [])
    if len(matches) <= 1:
        return matches[0] if matches else ""
    document = payload.get("document") if isinstance(payload.get("document"), dict) else {}
    source_provider = str(document.get("source_provider") or "").strip().lower()
    preferred_registry_type = ""
    if "companies_house" in source_provider:
        preferred_registry_type = "company"
    elif "charity" in source_provider:
        preferred_registry_type = "charity"
    if preferred_registry_type:
        preferred_matches = [
            node_id
            for node_id in matches
            if str((main_node_by_id.get(node_id) or {}).get("registry_type") or "").strip().lower() == preferred_registry_type
        ]
        if len(preferred_matches) == 1:
            return preferred_matches[0]
    return ""


def _build_unresolved_edge(
    *,
    parent_node_id: str,
    parent_label: str,
    child_node_id: str,
    child_label: str,
    entity: PdfExtractedEntity,
    evidence_id: int,
    document_title: str,
    document_url: str,
    notes: str,
) -> dict[str, Any]:
    phrase = _clean_text(entity.connection_phrase) or "is mentioned in filings for"
    note_lines = [line for line in [entity.role_label, notes] if line]
    tooltip_lines = [
        line
        for line in [
            f"{child_label} {phrase} {parent_label}",
            *note_lines,
            document_title and f"Source document: {document_title}",
        ]
        if line
    ]
    evidence = {
        "title": document_title or child_label,
        "document_url": document_url,
        "page_hint": _clean_text(entity.source_page_hint),
        "page_number": None,
        "notes": notes,
    }
    return {
        "id": f"{child_node_id}:edge:{parent_node_id}",
        "source": parent_node_id,
        "target": child_node_id,
        "kind": "org_link",
        "role_type": "unresolved_pdf_org",
        "role_label": _clean_text(entity.role_label) or "pdf organisation mention",
        "phrase": phrase,
        "source_provider": "pdf_gemini_extraction",
        "confidence": "low",
        "weight": 0.25,
        "tooltip": f"{child_label} {phrase} {parent_label}",
        "tooltip_lines": tooltip_lines,
        "is_low_confidence": True,
        "low_confidence_category": "unresolved_org",
        "evidence": evidence,
        "evidence_items": [evidence],
        "evidence_id": evidence_id,
    }


def build_unresolved_pdf_org_overlay(
    *,
    repository: Repository,
    run_ids: list[int],
    main_data: dict[str, Any],
) -> dict[str, Any]:
    if not run_ids:
        return {"nodes": [], "edges": [], "summary": {"run_ids": [], "node_count": 0, "edge_count": 0}}

    registry_lookup, name_lookup = _build_main_org_indexes(main_data)
    main_node_by_id = {
        str(node.get("id") or ""): node
        for node in (main_data.get("nodes") or [])
        if str(node.get("id") or "").strip()
    }

    run_placeholders = ",".join("?" for _ in run_ids)
    with repository.connect() as connection:
        evidence_rows = connection.execute(
            f"""
            SELECT id, run_id, title, url, raw_payload_json
            FROM evidence_items
            WHERE run_id IN ({run_placeholders})
              AND source = 'pdf_gemini_extraction'
            ORDER BY run_id ASC, id ASC
            """,
            run_ids,
        ).fetchall()
        resolved_rows = connection.execute(
            f"""
            SELECT metadata_json
            FROM run_organisations
            WHERE run_id IN ({run_placeholders})
              AND source = 'pdf_org_mention'
            """,
            run_ids,
        ).fetchall()

    resolved_evidence_ids: set[int] = set()
    for row in resolved_rows:
        metadata = _json_dict(row["metadata_json"])
        try:
            evidence_id = int(metadata.get("evidence_id") or 0)
        except (TypeError, ValueError):
            evidence_id = 0
        if evidence_id:
            resolved_evidence_ids.add(evidence_id)

    overlay_nodes: dict[str, dict[str, Any]] = {}
    overlay_edges: dict[str, dict[str, Any]] = {}

    for row in evidence_rows:
        evidence_id = int(row["id"])
        if evidence_id in resolved_evidence_ids:
            continue
        payload = _json_dict(row["raw_payload_json"])
        entity_payload = payload.get("entity")
        if not isinstance(entity_payload, dict):
            continue
        entity = PdfExtractedEntity(
            name=_clean_text(entity_payload.get("name")),
            entity_type=_clean_text(entity_payload.get("entity_type") or "other"),
            role_category=_clean_text(entity_payload.get("role_category") or "ignore"),
            role_label=_clean_text(entity_payload.get("role_label")),
            organisation_name=_clean_text(
                entity_payload.get("organisation_name")
                or payload.get("organisation_name")
                or payload.get("parent_organisation_name")
            ),
            source_document_url=_clean_text(
                entity_payload.get("source_document_url")
                or ((payload.get("document") or {}).get("url") if isinstance(payload.get("document"), dict) else "")
            ),
            connection_phrase=_clean_text(entity_payload.get("connection_phrase")),
            source_page_hint=_clean_text(entity_payload.get("source_page_hint")),
            confidence=float(entity_payload.get("confidence") or 0.0),
            registry_hint=_clean_text(entity_payload.get("registry_hint")),
            notes=_clean_text(entity_payload.get("notes")),
        )
        if entity.entity_type != "organisation" or entity.role_category != "organisation":
            continue
        if _is_notice_boilerplate_entity(entity):
            continue

        parent_node_id = _resolve_parent_node_id(
            payload,
            registry_lookup=registry_lookup,
            name_lookup=name_lookup,
            main_node_by_id=main_node_by_id,
        )
        parent_node = main_node_by_id.get(parent_node_id) if parent_node_id else None
        if not parent_node_id or not parent_node or str(parent_node.get("kind") or "") != "organisation":
            continue

        stable_key = _stable_unresolved_org_key(
            parent_node_id=parent_node_id,
            entity_name=entity.name,
            document_url=entity.source_document_url,
            source_page_hint=entity.source_page_hint,
            connection_phrase=entity.connection_phrase,
        )
        node_id = f"overlay:{stable_key}"
        document = payload.get("document") if isinstance(payload.get("document"), dict) else {}
        document_title = _clean_text(document.get("title") if isinstance(document, dict) else "")
        document_url = _clean_text(document.get("url") if isinstance(document, dict) else "")
        tooltip_lines = [
            line
            for line in [
                "Unresolved PDF organisation mention",
                _clean_text(entity.role_label),
                _clean_text(entity.connection_phrase),
                document_title and f"Source document: {document_title}",
                entity.notes,
            ]
            if line
        ]
        overlay_nodes.setdefault(
            node_id,
            {
                "id": node_id,
                "label": entity.name,
                "kind": "organisation",
                "lane": 2,
                "registry_type": "",
                "registry_number": "",
                "aliases": [],
                "tooltip_lines": tooltip_lines,
                "is_low_confidence": True,
                "low_confidence_category": "unresolved_org",
                "low_confidence_expandable": False,
                "unresolved_org_key": stable_key,
                "organisation_merge_keys": [stable_key],
                "source_document_url": document_url or entity.source_document_url,
                "source_page_hint": entity.source_page_hint,
                "source_run_ids": [int(row["run_id"])],
            },
        )
        existing_run_ids = overlay_nodes[node_id].setdefault("source_run_ids", [])
        if int(row["run_id"]) not in existing_run_ids:
            existing_run_ids.append(int(row["run_id"]))

        edge = _build_unresolved_edge(
            parent_node_id=parent_node_id,
            parent_label=str(parent_node.get("label") or parent_node_id),
            child_node_id=node_id,
            child_label=entity.name,
            entity=entity,
            evidence_id=evidence_id,
            document_title=document_title or str(row["title"] or entity.name),
            document_url=document_url or entity.source_document_url,
            notes=entity.notes,
        )
        overlay_edges.setdefault(edge["id"], edge)

    return {
        "nodes": list(overlay_nodes.values()),
        "edges": list(overlay_edges.values()),
        "summary": {
            "run_ids": [int(run_id) for run_id in run_ids],
            "node_count": len(overlay_nodes),
            "edge_count": len(overlay_edges),
        },
    }

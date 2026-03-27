from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any
import re

from src.storage.repository import Repository


@dataclass(slots=True)
class _Node:
    id: str
    label: str
    kind: str
    lane: int
    run_ids: set[int] = field(default_factory=set)
    degree: int = 0
    meta: dict[str, Any] = field(default_factory=dict)


def export_network_payload(repository: Repository, run_ids: list[int]) -> dict[str, Any]:
    cleaned = []
    seen: set[int] = set()
    for run_id in run_ids:
        value = int(run_id)
        if value in seen:
            continue
        seen.add(value)
        cleaned.append(value)

    node_map: dict[str, _Node] = {}
    edges: list[dict[str, Any]] = []
    adjacency: dict[str, set[str]] = {}
    edge_id_counts: dict[str, int] = {}
    edge_key_to_index: dict[tuple[Any, ...], int] = {}

    def ensure_node(
        node_id: str,
        *,
        label: str,
        kind: str,
        lane: int,
        run_id: int,
        meta: dict[str, Any] | None = None,
    ) -> None:
        node = node_map.get(node_id)
        if node is None:
            node = _Node(
                id=node_id,
                label=label,
                kind=kind,
                lane=lane,
                run_ids={run_id},
                meta=meta or {},
            )
            node_map[node_id] = node
            return
        node.run_ids.add(run_id)
        if meta:
            node.meta.update(meta)

    def add_edge(
        edge_id: str,
        from_id: str,
        to_id: str,
        *,
        run_id: int,
        role_type: str,
        role_label: str,
        source: str,
        explanation: str,
    ) -> None:
        edge_key = (run_id, from_id, to_id, role_type, role_label, source)
        existing_index = edge_key_to_index.get(edge_key)
        if existing_index is not None:
            edges[existing_index]["evidence_count"] = int(
                edges[existing_index].get("evidence_count", 1)
            ) + 1
            return

        # React Flow requires globally unique edge IDs. We may generate repeated
        # logical edge keys (e.g. same candidate/org pair with repeated evidence),
        # so suffix duplicates to keep rendering stable.
        count = edge_id_counts.get(edge_id, 0)
        edge_id_counts[edge_id] = count + 1
        unique_edge_id = edge_id if count == 0 else f"{edge_id}:{count + 1}"
        edges.append(
            {
                "id": unique_edge_id,
                "from": from_id,
                "to": to_id,
                "run_id": run_id,
                "role_type": role_type,
                "role_label": role_label,
                "source": source,
                "explanation": explanation,
                "evidence_count": 1,
            }
        )
        edge_key_to_index[edge_key] = len(edges) - 1
        adjacency.setdefault(from_id, set()).add(to_id)
        adjacency.setdefault(to_id, set()).add(from_id)

    for run_id in cleaned:
        run_row = repository.get_run(run_id)
        if run_row is None:
            continue
        seed_name = str(run_row["seed_name"])
        seed_id = f"seed:{run_id}"
        ensure_node(
            seed_id,
            label=seed_name,
            kind="seed",
            lane=0,
            run_id=run_id,
            meta={"seed_name": seed_name},
        )

        with repository.connect() as connection:
            candidate_rows = connection.execute(
                """
                SELECT
                    resolution_decisions.canonical_name AS canonical_name,
                    COUNT(*) AS link_count
                FROM resolution_decisions
                WHERE resolution_decisions.run_id = ?
                  AND resolution_decisions.status = 'match'
                GROUP BY resolution_decisions.canonical_name
                ORDER BY link_count DESC, canonical_name ASC
                """,
                (run_id,),
            ).fetchall()
            candidate_org_rows = connection.execute(
                """
                SELECT
                    resolution_decisions.canonical_name AS candidate_name,
                    organisations.id AS org_id,
                    organisations.name AS org_name,
                    organisations.registry_type AS registry_type,
                    organisations.registry_number AS registry_number,
                    organisations.suffix AS suffix,
                    resolution_decisions.confidence AS confidence,
                    candidate_matches.raw_payload_json AS raw_payload_json,
                    candidate_matches.source AS candidate_source
                FROM resolution_decisions
                JOIN candidate_matches
                    ON candidate_matches.id = resolution_decisions.candidate_match_id
                JOIN organisations
                    ON organisations.registry_type = candidate_matches.registry_type
                   AND organisations.registry_number = candidate_matches.registry_number
                   AND organisations.suffix = candidate_matches.suffix
                WHERE resolution_decisions.run_id = ?
                  AND resolution_decisions.status = 'match'
                """,
                (run_id,),
            ).fetchall()

        candidate_names: set[str] = set()
        for row in candidate_rows:
            candidate_name = str(row["canonical_name"] or "").strip()
            if not candidate_name:
                continue
            candidate_names.add(candidate_name)
            candidate_id = f"cand:{_slug(candidate_name)}"
            ensure_node(
                candidate_id,
                label=candidate_name,
                kind="candidate",
                lane=1,
                run_id=run_id,
            )
            add_edge(
                f"seedlink:{run_id}:{seed_id}:{candidate_id}",
                seed_id,
                candidate_id,
                run_id=run_id,
                role_type="seed_link",
                role_label="resolved_candidate",
                source="pipeline_seed",
                explanation="This name is treated as a possible alias of the seed name.",
            )

        scoped_org_rows = repository.get_run_scoped_organisations(run_id)
        run_org_rows = repository.get_run_organisations(run_id)
        run_org_rows = [
            row for row in run_org_rows
            if not _is_notice_org_mention(str(row["source"] or ""), _json_dict(row["run_metadata_json"]))
        ]
        org_ids_in_run: set[int] = set()
        scoped_org_by_id: dict[int, Any] = {}
        org_registry_lookup: dict[tuple[str, str, int], int] = {}
        for row in scoped_org_rows:
            org_id_int = int(row["id"])
            org_ids_in_run.add(org_id_int)
            scoped_org_by_id[org_id_int] = row
            org_registry_lookup[
                (
                    str(row["registry_type"] or ""),
                    str(row["registry_number"] or ""),
                    int(row["suffix"] or 0),
                )
            ] = org_id_int
            ensure_node(
                f"org:{org_id_int}",
                label=str(row["name"] or "").strip() or "Unknown organisation",
                kind="organisation",
                lane=2,
                run_id=run_id,
                meta={
                    "registry_type": str(row["registry_type"] or ""),
                    "registry_number": str(row["registry_number"] or ""),
                    "suffix": int(row["suffix"] or 0),
                },
            )

        for row in run_org_rows:
            child_org_id = int(row["id"])
            if child_org_id not in org_ids_in_run:
                continue
            link_metadata = _json_dict(row["run_metadata_json"])
            parent_org_id = _resolve_parent_org_id(link_metadata, org_registry_lookup)
            if parent_org_id is None or parent_org_id == child_org_id:
                continue
            if parent_org_id not in org_ids_in_run:
                continue
            source_name = str(scoped_org_by_id[parent_org_id]["name"] or "").strip() or "Unknown organisation"
            target_name = str(scoped_org_by_id[child_org_id]["name"] or "").strip() or "Unknown organisation"
            phrase = _linked_org_phrase(str(row["source"] or ""), link_metadata)
            detail = _linked_org_detail(link_metadata)
            explanation = (f"{target_name} {phrase} {source_name}.").replace("  ", " ")
            if detail:
                explanation += f" {detail}"
            add_edge(
                f"orgorg:{run_id}:org:{parent_org_id}:org:{child_org_id}:{_short_hash(str(row['source']) + phrase)}",
                f"org:{parent_org_id}",
                f"org:{child_org_id}",
                run_id=run_id,
                role_type="organisation_link",
                role_label=str(row["source"] or "organisation_link"),
                source=str(row["source"] or "run_organisation"),
                explanation=explanation,
            )

        for row in candidate_org_rows:
            candidate_name = str(row["candidate_name"] or "").strip()
            if not candidate_name:
                continue
            candidate_id = f"cand:{_slug(candidate_name)}"
            org_id_int = int(row["org_id"])
            org_id = f"org:{org_id_int}"
            org_name = str(row["org_name"] or "").strip() or "Unknown organisation"
            confidence_value = float(row["confidence"] or 0.5)
            candidate_payload = json.loads(str(row["raw_payload_json"] or "{}"))
            relation_phrase = _candidate_relationship_phrase(
                candidate_payload,
                str(row["candidate_source"] or ""),
            )
            add_edge(
                f"candorg:{run_id}:{candidate_id}:{org_id}:{_short_hash(str(confidence_value))}",
                candidate_id,
                org_id,
                run_id=run_id,
                role_type=str(candidate_payload.get("role_type") or "resolved_link"),
                role_label=str(candidate_payload.get("role_label") or "match_or_maybe"),
                source=str(row["candidate_source"] or "resolution"),
                explanation=f"{candidate_name} {relation_phrase} {org_name}.",
            )

        graph_rows = [
            row for row in repository.get_run_network_edges(run_id)
            if not _is_notice_role_row(row)
        ]
        expanded_people: set[str] = set()
        for row in graph_rows:
            person_name = str(row["person_name"] or "").strip()
            if not person_name or person_name in candidate_names:
                continue
            if int(row["organisation_id"]) not in org_ids_in_run:
                continue
            expanded_people.add(person_name)

        for person_name in sorted(expanded_people):
            expanded_id = f"exp:{_slug(person_name)}"
            ensure_node(
                expanded_id,
                label=person_name,
                kind="expanded_person",
                lane=4,
                run_id=run_id,
            )

        for row in repository.get_run_address_edges(run_id):
            org_id = int(row["organisation_id"])
            if org_id not in org_ids_in_run:
                continue
            address_id = int(row["address_id"])
            address_node_id = f"addr:{address_id}"
            ensure_node(
                address_node_id,
                label=str(row["address_label"] or "").strip() or "Unknown address",
                kind="address",
                lane=3,
                run_id=run_id,
                meta={
                    "postcode": str(row["postcode"] or ""),
                    "country": str(row["country"] or ""),
                },
            )
            add_edge(
                f"orgaddr:{run_id}:org:{org_id}:{address_node_id}",
                f"org:{org_id}",
                address_node_id,
                run_id=run_id,
                role_type="organisation_address",
                role_label="registered_address",
                source=str(row["source"] or "organisation_address"),
                explanation=(
                    f"{str(row['organisation_name'] or '')} "
                    f"{str(row['relationship_phrase'] or 'is registered at')} "
                    f"{str(row['address_label'] or '')}."
                ).replace("  ", " "),
            )

        for row in graph_rows:
            person_name = str(row["person_name"] or "").strip()
            if not person_name or person_name in candidate_names:
                continue
            if int(row["organisation_id"]) not in org_ids_in_run:
                continue
            expanded_id = f"exp:{_slug(person_name)}"
            org_id = f"org:{int(row['organisation_id'])}"
            role_type = str(row["role_type"] or "")
            role_label = str(row["role_label"] or "")
            role_phrase = _friendly_role_phrase(
                role_type,
                role_label,
                str(row["relationship_phrase"] or ""),
            )
            explanation = f"{person_name} {role_phrase} {str(row['organisation_name'] or '')}".strip()
            if str(row["source"] or "") == "pdf_gemini_extraction":
                detail = _pdf_person_detail(row)
                if detail:
                    explanation += f". {detail}"
            add_edge(
                f"orgexp:{run_id}:{org_id}:{expanded_id}:{_short_hash(role_type + role_label + str(row['source']))}",
                org_id,
                expanded_id,
                run_id=run_id,
                role_type=role_type or "expanded_link",
                role_label=role_label or role_type or "expanded_link",
                source=str(row["source"] or ""),
                explanation=(explanation + ".").replace("  ", " "),
            )

    for node_id, node in node_map.items():
        node.degree = len(adjacency.get(node_id, set()))

    nodes = [
        {
            "id": node.id,
            "label": node.label,
            "kind": node.kind,
            "lane": node.lane,
            "degree": node.degree,
            "run_ids": sorted(node.run_ids),
            **(node.meta or {}),
        }
        for node in node_map.values()
    ]
    nodes.sort(key=lambda item: (int(item["lane"]), str(item["label"]).lower()))

    edges.sort(key=lambda item: (int(item["run_id"]), item["from"], item["to"], item["id"]))
    return {
        "mode": "multi_run_export",
        "run_ids": cleaned,
        "nodes": nodes,
        "edges": edges,
    }


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower())
    return normalized.strip("_")[:80] or "node"


def _short_hash(text: str) -> str:
    return str(abs(hash(text)))[:8]


def _candidate_relationship_phrase(payload: dict[str, Any], source: str) -> str:
    phrase = str(payload.get("relationship_phrase") or "").strip()
    if phrase:
        return phrase
    if str(source).startswith("companies_house"):
        return "is linked at Companies House to"
    if str(source).startswith("charity_commission"):
        return "is linked in Charity Commission records to"
    return "is linked to"


def _json_dict(raw_value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(raw_value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_parent_org_id(
    metadata: dict[str, Any],
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


def _linked_org_phrase(source: str, metadata: dict[str, Any]) -> str:
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


def _linked_org_detail(metadata: dict[str, Any]) -> str:
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


def _is_notice_org_mention(source: str, metadata: dict[str, Any]) -> bool:
    if source != "pdf_org_mention":
        return False
    return any(
        _is_notice_boilerplate_text(str(metadata.get(key) or ""))
        for key in ("entity_name", "connection_phrase", "connection_detail")
    )


def _is_notice_role_row(row: Any) -> bool:
    if str(row["source"] or "") != "pdf_gemini_extraction":
        return False
    if _is_notice_boilerplate_text(str(row["relationship_phrase"] or "")):
        return True
    if _is_notice_boilerplate_text(str(row["role_label"] or "")):
        return True
    provenance = _json_dict(row["provenance_json"])
    pdf_entity = provenance.get("pdf_entity", {}) if isinstance(provenance, dict) else {}
    return any(
        _is_notice_boilerplate_text(str(pdf_entity.get(key) or ""))
        for key in ("name", "role_label", "connection_phrase", "notes")
    )


def _pdf_person_detail(row: Any) -> str:
    provenance = _json_dict(row["provenance_json"])
    pdf_entity = provenance.get("pdf_entity", {}) if isinstance(provenance, dict) else {}
    return str(pdf_entity.get("notes") or "").strip()


def _friendly_role_phrase(role_type: str, role_label: str, relationship_phrase: str = "") -> str:
    if relationship_phrase.strip():
        return relationship_phrase.strip()
    lowered = (role_type or "").lower()
    label = role_label.strip() or role_type.strip()
    if "trustee" in lowered:
        return "is a trustee of"
    if "director" in lowered:
        return "is a director of"
    if "secretary" in lowered:
        return "is a secretary of"
    if "accountant" in lowered or "auditor" in lowered or "examiner" in lowered:
        return "is named in governance/finance documents for"
    if label:
        return "is linked to"
    return "is linked to"

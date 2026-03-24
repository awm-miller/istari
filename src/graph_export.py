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
                  AND resolution_decisions.status IN ('match', 'maybe_match')
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
                  AND resolution_decisions.status IN ('match', 'maybe_match')
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

        org_ids_in_run: set[int] = set()
        for row in candidate_org_rows:
            candidate_name = str(row["candidate_name"] or "").strip()
            if not candidate_name:
                continue
            candidate_id = f"cand:{_slug(candidate_name)}"
            org_id_int = int(row["org_id"])
            org_ids_in_run.add(org_id_int)
            org_id = f"org:{org_id_int}"
            org_name = str(row["org_name"] or "").strip() or "Unknown organisation"
            ensure_node(
                org_id,
                label=org_name,
                kind="organisation",
                lane=2,
                run_id=run_id,
                meta={
                    "registry_type": str(row["registry_type"] or ""),
                    "registry_number": str(row["registry_number"] or ""),
                    "suffix": int(row["suffix"] or 0),
                },
            )
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

        graph_rows = repository.get_run_network_edges(run_id)
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
                lane=3,
                run_id=run_id,
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

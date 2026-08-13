from __future__ import annotations

import html
import json


UNUSED_NODE_FIELDS = frozenset({
    "adverse_media_count",
    "incorporation_date",
    "is_removed_charity",
    "registration_date",
    "sanction_has_eu_source",
    "seed_index",
    "seed_name",
})
UNUSED_EDGE_FIELDS = frozenset({"end_date", "is_former", "source_provider", "source_providers", "start_date"})


def _compact_node(node: dict) -> dict:
    return {key: value for key, value in node.items() if key not in UNUSED_NODE_FIELDS}


def _compact_edge(edge: dict) -> dict:
    compact = {key: value for key, value in edge.items() if key not in UNUSED_EDGE_FIELDS}
    evidence = compact.get("evidence")
    evidence_items = compact.get("evidence_items")
    if evidence is not None and isinstance(evidence_items, list) and evidence in evidence_items:
        compact.pop("evidence", None)
    return compact


def build_render_context(data: dict, *, title_override: str | None = None) -> dict[str, str]:
    nodes = [_compact_node(node) for node in data.get("nodes") or []]
    edges = [_compact_edge(edge) for edge in data.get("edges") or []]
    return {
        "title": html.escape(str(title_override or data.get("seed_name") or "Istari")),
        "node_count": str(len(nodes)),
        "edge_count": str(len(edges)),
        "nodes_json": json.dumps(nodes, ensure_ascii=False).replace("</", "<\\/"),
        "edges_json": json.dumps(edges, ensure_ascii=False).replace("</", "<\\/"),
        "focus_radius_json": json.dumps(data.get("focus_radius_metres")),
    }

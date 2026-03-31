from __future__ import annotations

import html
import json


def build_render_context(data: dict) -> dict[str, str]:
    nodes = list(data.get("nodes") or [])
    edges = list(data.get("edges") or [])
    return {
        "title": html.escape(str(data.get("seed_name") or "Istari")),
        "node_count": str(len(nodes)),
        "edge_count": str(len(edges)),
        "nodes_json": json.dumps(nodes, ensure_ascii=False).replace("</", "<\\/"),
        "edges_json": json.dumps(edges, ensure_ascii=False).replace("</", "<\\/"),
    }

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.graph.build import consolidate_multi_run
from src.graph.render import render_html


def build_generated_graph_bundle(
    *,
    run_ids: list[int],
    output_root: Path,
    graph_id: str,
    title: str,
) -> dict[str, Any]:
    if not run_ids:
        raise ValueError("Cannot build a generated graph without run IDs.")
    safe_graph_id = "".join(ch for ch in graph_id if ch.isalnum() or ch in {"-", "_"})
    if not safe_graph_id:
        raise ValueError("Invalid generated graph ID.")

    bundle_dir = output_root / safe_graph_id
    bundle_dir.mkdir(parents=True, exist_ok=True)
    data = consolidate_multi_run(run_ids)
    html = render_html(data, title_override=title)
    graph_json = json.dumps(data, ensure_ascii=False, indent=2)
    manifest = {
        "id": safe_graph_id,
        "title": title,
        "run_ids": run_ids,
        "path": f"/generated-graphs/{safe_graph_id}/",
        "node_count": len(data.get("nodes") or []),
        "edge_count": len(data.get("edges") or []),
    }

    (bundle_dir / "index.html").write_text(html, encoding="utf-8")
    (bundle_dir / "graph-data.json").write_text(graph_json, encoding="utf-8")
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def list_generated_graphs(output_root: Path) -> list[dict[str, Any]]:
    if not output_root.exists():
        return []
    graphs: list[dict[str, Any]] = []
    for manifest_path in sorted(output_root.glob("*/manifest.json"), reverse=True):
        try:
            graphs.append(json.loads(manifest_path.read_text(encoding="utf-8")))
        except Exception:
            continue
    return graphs

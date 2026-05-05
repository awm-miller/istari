from __future__ import annotations

import json
import shutil
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
    version: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    if not run_ids:
        raise ValueError("Cannot build a generated graph without run IDs.")
    safe_graph_id = sanitize_graph_id(graph_id)
    graph_dir = output_root / safe_graph_id
    version_id = normalize_version_id(version) if version else _next_version_id(graph_dir)
    version_dir = graph_dir / "versions" / version_id
    if version_dir.exists() and not overwrite:
        raise ValueError(f"Graph {safe_graph_id} version {version_id} already exists.")

    version_dir.mkdir(parents=True, exist_ok=True)
    data = consolidate_multi_run(run_ids)
    html = render_html(data, title_override=title)
    graph_json = json.dumps(data, ensure_ascii=False, indent=2)
    version_manifest = {
        "id": safe_graph_id,
        "title": title,
        "version": version_id,
        "run_ids": run_ids,
        "path": f"/generated-graphs/{safe_graph_id}/versions/{version_id}/",
        "node_count": len(data.get("nodes") or []),
        "edge_count": len(data.get("edges") or []),
    }

    (version_dir / "index.html").write_text(html, encoding="utf-8")
    (version_dir / "graph-data.json").write_text(graph_json, encoding="utf-8")
    (version_dir / "manifest.json").write_text(json.dumps(version_manifest, indent=2), encoding="utf-8")
    graph_manifest = _write_graph_manifest(graph_dir, safe_graph_id, title, active_version=version_id)
    return {**version_manifest, "graph": graph_manifest}


def list_generated_graphs(output_root: Path) -> list[dict[str, Any]]:
    if not output_root.exists():
        return []
    graphs: list[dict[str, Any]] = []
    for graph_dir in sorted((path for path in output_root.iterdir() if path.is_dir()), reverse=True):
        try:
            graphs.append(read_graph_manifest(graph_dir))
        except Exception:
            continue
    return graphs


def read_graph_manifest(graph_dir: Path) -> dict[str, Any]:
    manifest_path = graph_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = _legacy_manifest(graph_dir)
    versions = _list_versions(graph_dir)
    manifest["versions"] = versions
    if not manifest.get("active_version") and versions:
        manifest["active_version"] = versions[-1]["version"]
    manifest["path"] = f"/generated-graphs/{manifest['id']}/"
    return manifest


def set_active_graph_version(output_root: Path, graph_id: str, version: str) -> dict[str, Any]:
    safe_graph_id = sanitize_graph_id(graph_id)
    version_id = normalize_version_id(version)
    graph_dir = output_root / safe_graph_id
    if not (graph_dir / "versions" / version_id / "manifest.json").exists():
        raise ValueError(f"Graph {safe_graph_id} version {version_id} does not exist.")
    current = read_graph_manifest(graph_dir)
    return _write_graph_manifest(
        graph_dir,
        safe_graph_id,
        str(current.get("title") or safe_graph_id),
        active_version=version_id,
    )


def delete_generated_graph(output_root: Path, graph_id: str, version: str | None = None) -> dict[str, Any] | None:
    safe_graph_id = sanitize_graph_id(graph_id)
    graph_dir = output_root / safe_graph_id
    if not graph_dir.exists():
        raise ValueError(f"Graph {safe_graph_id} does not exist.")
    if version is None:
        shutil.rmtree(graph_dir)
        return None

    version_id = normalize_version_id(version)
    version_dir = graph_dir / "versions" / version_id
    if not version_dir.exists():
        raise ValueError(f"Graph {safe_graph_id} version {version_id} does not exist.")
    shutil.rmtree(version_dir)
    versions = _list_versions(graph_dir)
    if not versions:
        shutil.rmtree(graph_dir)
        return None
    current = read_graph_manifest(graph_dir)
    active_version = str(current.get("active_version") or "")
    if active_version == version_id:
        active_version = versions[-1]["version"]
    return _write_graph_manifest(
        graph_dir,
        safe_graph_id,
        str(current.get("title") or safe_graph_id),
        active_version=active_version,
    )


def generated_graph_file_path(output_root: Path, graph_id: str, filename: str, version: str | None = None) -> Path:
    safe_graph_id = sanitize_graph_id(graph_id)
    graph_dir = (output_root / safe_graph_id).resolve()
    root = output_root.resolve()
    graph_dir.relative_to(root)
    if version:
        version_id = normalize_version_id(version)
    else:
        version_id = str(read_graph_manifest(graph_dir).get("active_version") or "")
    if not version_id:
        raise FileNotFoundError(f"Graph {safe_graph_id} has no active version.")
    path = (graph_dir / "versions" / version_id / filename).resolve()
    path.relative_to(graph_dir)
    return path


def sanitize_graph_id(value: str) -> str:
    raw = str(value or "").strip().lower()
    safe = "".join(ch if ch.isalnum() else "-" for ch in raw).strip("-")
    while "--" in safe:
        safe = safe.replace("--", "-")
    if not safe:
        raise ValueError("Invalid generated graph ID.")
    return safe[:80]


def normalize_version_id(value: str) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("v"):
        text = text[1:]
    number = int(text)
    if number < 1:
        raise ValueError("Graph version must be at least 1.")
    return f"v{number}"


def _next_version_id(graph_dir: Path) -> str:
    existing = [
        int(version["version"][1:])
        for version in _list_versions(graph_dir)
        if str(version.get("version", "")).startswith("v") and str(version["version"])[1:].isdigit()
    ]
    return f"v{(max(existing) + 1) if existing else 1}"


def _list_versions(graph_dir: Path) -> list[dict[str, Any]]:
    version_root = graph_dir / "versions"
    if not version_root.exists():
        legacy = graph_dir / "manifest.json"
        if legacy.exists() and (graph_dir / "index.html").exists():
            data = json.loads(legacy.read_text(encoding="utf-8"))
            return [{**data, "version": "v1", "path": f"/generated-graphs/{data['id']}/versions/v1/"}]
        return []
    versions = []
    for manifest_path in sorted(version_root.glob("v*/manifest.json")):
        try:
            versions.append(json.loads(manifest_path.read_text(encoding="utf-8")))
        except Exception:
            continue
    return versions


def _write_graph_manifest(graph_dir: Path, graph_id: str, title: str, *, active_version: str) -> dict[str, Any]:
    versions = _list_versions(graph_dir)
    manifest = {
        "id": graph_id,
        "title": title,
        "path": f"/generated-graphs/{graph_id}/",
        "active_version": active_version,
        "versions": versions,
    }
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def _legacy_manifest(graph_dir: Path) -> dict[str, Any]:
    manifest_path = graph_dir / "manifest.json"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "id": data["id"],
        "title": data.get("title") or data["id"],
        "path": f"/generated-graphs/{data['id']}/",
        "active_version": "v1",
    }

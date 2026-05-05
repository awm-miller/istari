from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.tree_graph_artifacts import (
    build_generated_graph_bundle,
    delete_generated_graph,
    list_generated_graphs,
    set_active_graph_version,
)


class TreeGraphArtifactsTest(unittest.TestCase):
    def test_list_generated_graphs_reads_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            graph_dir = Path(tmp) / "abc123" / "versions" / "v1"
            graph_dir.mkdir(parents=True)
            (Path(tmp) / "abc123" / "manifest.json").write_text(
                json.dumps({"id": "abc123", "title": "Generated", "active_version": "v1"}),
                encoding="utf-8",
            )
            (graph_dir / "manifest.json").write_text(
                json.dumps({"id": "abc123", "title": "Generated", "version": "v1", "path": "/generated-graphs/abc123/versions/v1/"}),
                encoding="utf-8",
            )

            graphs = list_generated_graphs(Path(tmp))

        self.assertEqual("abc123", graphs[0]["id"])
        self.assertEqual("v1", graphs[0]["active_version"])
        self.assertEqual(["v1"], [version["version"] for version in graphs[0]["versions"]])

    def test_set_active_version_and_delete_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            graph_root = root / "abc123"
            for version in ("v1", "v2"):
                version_dir = graph_root / "versions" / version
                version_dir.mkdir(parents=True)
                (version_dir / "manifest.json").write_text(
                    json.dumps({"id": "abc123", "title": "Generated", "version": version}),
                    encoding="utf-8",
                )
            (graph_root / "manifest.json").write_text(
                json.dumps({"id": "abc123", "title": "Generated", "active_version": "v1"}),
                encoding="utf-8",
            )

            graph = set_active_graph_version(root, "abc123", "2")
            self.assertEqual("v2", graph["active_version"])

            graph = delete_generated_graph(root, "abc123", "v2")

        self.assertEqual("v1", graph["active_version"])
        self.assertEqual(["v1"], [version["version"] for version in graph["versions"]])

    def test_version_manifest_keeps_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            import src.tree_graph_artifacts as artifacts

            original_consolidate = artifacts.consolidate_multi_run
            try:
                artifacts.consolidate_multi_run = lambda _run_ids: {"nodes": [], "edges": []}
                manifest = build_generated_graph_bundle(
                    run_ids=[1],
                    output_root=Path(tmp),
                    graph_id="with metadata",
                    title="With metadata",
                    metadata={"negative_news": {"enabled": True, "source_database_key": "abc"}},
                )
            finally:
                artifacts.consolidate_multi_run = original_consolidate

        self.assertEqual({"enabled": True, "source_database_key": "abc"}, manifest["metadata"]["negative_news"])


if __name__ == "__main__":
    unittest.main()

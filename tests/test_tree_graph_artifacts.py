from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.tree_graph_artifacts import list_generated_graphs


class TreeGraphArtifactsTest(unittest.TestCase):
    def test_list_generated_graphs_reads_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            graph_dir = Path(tmp) / "abc123"
            graph_dir.mkdir()
            (graph_dir / "manifest.json").write_text(
                json.dumps({"id": "abc123", "title": "Generated", "path": "/generated-graphs/abc123/"}),
                encoding="utf-8",
            )

            graphs = list_generated_graphs(Path(tmp))

        self.assertEqual([{"id": "abc123", "title": "Generated", "path": "/generated-graphs/abc123/"}], graphs)


if __name__ == "__main__":
    unittest.main()

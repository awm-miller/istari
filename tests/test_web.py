from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.web import create_app


class WebTests(unittest.TestCase):
    def test_index_serves_generated_graph_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            graph_path = Path(temp_dir) / "latest_graph.html"
            graph_path.write_text("<html><body>Istari Graph</body></html>", encoding="utf-8")

            with patch("src.web.DEFAULT_GRAPH", graph_path):
                app = create_app()
                client = app.test_client()
                response = client.get("/")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Istari Graph", response.data)

    def test_index_returns_message_when_graph_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            graph_path = Path(temp_dir) / "missing_graph.html"

            with patch("src.web.DEFAULT_GRAPH", graph_path):
                app = create_app()
                client = app.test_client()
                response = client.get("/")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"No graph generated yet.", response.data)
            self.assertIn(b"python scripts/consolidate_and_graph.py RUN_IDS --out output/latest_graph.html", response.data)


if __name__ == "__main__":
    unittest.main()

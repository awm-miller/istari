from __future__ import annotations

import unittest

from src.graph.render import render_html


class GraphRenderingTests(unittest.TestCase):
    def test_leaflet_is_not_a_blocking_page_asset(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertNotIn('<script src="https://cdn.jsdelivr.net/npm/leaflet', html)
        self.assertNotIn('<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet', html)
        self.assertIn("LEAFLET_SCRIPT_URL", html)

    def test_labels_use_a_persistent_world_layer(self) -> None:
        html = render_html({"nodes": [{"id": "n1", "label": "Visible label"}], "edges": []})

        self.assertIn('labelWorld.className = "graph-label-world"', html)
        self.assertIn("return sceneNodes;", html)
        self.assertNotIn("labelLayer.innerHTML =", html)


if __name__ == "__main__":
    unittest.main()

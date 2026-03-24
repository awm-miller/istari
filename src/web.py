from __future__ import annotations

import argparse
from pathlib import Path

from flask import Flask

GRAPH_DIR = Path(__file__).resolve().parents[1] / "output"
DEFAULT_GRAPH = GRAPH_DIR / "latest_graph.html"


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index() -> str:
        if not DEFAULT_GRAPH.exists():
            return (
                "<!doctype html><html><head><meta charset='utf-8'><title>Istari</title>"
                "<style>body{font-family:system-ui;background:#0d1117;color:#c9d1d9;padding:40px}</style>"
                "</head><body><h1>Istari Graph</h1>"
                "<p>No graph generated yet. Run:</p>"
                "<pre>python scripts/consolidate_and_graph.py RUN_IDS --out output/latest_graph.html</pre>"
                "</body></html>"
            )
        return DEFAULT_GRAPH.read_text(encoding="utf-8")

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    create_app().run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()

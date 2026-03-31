from __future__ import annotations

from src.graph.render_context import build_render_context
from src.graph.render_page import render_viewer_html


def render_html(data: dict) -> str:
    return render_viewer_html(build_render_context(data))

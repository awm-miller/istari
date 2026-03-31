from __future__ import annotations

from src.graph.viewer_assets import load_asset_text, replace_tokens

D3_SCRIPT_TAG = '<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>'
PIXI_SCRIPT_TAG = '<script src="https://cdn.jsdelivr.net/npm/pixi.js@8/dist/pixi.min.js"></script>'
LEAFLET_CSS_TAG = """<link
  rel="stylesheet"
  href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
  crossorigin=""
>"""
LEAFLET_SCRIPT_TAG = '<script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>'


def render_viewer_html(context: dict[str, str], *, runtime_assets: tuple[str, ...] = ("viewer_runtime_webgl.js", "viewer_app.js")) -> str:
    css = load_asset_text("viewer_styles.css")
    markup = replace_tokens(load_asset_text("viewer_markup.html"), context)
    runtime = "\n".join(
        f"<script>\n{replace_tokens(load_asset_text(asset_name), context)}\n</script>"
        for asset_name in runtime_assets
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{context["title"]}</title>
{LEAFLET_CSS_TAG}
<style>
{css}
</style>
</head>
<body>
{markup}
{D3_SCRIPT_TAG}
{PIXI_SCRIPT_TAG}
{LEAFLET_SCRIPT_TAG}
{runtime}
</body>
</html>"""

"""Tests for full-page HTML-to-text extraction (adverse media pipeline)."""

from __future__ import annotations

from src.html_plain_text import extract_title_from_html, html_to_plain_text


def test_html_to_plain_text_preserves_long_article_body() -> None:
    """Ensure we do not drop tail content (whole-article sanity)."""
    paras = [f"Paragraph {i} with some words." for i in range(200)]
    body = "".join(f"<p>{p}</p>" for p in paras)
    html = f"""<!DOCTYPE html><html><head><title>Test Article</title></head>
    <body><nav>skip me</nav><article>{body}</article><footer>foot</footer></body></html>"""
    text = html_to_plain_text(html)
    assert "Paragraph 0" in text
    assert "Paragraph 199" in text
    assert "Paragraph 100" in text


def test_script_and_style_stripped() -> None:
    html = "<html><body><script>alert(1)</script><p>Visible</p><style>.x{{}}</style></body></html>"
    assert html_to_plain_text(html) == "Visible"


def test_extract_title_from_html() -> None:
    html = "<html><head><title>  My &amp; Title  </title></head><body></body></html>"
    assert extract_title_from_html(html, "x") == "My & Title"

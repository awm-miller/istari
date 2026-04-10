"""Convert HTML to plain text without pulling in the full mapping enrichment stack.

Used for adverse-media / article extraction where we want the full page body text
(up to a caller-defined cap), not a focused excerpt."""

from __future__ import annotations

import html as html_module
import re
from html.parser import HTMLParser

_HTML_SKIP_RE = re.compile(
    r"<(script|style|noscript|svg)[^>]*>.*?</\1>",
    flags=re.IGNORECASE | re.DOTALL,
)
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _sanitize_block_text(value: str) -> str:
    text = _CONTROL_CHAR_RE.sub(" ", str(value or ""))
    lines = [
        re.sub(r"[ \t]+", " ", line).strip()
        for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    compacted: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line
        if is_blank:
            if not previous_blank:
                compacted.append("")
            previous_blank = True
            continue
        compacted.append(line)
        previous_blank = False
    return "\n".join(compacted).strip()


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        value = str(data or "").strip()
        if value:
            self.parts.append(value)

    def get_text(self) -> str:
        return "\n".join(self.parts)


def html_to_plain_text(html_text: str) -> str:
    """Strip scripts/styles/comments, walk visible text nodes, normalize whitespace."""
    without_comments = _HTML_COMMENT_RE.sub(" ", html_text)
    without_skipped = _HTML_SKIP_RE.sub(" ", without_comments)
    parser = _TextExtractor()
    parser.feed(without_skipped)
    text = parser.get_text()
    return _sanitize_block_text(html_module.unescape(text))


def extract_title_from_html(html_text: str, fallback: str = "") -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return fallback
    inner = re.sub(r"<[^>]+>", " ", match.group(1))
    return _sanitize_block_text(html_module.unescape(inner)) or fallback

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

ASSET_DIR = Path(__file__).resolve().parent


@lru_cache(maxsize=None)
def load_asset_text(filename: str) -> str:
    text = (ASSET_DIR / filename).read_text(encoding="utf-8")
    return text.replace("{{", "{").replace("}}", "}")


def replace_tokens(template: str, tokens: dict[str, str]) -> str:
    rendered = template
    for key, value in tokens.items():
        rendered = rendered.replace(f"{{{key}}}", value)
    return rendered

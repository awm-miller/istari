from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from src.config import Settings


_PUNCTUATION_RE = re.compile(r"[^\w\s\u0600-\u06FF-]+", re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")
_ARABIC_NORMALIZATION = str.maketrans(
    {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ٱ": "ا",
        "ى": "ي",
        "ئ": "ي",
        "ؤ": "و",
        "ة": "ه",
    }
)


def default_egypt_judgments_screen_path(settings: Settings) -> Path:
    return settings.project_root / "data" / "egypt_judgments_screen.json"


def normalize_egypt_judgment_name(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.translate(_ARABIC_NORMALIZATION)
    text = _PUNCTUATION_RE.sub(" ", text)
    text = text.replace("ـ", " ")
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _node_match_candidates(node: dict[str, Any]) -> list[str]:
    values = [str(node.get("label") or "").strip()]
    values.extend(str(alias or "").strip() for alias in (node.get("aliases") or []))
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = normalize_egypt_judgment_name(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _load_egypt_judgment_entries(dataset_path: Path) -> list[dict[str, Any]]:
    if not dataset_path.exists():
        return []
    try:
        payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    entries = payload.get("entries") if isinstance(payload, dict) else []
    return [entry for entry in entries if isinstance(entry, dict)]


def _build_match_index(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        candidate_names = [
            str(entry.get("canonical_name") or "").strip(),
            *(str(alias or "").strip() for alias in (entry.get("aliases") or [])),
        ]
        for candidate in candidate_names:
            key = normalize_egypt_judgment_name(candidate)
            if not key:
                continue
            index.setdefault(key, []).append(entry)
    return index


def annotate_graph_with_egypt_judgments(
    data: dict[str, Any],
    *,
    settings: Settings,
    dataset_path: Path | None = None,
) -> dict[str, Any]:
    path = dataset_path or default_egypt_judgments_screen_path(settings)
    entries = _load_egypt_judgment_entries(path)
    if not entries:
        return data
    index = _build_match_index(entries)

    for node in data.get("nodes", []):
        if not isinstance(node, dict) or str(node.get("kind") or "") != "person":
            continue
        matches: list[dict[str, Any]] = []
        seen_match_keys: set[tuple[str, str, str]] = set()
        for candidate_name in _node_match_candidates(node):
            candidate_key = normalize_egypt_judgment_name(candidate_name)
            for entry in index.get(candidate_key, []):
                dataset_names = [
                    str(entry.get("canonical_name") or "").strip(),
                    *(str(alias or "").strip() for alias in (entry.get("aliases") or [])),
                ]
                matched_alias = next(
                    (
                        name
                        for name in dataset_names
                        if normalize_egypt_judgment_name(name) == candidate_key
                    ),
                    str(entry.get("canonical_name") or "").strip(),
                )
                for source in (entry.get("sources") or []):
                    if not isinstance(source, dict):
                        continue
                    match_key = (
                        str(entry.get("canonical_name") or "").strip(),
                        str(source.get("source_id") or "").strip(),
                        str(source.get("list_name") or "").strip(),
                    )
                    if match_key in seen_match_keys:
                        continue
                    seen_match_keys.add(match_key)
                    matches.append(
                        {
                            "canonical_name": str(entry.get("canonical_name") or "").strip(),
                            "matched_name": candidate_name,
                            "matched_alias": matched_alias,
                            "source_type": str(source.get("source_type") or "").strip(),
                            "source_label": str(source.get("source_label") or "").strip(),
                            "source_url": str(source.get("source_url") or "").strip(),
                            "list_name": str(source.get("list_name") or "").strip(),
                        }
                    )
        if not matches:
            continue
        matches.sort(
            key=lambda item: (
                str(item.get("canonical_name") or "").lower(),
                str(item.get("source_type") or "").lower(),
                str(item.get("list_name") or "").lower(),
            )
        )
        node["egypt_judgment_hit"] = True
        node["egypt_judgment_count"] = len(matches)
        node["egypt_judgment_matches"] = matches
    return data

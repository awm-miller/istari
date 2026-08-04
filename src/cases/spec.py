from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


CASE_SPEC_VERSION = 1
INPUT_KINDS = {"person", "company", "charity", "address"}
PIVOT_KINDS = {"company", "charity", "address"}
LEAF_KINDS = {"person"}


@dataclass(frozen=True, slots=True)
class Recipe:
    name: str
    max_rounds: int
    max_entities: int
    pivot_kinds: tuple[str, ...] = ("address", "company", "charity")
    leaf_kinds: tuple[str, ...] = ("person",)


RECIPES = {
    "registry-light": Recipe("registry-light", max_rounds=2, max_entities=500),
    "address-network": Recipe("address-network", max_rounds=3, max_entities=750),
}


@dataclass(frozen=True, slots=True)
class CaseInput:
    kind: str
    value: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CaseInput":
        kind = _clean(payload.get("kind")).lower()
        value = _clean(payload.get("value"))
        if kind not in INPUT_KINDS:
            raise ValueError(f"Unsupported case input kind '{kind}'. Expected one of {sorted(INPUT_KINDS)}.")
        if not value:
            raise ValueError("Case input value cannot be empty.")
        return cls(kind=kind, value=value)

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "value": self.value}


@dataclass(frozen=True, slots=True)
class DiscoveryPolicy:
    pivot_kinds: tuple[str, ...]
    leaf_kinds: tuple[str, ...]
    max_rounds: int
    max_entities: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None, recipe: Recipe) -> "DiscoveryPolicy":
        data = payload or {}
        pivot_kinds = _dedupe_kinds(data.get("pivot_kinds"), recipe.pivot_kinds)
        leaf_kinds = _dedupe_kinds(data.get("leaf_kinds"), recipe.leaf_kinds)
        if not set(pivot_kinds).issubset(PIVOT_KINDS):
            raise ValueError(f"pivot_kinds currently supports only {sorted(PIVOT_KINDS)}.")
        if not set(leaf_kinds).issubset(LEAF_KINDS):
            raise ValueError(f"leaf_kinds currently supports only {sorted(LEAF_KINDS)}.")
        max_rounds = int(data.get("max_rounds", recipe.max_rounds))
        max_entities = int(data.get("max_entities", recipe.max_entities))
        if not 1 <= max_rounds <= 10:
            raise ValueError("max_rounds must be between 1 and 10.")
        if not 1 <= max_entities <= 10_000:
            raise ValueError("max_entities must be between 1 and 10000.")
        return cls(
            pivot_kinds=pivot_kinds,
            leaf_kinds=leaf_kinds,
            max_rounds=max_rounds,
            max_entities=max_entities,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "pivot_kinds": list(self.pivot_kinds),
            "leaf_kinds": list(self.leaf_kinds),
            "max_rounds": self.max_rounds,
            "max_entities": self.max_entities,
        }


@dataclass(frozen=True, slots=True)
class Enrichments:
    sanctions: bool = True
    documents: bool = False
    negative_news: bool = False

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "Enrichments":
        data = payload or {}
        return cls(
            sanctions=_as_bool(data.get("sanctions"), True),
            documents=_as_bool(data.get("documents"), False),
            negative_news=_as_bool(data.get("negative_news"), False),
        )

    def to_dict(self) -> dict[str, bool]:
        return {
            "sanctions": self.sanctions,
            "documents": self.documents,
            "negative_news": self.negative_news,
        }


@dataclass(frozen=True, slots=True)
class CaseSpec:
    id: str
    title: str
    inputs: tuple[CaseInput, ...]
    recipe: str
    policy: DiscoveryPolicy
    enrichments: Enrichments
    version: int = CASE_SPEC_VERSION

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CaseSpec":
        if not isinstance(payload, dict):
            raise ValueError("Case specification must be an object.")
        version = int(payload.get("version", CASE_SPEC_VERSION))
        if version != CASE_SPEC_VERSION:
            raise ValueError(f"Unsupported case specification version {version}.")
        title = _clean(payload.get("title"))
        if not title:
            raise ValueError("Case title is required.")
        case_id = slugify(payload.get("id") or title)
        recipe_name = _clean(payload.get("recipe") or "registry-light").lower()
        recipe = RECIPES.get(recipe_name)
        if recipe is None:
            raise ValueError(f"Unknown recipe '{recipe_name}'. Expected one of {sorted(RECIPES)}.")
        raw_inputs = payload.get("inputs")
        if not isinstance(raw_inputs, list) or not raw_inputs:
            raise ValueError("Case specification requires at least one input.")
        inputs = _dedupe_inputs(CaseInput.from_dict(item) for item in raw_inputs if isinstance(item, dict))
        if not inputs:
            raise ValueError("Case specification requires at least one valid input.")
        if recipe_name == "address-network" and not any(item.kind == "address" for item in inputs):
            raise ValueError("address-network requires at least one address input.")
        return cls(
            id=case_id,
            title=title,
            inputs=inputs,
            recipe=recipe_name,
            policy=DiscoveryPolicy.from_dict(payload.get("policy"), recipe),
            enrichments=Enrichments.from_dict(payload.get("enrichments")),
            version=version,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "id": self.id,
            "title": self.title,
            "recipe": self.recipe,
            "inputs": [item.to_dict() for item in self.inputs],
            "policy": self.policy.to_dict(),
            "enrichments": self.enrichments.to_dict(),
        }


def load_case_spec(path: Path) -> CaseSpec:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    payload = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    return CaseSpec.from_dict(payload)


def write_case_spec(spec: CaseSpec, path: Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = spec.to_dict()
    if target.suffix.lower() == ".json":
        text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    else:
        text = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    target.write_text(text, encoding="utf-8")
    return target


def slugify(value: Any) -> str:
    raw = _clean(value).lower()
    safe = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    if not safe:
        raise ValueError("Case id must contain letters or numbers.")
    return safe[:80]


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_kinds(value: Any, default: tuple[str, ...]) -> tuple[str, ...]:
    values = value if isinstance(value, (list, tuple)) else default
    return tuple(dict.fromkeys(_clean(item).lower() for item in values if _clean(item)))


def _dedupe_inputs(values: Any) -> tuple[CaseInput, ...]:
    out: list[CaseInput] = []
    seen: set[tuple[str, str]] = set()
    for item in values:
        key = (item.kind, item.value.casefold())
        if key not in seen:
            out.append(item)
            seen.add(key)
    return tuple(out)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

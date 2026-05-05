from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from src.charity_commission.client import CharityCommissionClient
from src.pipeline import (
    add_organisation_to_run,
    resume_run_pipeline,
    run_name_pipeline,
    run_org_roots_pipeline,
)
from src.resolution.matcher import HybridMatcher
from src.search.provider import SearchProvider
from src.storage.repository import Repository


VALID_TREE_MODES = {"name_seed", "org_rooted", "org_chained"}


@dataclass(frozen=True, slots=True)
class OrgRootSpec:
    registry_type: str
    registry_number: str
    suffix: int = 0

    def as_pipeline_root(self) -> dict[str, Any]:
        return {
            "registry_type": self.registry_type,
            "registry_number": self.registry_number,
            "suffix": self.suffix,
        }


@dataclass(frozen=True, slots=True)
class TreeBuildRequest:
    mode: str
    seed_name: str = ""
    seed_names: tuple[str, ...] = ()
    roots: tuple[OrgRootSpec, ...] = ()
    target_names: tuple[str, ...] = ()
    creativity_level: str = "balanced"
    limit: int = 25
    notify_email: str = ""


class TreePipelineRunner(Protocol):
    def run_name(self, request: TreeBuildRequest) -> dict[str, Any]:
        ...

    def run_org_rooted(self, request: TreeBuildRequest) -> dict[str, Any]:
        ...

    def run_org_chained(self, request: TreeBuildRequest) -> dict[str, Any]:
        ...


def parse_org_root_spec(value: str) -> OrgRootSpec:
    raw = " ".join(str(value or "").split()).strip()
    parts = raw.split(":")
    if len(parts) not in {2, 3}:
        raise ValueError(
            f"Invalid org root '{value}'. Use charity:1095626, charity:1095626:0, or company:01234567."
        )
    registry_type = parts[0].strip().lower()
    registry_number = parts[1].strip()
    suffix_text = parts[2].strip() if len(parts) == 3 else "0"
    if registry_type not in {"charity", "company"}:
        raise ValueError(f"Invalid registry type '{parts[0]}'. Expected charity or company.")
    if not registry_number:
        raise ValueError(f"Invalid org root '{value}'. Registry number is required.")
    try:
        suffix = int(suffix_text or "0")
    except ValueError as exc:
        raise ValueError(f"Invalid suffix '{suffix_text}' in org root '{value}'.") from exc
    return OrgRootSpec(registry_type=registry_type, registry_number=registry_number, suffix=suffix)


def normalize_tree_build_request(payload: dict[str, Any]) -> TreeBuildRequest:
    mode = str(payload.get("mode") or "").strip().lower()
    if mode not in VALID_TREE_MODES:
        raise ValueError(f"Invalid tree build mode '{mode}'. Expected one of {sorted(VALID_TREE_MODES)}.")

    roots = tuple(_dedupe_roots(payload.get("roots") or []))
    seed_name = _clean_text(payload.get("seed_name"))
    seed_names = tuple(_dedupe_texts(payload.get("seed_names") or []))
    target_names = tuple(_dedupe_texts(payload.get("target_names") or []))
    creativity_level = str(payload.get("creativity_level") or "balanced").strip().lower()
    if creativity_level not in {"strict", "balanced", "exploratory"}:
        raise ValueError("creativity_level must be strict, balanced, or exploratory.")
    limit = int(payload.get("limit") or 25)
    if limit < 1:
        raise ValueError("limit must be at least 1.")
    notify_email = _clean_text(payload.get("notify_email"))

    if mode == "name_seed" and not seed_name:
        raise ValueError("name_seed builds require seed_name.")
    if mode == "org_rooted" and not roots:
        raise ValueError("org_rooted builds require at least one organisation root.")
    if mode == "org_chained":
        if not seed_names:
            seed_names = (seed_name,) if seed_name else ()
        if not seed_names:
            raise ValueError("org_chained builds require at least one seed name.")
        if not roots:
            raise ValueError("org_chained builds require at least one organisation root.")

    return TreeBuildRequest(
        mode=mode,
        seed_name=seed_name,
        seed_names=seed_names,
        roots=roots,
        target_names=target_names,
        creativity_level=creativity_level,
        limit=limit,
        notify_email=notify_email,
    )


def execute_tree_build(request: TreeBuildRequest, runner: TreePipelineRunner) -> dict[str, Any]:
    if request.mode == "name_seed":
        return runner.run_name(request)
    if request.mode == "org_rooted":
        return runner.run_org_rooted(request)
    if request.mode == "org_chained":
        return runner.run_org_chained(request)
    raise ValueError(f"Unsupported tree build mode: {request.mode}")


@dataclass(slots=True)
class DefaultTreePipelineRunner:
    repository: Repository
    settings: Any
    charity_client: CharityCommissionClient
    search_providers: list[SearchProvider]
    matcher: HybridMatcher

    def run_name(self, request: TreeBuildRequest) -> dict[str, Any]:
        return run_name_pipeline(
            repository=self.repository,
            settings=self.settings,
            charity_client=self.charity_client,
            search_providers=self.search_providers,
            matcher=self.matcher,
            seed_name=request.seed_name,
            creativity_level=request.creativity_level,
            limit=request.limit,
        )

    def run_org_rooted(self, request: TreeBuildRequest) -> dict[str, Any]:
        return run_org_roots_pipeline(
            repository=self.repository,
            settings=self.settings,
            charity_client=self.charity_client,
            search_providers=self.search_providers,
            matcher=self.matcher,
            roots=[root.as_pipeline_root() for root in request.roots],
            creativity_level=request.creativity_level,
            limit=request.limit,
            seed_name=request.seed_name or None,
            target_names=list(request.target_names),
        )

    def run_org_chained(self, request: TreeBuildRequest) -> dict[str, Any]:
        runs: list[dict[str, Any]] = []
        run_ids: list[int] = []
        for seed_name in request.seed_names:
            seed_request = TreeBuildRequest(
                mode="name_seed",
                seed_name=seed_name,
                creativity_level=request.creativity_level,
                limit=request.limit,
            )
            seed_result = self.run_name(seed_request)
            run_id = int(seed_result["run_id"])
            linked_roots = []
            for root in request.roots:
                linked_roots.append(
                    add_organisation_to_run(
                        repository=self.repository,
                        settings=self.settings,
                        charity_client=self.charity_client,
                        run_id=run_id,
                        registry_type=root.registry_type,
                        registry_number=root.registry_number,
                        suffix=root.suffix,
                        limit=request.limit,
                        rerun_downstream=False,
                    )
                )
            resumed = resume_run_pipeline(
                repository=self.repository,
                settings=self.settings,
                charity_client=self.charity_client,
                search_providers=self.search_providers,
                matcher=self.matcher,
                run_id=run_id,
                limit=request.limit,
            )
            run_ids.append(run_id)
            runs.append(
                {
                    "seed_name": seed_name,
                    "run_id": run_id,
                    "root_organisations": linked_roots,
                    "initial_result": seed_result,
                    "resumed_result": resumed,
                }
            )
        return {
            "mode": "org_chained",
            "seed_names": list(request.seed_names),
            "run_ids": run_ids,
            "runs": runs,
        }


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_texts(values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        key = text.lower()
        if text and key not in seen:
            out.append(text)
            seen.add(key)
    return out


def _dedupe_roots(values: Any) -> list[OrgRootSpec]:
    out: list[OrgRootSpec] = []
    seen: set[tuple[str, str, int]] = set()
    for value in values:
        root = _coerce_root(value)
        key = (root.registry_type, root.registry_number, root.suffix)
        if key not in seen:
            out.append(root)
            seen.add(key)
    return out


def _coerce_root(value: Any) -> OrgRootSpec:
    if isinstance(value, str):
        return parse_org_root_spec(value)
    if isinstance(value, dict):
        return OrgRootSpec(
            registry_type=str(value.get("registry_type") or "").strip().lower(),
            registry_number=str(value.get("registry_number") or "").strip(),
            suffix=int(value.get("suffix") or 0),
        )
    raise ValueError(f"Invalid organisation root: {value!r}")

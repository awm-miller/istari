from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from src.address_pivot import AddressPivotSearcher
from src.addresses import normalize_address_label
from src.cases.spec import CaseSpec, write_case_spec
from src.charity_commission.client import CharityCommissionClient
from src.companies_house.client import CompaniesHouseClient
from src.config import Settings, load_settings
from src.graph.adverse_media import annotate_graph_with_adverse_media, resolve_negative_news_db_path
from src.negative_news import run_negative_news_cluster_batch
from src.resolution.matcher import HybridMatcher
from src.search.provider import build_search_providers
from src.services.mvp_pipeline import (
    add_organisation_to_run,
    step1_expand_seed,
    step2_expand_connected_organisations,
    step2b_enrich_from_pdfs,
    step3_expand_connected_people,
    step4_ofac_screening,
)
from src.services.pipeline_services import RankingService
from src.storage.repository import Repository
from src.tree_builder import OrgRootSpec, parse_org_root_spec
from src.tree_graph_artifacts import build_generated_graph_bundle


EventSink = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class CasePaths:
    root: Path
    database: Path
    cache: Path
    artifacts: Path
    events: Path
    state: Path
    specification: Path
    negative_news_database: Path
    lock: Path

    @classmethod
    def for_case(cls, workspace: Path, case_id: str) -> "CasePaths":
        root = Path(workspace) / case_id
        return cls(
            root=root,
            database=root / "case.db",
            cache=root / "cache",
            artifacts=root / "artifacts",
            events=root / "events.jsonl",
            state=root / "state.json",
            specification=root / "case.yaml",
            negative_news_database=root / "negative_news.db",
            lock=root / "case.lock",
        )


class CaseRunner:
    def __init__(self, *, workspace: Path, settings: Settings | None = None) -> None:
        self.workspace = Path(workspace)
        self.settings = settings or load_settings()

    def run(self, spec: CaseSpec, *, force: bool = False) -> dict[str, Any]:
        paths = CasePaths.for_case(self.workspace, spec.id)
        paths.root.mkdir(parents=True, exist_ok=True)
        with _case_lock(paths.lock):
            return self._run_locked(spec, paths=paths, force=force)

    def _run_locked(self, spec: CaseSpec, *, paths: CasePaths, force: bool) -> dict[str, Any]:
        write_case_spec(spec, paths.specification)
        state = self._load_state(paths)
        if state.get("status") == "completed" and not force:
            return state
        if force:
            state = {}

        state.update(
            {
                "case_id": spec.id,
                "title": spec.title,
                "status": "running",
                "stage": "initialising",
                "run_ids": list(state.get("run_ids") or []),
                "input_runs": dict(state.get("input_runs") or {}),
                "resolved_roots": list(state.get("resolved_roots") or []),
                "updated_at": _utc_now(),
            }
        )
        state.pop("error", None)
        self._save_state(paths, state)

        def emit(event: str, details: dict[str, Any] | None = None) -> None:
            payload = {
                "at": _utc_now(),
                "event": event,
                "case_id": spec.id,
                "details": details or {},
            }
            with paths.events.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

        emit("case_started", {"recipe": spec.recipe, "force": force})
        case_settings = replace(
            self.settings,
            database_path=paths.database,
            cache_dir=paths.cache,
            pdf_enrichment_enabled=spec.enrichments.documents,
        )
        repository = Repository(paths.database, case_settings.project_root / "src" / "storage" / "schema.sql")
        repository.init_db()
        charity_client = CharityCommissionClient(case_settings)
        companies_house_client = CompaniesHouseClient(case_settings)
        matcher = HybridMatcher(case_settings)
        providers = build_search_providers(case_settings, include_web_dork=False)

        try:
            roots = self._resolve_roots(
                spec,
                state=state,
                paths=paths,
                settings=case_settings,
                charity_client=charity_client,
                companies_house_client=companies_house_client,
                emit=emit,
            )
            run_ids = self._prepare_runs(
                spec,
                roots=roots,
                state=state,
                paths=paths,
                repository=repository,
                settings=case_settings,
                charity_client=charity_client,
                matcher=matcher,
                providers=providers,
                emit=emit,
            )
            state["stage"] = "discovering"
            self._save_state(paths, state)
            summaries = []
            for run_id in run_ids:
                summaries.append(
                    self._expand_run(
                        spec,
                        run_id=run_id,
                        repository=repository,
                        settings=case_settings,
                        charity_client=charity_client,
                        emit=emit,
                    )
                )

            negative_news = self._negative_news(
                spec,
                paths=paths,
                settings=case_settings,
                repository=repository,
                emit=emit,
            )
            state["stage"] = "building_graph"
            self._save_state(paths, state)
            with _temporary_environment(
                DATABASE_PATH=str(paths.database),
                CACHE_DIR=str(paths.cache),
                NEGATIVE_NEWS_DB_PATH=str(paths.negative_news_database),
            ):
                manifest = build_generated_graph_bundle(
                    run_ids=run_ids,
                    output_root=paths.artifacts,
                    graph_id=spec.id,
                    title=spec.title,
                    metadata={
                        "case": spec.to_dict(),
                        "source_coverage": _source_coverage(case_settings),
                        "discovery": summaries,
                        "negative_news": negative_news,
                    },
                    transform_data=(
                        lambda data: annotate_graph_with_adverse_media(
                            data,
                            settings=case_settings,
                            database_path=resolve_negative_news_db_path(case_settings),
                        )
                    )
                    if negative_news
                    else None,
                )
            state.update(
                {
                    "status": "completed",
                    "stage": "completed",
                    "run_ids": run_ids,
                    "artifact": manifest,
                    "discovery": summaries,
                    "updated_at": _utc_now(),
                }
            )
            state.pop("error", None)
            self._save_state(paths, state)
            emit("case_completed", {"run_ids": run_ids, "artifact": manifest.get("path")})
            return state
        except Exception as exc:
            state.update(
                {
                    "status": "failed",
                    "stage": state.get("stage") or "failed",
                    "error": str(exc),
                    "updated_at": _utc_now(),
                }
            )
            self._save_state(paths, state)
            emit("case_failed", {"error": str(exc), "error_type": type(exc).__name__})
            raise

    def _resolve_roots(
        self,
        spec: CaseSpec,
        *,
        state: dict[str, Any],
        paths: CasePaths,
        settings: Settings,
        charity_client: CharityCommissionClient,
        companies_house_client: CompaniesHouseClient,
        emit: EventSink,
    ) -> list[OrgRootSpec]:
        if state.get("resolved_roots"):
            return [_root_from_dict(item) for item in state["resolved_roots"]]
        roots: list[OrgRootSpec] = []
        for item in spec.inputs:
            if item.kind in {"company", "charity"}:
                value = item.value if ":" in item.value else f"{item.kind}:{item.value}"
                roots.append(parse_org_root_spec(value))
            elif item.kind == "address":
                address = normalize_address_label(item.value)
                searcher = AddressPivotSearcher(
                    settings=settings,
                    charity_client=charity_client,
                    companies_house_client=companies_house_client,
                )
                rows = searcher.find_organisations(address)
                emit(
                    "address_resolved",
                    {
                        "address": address.label,
                        "organisation_count": len(rows),
                        "registry_numbers": [str(row.get("registry_number") or "") for row in rows],
                    },
                )
                roots.extend(
                    OrgRootSpec(
                        registry_type=str(row["registry_type"]),
                        registry_number=str(row["registry_number"]),
                        suffix=int(row.get("suffix", 0)),
                    )
                    for row in rows
                )
        roots = _dedupe_roots(roots)
        if any(item.kind in {"address", "company", "charity"} for item in spec.inputs) and not roots:
            raise RuntimeError("No exact registry organisations were resolved from the case inputs.")
        state["resolved_roots"] = [_root_to_dict(root) for root in roots]
        self._save_state(paths, state)
        return roots

    def _prepare_runs(
        self,
        spec: CaseSpec,
        *,
        roots: list[OrgRootSpec],
        state: dict[str, Any],
        paths: CasePaths,
        repository: Repository,
        settings: Settings,
        charity_client: CharityCommissionClient,
        matcher: HybridMatcher,
        providers: list[Any],
        emit: EventSink,
    ) -> list[int]:
        run_map = dict(state.get("input_runs") or {})
        people = [item.value for item in spec.inputs if item.kind == "person"]
        run_keys = [f"person:{name.casefold()}" for name in people] or ["case"]
        run_ids: list[int] = []
        for index, run_key in enumerate(run_keys):
            run_id = int(run_map.get(run_key) or 0)
            if not run_id or repository.get_run(run_id) is None:
                if people:
                    person = people[index]
                    result = step1_expand_seed(
                        repository=repository,
                        charity_client=charity_client,
                        search_providers=providers,
                        matcher=matcher,
                        seed_name=person,
                        creativity_level="balanced",
                    )
                    run_id = int(result["run_id"])
                    emit("person_seed_resolved", {"person": person, "run_id": run_id})
                else:
                    run_id = repository.create_run(spec.title, "balanced")
                    emit("case_run_created", {"run_id": run_id})
                run_map[run_key] = run_id
                state["input_runs"] = run_map
                state["run_ids"] = list(dict.fromkeys([*state.get("run_ids", []), run_id]))
                self._save_state(paths, state)
            for root in roots:
                add_organisation_to_run(
                    repository=repository,
                    settings=settings,
                    charity_client=charity_client,
                    run_id=run_id,
                    registry_type=root.registry_type,
                    registry_number=root.registry_number,
                    suffix=root.suffix,
                    limit=25,
                    rerun_downstream=False,
                )
            run_ids.append(run_id)
        state["run_ids"] = list(dict.fromkeys(run_ids))
        self._save_state(paths, state)
        return state["run_ids"]

    def _expand_run(
        self,
        spec: CaseSpec,
        *,
        run_id: int,
        repository: Repository,
        settings: Settings,
        charity_client: CharityCommissionClient,
        emit: EventSink,
    ) -> dict[str, Any]:
        rounds: list[dict[str, Any]] = []
        truncated = False
        for round_number in range(1, spec.policy.max_rounds + 1):
            before = len(repository.get_run_scoped_organisations(run_id))
            result = step2_expand_connected_organisations(
                repository=repository,
                charity_client=charity_client,
                run_id=run_id,
            )
            after = len(repository.get_run_scoped_organisations(run_id))
            summary = {
                "round": round_number,
                "organisations_before": before,
                "organisations_after": after,
                "result": result,
            }
            rounds.append(summary)
            emit("organisation_round_completed", {"run_id": run_id, **summary})
            if after >= spec.policy.max_entities:
                truncated = True
                emit(
                    "entity_limit_reached",
                    {"run_id": run_id, "max_entities": spec.policy.max_entities, "actual": after},
                )
                break
            if after <= before:
                break

        documents = None
        if spec.enrichments.documents:
            documents = step2b_enrich_from_pdfs(
                repository=repository,
                settings=settings,
                charity_client=charity_client,
                run_id=run_id,
            )
            emit("document_enrichment_completed", {"run_id": run_id, "result": documents})

        if "person" in spec.policy.leaf_kinds:
            people = step3_expand_connected_people(
                repository=repository,
                settings=settings,
                charity_client=charity_client,
                run_id=run_id,
                limit=500,
            )
            ranking = list(people.get("ranking") or [])
        else:
            ranking = RankingService().rank(repository, run_id=run_id, limit=500)
            people = {"processed_organisation_count": 0, "inserted_roles": 0, "ranking": ranking}
        emit(
            "people_leaf_expansion_completed",
            {
                "run_id": run_id,
                "processed_organisation_count": people.get("processed_organisation_count", 0),
                "inserted_roles": people.get("inserted_roles", 0),
            },
        )

        sanctions = None
        if spec.enrichments.sanctions:
            sanctions = step4_ofac_screening(
                repository=repository,
                settings=settings,
                ranking=ranking,
            )
            emit("sanctions_completed", {"run_id": run_id, "result": sanctions})
        return {
            "run_id": run_id,
            "rounds": rounds,
            "truncated": truncated,
            "documents": documents,
            "people": {
                "processed_organisation_count": people.get("processed_organisation_count", 0),
                "inserted_roles": people.get("inserted_roles", 0),
            },
            "sanctions": sanctions,
        }

    def _negative_news(
        self,
        spec: CaseSpec,
        *,
        paths: CasePaths,
        settings: Settings,
        repository: Repository,
        emit: EventSink,
    ) -> dict[str, Any] | None:
        if not spec.enrichments.negative_news:
            return None
        with _temporary_environment(
            DATABASE_PATH=str(paths.database),
            NEGATIVE_NEWS_DB_PATH=str(paths.negative_news_database),
        ):
            result = run_negative_news_cluster_batch(
                settings,
                repository,
                offset=0,
                limit=5,
                broad_pages=1,
                org_pages=1,
                max_articles_per_cluster=10,
            )
        emit("negative_news_completed", {"meta": result.get("meta", {})})
        return result.get("meta") if isinstance(result, dict) else {"enabled": True}

    @staticmethod
    def _load_state(paths: CasePaths) -> dict[str, Any]:
        if not paths.state.exists():
            return {}
        return json.loads(paths.state.read_text(encoding="utf-8"))

    @staticmethod
    def _save_state(paths: CasePaths, state: dict[str, Any]) -> None:
        state["updated_at"] = _utc_now()
        paths.root.mkdir(parents=True, exist_ok=True)
        temp_path = paths.state.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps(state, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        temp_path.replace(paths.state)


def _source_coverage(settings: Settings) -> dict[str, bool]:
    return {
        "companies_house": bool(settings.companies_house_api_key),
        "charity_commission": bool(settings.charity_api_key),
        "charity_address_search": bool(settings.charity_api_key and settings.serper_api_key),
        "serper": bool(settings.serper_api_key),
    }


def _root_from_dict(payload: dict[str, Any]) -> OrgRootSpec:
    return OrgRootSpec(
        registry_type=str(payload["registry_type"]),
        registry_number=str(payload["registry_number"]),
        suffix=int(payload.get("suffix", 0)),
    )


def _root_to_dict(root: OrgRootSpec) -> dict[str, Any]:
    return {
        "registry_type": root.registry_type,
        "registry_number": root.registry_number,
        "suffix": root.suffix,
    }


def _dedupe_roots(roots: list[OrgRootSpec]) -> list[OrgRootSpec]:
    deduped: list[OrgRootSpec] = []
    seen: set[tuple[str, str, int]] = set()
    for root in roots:
        key = (root.registry_type, root.registry_number, root.suffix)
        if key not in seen:
            deduped.append(root)
            seen.add(key)
    return deduped


@contextmanager
def _temporary_environment(**values: str) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def _case_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    current_pid = os.getpid()
    descriptor: int | None = None
    for _attempt in range(2):
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            owner_pid = _lock_owner(path)
            if owner_pid and _process_exists(owner_pid):
                raise RuntimeError(f"Case is already running in process {owner_pid}.")
            path.unlink(missing_ok=True)
    if descriptor is None:
        raise RuntimeError(f"Could not acquire case lock: {path}")
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write(str(current_pid))
        yield
    finally:
        if _lock_owner(path) == current_pid:
            path.unlink(missing_ok=True)


def _lock_owner(path: Path) -> int:
    try:
        return int(path.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return 0


def _process_exists(process_id: int) -> bool:
    if process_id <= 0:
        return False
    if os.name == "nt":
        return _windows_process_exists(process_id)
    try:
        os.kill(process_id, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _windows_process_exists(process_id: int) -> bool:
    import ctypes

    process_query_limited_information = 0x1000
    error_access_denied = 5
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
    open_process.restype = ctypes.c_void_p
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int

    handle = open_process(process_query_limited_information, False, process_id)
    if not handle:
        return ctypes.get_last_error() == error_access_denied
    close_handle(handle)
    return True


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

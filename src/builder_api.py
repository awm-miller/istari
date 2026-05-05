from __future__ import annotations

import json
import os
import smtplib
import threading
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import request

from flask import Flask, abort, jsonify, make_response, request as flask_request, send_from_directory

from src.charity_commission.client import CharityCommissionClient
from src.companies_house.client import CompaniesHouseClient
from src.config import Settings, load_settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.negative_news import run_negative_news_cluster_batch
from src.resolution.matcher import HybridMatcher
from src.search.provider import build_search_providers
from src.storage.repository import Repository
from src.tree_input_normalizer import normalize_builder_payload
from src.tree_builder import (
    DefaultTreePipelineRunner,
    execute_tree_build,
    normalize_tree_build_request,
)
from src.tree_graph_artifacts import build_generated_graph_bundle, list_generated_graphs
from src.tree_graph_artifacts import delete_generated_graph, generated_graph_file_path, set_active_graph_version


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOB_DIR = Path(os.getenv("TREE_BUILDER_JOB_DIR", PROJECT_ROOT / "data" / "tree_jobs"))
GENERATED_GRAPH_DIR = Path(os.getenv("TREE_BUILDER_GRAPH_DIR", PROJECT_ROOT / "data" / "generated_graphs"))
EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("TREE_BUILDER_WORKERS", "1")))
LOCK = threading.Lock()


def create_app() -> Flask:
    app = Flask(__name__)

    @app.after_request
    def add_cors_headers(response):
        origin = flask_request.headers.get("Origin", "")
        allowed_origin = _allowed_cors_origin(origin)
        if allowed_origin:
            response.headers["Access-Control-Allow-Origin"] = allowed_origin
            response.headers["Vary"] = "Origin"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
            response.headers["Access-Control-Allow-Methods"] = "DELETE, GET, POST, OPTIONS"
        return response

    @app.route("/health", methods=["GET", "OPTIONS"])
    def health():
        if flask_request.method == "OPTIONS":
            return _empty_response()
        return jsonify({"ok": True})

    @app.route("/api/key-tests/gemini", methods=["POST", "OPTIONS"])
    def test_gemini_key():
        if flask_request.method == "OPTIONS":
            return _empty_response()
        payload = _json_payload()
        settings = _settings_with_credentials(payload)
        api_key = settings.gemini_api_key
        if not api_key:
            return jsonify({"ok": False, "error": "GEMINI_API_KEY is not configured."}), 400
        try:
            client = GeminiClient(
                api_key=api_key,
                cache_dir=settings.cache_dir / "gemini_key_tests",
                timeout_seconds=20,
                attempts=1,
            )
            response = client.generate(
                model=str(payload.get("gemini_model") or settings.gemini_resolution_model),
                prompt='Return JSON only: {"ok": true}',
                temperature=0.0,
            )
            text = extract_gemini_text(response)
            return jsonify({"ok": True, "message": f"Gemini responded ({len(text)} chars)."})
        except Exception as exc:
            return jsonify({"ok": False, "error": _safe_error(exc)}), 400

    @app.route("/api/key-tests/serper", methods=["POST", "OPTIONS"])
    def test_serper_key():
        if flask_request.method == "OPTIONS":
            return _empty_response()
        payload = _json_payload()
        settings = _settings_with_credentials(payload)
        api_key = settings.serper_api_key
        if not api_key:
            return jsonify({"ok": False, "error": "SERPER_API_KEY is not configured."}), 400
        try:
            result_count = _test_serper(settings)
            return jsonify({"ok": True, "message": f"Serper responded with {result_count} result(s)."})
        except Exception as exc:
            return jsonify({"ok": False, "error": _safe_error(exc)}), 400

    @app.route("/api/tree-jobs", methods=["POST", "OPTIONS"])
    def create_tree_job():
        if flask_request.method == "OPTIONS":
            return _empty_response()
        payload = _json_payload()
        try:
            settings = _settings_with_credentials(payload)
            normalized_payload = normalize_builder_payload(
                payload,
                charity_client=CharityCommissionClient(settings),
                companies_house_client=CompaniesHouseClient(settings),
                gemini_client=_input_normalizer_gemini(settings),
                gemini_model=settings.gemini_resolution_model,
            )
            tree_request = normalize_tree_build_request(normalized_payload)
        except Exception as exc:
            return jsonify({"ok": False, "error": _safe_error(exc)}), 400

        job_id = uuid.uuid4().hex
        _write_job(
            job_id,
            {
                "id": job_id,
                "status": "queued",
                "request": _sanitize_tree_request(normalized_payload),
                "result": None,
                "error": "",
            },
        )
        EXECUTOR.submit(_run_tree_job, job_id, normalized_payload)
        return jsonify({"ok": True, "job": _read_job(job_id)}), 202

    @app.route("/api/tree-jobs/<job_id>", methods=["GET", "OPTIONS"])
    def get_tree_job(job_id: str):
        if flask_request.method == "OPTIONS":
            return _empty_response()
        job = _read_job(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Job not found."}), 404
        return jsonify({"ok": True, "job": job})

    @app.route("/api/generated-graphs", methods=["GET", "OPTIONS"])
    def generated_graphs():
        if flask_request.method == "OPTIONS":
            return _empty_response()
        return jsonify({"ok": True, "graphs": list_generated_graphs(GENERATED_GRAPH_DIR)})

    @app.route("/api/generated-graphs/<graph_id>/active", methods=["POST", "OPTIONS"])
    def activate_generated_graph_version(graph_id: str):
        if flask_request.method == "OPTIONS":
            return _empty_response()
        payload = _json_payload()
        try:
            graph = set_active_graph_version(GENERATED_GRAPH_DIR, graph_id, str(payload.get("version") or ""))
            return jsonify({"ok": True, "graph": graph})
        except Exception as exc:
            return jsonify({"ok": False, "error": _safe_error(exc)}), 400

    @app.route("/api/generated-graphs/<graph_id>", methods=["DELETE", "OPTIONS"])
    def delete_generated_graph_route(graph_id: str):
        if flask_request.method == "OPTIONS":
            return _empty_response()
        try:
            delete_generated_graph(GENERATED_GRAPH_DIR, graph_id)
            return jsonify({"ok": True})
        except Exception as exc:
            return jsonify({"ok": False, "error": _safe_error(exc)}), 400

    @app.route("/api/generated-graphs/<graph_id>/versions/<version>", methods=["DELETE", "OPTIONS"])
    def delete_generated_graph_version_route(graph_id: str, version: str):
        if flask_request.method == "OPTIONS":
            return _empty_response()
        try:
            graph = delete_generated_graph(GENERATED_GRAPH_DIR, graph_id, version)
            return jsonify({"ok": True, "graph": graph})
        except Exception as exc:
            return jsonify({"ok": False, "error": _safe_error(exc)}), 400

    @app.route("/generated-graphs/<graph_id>/", methods=["GET"])
    def generated_graph_index(graph_id: str):
        return _send_generated_graph_file(graph_id, "index.html")

    @app.route("/generated-graphs/<graph_id>/<path:filename>", methods=["GET"])
    def generated_graph_file(graph_id: str, filename: str):
        parts = filename.split("/", 2)
        if len(parts) >= 2 and parts[0] == "versions":
            nested_filename = parts[2] if len(parts) == 3 and parts[2] else "index.html"
            return _send_generated_graph_file(graph_id, nested_filename, version=parts[1])
        return _send_generated_graph_file(graph_id, filename)

    @app.route("/generated-graphs/<graph_id>/versions/<version>/", methods=["GET"])
    def generated_graph_version_index(graph_id: str, version: str):
        return _send_generated_graph_file(graph_id, "index.html", version=version)

    @app.route("/generated-graphs/<graph_id>/versions/<version>/<path:filename>", methods=["GET"])
    def generated_graph_version_file(graph_id: str, version: str, filename: str):
        return _send_generated_graph_file(graph_id, filename, version=version)

    return app


def _run_tree_job(job_id: str, payload: dict[str, Any]) -> None:
    try:
        _update_job(job_id, status="running")
        tree_request = normalize_tree_build_request(payload)
        settings = _settings_with_credentials(payload)
        if not settings.gemini_api_key:
            raise ValueError("GEMINI_API_KEY must be configured before tree builds can run.")
        if not settings.serper_api_key:
            raise ValueError("SERPER_API_KEY must be configured before tree builds can run.")

        repository = Repository(
            settings.database_path,
            settings.project_root / "src" / "storage" / "schema.sql",
        )
        repository.init_db()
        charity_client = CharityCommissionClient(settings)
        search_providers = build_search_providers(settings, include_web_dork=False)
        matcher = HybridMatcher(settings)
        runner = DefaultTreePipelineRunner(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            search_providers=search_providers,
            matcher=matcher,
        )
        result = execute_tree_build(tree_request, runner)
        safe_result = _sanitize_result(result)
        run_ids = _result_run_ids(safe_result)
        negative_news_result = _run_negative_news_if_requested(settings, repository, tree_request)
        if negative_news_result:
            safe_result["negative_news"] = negative_news_result
        if run_ids:
            manifest = build_generated_graph_bundle(
                run_ids=run_ids,
                output_root=GENERATED_GRAPH_DIR,
                graph_id=_requested_graph_id(payload, job_id, tree_request),
                title=_requested_graph_title(payload, tree_request),
                version=str(payload.get("graph_version") or "").strip() or None,
                overwrite=str(payload.get("save_mode") or "") == "overwrite_version",
                metadata={"negative_news": negative_news_result} if negative_news_result else None,
            )
            safe_result["graph"] = manifest
        _update_job(job_id, status="completed", result=safe_result)
        _send_completion_email(tree_request.notify_email, job_id, safe_result, success=True)
    except Exception as exc:
        error = _safe_error(exc)
        _update_job(job_id, status="failed", error=error, traceback=traceback.format_exc(limit=8))
        try:
            tree_request = normalize_tree_build_request(payload)
            _send_completion_email(tree_request.notify_email, job_id, {"error": error}, success=False)
        except Exception:
            pass


def _settings_with_credentials(payload: dict[str, Any]) -> Settings:
    settings = load_settings()
    credentials = payload.get("credentials") if isinstance(payload.get("credentials"), dict) else {}
    return replace(
        settings,
        gemini_api_key=_credential_value(credentials, "gemini_api_key") or settings.gemini_api_key,
        serper_api_key=_credential_value(credentials, "serper_api_key") or settings.serper_api_key,
        charity_api_key=_credential_value(credentials, "charity_api_key") or settings.charity_api_key,
        companies_house_api_key=_credential_value(credentials, "companies_house_api_key") or settings.companies_house_api_key,
    )


def _input_normalizer_gemini(settings: Settings) -> GeminiClient | None:
    if not settings.gemini_api_key:
        return None
    return GeminiClient(
        api_key=settings.gemini_api_key,
        cache_dir=settings.cache_dir / "builder_input_normalizer",
        timeout_seconds=20,
        attempts=1,
    )


def _credential_value(credentials: dict[str, Any], key: str) -> str | None:
    value = " ".join(str(credentials.get(key) or "").split()).strip()
    return value or None


def _test_serper(settings: Settings) -> int:
    payload = json.dumps({"q": "test", "num": 1}).encode("utf-8")
    req = request.Request(
        url=f"{settings.serper_base_url}/search",
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-API-KEY": settings.serper_api_key or "",
        },
    )
    with request.urlopen(req, timeout=20) as response:
        data = json.loads(response.read().decode("utf-8"))
    return len(data.get("organic") or [])


def _run_negative_news_if_requested(settings: Settings, repository: Repository, tree_request: Any) -> dict[str, Any] | None:
    if not getattr(tree_request, "run_negative_news", False):
        return None
    limit = int(os.getenv("TREE_BUILDER_NEGATIVE_NEWS_LIMIT", "5"))
    result = run_negative_news_cluster_batch(
        settings,
        repository,
        offset=0,
        limit=limit,
        broad_pages=int(os.getenv("TREE_BUILDER_NEGATIVE_NEWS_BROAD_PAGES", "1")),
        org_pages=int(os.getenv("TREE_BUILDER_NEGATIVE_NEWS_ORG_PAGES", "1")),
        max_articles_per_cluster=int(os.getenv("TREE_BUILDER_NEGATIVE_NEWS_MAX_ARTICLES", "10")),
    )
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    return {
        "enabled": True,
        "cluster_limit": limit,
        "batch_run_id": meta.get("batch_run_id"),
        "source_database_key": meta.get("source_database_key"),
        "pending_cluster_count": meta.get("pending_cluster_count"),
        "historically_screened_count": meta.get("historically_screened_count"),
        "negative_news_db_path": meta.get("negative_news_db_path"),
    }


def _send_completion_email(to_address: str, job_id: str, result: dict[str, Any], *, success: bool) -> None:
    if not to_address:
        return
    host = os.getenv("SMTP_HOST", "").strip()
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    from_address = os.getenv("SMTP_FROM", username).strip()
    if not host or not username or not password or not from_address:
        return

    message = EmailMessage()
    message["Subject"] = f"Istari graph {'ready' if success else 'failed'}: {job_id}"
    message["From"] = from_address
    message["To"] = to_address
    status = "ready" if success else "failed"
    run_ids = ", ".join(str(value) for value in result.get("run_ids", []) if value)
    if not run_ids and result.get("run_id"):
        run_ids = str(result["run_id"])
    graph = result.get("graph") if isinstance(result.get("graph"), dict) else {}
    message.set_content(
        "\n".join(
            [
                f"Your Istari graph job {job_id} is {status}.",
                f"Run IDs: {run_ids or 'n/a'}",
                f"Graph path: {graph.get('path', 'n/a')}",
                "",
                "Open the Builder page to add it to the graph list.",
            ]
        )
    )

    port = int(os.getenv("SMTP_PORT", "587"))
    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)


def _json_payload() -> dict[str, Any]:
    data = flask_request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def _allowed_cors_origin(origin: str) -> str:
    allowed = [value.strip() for value in os.getenv("TREE_BUILDER_ALLOWED_ORIGINS", "").split(",") if value.strip()]
    if not allowed:
        return origin if origin.startswith(("http://localhost:", "http://127.0.0.1:")) else ""
    if "*" in allowed:
        return origin
    return origin if origin in allowed else ""


def _empty_response():
    return make_response("", 204)


def _job_path(job_id: str) -> Path:
    safe_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})
    return JOB_DIR / f"{safe_id}.json"


def _read_job(job_id: str) -> dict[str, Any] | None:
    path = _job_path(job_id)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_job(job_id: str, data: dict[str, Any]) -> None:
    JOB_DIR.mkdir(parents=True, exist_ok=True)
    path = _job_path(job_id)
    temp_path = path.with_suffix(".json.tmp")
    with LOCK:
        temp_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(path)


def _update_job(job_id: str, **updates: Any) -> None:
    job = _read_job(job_id) or {"id": job_id}
    job.update(updates)
    _write_job(job_id, job)


def _sanitize_tree_request(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "credentials"}


def _sanitize_result(result: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(result, default=str))


def _result_run_ids(result: dict[str, Any]) -> list[int]:
    if isinstance(result.get("run_ids"), list):
        return [int(value) for value in result["run_ids"] if str(value).strip()]
    if result.get("run_id"):
        return [int(result["run_id"])]
    return []


def _requested_graph_id(payload: dict[str, Any], job_id: str, tree_request: Any) -> str:
    explicit = " ".join(str(payload.get("graph_id") or "").split()).strip()
    if explicit:
        return explicit
    if tree_request.seed_name:
        return tree_request.seed_name
    if tree_request.seed_names:
        return tree_request.seed_names[0]
    return job_id


def _requested_graph_title(payload: dict[str, Any], request: Any) -> str:
    explicit = " ".join(str(payload.get("graph_title") or "").split()).strip()
    if explicit:
        return explicit
    if request.seed_name:
        return f"Istari: {request.seed_name}"
    if request.seed_names:
        return f"Istari: {', '.join(request.seed_names[:3])}"
    if request.roots:
        return f"Istari: {', '.join(root.registry_number for root in request.roots[:3])}"
    return "Istari Generated Graph"


def _send_generated_graph_file(graph_id: str, filename: str, *, version: str | None = None):
    try:
        path = generated_graph_file_path(GENERATED_GRAPH_DIR, graph_id, filename, version)
    except Exception:
        abort(404)
    if not path.is_file():
        abort(404)
    return send_from_directory(path.parent, path.name)


def _safe_error(exc: Exception) -> str:
    text = str(exc)
    for key in ("GEMINI_API_KEY", "SERPER_API_KEY", "COMPANIES_HOUSE_API_KEY", "CHARITY_COMMISSION_API_KEY"):
        text = text.replace(os.getenv(key, ""), "[redacted]") if os.getenv(key) else text
    return text


app = create_app()


if __name__ == "__main__":
    app.run(host=os.getenv("TREE_BUILDER_HOST", "127.0.0.1"), port=int(os.getenv("TREE_BUILDER_PORT", "8000")))

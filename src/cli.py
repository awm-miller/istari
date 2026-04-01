from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from src.charity_commission.client import CharityCommissionClient
from src.companies_house.client import CompaniesHouseClient
from src.config import load_settings
from src.gemini_api import GeminiClient, extract_gemini_text
from src.graph_export import export_network_payload
from src.pipeline import (
    add_organisation_to_run,
    run_name_pipeline,
    run_seed_batch_pipeline,
    step1_expand_seed,
    step2_expand_connected_organisations,
    step2b_enrich_from_pdfs,
    step3_expand_connected_people,
    step4_ofac_screening,
)
from src.ranking import rank_people
from src.resolution.matcher import HybridMatcher
from src.search.provider import build_search_providers
from src.storage.repository import Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Registry-only MVP pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create the SQLite schema.")

    step1_parser = subparsers.add_parser(
        "step1-seed",
        help="MVP step 1: expand one seed through Charity Commission and Companies House.",
    )
    step1_parser.add_argument("name")
    step1_parser.add_argument(
        "--creativity",
        choices=["strict", "balanced", "exploratory"],
        default="balanced",
    )

    step2_parser = subparsers.add_parser(
        "step2-orgs",
        help="MVP step 2: expand connected companies and charities for a run.",
    )
    step2_parser.add_argument("run_id", type=int)

    add_org_parser = subparsers.add_parser(
        "add-org",
        help="Add a specific company or charity into an existing run and rerun downstream steps.",
    )
    add_org_parser.add_argument("run_id", type=int)
    add_org_parser.add_argument(
        "--registry-type",
        choices=["charity", "company"],
        required=True,
    )
    add_org_parser.add_argument("--registry-number", required=True)
    add_org_parser.add_argument("--suffix", type=int, default=0)
    add_org_parser.add_argument("--limit", type=int, default=25)
    add_org_parser.add_argument(
        "--link-only",
        action="store_true",
        help="Only attach the organisation to the run without rerunning downstream steps.",
    )

    step3_parser = subparsers.add_parser(
        "step3-people",
        help="MVP step 3: expand people for the scoped organisations in a run.",
    )
    step3_parser.add_argument("run_id", type=int)
    step3_parser.add_argument("--limit", type=int, default=25)

    pdf_parser = subparsers.add_parser(
        "pdf-enrich",
        help="Enrich a run from scoped charity/company PDFs using OpenDataLoader and Gemini.",
    )
    pdf_parser.add_argument("run_id", type=int)

    step4_parser = subparsers.add_parser(
        "step4-ofac",
        help="MVP step 4: screen ranked people against sanctions lists.",
    )
    step4_parser.add_argument("run_id", type=int)
    step4_parser.add_argument("--limit", type=int, default=25)

    run_parser = subparsers.add_parser(
        "run-name",
        help="Run the full 3-step registry-only MVP flow for one seed name.",
    )
    run_parser.add_argument("name")
    run_parser.add_argument(
        "--creativity",
        choices=["strict", "balanced", "exploratory"],
        default="balanced",
    )
    run_parser.add_argument("--limit", type=int, default=25)

    run_seeds_parser = subparsers.add_parser(
        "run-seeds",
        help="Run the registry-only MVP flow per seed and aggregate overlap across runs.",
    )
    run_seeds_parser.add_argument("names", nargs="*", help="Seed names.")
    run_seeds_parser.add_argument("--seed-file", help="Optional text file with one seed per line.")
    run_seeds_parser.add_argument(
        "--creativity",
        choices=["strict", "balanced", "exploratory"],
        default="balanced",
    )
    run_seeds_parser.add_argument("--limit", type=int, default=25)
    run_seeds_parser.add_argument("--overlap-limit", type=int, default=25)

    rank_parser = subparsers.add_parser("rank", help="Rank people by connected organisations.")
    rank_parser.add_argument("--limit", type=int, default=25)

    health_parser = subparsers.add_parser(
        "healthcheck",
        help="Run integration checks for keys and local tooling.",
    )
    health_parser.add_argument(
        "--with-ai",
        action="store_true",
        help="Also test a live Gemini API call (incurs tiny charge).",
    )

    web_parser = subparsers.add_parser("web-ui", help="Run the MVP Flask UI.")
    web_parser.add_argument("--host", default="127.0.0.1")
    web_parser.add_argument("--port", type=int, default=5000)
    web_parser.add_argument("--debug", action="store_true")

    export_parser = subparsers.add_parser(
        "export-network",
        help="Export one or more run IDs as a graph JSON payload.",
    )
    export_parser.add_argument(
        "--run-id",
        dest="run_ids",
        type=int,
        action="append",
        required=True,
        help="Run ID to include. Repeat this flag for multiple runs.",
    )
    export_parser.add_argument(
        "--out",
        default="netlify_graph_viewer/graph-data.json",
        help="Path to output JSON payload.",
    )

    return parser


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(errors="replace")
        except Exception:
            pass
    if hasattr(sys.stderr, "reconfigure"):
        try:
            sys.stderr.reconfigure(errors="replace")
        except Exception:
            pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    parser = build_parser()
    args = parser.parse_args()

    settings = load_settings()
    if args.command in {"init-db", "step1-seed", "step2-orgs", "add-org", "pdf-enrich", "step3-people", "run-name", "run-seeds"}:
        _startup_stop_other_pipeline_processes()

    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()

    if args.command == "init-db":
        print(f"Initialized database at {settings.database_path}")
        return

    charity_client = CharityCommissionClient(settings)
    matcher = HybridMatcher(settings)

    if args.command == "step1-seed":
        result = step1_expand_seed(
            repository=repository,
            charity_client=charity_client,
            search_providers=build_search_providers(settings, include_web_dork=False),
            matcher=matcher,
            seed_name=args.name,
            creativity_level=args.creativity,
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "step2-orgs":
        result = step2_expand_connected_organisations(
            repository=repository,
            charity_client=charity_client,
            run_id=int(args.run_id),
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "add-org":
        result = add_organisation_to_run(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            run_id=int(args.run_id),
            registry_type=str(args.registry_type),
            registry_number=str(args.registry_number),
            suffix=int(args.suffix),
            limit=int(args.limit),
            rerun_downstream=not bool(args.link_only),
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "pdf-enrich":
        result = step2b_enrich_from_pdfs(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            run_id=int(args.run_id),
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "step3-people":
        result = step3_expand_connected_people(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            run_id=int(args.run_id),
            limit=int(args.limit),
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "step4-ofac":
        from dataclasses import asdict

        ranking = [
            asdict(entry)
            for entry in rank_people(repository, limit=int(args.limit), run_id=int(args.run_id))
        ]
        result = step4_ofac_screening(
            repository=repository,
            settings=settings,
            ranking=ranking,
        )
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "run-name":
        result = run_name_pipeline(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            search_providers=build_search_providers(settings, include_web_dork=False),
            matcher=matcher,
            seed_name=args.name,
            creativity_level=args.creativity,
            limit=int(args.limit),
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "run-seeds":
        seed_names = _collect_seed_names(getattr(args, "names", []), getattr(args, "seed_file", None))
        if not seed_names:
            parser.error("run-seeds requires at least one seed name (arg or --seed-file).")
        result = run_seed_batch_pipeline(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            search_providers=build_search_providers(settings, include_web_dork=False),
            matcher=matcher,
            seed_names=seed_names,
            creativity_level=args.creativity,
            limit=int(args.limit),
            overlap_limit=int(args.overlap_limit),
        )
        print(json.dumps(result, indent=2))
        return

    if args.command == "rank":
        print(json.dumps([entry.__dict__ for entry in rank_people(repository, limit=args.limit)], indent=2))
        return

    if args.command == "healthcheck":
        checks = run_healthcheck(
            settings=settings,
            charity_client=charity_client,
            with_ai=bool(args.with_ai),
        )
        print(json.dumps(checks, indent=2))
        return

    if args.command == "web-ui":
        from src.web import create_app

        app = create_app()
        app.run(host=args.host, port=int(args.port), debug=bool(args.debug))
        return

    if args.command == "export-network":
        payload = export_network_payload(repository, run_ids=list(args.run_ids))
        output_path = Path(args.out)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "output_path": str(output_path),
                    "run_ids": payload.get("run_ids", []),
                    "node_count": len(payload.get("nodes", [])),
                    "edge_count": len(payload.get("edges", [])),
                },
                indent=2,
            )
        )
        return

    parser.error(f"Unknown command: {args.command}")


def run_healthcheck(
    *,
    settings: object,
    charity_client: CharityCommissionClient,
    with_ai: bool,
) -> dict[str, object]:
    def ok_result(ok: bool, detail: str) -> dict[str, object]:
        return {"ok": ok, "detail": detail}

    checks: dict[str, object] = {}

    java_path = shutil.which("java")
    if not java_path:
        checks["java"] = ok_result(False, "java not found on PATH")
    else:
        try:
            proc = subprocess.run(
                ["java", "-version"],
                capture_output=True,
                text=True,
                check=False,
            )
            checks["java"] = ok_result(
                proc.returncode == 0,
                (proc.stderr or proc.stdout).splitlines()[0] if (proc.stderr or proc.stdout) else "java executed",
            )
        except Exception as exc:
            checks["java"] = ok_result(False, f"java check failed: {exc}")

    try:
        sample = charity_client.search_charities_by_name("FINSBURY PARK MOSQUE")
        checks["charity_commission_api"] = ok_result(
            True,
            f"search ok; returned {len(sample)} rows",
        )
    except Exception as exc:
        checks["charity_commission_api"] = ok_result(False, str(exc))

    try:
        ch_client = CompaniesHouseClient(settings)
        sample = ch_client.search_officers("Mohamed Kozbar", items_per_page=1)
        checks["companies_house_api"] = ok_result(
            True,
            f"search ok; returned {len(sample.get('items', []))} rows",
        )
    except Exception as exc:
        checks["companies_house_api"] = ok_result(False, str(exc))

    if with_ai:
        gemini_key = getattr(settings, "gemini_api_key", None)
        if not gemini_key:
            checks["gemini_api"] = ok_result(False, "GEMINI_API_KEY not configured")
        else:
            try:
                gemini = GeminiClient(
                    api_key=gemini_key,
                    cache_dir=settings.cache_dir / "gemini_healthcheck",
                )
                response = gemini.generate(
                    model=settings.gemini_resolution_model,
                    prompt='Return JSON only: {"ok": true}',
                    temperature=0.0,
                )
                text = extract_gemini_text(response)
                checks["gemini_api"] = ok_result(True, f"response received ({len(text)} chars)")
            except Exception as exc:
                checks["gemini_api"] = ok_result(False, str(exc))
    else:
        checks["gemini_api"] = ok_result(
            bool(getattr(settings, "gemini_api_key", None)),
            "key presence checked only (use --with-ai for live test)",
        )

    return checks


def _collect_seed_names(raw_names: list[str], seed_file: str | None) -> list[str]:
    values: list[str] = []
    for item in raw_names:
        value = " ".join(str(item).split()).strip()
        if value:
            values.append(value)

    if seed_file:
        for line in open(seed_file, encoding="utf-8", errors="replace").read().splitlines():
            value = " ".join(line.split()).strip()
            if value:
                values.append(value)

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _startup_stop_other_pipeline_processes() -> None:
    logger = logging.getLogger("istari.startup")
    if not sys.platform.startswith("win"):
        return
    current_pid = os.getpid()
    powershell_script = f"""
$targets = Get-CimInstance Win32_Process |
  Where-Object {{
    $_.ProcessId -ne {current_pid} -and
    $_.CommandLine -and
    $_.CommandLine -match 'src\\.cli' -and
    $_.CommandLine -match '(run-name|run-seeds|step1-seed|step2-orgs|step3-people|add-org)'
  }}
$killed = @()
foreach ($p in $targets) {{
  try {{
    Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
    $killed += [PSCustomObject]@{{ pid = $p.ProcessId; command = $p.CommandLine }}
  }} catch {{
  }}
}}
$killed | ConvertTo-Json -Compress
"""
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", powershell_script],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            if detail:
                logger.warning("Startup process cleanup failed: %s", detail)
            return
        raw = (proc.stdout or "").strip()
        if not raw:
            return
        payload = json.loads(raw)
        if isinstance(payload, dict):
            payload = [payload]
        if isinstance(payload, list) and payload:
            pids = [str(item.get("pid")) for item in payload if item.get("pid") is not None]
            if pids:
                logger.info(
                    "Startup stopped stale pipeline process(es): %s",
                    ", ".join(pids),
                )
    except Exception as exc:
        logger.warning("Startup process cleanup skipped: %s", exc)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger("istari.cli").warning("Interrupted by user (Ctrl+C).")
        raise SystemExit(130)
    except Exception:
        logging.getLogger("istari.cli").exception("Fatal CLI error")
        raise SystemExit(1)

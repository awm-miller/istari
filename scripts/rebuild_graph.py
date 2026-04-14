"""Rebuild graph HTML from graph modules, then copy to netlify."""
from dataclasses import asdict
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.config import load_settings
from src.graph.adverse_media import (
    annotate_graph_with_adverse_media,
    resolve_negative_news_db_path,
)
from src.graph.address_coordinates import (
    build_address_coordinate_index,
    default_address_coordinate_cache_path,
)
from src.graph.build import consolidate_multi_run
from src.graph.egypt_judgments import annotate_graph_with_egypt_judgments
from src.graph.unresolved_pdf_orgs import build_unresolved_pdf_org_overlay
from src.graph.render import render_html
from src.mapping_low_confidence import (
    build_low_confidence_overlay,
    rebuild_overlay_mapping_db,
)
from src.ranking import rank_people
from src.services.mvp_pipeline import hydrate_cached_sanctions, step4_ofac_screening
from src.storage.repository import Repository

settings = load_settings()
repository = Repository(
    settings.database_path,
    settings.project_root / "src" / "storage" / "schema.sql",
)
repository.init_db()


def refresh_sanctions_for_runs(run_ids: list[int], *, ranking_limit: int = 5000) -> None:
    print(f"Refreshing sanctions for runs {run_ids}...", flush=True)
    total_hits = 0
    total_refreshed = 0
    total_cached = 0
    for run_id in run_ids:
        ranking = [asdict(entry) for entry in rank_people(repository, limit=ranking_limit, run_id=run_id)]
        if not ranking:
            continue
        cache_result = hydrate_cached_sanctions(repository, ranking)
        pending_ranking = list(cache_result.get("pending_ranking") or [])
        cached_count = int(cache_result.get("cached_count") or 0)
        cached_hit_count = int(cache_result.get("cached_hit_count") or 0)
        refreshed_count = 0
        refreshed_hit_count = 0
        if pending_ranking:
            result = step4_ofac_screening(
                repository=repository,
                settings=settings,
                ranking=pending_ranking,
                enable_remote_sources=False,
            )
            refreshed_count = int(result.get("screened_count") or 0)
            refreshed_hit_count = len(result.get("sanctions_hits") or {})
        hit_count = cached_hit_count + refreshed_hit_count
        total_hits += hit_count
        total_refreshed += refreshed_count
        total_cached += cached_count
        print(
            f"  run {run_id}: reused {cached_count}, screened {refreshed_count}, {hit_count} sanction hit(s)",
            flush=True,
        )
    print(
        f"Sanctions refresh complete: reused {total_cached}, screened {total_refreshed} across {len(run_ids)} runs, "
        f"found {total_hits} hit(s)",
        flush=True,
    )

run_ids = repository.get_latest_unique_run_ids()
if not run_ids:
    raise SystemExit("No runs found.")

refresh_sanctions_for_runs(run_ids)

print(f"Consolidating runs {run_ids}...", flush=True)
data = consolidate_multi_run(run_ids)
data = annotate_graph_with_egypt_judgments(data, settings=settings)
negative_news_db_path = resolve_negative_news_db_path(settings)
data = annotate_graph_with_adverse_media(data, settings=settings, database_path=negative_news_db_path)
print(f"  {len(data['nodes'])} nodes, {len(data['edges'])} edges", flush=True)

open_letters_data = {"nodes": [], "edges": [], "summary": {"run_key": str(data.get("run_id", ""))}}
low_confidence_nodes_data = {"nodes": [], "edges": [], "summary": {"run_ids": run_ids}}
address_coordinates = {"coordinates": [], "summary": {}}
mapping_db_path = rebuild_overlay_mapping_db(settings.project_root)
if mapping_db_path.exists():
    try:
        open_letters_data = build_low_confidence_overlay(
            main_data=data,
            database_path=mapping_db_path,
            run_key=str(data.get("run_id", "")),
            include_unmatched=True,
            include_generated_links=True,
            enable_ai_org_matching=True,
            settings=settings,
        )
        print(
            "Loaded open-letters overlay "
            f"({len(open_letters_data.get('nodes') or [])} nodes, "
            f"{len(open_letters_data.get('edges') or [])} edges)",
            flush=True,
        )
    except Exception as error:
        print(f"Warning: failed to build open-letters overlay: {error}", flush=True)

try:
    low_confidence_nodes_data = build_unresolved_pdf_org_overlay(
        repository=repository,
        run_ids=run_ids,
        main_data=data,
    )
    print(
        "Loaded low-confidence nodes "
        f"({len(low_confidence_nodes_data.get('nodes') or [])} nodes, "
        f"{len(low_confidence_nodes_data.get('edges') or [])} edges)",
        flush=True,
    )
except Exception as error:
    print(f"Warning: failed to build low-confidence nodes: {error}", flush=True)

address_overlay_data = {
    "nodes": [
        *(open_letters_data.get("nodes") or []),
        *(low_confidence_nodes_data.get("nodes") or []),
    ],
    "edges": [
        *(open_letters_data.get("edges") or []),
        *(low_confidence_nodes_data.get("edges") or []),
    ],
    "summary": {},
}

try:
    address_coordinates = build_address_coordinate_index(
        main_data=data,
        low_confidence_data=address_overlay_data,
        cache_path=default_address_coordinate_cache_path(settings.project_root),
        user_agent=settings.user_agent,
    )
    print(
        "Loaded address coordinate index "
        f"({len(address_coordinates.get('coordinates') or [])} coordinates)",
        flush=True,
    )
except Exception as error:
    print(f"Warning: failed to build address coordinate index: {error}", flush=True)

html = render_html(data)
print(f"Rendered HTML ({len(html)} bytes)", flush=True)

out = pathlib.Path("output/latest_graph.html")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(html, encoding="utf-8")
print(f"Wrote {out} ({len(html)} bytes)", flush=True)

graph_json = json.dumps(data, ensure_ascii=False, indent=2)
graph_out = pathlib.Path("output/graph-data.json")
graph_out.write_text(graph_json, encoding="utf-8")
print(f"Wrote {graph_out} ({len(graph_json)} bytes)", flush=True)

open_letters_json = json.dumps(open_letters_data, ensure_ascii=False, indent=2)
open_letters_out = pathlib.Path("output/graph-data-open-letters.json")
open_letters_out.write_text(open_letters_json, encoding="utf-8")
print(f"Wrote {open_letters_out} ({len(open_letters_json)} bytes)", flush=True)

low_conf_json = json.dumps(low_confidence_nodes_data, ensure_ascii=False, indent=2)
low_conf_out = pathlib.Path("output/graph-data-low-confidence-nodes.json")
low_conf_out.write_text(low_conf_json, encoding="utf-8")
print(f"Wrote {low_conf_out} ({len(low_conf_json)} bytes)", flush=True)

legacy_low_conf_out = pathlib.Path("output/graph-data-low-confidence.json")
legacy_low_conf_out.write_text(open_letters_json, encoding="utf-8")
print(f"Wrote {legacy_low_conf_out} ({len(open_letters_json)} bytes)", flush=True)

address_coords_json = json.dumps(address_coordinates, ensure_ascii=False, indent=2)
address_coords_out = pathlib.Path("output/address-coordinates.json")
address_coords_out.write_text(address_coords_json, encoding="utf-8")
print(f"Wrote {address_coords_out} ({len(address_coords_json)} bytes)", flush=True)

netlify = pathlib.Path("netlify_graph_viewer/index.html")
if netlify.parent.exists():
    netlify.write_text(html, encoding="utf-8")
    print(f"Wrote {netlify} ({len(html)} bytes)", flush=True)
    netlify_graph = netlify.parent / "graph-data.json"
    netlify_graph.write_text(graph_json, encoding="utf-8")
    print(f"Wrote {netlify_graph} ({len(graph_json)} bytes)", flush=True)
    netlify_open_letters = netlify.parent / "graph-data-open-letters.json"
    netlify_open_letters.write_text(open_letters_json, encoding="utf-8")
    print(f"Wrote {netlify_open_letters} ({len(open_letters_json)} bytes)", flush=True)
    netlify_low_conf = netlify.parent / "graph-data-low-confidence-nodes.json"
    netlify_low_conf.write_text(low_conf_json, encoding="utf-8")
    print(f"Wrote {netlify_low_conf} ({len(low_conf_json)} bytes)", flush=True)
    netlify_legacy_low_conf = netlify.parent / "graph-data-low-confidence.json"
    netlify_legacy_low_conf.write_text(open_letters_json, encoding="utf-8")
    print(f"Wrote {netlify_legacy_low_conf} ({len(open_letters_json)} bytes)", flush=True)
    netlify_address_coords = netlify.parent / "address-coordinates.json"
    netlify_address_coords.write_text(address_coords_json, encoding="utf-8")
    print(f"Wrote {netlify_address_coords} ({len(address_coords_json)} bytes)", flush=True)

print("Done.", flush=True)

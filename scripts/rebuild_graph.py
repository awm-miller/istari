"""Rebuild graph HTML from graph modules, then copy to netlify."""
from dataclasses import asdict
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.config import load_settings
from src.graph.address_coordinates import (
    build_address_coordinate_index,
    default_address_coordinate_cache_path,
)
from src.graph.build import consolidate_multi_run
from src.graph.render import render_html
from src.mapping_low_confidence import (
    build_low_confidence_overlay,
    rebuild_overlay_mapping_db,
)
from src.ranking import rank_people
from src.services.mvp_pipeline import step4_ofac_screening
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
    total_screened = 0
    for run_id in run_ids:
        ranking = [asdict(entry) for entry in rank_people(repository, limit=ranking_limit, run_id=run_id)]
        if not ranking:
            continue
        result = step4_ofac_screening(
            repository=repository,
            settings=settings,
            ranking=ranking,
            enable_remote_sources=False,
        )
        hit_count = len(result.get("sanctions_hits") or {})
        screened_count = int(result.get("screened_count") or 0)
        total_hits += hit_count
        total_screened += screened_count
        print(
            f"  run {run_id}: screened {screened_count} people, {hit_count} sanction hit(s)",
            flush=True,
        )
    print(
        f"Sanctions refresh complete: screened {total_screened} people across {len(run_ids)} runs, "
        f"found {total_hits} hit(s)",
        flush=True,
    )

run_ids = repository.get_latest_unique_run_ids()
if not run_ids:
    raise SystemExit("No runs found.")

refresh_sanctions_for_runs(run_ids)

print(f"Consolidating runs {run_ids}...", flush=True)
data = consolidate_multi_run(run_ids)
print(f"  {len(data['nodes'])} nodes, {len(data['edges'])} edges", flush=True)

low_confidence_data = {"nodes": [], "edges": [], "summary": {"run_key": str(data.get("run_id", ""))}}
address_coordinates = {"coordinates": [], "summary": {}}
mapping_db_path = rebuild_overlay_mapping_db(settings.project_root)
if mapping_db_path.exists():
    try:
        low_confidence_data = build_low_confidence_overlay(
            main_data=data,
            database_path=mapping_db_path,
            run_key=str(data.get("run_id", "")),
            include_unmatched=True,
            include_generated_links=True,
        )
        print(
            "Loaded low-confidence overlay "
            f"({len(low_confidence_data.get('nodes') or [])} nodes, "
            f"{len(low_confidence_data.get('edges') or [])} edges)",
            flush=True,
        )
    except Exception as error:
        print(f"Warning: failed to build low-confidence overlay: {error}", flush=True)

try:
    address_coordinates = build_address_coordinate_index(
        main_data=data,
        low_confidence_data=low_confidence_data,
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

html = render_html({**data, "low_confidence": low_confidence_data})
print(f"Rendered HTML ({len(html)} bytes)", flush=True)

out = pathlib.Path("output/latest_graph.html")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(html, encoding="utf-8")
print(f"Wrote {out} ({len(html)} bytes)", flush=True)

graph_json = json.dumps(data, ensure_ascii=False, indent=2)
graph_out = pathlib.Path("output/graph-data.json")
graph_out.write_text(graph_json, encoding="utf-8")
print(f"Wrote {graph_out} ({len(graph_json)} bytes)", flush=True)

low_conf_json = json.dumps(low_confidence_data, ensure_ascii=False, indent=2)
low_conf_out = pathlib.Path("output/graph-data-low-confidence.json")
low_conf_out.write_text(low_conf_json, encoding="utf-8")
print(f"Wrote {low_conf_out} ({len(low_conf_json)} bytes)", flush=True)

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
    netlify_low_conf = netlify.parent / "graph-data-low-confidence.json"
    netlify_low_conf.write_text(low_conf_json, encoding="utf-8")
    print(f"Wrote {netlify_low_conf} ({len(low_conf_json)} bytes)", flush=True)
    netlify_address_coords = netlify.parent / "address-coordinates.json"
    netlify_address_coords.write_text(address_coords_json, encoding="utf-8")
    print(f"Wrote {netlify_address_coords} ({len(address_coords_json)} bytes)", flush=True)

print("Done.", flush=True)

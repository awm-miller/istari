"""Rebuild graph HTML from graph modules, then copy to netlify."""
from dataclasses import asdict
from hashlib import sha1
import json
import os
import pathlib
import sqlite3
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
from src.search.queries import generate_name_variants, normalize_name
from src.services.mvp_pipeline import hydrate_cached_sanctions, step4_ofac_screening
from src.storage.repository import Repository

settings = load_settings()
repository = Repository(
    settings.database_path,
    settings.project_root / "src" / "storage" / "schema.sql",
)
repository.init_db()
skip_sanctions_refresh = os.environ.get("SKIP_SANCTIONS_REFRESH", "").strip().lower() in {"1", "true", "yes"}


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


_SEED_PROMOTION_IGNORED_TOKENS = {
    "al",
    "ayatollah",
    "brigadier",
    "dr",
    "general",
    "miss",
    "mr",
    "mrs",
    "ms",
    "prof",
    "professor",
    "sir",
}

_SEED_PROMOTION_TOKEN_SWAPS = (
    ("mohammad", "mohammed"),
    ("mohamed", "mohammed"),
    ("muhammad", "mohammed"),
    ("saied", "saeed"),
    ("sayed", "seyed"),
)


def _seed_promotion_token_sets(name: str, *, include_generated_variants: bool = True) -> set[frozenset[str]]:
    raw_names = {str(name or "").strip()}
    if include_generated_variants:
        for variant in generate_name_variants(str(name or ""), "balanced"):
            value = str(variant.name or "").strip()
            if value:
                raw_names.add(value)

    token_sets: set[frozenset[str]] = set()
    for raw_name in raw_names:
        normalized = normalize_name(raw_name)
        tokens = [
            token
            for token in normalized.split()
            if token and token not in _SEED_PROMOTION_IGNORED_TOKENS
        ]
        if not tokens:
            continue
        joined = " ".join(tokens)
        variants = {joined}
        for old, new in _SEED_PROMOTION_TOKEN_SWAPS:
            variants.add(joined.replace(old, new))
            variants.add(joined.replace(new, old))
        for variant in variants:
            variant_tokens = frozenset(token for token in variant.split() if token)
            if len(variant_tokens) >= 2:
                token_sets.add(variant_tokens)
    return token_sets


def _promoted_seed_target(seed_name: str, nodes: list[dict]) -> dict | None:
    seed_token_sets = _seed_promotion_token_sets(seed_name)
    if not seed_token_sets:
        return None
    candidates: list[tuple[int, int, str, dict]] = []
    for node in nodes:
        if str(node.get("kind") or "") not in {"person", "seed_alias"}:
            continue
        node_token_sets: set[frozenset[str]] = set()
        for name in [node.get("label", ""), *(node.get("aliases") or [])]:
            node_token_sets.update(
                _seed_promotion_token_sets(str(name or ""), include_generated_variants=False)
            )
        if not node_token_sets:
            continue
        best_overlap = 0
        best_size_delta = 99
        for seed_tokens in seed_token_sets:
            for node_tokens in node_token_sets:
                if seed_tokens.issubset(node_tokens) or node_tokens.issubset(seed_tokens):
                    overlap = len(seed_tokens & node_tokens)
                    size_delta = abs(len(seed_tokens) - len(node_tokens))
                    if overlap > best_overlap or (overlap == best_overlap and size_delta < best_size_delta):
                        best_overlap = overlap
                        best_size_delta = size_delta
        if best_overlap:
            candidates.append((best_overlap, -best_size_delta, str(node.get("label") or ""), node))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return candidates[0][3]


def promote_external_seed_names(data: dict) -> dict:
    raw_path = os.environ.get("PROMOTE_SEED_DATABASE_PATH", "").strip()
    if not raw_path:
        return data
    seed_database_path = pathlib.Path(raw_path)
    if not seed_database_path.exists():
        print(f"Warning: seed promotion database does not exist: {seed_database_path}", flush=True)
        return data

    connection = sqlite3.connect(seed_database_path)
    try:
        seed_names = [
            str(row[0] or "").strip()
            for row in connection.execute("SELECT DISTINCT seed_name FROM runs ORDER BY seed_name")
            if str(row[0] or "").strip()
        ]
    finally:
        connection.close()
    if not seed_names:
        return data

    nodes = list(data.get("nodes") or [])
    edges = list(data.get("edges") or [])
    existing_node_ids = {str(node.get("id") or "") for node in nodes if isinstance(node, dict)}
    existing_edge_keys = {
        (str(edge.get("source") or ""), str(edge.get("target") or ""), str(edge.get("kind") or ""))
        for edge in edges
        if isinstance(edge, dict)
    }
    promoted = 0
    for seed_name in seed_names:
        target = _promoted_seed_target(seed_name, nodes)
        if not target:
            continue
        digest = sha1(seed_name.casefold().encode("utf-8")).hexdigest()[:12]
        seed_id = f"external_seed:{digest}"
        if seed_id not in existing_node_ids:
            nodes.append(
                {
                    "id": seed_id,
                    "label": seed_name,
                    "kind": "seed",
                    "lane": 0,
                    "seed_name": seed_name,
                    "promoted_seed": True,
                    "tooltip_lines": [f"Seed: {seed_name}", "Promoted from the Iran individual seed run."],
                }
            )
            existing_node_ids.add(seed_id)
        target_id = str(target.get("id") or "")
        edge_key = (seed_id, target_id, "alias")
        if target_id and edge_key not in existing_edge_keys:
            edges.append(
                {
                    "source": seed_id,
                    "target": target_id,
                    "kind": "alias",
                    "tooltip": f"{seed_name} = {target.get('label') or target_id}",
                    "promoted_seed": True,
                }
            )
            existing_edge_keys.add(edge_key)
        promoted += 1
    data["nodes"] = nodes
    data["edges"] = edges
    print(f"Promoted {promoted} external seed name(s) from {seed_database_path}", flush=True)
    return data

run_ids = repository.get_latest_unique_run_ids()
if not run_ids:
    raise SystemExit("No runs found.")

if skip_sanctions_refresh:
    print("Skipping sanctions refresh.", flush=True)
else:
    refresh_sanctions_for_runs(run_ids)

print(f"Consolidating runs {run_ids}...", flush=True)
data = consolidate_multi_run(run_ids)
data = annotate_graph_with_egypt_judgments(data, settings=settings)
negative_news_db_path = resolve_negative_news_db_path(settings)
data = annotate_graph_with_adverse_media(data, settings=settings, database_path=negative_news_db_path)
data = promote_external_seed_names(data)
print(f"  {len(data['nodes'])} nodes, {len(data['edges'])} edges", flush=True)

open_letters_data = {"nodes": [], "edges": [], "summary": {"run_key": str(data.get("run_id", ""))}}
low_confidence_nodes_data = {"nodes": [], "edges": [], "summary": {"run_ids": run_ids}}
address_coordinates = {"coordinates": [], "summary": {}}
mapping_db_path = rebuild_overlay_mapping_db(
    settings.project_root,
    source_directories=[settings.database_path.parent],
)
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

html = render_html(data, title_override=os.environ.get("GRAPH_VIEW_TITLE") or None)
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

from __future__ import annotations

import html
import json
from typing import Any

import networkx as nx
from flask import Flask, request

from src.charity_commission.client import CharityCommissionClient
from src.config import load_settings
from src.ofac.screening import OFACScreener
from src.pipeline import run_name_pipeline, run_seed_batch_pipeline
from src.resolution.matcher import HybridMatcher
from src.search.provider import build_search_providers
from src.storage.repository import Repository


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index() -> str:
        return _render_page()

    @app.get("/network/<int:run_id>")
    def network(run_id: int) -> str:
        settings = load_settings()
        repository = Repository(
            settings.database_path,
            settings.project_root / "src" / "storage" / "schema.sql",
        )
        repository.init_db()
        run_row = repository.get_run(run_id)
        seed_name = str(run_row["seed_name"]) if run_row else "Seed"

        screener = OFACScreener()
        sdn_path = settings.project_root / "data" / "sdn.csv"
        if sdn_path.exists():
            screener.load_csv(sdn_path)

        payload = _build_run_network_payload(repository, run_id, seed_name, screener=screener)
        return _render_network_page(run_id, payload)

    @app.post("/run")
    def run() -> str:
        seed_name = (request.form.get("seed_name") or "").strip()
        multi_seeds_raw = request.form.get("multi_seeds") or ""
        creativity = (request.form.get("creativity") or "strict").strip()
        limit = int(request.form.get("limit") or "25")

        multi_seed_names = _split_lines_or_csv(multi_seeds_raw)
        if not seed_name and not multi_seed_names:
            return _render_page(error="Seed name is required.")

        settings = load_settings()
        repository = Repository(
            settings.database_path,
            settings.project_root / "src" / "storage" / "schema.sql",
        )
        repository.init_db()
        charity_client = CharityCommissionClient(settings)
        matcher = HybridMatcher(settings)
        search_providers = build_search_providers(settings, include_web_dork=False)

        if len(multi_seed_names) >= 2:
            batch_result = run_seed_batch_pipeline(
                repository=repository,
                settings=settings,
                charity_client=charity_client,
                search_providers=search_providers,
                matcher=matcher,
                seed_names=multi_seed_names,
                creativity_level=creativity,
                limit=limit,
                overlap_limit=min(100, max(10, limit)),
            )
            return _render_page(
                values={
                    "seed_name": seed_name,
                    "multi_seeds": multi_seeds_raw,
                    "creativity": creativity,
                    "limit": str(limit),
                },
                result=batch_result,
            )

        result = run_name_pipeline(
            repository=repository,
            settings=settings,
            charity_client=charity_client,
            search_providers=search_providers,
            matcher=matcher,
            seed_name=seed_name,
            creativity_level=creativity,
            limit=limit,
        )
        result["network"] = _build_run_network_edges(repository, int(result["run_id"]))

        return _render_page(
            values={
                "seed_name": seed_name,
                "multi_seeds": multi_seeds_raw,
                "creativity": creativity,
                "limit": str(limit),
            },
            result=result,
        )

    return app


def _split_lines_or_csv(raw: str) -> list[str]:
    values: list[str] = []
    for line in raw.replace(",", "\n").splitlines():
        value = " ".join(line.split()).strip()
        if value:
            values.append(value)
    return values


def _build_run_network_edges(repository: Repository, run_id: int, max_edges: int = 300) -> list[dict[str, Any]]:
    rows = repository.get_run_network_edges(run_id)[:max_edges]
    return [
        {
            "person": str(row["person_name"]),
            "org": str(row["organisation_name"]),
            "weight": float(row["edge_weight"] or 0.0),
            "confidence": str(row["confidence_class"]),
            "role_type": str(row["role_type"]),
            "role_label": str(row["role_label"]),
            "source": str(row["source"]),
        }
        for row in rows
    ]


def _friendly_role_phrase(role_type: str, role_label: str) -> str:
    role = (role_type or "").lower()
    label = (role_label or "").lower()
    if "trustee" in role or "trustee" in label:
        return "is a trustee of"
    if "accountant" in role or "accountant" in label:
        return "is named in governance/finance documents for"
    if "director" in role or "officer" in role or "director" in label:
        return "is a director/officer of"
    if "secretary" in role or "secretary" in label:
        return "is a secretary of"
    return "is linked with"


def _build_run_network_payload(
    repository: Repository,
    run_id: int,
    seed_name: str,
    max_candidates: int = 16,
    max_orgs: int = 16,
    max_expanded_people: int = 24,
    screener: OFACScreener | None = None,
) -> dict[str, Any]:
    with repository.connect() as connection:
        candidate_rows = connection.execute(
            """
            SELECT
                resolution_decisions.canonical_name AS canonical_name,
                COUNT(*) AS link_count
            FROM resolution_decisions
            WHERE resolution_decisions.run_id = ?
              AND resolution_decisions.status IN ('match', 'maybe_match')
            GROUP BY resolution_decisions.canonical_name
            ORDER BY link_count DESC, canonical_name ASC
            LIMIT ?
            """,
            (run_id, max_candidates),
        ).fetchall()
        org_rows = connection.execute(
            """
            SELECT
                organisations.id AS org_id,
                organisations.name AS org_name,
                COUNT(*) AS link_count
            FROM resolution_decisions
            JOIN candidate_matches
                ON candidate_matches.id = resolution_decisions.candidate_match_id
            JOIN organisations
                ON organisations.registry_type = candidate_matches.registry_type
               AND organisations.registry_number = candidate_matches.registry_number
               AND organisations.suffix = candidate_matches.suffix
            WHERE resolution_decisions.run_id = ?
              AND resolution_decisions.status IN ('match', 'maybe_match')
            GROUP BY organisations.id, organisations.name
            ORDER BY link_count DESC, organisations.name ASC
            LIMIT ?
            """,
            (run_id, max_orgs),
        ).fetchall()
        candidate_org_rows = connection.execute(
            """
            SELECT
                resolution_decisions.canonical_name AS candidate_name,
                organisations.id AS org_id,
                organisations.name AS org_name,
                resolution_decisions.confidence AS confidence,
                candidate_matches.raw_payload_json AS raw_payload_json,
                candidate_matches.source AS candidate_source
            FROM resolution_decisions
            JOIN candidate_matches
                ON candidate_matches.id = resolution_decisions.candidate_match_id
            JOIN organisations
                ON organisations.registry_type = candidate_matches.registry_type
               AND organisations.registry_number = candidate_matches.registry_number
               AND organisations.suffix = candidate_matches.suffix
            WHERE resolution_decisions.run_id = ?
              AND resolution_decisions.status IN ('match', 'maybe_match')
            """,
            (run_id,),
        ).fetchall()

    graph_rows = repository.get_run_network_edges(run_id)
    candidate_names = {str(row["canonical_name"]) for row in candidate_rows}
    org_id_set = {int(row["org_id"]) for row in org_rows}

    expanded_counts: dict[str, int] = {}
    for row in graph_rows:
        org_id = int(row["organisation_id"])
        person_name = str(row["person_name"])
        if org_id not in org_id_set:
            continue
        if person_name in candidate_names:
            continue
        expanded_counts[person_name] = expanded_counts.get(person_name, 0) + 1

    expanded_people = [
        name
        for name, _count in sorted(
            expanded_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:max_expanded_people]
    ]

    g = nx.Graph()
    g.add_node("seed", label=seed_name, lane=0, kind="seed")

    for idx, row in enumerate(candidate_rows, 1):
        node_id = f"cand:{idx}"
        g.add_node(
            node_id,
            label=str(row["canonical_name"]),
            lane=1,
            kind="candidate",
        )
        g.add_edge(
            "seed",
            node_id,
            label="Possible same person",
            source="pipeline_seed",
            role_type="seed_link",
            role_label="resolved_candidate",
            confidence="high",
            weight=1.0,
            color="#d1242f",
            explanation="This name is treated as a possible alias of the seed name.",
        )

    org_node_by_id: dict[int, str] = {}
    for row in org_rows:
        org_id = int(row["org_id"])
        node_id = f"org:{org_id}"
        org_node_by_id[org_id] = node_id
        g.add_node(
            node_id,
            label=str(row["org_name"]),
            lane=2,
            kind="organisation",
        )

    candidate_node_by_name = {
        str(data["label"]): node_id
        for node_id, data in g.nodes(data=True)
        if data.get("kind") == "candidate"
    }
    for row in candidate_org_rows:
        candidate_node = candidate_node_by_name.get(str(row["candidate_name"]))
        org_node = org_node_by_id.get(int(row["org_id"]))
        if not candidate_node or not org_node:
            continue
        raw_payload = json.loads(str(row["raw_payload_json"] or "{}"))
        relation_phrase = str(raw_payload.get("relationship_phrase") or "").strip() or "is linked to"
        g.add_edge(
            candidate_node,
            org_node,
            label=relation_phrase,
            source=str(row["candidate_source"] or "resolution"),
            role_type=str(raw_payload.get("role_type") or "resolved_link"),
            role_label=str(raw_payload.get("role_label") or "match_or_maybe"),
            confidence=str(round(float(row["confidence"] or 0.0), 2)),
            weight=float(row["confidence"] or 0.5),
            color="#6f42c1",
            explanation=(
                f"{str(row['candidate_name'])} {relation_phrase} {str(row['org_name'])}."
            ),
        )

    expanded_node_by_name: dict[str, str] = {}
    for idx, person_name in enumerate(expanded_people, 1):
        node_id = f"exp:{idx}"
        expanded_node_by_name[person_name] = node_id
        g.add_node(
            node_id,
            label=person_name,
            lane=3,
            kind="expanded_person",
        )

    for row in graph_rows:
        org_node = org_node_by_id.get(int(row["organisation_id"]))
        expanded_node = expanded_node_by_name.get(str(row["person_name"]))
        if not org_node or not expanded_node:
            continue

        role_type = str(row["role_type"])
        role_low = role_type.lower()
        color = "#7e8aa5"
        if "trustee" in role_low:
            color = "#1f6feb"
        elif "accountant" in role_low:
            color = "#9a6700"
        elif "director" in role_low or "officer" in role_low:
            color = "#8250df"
        elif "secretary" in role_low:
            color = "#0550ae"

        g.add_edge(
            org_node,
            expanded_node,
            label=str(row["relationship_phrase"] or "") or _friendly_role_phrase(role_type, str(row["role_label"])),
            source=str(row["source"]),
            role_type=role_type,
            role_label=str(row["role_label"]),
            confidence=str(row["confidence_class"]),
            weight=float(row["edge_weight"] or 0.35),
            color=color,
            explanation=(
                f"{str(row['person_name'])} "
                f"{str(row['relationship_phrase'] or '') or _friendly_role_phrase(role_type, str(row['role_label']))} "
                f"{str(row['organisation_name'])}."
            ),
        )

    ofac_hit_set: set[str] = set()
    if screener and screener.loaded:
        all_person_labels = [
            str(data.get("label", ""))
            for _, data in g.nodes(data=True)
            if data.get("kind") in ("candidate", "expanded_person")
        ]
        ofac_results = screener.screen_names(all_person_labels)
        ofac_hit_set = set(ofac_results.keys())

    nodes = []
    for node_id, data in g.nodes(data=True):
        label = str(data.get("label", node_id))
        nodes.append(
            {
                "id": node_id,
                "label": label,
                "lane": int(data.get("lane", 0)),
                "kind": str(data.get("kind", "node")),
                "degree": int(g.degree(node_id)),
                "ofac_hit": label in ofac_hit_set,
            }
        )

    edges = []
    for idx, (source, target, data) in enumerate(g.edges(data=True), 1):
        edges.append(
            {
                "id": f"e:{idx}",
                "from": source,
                "to": target,
                "label": str(data.get("label", "")),
                "source": str(data.get("source", "")),
                "role_type": str(data.get("role_type", "")),
                "role_label": str(data.get("role_label", "")),
                "confidence": str(data.get("confidence", "")),
                "weight": float(data.get("weight", 0.5)),
                "color": str(data.get("color", "#7e8aa5")),
                "title": str(data.get("label", "")),
                "explanation": str(data.get("explanation", "")),
            }
        )

    return {"nodes": nodes, "edges": edges}


def _render_page(
    *,
    values: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    error: str | None = None,
) -> str:
    values = values or {}
    seed_name = html.escape(values.get("seed_name", ""))
    multi_seeds = html.escape(values.get("multi_seeds", ""))
    creativity = values.get("creativity", "strict")
    limit = html.escape(values.get("limit", "25"))

    options = []
    for option in ("strict", "balanced", "exploratory"):
        selected = " selected" if creativity == option else ""
        options.append(f'<option value="{option}"{selected}>{option}</option>')
    options_html = "".join(options)
    result_html = ""
    if error:
        result_html += f'<p style="color:#b00020;"><strong>Error:</strong> {html.escape(error)}</p>'
    if result is not None:
        if result.get("mode") == "multi_seed":
            summary = {
                "mode": result.get("mode"),
                "seed_names": result.get("seed_names", []),
                "run_ids": result.get("run_ids", []),
                "aggregate_resolution_metrics": result.get("aggregate_resolution_metrics", {}),
                "top_overlap_people": result.get("overlap_people", [])[:10],
                "top_overlap_organisations": result.get("overlap_organisations", [])[:10],
            }
            result_html += (
                "<h2>Multi-Seed Overlap Result</h2>"
                f"<pre>{html.escape(str(summary))}</pre>"
                "<details><summary>Per-seed runs</summary>"
                f"<pre>{html.escape(str(result.get('runs', [])))}</pre>"
                "</details>"
            )
        else:
            network_json = _json_for_script(result.get("network", []))
            step4 = result.get("step4", {})
            ofac_hits = step4.get("ofac_hits", {})
            ranking_top = result["ranking"][:10]
            ranking_display = []
            for entry in ranking_top:
                name = entry.get("canonical_name", "")
                is_hit = entry.get("ofac_hit", False) or name in ofac_hits
                prefix = "\u26A0\uFE0F OFAC HIT: " if is_hit else ""
                ranking_display.append(f"{prefix}{name} (orgs={entry.get('organisation_count',0)}, roles={entry.get('role_count',0)}, score={entry.get('weighted_organisation_score',0)})")

            ofac_summary = {
                "screened_count": step4.get("screened_count", 0),
                "sdn_entry_count": step4.get("sdn_entry_count", 0),
                "hits": list(ofac_hits.keys()),
            }
            summary = {
                "run_id": result["run_id"],
                "mode": result.get("mode"),
                "search_summary": result["search_summary"],
                "decision_count": result["decision_count"],
                "resolution_metrics": result.get("resolution_metrics", {}),
                "step1": result.get("step1", {}),
                "step2": result.get("step2", {}),
                "step3": {
                    "run_id": result.get("step3", {}).get("run_id"),
                    "processed_organisation_count": result.get("step3", {}).get("processed_organisation_count"),
                    "inserted_roles": result.get("step3", {}).get("inserted_roles"),
                },
                "step4_ofac": ofac_summary,
                "ranking_top_10": ranking_display,
            }

            ofac_hit_names_json = _json_for_script(list(ofac_hits.keys()))
            ofac_banner = ""
            if ofac_hits:
                hit_list_items = "".join(
                    f"<li><strong>{html.escape(name)}</strong>: {html.escape(hits[0].get('program', ''))} &mdash; {html.escape(hits[0].get('remarks', '')[:120])}</li>"
                    for name, hits in ofac_hits.items()
                )
                ofac_banner = (
                    "<div class='ofac-banner'>"
                    "  <span class='ofac-icon'>&#x1F6A8;</span>"
                    f"  <strong>OFAC Sanctions Hits ({len(ofac_hits)})</strong>"
                    f"  <ul>{hit_list_items}</ul>"
                    "</div>"
                )

            result_html += (
                "<h2>Run Result</h2>"
                f"{ofac_banner}"
                f"<pre>{html.escape(str(summary))}</pre>"
                "<div class='network-box'>"
                "  <div class='network-header'>"
                "    <h3>Network Diagram</h3>"
                "    <label>Connectedness "
                "      <input id='connectedness-slider' type='range' min='0' max='100' value='65' />"
                "      <span id='connectedness-value'>65</span>"
                "    </label>"
                "  </div>"
                "  <div id='network-empty' class='hint' style='display:none;'>No edges found for this run yet.</div>"
                "  <svg id='network-svg' viewBox='0 0 900 420' role='img' aria-label='Network diagram'></svg>"
                f"  <script id='network-data' type='application/json'>{network_json}</script>"
                f"  <script id='ofac-hit-names' type='application/json'>{ofac_hit_names_json}</script>"
                "</div>"
                "<details><summary>Step Details</summary>"
                f"<pre>{html.escape(str({'step1': result.get('step1', {}), 'step2': result.get('step2', {}), 'step3': result.get('step3', {}), 'step4_ofac': ofac_summary}))}</pre>"
                "</details>"
            )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Istari Pivot Runner</title>
  <style>
    body {{ font-family: sans-serif; margin: 24px; max-width: 960px; }}
    label {{ display: block; margin-top: 12px; font-weight: 600; }}
    input, textarea, select {{ width: 100%; padding: 8px; box-sizing: border-box; }}
    .row {{ display: grid; grid-template-columns: 1fr 160px; gap: 12px; }}
    button {{ margin-top: 16px; padding: 10px 14px; }}
    pre {{ background: #f5f5f5; padding: 12px; overflow-x: auto; }}
    .network-box {{ margin-top: 16px; border: 1px solid #ddd; border-radius: 8px; padding: 12px; background: #fff; }}
    .network-header {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 8px; }}
    .network-header h3 {{ margin: 0; }}
    .network-header label {{ margin: 0; font-weight: 600; display: inline-flex; align-items: center; gap: 8px; }}
    #connectedness-slider {{ width: 220px; }}
    #network-svg {{ width: 100%; height: auto; border: 1px solid #eee; border-radius: 6px; background: #fbfbfb; }}
    .hint {{ color: #666; margin-bottom: 8px; }}
    .ofac-banner {{ background: #fef2f2; border: 2px solid #dc2626; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; }}
    .ofac-banner ul {{ margin: 6px 0 0 0; padding-left: 20px; }}
    .ofac-banner li {{ font-size: 13px; color: #7f1d1d; margin-bottom: 2px; }}
    .ofac-icon {{ font-size: 18px; vertical-align: middle; margin-right: 4px; }}
  </style>
</head>
<body>
  <h1>Istari Pivot Runner</h1>
  <p>Run the registry-only MVP from a seed name, or compare overlap across multiple seeds.</p>
  <form method="post" action="/run">
    <label>Seed Name</label>
    <input name="seed_name" value="{seed_name}" placeholder="MOHAMAD ABDUL KARIM KOZBAR" />

    <label>Multiple Seeds (newline or comma separated; if 2+ entries, overlap mode runs)</label>
    <textarea name="multi_seeds" rows="4" placeholder="MOHAMAD ABDUL KARIM KOZBAR&#10;ANOTHER PERSON">{multi_seeds}</textarea>

    <div class="row">
      <div>
        <label>Creativity</label>
        <select name="creativity">{options_html}</select>
      </div>
      <div>
        <label>Rank Limit</label>
        <input name="limit" value="{limit}" />
      </div>
    </div>
    <button type="submit">Run Pipeline</button>
  </form>
  {result_html}
  <script>
    (() => {{
      const dataTag = document.getElementById("network-data");
      const slider = document.getElementById("connectedness-slider");
      const valueTag = document.getElementById("connectedness-value");
      const svg = document.getElementById("network-svg");
      const emptyTag = document.getElementById("network-empty");
      if (!dataTag || !slider || !valueTag || !svg || !emptyTag) return;

      const ofacTag = document.getElementById("ofac-hit-names");
      const ofacHitNames = new Set(ofacTag ? JSON.parse(ofacTag.textContent || "[]").map(n => n.toLowerCase()) : []);

      const edges = JSON.parse(dataTag.textContent || "[]");
      if (!Array.isArray(edges) || edges.length === 0) {{
        svg.style.display = "none";
        emptyTag.style.display = "block";
        return;
      }}

      const personCounts = new Map();
      const orgCounts = new Map();
      for (const edge of edges) {{
        personCounts.set(edge.person, (personCounts.get(edge.person) || 0) + 1);
        orgCounts.set(edge.org, (orgCounts.get(edge.org) || 0) + 1);
      }}

      const topPeople = [...personCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 14).map(([name]) => name);
      const topOrgs = [...orgCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 14).map(([name]) => name);
      const personSet = new Set(topPeople);
      const orgSet = new Set(topOrgs);
      const trimmed = edges.filter((edge) => personSet.has(edge.person) && orgSet.has(edge.org));
      const weights = trimmed.map((edge) => Number(edge.weight) || 0);
      const minWeight = Math.min(...weights);
      const maxWeight = Math.max(...weights);

      const nodeLabel = (text, max = 28) => text.length > max ? `${{text.slice(0, max - 1)}}...` : text;
      const edgeColor = (edge) => {{
        const role = (edge.role_type || "").toLowerCase();
        if (role.includes("trustee")) return "#1f6feb";
        if (role.includes("accountant")) return "#9a6700";
        if (role.includes("director") || role.includes("officer")) return "#8250df";
        if (role.includes("secretary")) return "#0550ae";
        return "#7e8aa5";
      }};
      const yPosition = (i, total) => {{
        if (total <= 1) return 210;
        const top = 24;
        const bottom = 396;
        return top + (i * (bottom - top)) / (total - 1);
      }};

      const peopleY = new Map(topPeople.map((name, i) => [name, yPosition(i, topPeople.length)]));
      const orgY = new Map(topOrgs.map((name, i) => [name, yPosition(i, topOrgs.length)]));

      const render = () => {{
        const connectedness = Number(slider.value) || 0;
        valueTag.textContent = String(connectedness);
        const threshold = minWeight + ((100 - connectedness) / 100) * (maxWeight - minWeight);
        const visible = trimmed.filter((edge) => (Number(edge.weight) || 0) >= threshold);

        svg.innerHTML = "";
        const lineLayer = document.createElementNS("http://www.w3.org/2000/svg", "g");
        const nodeLayer = document.createElementNS("http://www.w3.org/2000/svg", "g");
        svg.appendChild(lineLayer);
        svg.appendChild(nodeLayer);

        for (const edge of visible) {{
          const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
          line.setAttribute("x1", "220");
          line.setAttribute("y1", String(peopleY.get(edge.person)));
          line.setAttribute("x2", "680");
          line.setAttribute("y2", String(orgY.get(edge.org)));
          line.setAttribute("stroke", edgeColor(edge));
          line.setAttribute("stroke-opacity", "0.45");
          line.setAttribute("stroke-width", String(1 + (Number(edge.weight) || 0) * 1.8));
          const tip = document.createElementNS("http://www.w3.org/2000/svg", "title");
          tip.textContent = `${{edge.role_type}} / ${{edge.role_label}} | ${{edge.source}} | ${{edge.confidence}}`;
          line.appendChild(tip);
          lineLayer.appendChild(line);
        }}

        for (const [name, y] of peopleY.entries()) {{
          const isOfac = ofacHitNames.has(name.toLowerCase());
          const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
          c.setAttribute("cx", "220");
          c.setAttribute("cy", String(y));
          c.setAttribute("r", isOfac ? "7" : "6");
          c.setAttribute("fill", isOfac ? "#dc2626" : "#1f6feb");
          if (isOfac) c.setAttribute("stroke", "#7f1d1d");
          if (isOfac) c.setAttribute("stroke-width", "2");
          nodeLayer.appendChild(c);

          if (isOfac) {{
            const d = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
            d.setAttribute("points", `${{220 - 8}},${{y - 15}} ${{220}},${{y - 23}} ${{220 + 8}},${{y - 15}} ${{220}},${{y - 7}}`);
            d.setAttribute("fill", "#dc2626");
            d.setAttribute("stroke", "#7f1d1d");
            d.setAttribute("stroke-width", "1");
            const tip = document.createElementNS("http://www.w3.org/2000/svg", "title");
            tip.textContent = "OFAC SDN Sanctions List Match";
            d.appendChild(tip);
            nodeLayer.appendChild(d);
            const ex = document.createElementNS("http://www.w3.org/2000/svg", "text");
            ex.setAttribute("x", "220");
            ex.setAttribute("y", String(y - 12));
            ex.setAttribute("text-anchor", "middle");
            ex.setAttribute("font-size", "10");
            ex.setAttribute("font-weight", "bold");
            ex.setAttribute("fill", "#ffffff");
            ex.textContent = "!";
            nodeLayer.appendChild(ex);
          }}

          const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
          t.setAttribute("x", "210");
          t.setAttribute("y", String(y + 4));
          t.setAttribute("text-anchor", "end");
          t.setAttribute("font-size", "11");
          t.setAttribute("fill", isOfac ? "#dc2626" : "#243040");
          t.setAttribute("font-weight", isOfac ? "bold" : "normal");
          t.textContent = isOfac ? "\u25C6\u26A0 " + nodeLabel(name) : nodeLabel(name);
          nodeLayer.appendChild(t);
        }}

        for (const [name, y] of orgY.entries()) {{
          const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
          c.setAttribute("cx", "680");
          c.setAttribute("cy", String(y));
          c.setAttribute("r", "6");
          c.setAttribute("fill", "#2ea043");
          nodeLayer.appendChild(c);

          const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
          t.setAttribute("x", "690");
          t.setAttribute("y", String(y + 4));
          t.setAttribute("font-size", "11");
          t.setAttribute("fill", "#243040");
          t.textContent = nodeLabel(name);
          nodeLayer.appendChild(t);
        }}

        emptyTag.style.display = visible.length ? "none" : "block";
        emptyTag.textContent = visible.length ? "" : "No edges visible at this slider setting.";
      }};

      slider.addEventListener("input", render);
      render();
    }})();
  </script>
</body>
</html>"""


def _render_network_page(run_id: int, payload: dict[str, Any]) -> str:
    payload_json = _json_for_script(payload)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Istari Network - Run {run_id}</title>
  <link rel="stylesheet" href="https://unpkg.com/@xyflow/react/dist/style.css" />
  <style>
    html, body, #app-root {{ margin: 0; width: 100%; height: 100%; overflow: hidden; font-family: Segoe UI, sans-serif; }}
    .shell {{ width: 100%; height: 100%; display: grid; grid-template-rows: auto 1fr auto; background: #f6f8fb; }}
    .topbar {{ display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 10px 14px; border-bottom: 1px solid #d9dce3; background: #fff; }}
    .topbar a {{ color: #0969da; text-decoration: none; font-weight: 600; }}
    .title {{ font-weight: 700; color: #1f2328; }}
    .controls {{ display: inline-flex; align-items: center; gap: 8px; font-size: 13px; color: #444; flex-wrap: wrap; }}
    .controls input[type="search"] {{ width: 180px; border: 1px solid #c8ccd5; border-radius: 8px; padding: 5px 8px; font-size: 12px; }}
    .controls button {{ border: 1px solid #c8ccd5; background: #fff; border-radius: 8px; padding: 4px 10px; cursor: pointer; font-size: 12px; }}
    .controls button:hover {{ background: #f3f4f6; }}
    #degreeSlider {{ width: 160px; }}
    .canvas {{ width: 100%; height: 100%; }}
    .meta {{ border-top: 1px solid #d9dce3; background: #fff; padding: 10px 14px; font-size: 13px; color: #334; min-height: 48px; max-height: 160px; overflow-y: auto; }}
    .badge {{ display: inline-block; padding: 2px 6px; border-radius: 999px; background: #eef2ff; margin-right: 6px; }}
    .lane-help {{ font-size: 12px; color: #596275; }}
  </style>
</head>
<body>
  <div id="app-root"></div>
  <script id="network-data" type="application/json">{payload_json}</script>
  <script>
    (async () => {{
      const importAny = async (urls) => {{
        let lastError = null;
        for (const url of urls) {{
          try {{
            return await import(url);
          }} catch (err) {{
            lastError = err;
          }}
        }}
        throw lastError || new Error("Failed to import module");
      }};

      try {{
        const ReactMod = await importAny([
          "https://esm.sh/react@18",
          "https://cdn.jsdelivr.net/npm/react@18/+esm",
        ]);
        const ReactDomMod = await importAny([
          "https://esm.sh/react-dom@18/client",
          "https://cdn.jsdelivr.net/npm/react-dom@18/client/+esm",
        ]);
        const ReactFlowMod = await importAny([
          "https://esm.sh/@xyflow/react@12?deps=react@18,react-dom@18",
          "https://cdn.jsdelivr.net/npm/@xyflow/react@12/+esm",
        ]);

        const React = ReactMod.default || ReactMod;
        const createRoot = ReactDomMod.createRoot;
        const ReactFlow = ReactFlowMod.default || ReactFlowMod.ReactFlow;
        const Background = ReactFlowMod.Background;
        const Controls = ReactFlowMod.Controls;
        const MiniMap = ReactFlowMod.MiniMap;
        const MarkerType = ReactFlowMod.MarkerType;
        const useEdgesState = ReactFlowMod.useEdgesState;
        const useNodesState = ReactFlowMod.useNodesState;

        const payload = JSON.parse(document.getElementById("network-data").textContent || "{{}}");
        const allNodes = Array.isArray(payload.nodes) ? payload.nodes : [];
        const allEdges = Array.isArray(payload.edges) ? payload.edges : [];

        const colorForKind = (kind, ofacHit) => {{
          if (ofacHit) return "#dc2626";
          if (kind === "seed") return "#d1242f";
          if (kind === "candidate") return "#1f6feb";
          if (kind === "organisation") return "#2ea043";
          return "#8b949e";
        }};

        const laneName = (lane) => {{
          if (lane === 0) return "Seed";
          if (lane === 1) return "Matched names";
          if (lane === 2) return "Linked organisations";
          return "Expanded people";
        }};

        const adj = (() => {{
          const m = new Map();
          for (const e of allEdges) {{
            if (!m.has(e.from)) m.set(e.from, []);
            if (!m.has(e.to)) m.set(e.to, []);
            m.get(e.from).push({{ neighbor: e.to, edge: e }});
            m.get(e.to).push({{ neighbor: e.from, edge: e }});
          }}
          return m;
        }})();

        const bfsPath = (startId, targetId) => {{
          if (startId === targetId) return {{ nodeIds: [startId], edges: [] }};
          const prev = new Map();
          prev.set(startId, null);
          const queue = [startId];
          let found = false;
          while (queue.length && !found) {{
            const cur = queue.shift();
            for (const {{ neighbor, edge }} of (adj.get(cur) || [])) {{
              if (prev.has(neighbor)) continue;
              prev.set(neighbor, {{ parent: cur, edge }});
              if (neighbor === targetId) {{ found = true; break; }}
              queue.push(neighbor);
            }}
          }}
          if (!found) return null;
          const nodeIds = [];
          const edges = [];
          let node = targetId;
          while (node !== null) {{
            nodeIds.push(node);
            const entry = prev.get(node);
            if (entry) {{ edges.push(entry.edge); node = entry.parent; }}
            else node = null;
          }}
          nodeIds.reverse();
          edges.reverse();
          return {{ nodeIds, edges }};
        }};

        const traceToSeed = (targetId) => {{
          const result = bfsPath("seed", targetId);
          if (!result) return {{ nodeIds: new Set(), edges: [], path: null }};
          return {{ nodeIds: new Set(result.nodeIds), edges: result.edges, path: result }};
        }};

        const nodeById = (() => {{
          const m = new Map();
          for (const n of allNodes) m.set(n.id, n);
          return m;
        }})();

        function App() {{
          const maxDegree = Math.max(2, ...allNodes.map((n) => Number(n.degree || 0)));
          const [minDegree, setMinDegree] = React.useState(1);
          const [searchText, setSearchText] = React.useState("");
          const [traceNodeId, setTraceNodeId] = React.useState(null);
          const [selectedEdge, setSelectedEdge] = React.useState(null);
          const [traceDesc, setTraceDesc] = React.useState(null);

          const activeMode = traceNodeId ? "trace" : "normal";

          const filtered = React.useMemo(() => {{
            const query = searchText.trim().toLowerCase();
            let nodes, edges, highlightId = null;

            if (activeMode === "trace") {{
              const trace = traceToSeed(traceNodeId);
              nodes = allNodes.filter((n) => trace.nodeIds.has(n.id));
              edges = trace.edges;
              highlightId = traceNodeId;
            }} else {{
              const keep = new Set();
              for (const n of allNodes) {{
                if (n.id === "seed" || Number(n.degree || 0) >= minDegree) keep.add(n.id);
              }}
              edges = allEdges.filter((e) => keep.has(e.from) && keep.has(e.to));
              for (const e of edges) {{ keep.add(e.from); keep.add(e.to); }}
              nodes = allNodes.filter((n) => keep.has(n.id));

              if (query) {{
                const matchIds = new Set(
                  allNodes.filter((n) => String(n.label || "").toLowerCase().includes(query)).map((n) => n.id)
                );
                const keepIds = new Set([...matchIds, "seed"]);
                for (const e of allEdges) {{
                  if (matchIds.has(e.from) || matchIds.has(e.to)) {{
                    keepIds.add(e.from);
                    keepIds.add(e.to);
                  }}
                }}
                nodes = allNodes.filter((n) => keepIds.has(n.id));
                const visibleIds = new Set(nodes.map((n) => n.id));
                edges = allEdges.filter((e) => visibleIds.has(e.from) && visibleIds.has(e.to));
              }}
            }}

            const baseStyle = {{
              border: "1px solid #c8ccd5", borderRadius: "10px", padding: "8px 10px",
              background: "#fff", fontSize: "12px", minWidth: "120px", textAlign: "center",
              boxShadow: "0 1px 2px rgba(0,0,0,0.06)"
            }};
            const lanes = new Map();
            for (const n of nodes) {{
              const lane = Number(n.lane || 0);
              if (!lanes.has(lane)) lanes.set(lane, []);
              lanes.get(lane).push(n);
            }}
            for (const list of lanes.values()) list.sort((a, b) => String(a.label).localeCompare(String(b.label)));
            const laneGapY = 220; const nodeGapX = 230; const centerX = 600;
            const outNodes = [];
            for (const [lane, list] of [...lanes.entries()].sort((a, b) => a[0] - b[0])) {{
              const totalWidth = (list.length - 1) * nodeGapX;
              const left = centerX - totalWidth / 2;
              list.forEach((n, i) => {{
              const isHL = n.id === highlightId;
                    const isSeed = n.id === "seed";
                    const isOfac = Boolean(n.ofac_hit);
                    const nodeLabel = isOfac ? "\u25C6\u26A0 " + n.label : n.label;
                    outNodes.push({{
                      id: n.id,
                      position: {{ x: left + i * nodeGapX, y: 40 + lane * laneGapY }},
                      data: {{ label: nodeLabel, kind: n.kind, lane: lane, degree: Number(n.degree || 0), ofac_hit: isOfac }},
                      draggable: true,
                      style: {{
                        ...baseStyle,
                        border: isOfac ? "2px solid #dc2626" : isHL ? "2px solid #d4a017" : isSeed ? "2px solid #d1242f" : baseStyle.border,
                        background: isOfac ? "#fef2f2" : isHL ? "#fffbe6" : baseStyle.background,
                        color: isOfac ? "#7f1d1d" : undefined,
                        fontWeight: isOfac ? "bold" : undefined,
                      }},
                      sourcePosition: "bottom",
                      targetPosition: "top",
                      degree: Number(n.degree || 0),
                      lane: lane,
                      kind: n.kind,
                      ofac_hit: isOfac,
                    }});
              }});
            }}
            const nodeIdSet = new Set(outNodes.map((n) => n.id));
            const outEdges = edges
              .filter((e) => nodeIdSet.has(e.from) && nodeIdSet.has(e.to))
              .map((e) => ({{
                id: e.id, source: e.from, target: e.to, type: "smoothstep",
                markerEnd: {{ type: MarkerType.ArrowClosed, color: e.color || "#7e8aa5" }},
                style: {{
                  stroke: e.color || "#7e8aa5",
                  strokeWidth: Math.max(1, Math.min(6, Number(e.weight || 0.5) * 2.8)),
                  opacity: 0.75
                }},
                data: e, label: "",
              }}));
            return {{ nodes: outNodes, edges: outEdges }};
          }}, [minDegree, searchText, traceNodeId, activeMode]);

          const [nodes, setNodes, onNodesChange] = useNodesState(filtered.nodes);
          const [edges, setEdges, onEdgesChange] = useEdgesState(filtered.edges);
          React.useEffect(() => {{
            setNodes(filtered.nodes);
            setEdges(filtered.edges);
          }}, [filtered, setNodes, setEdges]);

          const onEdgeClick = React.useCallback((_evt, edge) => {{
            setSelectedEdge(edge?.data || null);
          }}, []);

          const onNodeDoubleClick = React.useCallback((_evt, node) => {{
            setTraceNodeId(node.id);
            setSelectedEdge(null);
            const trace = traceToSeed(node.id);
            if (!trace.path) {{
              setTraceDesc({{ label: node?.data?.label || node.id, steps: ["No path from seed found."] }});
              return;
            }}
            if (trace.path.nodeIds.length <= 1) {{
              setTraceDesc({{ label: node?.data?.label || node.id, steps: ["This is the seed node."] }});
              return;
            }}
            const steps = [];
            for (let k = 0; k < trace.path.nodeIds.length - 1; k++) {{
              const fromLabel = nodeById.get(trace.path.nodeIds[k])?.label || trace.path.nodeIds[k];
              const toLabel = nodeById.get(trace.path.nodeIds[k + 1])?.label || trace.path.nodeIds[k + 1];
              const edge = trace.path.edges[k];
              steps.push(edge?.explanation || (fromLabel + " \u2192 " + toLabel));
            }}
            setTraceDesc({{ label: node?.data?.label || node.id, steps }});
          }}, []);

          const onPaneClick = React.useCallback(() => {{ setSelectedEdge(null); }}, []);

          const resetView = React.useCallback(() => {{
            setTraceNodeId(null); setSearchText(""); setSelectedEdge(null); setTraceDesc(null);
          }}, []);

          const h = React.createElement;

          let metaContent;
          if (selectedEdge) {{
            metaContent = h(React.Fragment, null,
              h("span", {{ className: "badge" }}, selectedEdge.title || "link"),
              h("span", {{ className: "badge" }}, selectedEdge.role_type || "role"),
              h("span", {{ className: "badge" }}, selectedEdge.role_label || "detail"),
              h("span", {{ className: "badge" }}, selectedEdge.source || "source"),
              h("span", {{ className: "badge" }}, "confidence: " + (selectedEdge.confidence || "")),
              h("span", null, " " + (selectedEdge.explanation || ""))
            );
          }} else if (traceDesc) {{
            metaContent = h(React.Fragment, null,
              h("span", {{ className: "badge", style: {{ background: "#fff3cd", color: "#664d03" }} }}, "Trace mode"),
              h("strong", null, " " + traceDesc.label + " "),
              traceDesc.steps.map((step, i) =>
                h("div", {{ key: i, style: {{ marginTop: "2px", fontSize: "12px", paddingLeft: "12px" }} }}, (i + 1) + ". " + step)
              )
            );
          }} else {{
            metaContent = h("span", {{ className: "lane-help" }},
              "Double-click any node to trace the path back to the seed. Click an edge for link details.");
          }}

          return h("div", {{ className: "shell" }},
            h("div", {{ className: "topbar" }},
              h("a", {{ href: "/" }}, "Back to runner"),
              h("div", {{ className: "title" }}, "Run {run_id} process network"),
              h("div", {{ className: "controls" }},
                activeMode === "normal"
                  ? h(React.Fragment, null,
                      "Min degree ",
                      h("input", {{
                        id: "degreeSlider", type: "range", min: 1, max: maxDegree,
                        value: minDegree, onChange: (e) => setMinDegree(Number(e.target.value))
                      }}),
                      h("strong", null, String(minDegree)),
                      h("input", {{
                        type: "search", placeholder: "Search nodes\u2026",
                        value: searchText, onChange: (e) => setSearchText(String(e.target.value || ""))
                      }}),
                    )
                  : null,
                activeMode === "trace"
                  ? h("span", {{ style: {{
                      display: "inline-flex", alignItems: "center", gap: "4px",
                      padding: "3px 8px", borderRadius: "8px", fontSize: "12px",
                      fontWeight: 600, background: "#fff3cd", color: "#664d03"
                    }} }}, "Tracing: " + (traceDesc?.label || ""))
                  : null,
                activeMode !== "normal"
                  ? h("button", {{ type: "button", onClick: resetView }}, "Reset view")
                  : null,
              )
            ),
            h("div", {{ className: "canvas" }},
              h(ReactFlow, {{
                nodes, edges, fitView: true,
                onNodesChange, onEdgesChange, onEdgeClick, onNodeDoubleClick, onPaneClick,
                defaultEdgeOptions: {{ animated: false }},
                proOptions: {{ hideAttribution: true }}
              }},
                h(Background, {{ gap: 22, size: 1 }}),
                h(MiniMap, {{ pannable: true, zoomable: true, nodeColor: (n) => colorForKind(n.kind || "other", n.ofac_hit) }}),
                h(Controls, null)
              )
            ),
            h("div", {{ className: "meta" }}, metaContent)
          );
        }}

        createRoot(document.getElementById("app-root")).render(React.createElement(App));
      }} catch (err) {{
        const root = document.getElementById("app-root");
        if (root) {{
          root.innerHTML = `<div style="padding:16px;font-family:Segoe UI,sans-serif;">
            <h3>Network UI failed to load</h3>
            <p>${{String(err)}}</p>
            <p>Try refreshing or restarting the web server.</p>
          </div>`;
        }}
      }}
    }})();
  </script>
</body>
</html>"""


def _json_for_script(value: Any) -> str:
    # Keep valid JSON in <script> blocks while preventing premature tag close.
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run simple Flask pivot UI.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    app = create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()

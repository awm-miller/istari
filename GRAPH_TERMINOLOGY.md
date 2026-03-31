# Graph Terminology

This document describes how the current graph pipeline and viewer work in practice.

The main implementation lives in:

- `scripts/consolidate_and_graph.py`
- `src/graph/render.py`
- `src/graph/render_context.py`
- `src/graph/render_page.py`
- `src/graph/viewer_app.js`
- `src/graph/viewer_runtime_webgl.js`
- `src/graph/viewer_styles.css`
- `src/graph/viewer_markup.html`
- `scripts/rebuild_graph.py`
- `src/mapping_low_confidence.py`

`src/graph/build.py` is just a thin re-export of the consolidation functions from `scripts/consolidate_and_graph.py`.

## Canonical Build Flow

The normal development/build path is:

1. `python scripts/rebuild_graph.py`
2. `scripts/rebuild_graph.py` loads recent runs from the main SQLite database
3. it calls `src.graph.build.consolidate_multi_run()`
4. `consolidate_multi_run()` delegates to `scripts/consolidate_and_graph.py`
5. the resulting data is rendered by `src/graph/render.py`
6. outputs are written to:
   `output/latest_graph.html`
   `output/graph-data.json`
   `output/graph-data-low-confidence.json`
   `netlify_graph_viewer/index.html`
   `netlify_graph_viewer/graph-data.json`
   `netlify_graph_viewer/graph-data-low-confidence.json`

`src/graph/render.py` is now just a thin entry point.
`render_context.py` prepares the JSON payloads, `render_page.py` assembles the page shell, and the browser runtime lives in the split viewer asset files.

The main graph is embedded directly into the HTML.
The low-confidence overlay is built alongside it as a separate JSON file and loaded at runtime when the overlay toggle is enabled.

## Two Graph Layers

There are now two graph layers:

- `main graph`
  The normal graph built from pipeline/database entities.
- `low-confidence overlay`
  Optional mapping/imported links from `src/mapping_low_confidence.py`.

The overlay is only built if the mapping SQLite database exists.
In the viewer, the main graph is present immediately, while the low-confidence layer is fetched from `graph-data-low-confidence.json` on demand and then merged into the in-browser graph state.

The low-confidence overlay is intentionally optional and visually distinct.

## Lanes

The graph is laid out vertically by lane.

- `lane 0`
  Seed nodes. These exist in the data model but are usually hidden in the UI.
- `lane 1`
  `seed_alias` identity nodes. These are the seed-linked upstream identities that matter most when tracing a network.
- `lane 2`
  Organisation nodes. This includes companies, charities, and other organisations.
- `lane 3`
  Address nodes.
- `lane 4`
  Expanded people, meaning non-seed people pulled from organisation records.

Low-confidence overlay nodes can also occupy lanes 2, 3, or 4 depending on their inferred type.

## Node Kinds

- `seed`
  The original searched/run seed.
- `seed_alias`
  A lane-1 identity node tied to a seed.
- `organisation`
  A lane-2 organisation node.
- `address`
  A lane-3 address node.
- `person`
  A lane-4 person node.

Important organisation sub-types are carried as metadata, not separate kinds:

- `registry_type = company`
- `registry_type = charity`
- other organisation-like values remain generic organisations

Important node flags:

- `sanctioned`
  Added by OFAC screening during graph build.
- `is_low_confidence`
  Marks nodes coming from the mapping overlay.

## Edge Kinds

- `alias`
  Seed to identity edge.
- `role`
  Person or identity to organisation edge.
- `org_link`
  Organisation to organisation edge.
- `address_link`
  Organisation to address edge.
- `hidden_connection`
  A derived viewer-only bridge edge representing a real multi-hop path.
- `mapping_link`
  A low-confidence overlay edge imported from spreadsheets/mapping files.
- `shared_org`
  Present in some intermediate graph data but filtered out of the rendered viewer.
- `cross_seed`
  Also filtered out of the rendered viewer.

The viewer filters out `shared_org` and `cross_seed` before rendering.

## Upstream And Downstream

These terms are about how the graph is interpreted, not about stored edge direction.

- `upstream`
  Moving from organisations or addresses toward lane-1 identities.
- `downstream`
  Moving from identities or organisations toward addresses and lane-4 people.

Typical examples:

- upstream: `identity -> organisation`
- downstream: `organisation -> address`
- downstream: `organisation -> person`

## Identity, Person, And Alias Consolidation

There are several different merge/consolidation concepts in the codebase.

- `alias grouping`
  Happens during graph build. Similar names can be merged into a single logical person/identity grouping.
- `DOB conflict guard`
  Prevents over-merging when identity keys imply conflicting dates of birth.
- `multi-run merge`
  `consolidate_multi_run()` merges compatible entities across recent runs into a single combined graph.
- `manual merge override`
  A viewer-side permanent merge chosen by the user and stored locally plus synced to a server endpoint.

Current permanent merge kinds in the viewer are:

- `address`
- `person`
- `identity`

Viewer merge state is handled in `src/graph/render.py` and synced via `/.netlify/functions/merge-overrides`.
The current viewer runtime no longer exposes merge actions in the node context menu, but the merge override function still exists on the Netlify side.

## Role Semantics

Role edges are normalized semantically during graph build so equivalent wording collapses.

Examples:

- `director`
- `listed as director`
- `appointed as director`

all normalize to the same canonical phrase:

- `is a director of`

The same idea applies to:

- trustees
- secretaries
- accountant/auditor/examiner style governance roles

This matters because dedupe is done on semantic role phrases, not just raw source text.

## Evidence

Evidence is attached to edges, not nodes.

The viewer now expects edge evidence in either of these shapes:

- `edge.evidence`
  Single evidence item.
- `edge.evidence_items`
  Multiple evidence items.

Evidence is surfaced from several sources:

- PDF enrichment evidence
- Companies House role provenance
- low-confidence mapping link evidence

### PDF Evidence

PDF-derived role edges can carry:

- document title
- document URL
- local PDF path
- filing description
- page hint
- page number
- notes
- evidence ID

This is extracted in `scripts/consolidate_and_graph.py` from role provenance created by the PDF enrichment pipeline.

### Companies House Evidence

Companies House role edges can now carry viewer-openable evidence too.

This is derived from role provenance for:

- `companies_house_officer_appointments`
- `companies_house_company_officers`

Typical targets are Companies House officer appointment pages or company pages.

### Evidence Preservation During Merge

One subtle part of the code is that evidence can be lost if edges are deduped carelessly.

The current build logic preserves evidence while:

- deduping same-role edges within a run
- serializing run-level `graph_edges`
- deduping merged edges across runs

This is important because a visible edge may be the result of several equivalent raw source rows.

### Evidence URLs In The Viewer

`src/graph/viewer_app.js` converts evidence payloads into browser URLs.

Special handling exists for Companies House document API URLs:

- raw CH document API links are not browser-friendly
- the viewer routes them through `/.netlify/functions/evidence-file`
- that function can authenticate and redirect to a browser-openable file URL

This is how browser `401` problems are avoided for Companies House documents.

## Right-Click Behavior

There are now two different right-click flows.

- `node context menu`
  Used for node actions such as registry page links, clearing focus, and selecting nodes for connection analysis.
- `edge context menu`
  Used for evidence on the specific link that was clicked.

This separation is intentional:

- node menus are about the entity
- edge menus are about the relationship

## Connection Analysis

Connection analysis is no longer a popup flow.

The current flow is:

1. right-click nodes to select two analysis nodes
2. trigger `Analyze connection`
3. the viewer calls `/.netlify/functions/analyze-connection`
4. the result renders into the `Ranked` sidebar tab

The analysis result can include:

- summary text
- claims
- evidence links
- path items
- a copy button

Low-confidence overlay nodes are excluded from this analysis flow.

## Sidebar, Legend, And Compact Legend

The viewer has a right-hand tools sidebar.

Current sidebar tabs are:

- `Filter`
- `Map`
- `Ranked`

Important related UI pieces:

- `legend`
  Full legend/filter section inside the sidebar.
- `sidebar-handle`
  Arrow button that shows or hides the sidebar.

## Filters

The viewer filters by node type using legend checkboxes.

Current filterable concepts include:

- identities
- charities
- companies
- people
- addresses
- low-confidence overlay
- indirect-only mode

Filtering is implemented in `applyTypeFilters()` and related projection helpers in `src/graph/viewer_app.js`.

## Search And Projection

The viewer does not always render the entire graph.

Instead it computes a visible projection based on:

- current search query
- current type filters
- indirect-only mode
- whether low-confidence overlay is enabled
- focused nodes

Important projection helpers in `src/graph/viewer_app.js` include:

- search projection
- indirect-org projection
- connected-subgraph projection

These projections determine:

- which nodes are visible
- which direct edges are visible
- when derived hidden bridge edges should be shown

## Hidden Connections

`hidden_connection` edges are synthetic viewer edges.

They mean:

- there is a real multi-hop path in the underlying graph
- some intermediate nodes are hidden in the current view
- the viewer still wants to show that a relationship exists

These edges are visually dashed and mainly appear in narrowed search/focus modes.

## Indirect-Only View

Indirect-only is a special discovery mode.

It is designed to surface organisations that are connected through indirect structure rather than simple direct role links.

Typical path ingredients are:

- organisation-to-organisation links
- shared addresses
- upstream identity/person context

The intent is discovery first, then drill down later with normal search.

## Ranked Panel

The `Ranked` tab shows the highest-ranked visible identity/person nodes.

The score shown there is derived from the graph build process and reflects weighted linked roles/organisations after consolidation.

The panel is view-dependent:

- only visible nodes are ranked
- ranking updates when filters/search change

## Map

The `Map` tab renders connected address nodes on a Leaflet map.

Current behavior:

- visible address nodes are considered
- addresses attached to currently visible organisations are also considered
- coordinates are prefetched during `scripts/rebuild_graph.py`
- the prefetched results are written to `output/address-coordinates.json`
- the same file is copied to `netlify_graph_viewer/address-coordinates.json`
- the browser fetches that coordinate payload once and then only shows or hides markers for the currently connected address set
- the map refreshes automatically when the visible graph projection changes while the map tab is open

Address nodes can be merged across runs, so one map point may represent several equivalent address records.

## Low-Confidence Overlay

The low-confidence overlay is built from mapping spreadsheets imported into a separate SQLite database.

Main concepts:

- mapping entities
- mapping links
- mapping evidence
- mapping matches

Overlay edges are matched back onto main graph nodes by normalized label or alias.

If a mapping endpoint cannot be matched uniquely, an overlay-only node is created.

Overlay-only organisation nodes now infer `registry_type` where possible so the current viewer can style charity and company pills consistently with the main graph.

Overlay items are marked with:

- `is_low_confidence: True`

In the viewer they are styled differently and gated behind the low-confidence toggle.
Low-confidence edges are drawn dashed, and low-confidence nodes keep their normal type fill but use a yellow dashed outline.

The overlay can also carry evidence extracted from workbook row descriptions, including URLs.

## PDF Enrichment

`src/services/pdf_enrichment.py` is the main PDF enrichment service.

Important behavior relevant to the graph:

- discovers source documents
- downloads and converts PDFs
- extracts entities and roles
- writes people/organisation-role evidence into the main database

The current graph-related PDF sources include:

- Companies House filings
- Charity Commission `Accounts and TAR`

The Charity Commission path now scrapes the public accounts-and-annual-returns page and builds source documents from those links.

## Companies House Terminology

There are two common Companies House role sources in the graph:

- `companies_house_company_officers`
  Officer data from company officer listings.
- `companies_house_officer_appointments`
  Officer appointment data keyed from the officer side.

These often represent the same underlying role and are semantically merged in the graph.

## Practical Reading Guide

When debugging a graph issue, the fastest order is usually:

1. `scripts/rebuild_graph.py`
   Check what build path is being used.
2. `scripts/consolidate_and_graph.py`
   Check whether the graph data is being constructed correctly.
3. `output/graph-data.json`
   Check whether the final graph payload actually contains the node/edge/evidence you expect.
4. `src/graph/render.py`
   Check whether the page shell is embedding the expected runtime assets.
5. `src/graph/viewer_app.js`
   Check whether projection, tooltips, context actions, evidence labels, or low-confidence loading are changing it.
6. `src/graph/viewer_runtime_webgl.js`
   Check whether the renderer is restyling, culling, or hit-testing it incorrectly.

This is especially important for evidence bugs because evidence can disappear at any of these layers:

- not present in DB provenance
- not extracted into run-level edge data
- lost during dedupe
- not serialized into final graph edges
- hidden by viewer logic

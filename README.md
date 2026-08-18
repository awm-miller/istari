# Project Istari

Istari turns a natural-language investigation brief into an editable discovery
plan, runs bounded UK registry discovery, and renders the result as an
evidence-first relationship graph.

The production application is [projectistari.netlify.app](https://projectistari.netlify.app).
Netlify serves the graph viewer and proxies protected discovery requests to the
separate Istari API and worker on DigitalOcean.

## Production flow

1. The Builder sends either a natural-language brief or a directly completed research contract to the backend.
2. Briefs use strict-schema OpenRouter planning; direct contracts skip the model and open at approval.
3. The user reviews and changes the plan before approving it.
4. The backend runs a resumable Companies House and Charity Commission work queue.
5. Completed graph versions appear at `/generated-graphs/<case-id>/`.

The review form has one control per executable policy: graph name and optional
URL key, typed seeds, complete expansion cycles, entity ceiling, people expansion,
default-on document enrichment, former roles, and a mapped nearby-address radius.
The progress strip opens the live run log. Generated graphs can be
deleted from the graph menu after confirmation; deletion also removes their
stored graph job state.

Generated graph node menus can add the direct relationships of one address,
person, company, or charity. For example, expanding a company adds its officers
without continuing through their addresses or other organisations. The viewer
also supports Ctrl-click selection of up to 25 nodes. One selected-node action
can expand all valid centres in a single durable task or promote all selected
people to seeds in one atomic write. Selecting exactly two compatible nodes
also enables a merge action; the first selected node remains visible. Expanding
a merged node expands every registry identity represented by that node. This
avoids competing graph versions.
The viewer opens the new immutable version automatically, preserves the current node
positions and viewport, repairs stale-position collisions, and shows newly
added nodes immediately. The same centre-node action can hide that latest
round. A final evidence-based person-resolution pass runs before publication.

Discovery pivots through organisations, addresses, and optionally people.
Every researched relationship keeps its source URL, exact node referents, provider,
dates, confidence, and a structured evidence record. The viewer also presents
legacy registry edges that predate structured evidence by using their stored
source fields.

## Quick start

Requirements: Python 3.11 or later, Node.js 22 or later, and the registry/API
credentials listed in `.env.example`.

```powershell
git clone https://github.com/awm-miller/istari.git
cd istari
python -m venv .venv
.venv\Scripts\python -m pip install -e .
npm install
Copy-Item .env.example .env
.venv\Scripts\python -m src.cli init-db
.venv\Scripts\python -m src.cli web-ui --port 8765
```

Open `http://127.0.0.1:8765/mb/` and select **Builder**.

The local case CLI uses the same bounded case contract:

```powershell
istari plan-case "Map organisations connected to 94 Park Avenue North, London, NW10 1JY"
istari run-case data/cases/94-park-avenue-north/case.yaml
istari discover "Map organisations connected to 94 Park Avenue North, London, NW10 1JY"
```

Case state is isolated under `data/cases/<case-id>/`. The schemas are in
`docs/istari-case-v1.schema.json` and `docs/istari-graph-v1.schema.json`.

## AI routing

Case planning and entity resolution use OpenRouter chat
completions. The default is `~deepseek/deepseek-v4-flash-latest`. Requests set
`provider.data_collection=deny` and `provider.zdr=true`, so OpenRouter must
select a zero-data-retention route or fail. Registry records remain the source
of truth; model memory is not accepted as relationship evidence.

Planning disables model reasoning so the output budget is reserved for the
research contract. Explicit locations and registry identifiers are interpreted
by the model but are not sent to web search. Pasted Companies House officer
appointment links use their exact officer ID and bypass fuzzy person matching.
Manual resolution decisions can merge duplicate identities and promote the
canonical person to a reversible seed identity across graph rebuilds. Promotion
moves the person to a dedicated root lane above addresses and makes them the
active graph root.
Bulk identity consolidation is stored in one atomic graph-scoped write so
closely spaced decisions cannot replace one another.

After interpretation, direct address inputs without a nearby radius are
normalized to the address-network route and its three-round default.

Production document enrichment uses OpenRouter only. It checks every registry-listed
Companies House account filing and Charity Commission account or annual-return document for explicit named relationships;
these appear as source-backed document evidence rather than registry facts.

## Validation

```powershell
.venv\Scripts\python -m pytest
node --test tests/netlify_graph_functions.test.js tests/netlify_job_pump.test.mjs tests/netlify_sites_proxy.test.js
npm run audit
```

Refresh viewer shells after frontend changes:

```powershell
.venv\Scripts\python -m scripts.build_multi_graph_site --viewer-only
```

## Architecture

The local Python discovery pipeline remains available for direct and offline
work. The production Builder uses the durable Node worker and SQLite store in the
separate private `istari-juicer-sites` repository. Netlify contains no registry
credentials and adds the shared proxy token server-side.
Job-list `status` and `limit` filters are allowlisted through this proxy; other
query parameters are not forwarded to the backend.

See [docs/HANDOFF.md](docs/HANDOFF.md) for deployment, environment, recovery,
and source-of-truth details.

# Project Istari

Istari turns a natural-language investigation brief into an editable discovery
plan, runs bounded UK registry discovery, and renders the result as an
evidence-first relationship graph.

The production application is [projectistari.netlify.app](https://projectistari.netlify.app).
Netlify serves the graph viewer and proxies protected discovery requests to the
separate Istari Juicer service on ChatGPT Sites.

## Production flow

1. The Builder sends a natural-language brief to the Sites backend.
2. OpenRouter plans researched subjects, exact entities, addresses, and editable operations.
3. The user reviews and changes the plan before approving it.
4. The backend runs a resumable Companies House and Charity Commission work queue.
5. Completed graph versions appear at `/generated-graphs/<case-id>/`.

Discovery pivots through organisations and addresses. People are graph leaves.
Every researched relationship keeps its source URL and exact node referents.

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

Case planning, entity resolution, and graph questions use OpenRouter chat
completions. The default is `~deepseek/deepseek-v4-flash-latest`. Requests set
`provider.data_collection=deny` and `provider.zdr=true`, so OpenRouter must
select a zero-data-retention route or fail. Registry records remain the source
of truth; model memory is not accepted as relationship evidence.

Some legacy enrichment commands still use Gemini when their specific feature
is enabled. They are not part of the production Builder path.

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
work. The production Builder uses the durable Sites worker and D1 store in the
separate private `istari-juicer-sites` repository. Netlify contains no registry
credentials and adds the shared proxy token server-side.

See [docs/HANDOFF.md](docs/HANDOFF.md) for deployment, environment, recovery,
and source-of-truth details.

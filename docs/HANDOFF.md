# Istari handoff

## Source of truth

- Frontend and local CLI: `https://github.com/awm-miller/istari`, branch `main`.
- Discovery backend: `https://github.com/awm-miller/istari-juicer-sites`, branch `main`.
- Production frontend: `https://projectistari.netlify.app`.
- Production backend: `https://istari.168-144-192-99.sslip.io`.
- Generated graph pages use `netlify_graph_viewer/generated-viewer-template.html`; copy that built template to the backend repository's `public/generated-viewer-template.html` before backend deployment.

The Netlify site is the only browser application surface. The backend API and
generated graphs reject requests that do not include the server-side proxy token.

## Service boundary

Netlify owns the viewer, Builder, graph-specific functions, password gate, and
authenticated proxy. The DigitalOcean API and continuous worker own planning,
registry and document adapters, the durable SQLite queue, immutable graph
versions, and generated graph responses.

Planning uses OpenRouter strict JSON-schema output with reasoning disabled and
requires a provider that supports the requested parameters. Explicit locations
remain model-interpreted but do not trigger web research. Exact Companies House
officer appointment links are authoritative inputs and bypass fuzzy name search.
The planner fills the same typed seed and expansion contract that Manual search
edits. It does not run registry or document discovery while planning.

The Netlify proxy must preserve the requested path and add
`X-Istari-Proxy-Token`. The same token must be stored as
`ISTARI_SITES_PROXY_TOKEN` on Netlify and `ISTARI_PROXY_TOKEN` on the backend.

## Environment

Netlify requires:

- `ISTARI_SITES_ORIGIN`
- `ISTARI_SITES_PROXY_TOKEN`
- `OPENROUTER_API_KEY`
- `OPENROUTER_RESOLUTION_MODEL`

The droplet backend requires:

- `OPENROUTER_API_KEY`
- `OPENROUTER_CASE_MODEL`
- `OPENROUTER_RESOLUTION_MODEL`
- `COMPANIES_HOUSE_API_KEY`
- `CHARITY_COMMISSION_API_KEY`
- `SERPER_API_KEY`
- `ISTARI_PROXY_TOKEN`

Do not commit values. Use `~deepseek/deepseek-v4-flash-latest` for both
OpenRouter model variables. OpenRouter calls require ZDR routes in code.

## Validate and deploy

Frontend:

```powershell
.venv\Scripts\python -m pytest
node --test tests/netlify_graph_functions.test.js tests/netlify_job_pump.test.mjs tests/netlify_sites_proxy.test.js
npm run audit
.venv\Scripts\python -m scripts.build_multi_graph_site --viewer-only
npx netlify deploy --prod --dir netlify_graph_viewer
```

Backend:

```powershell
npm install
npm test
```

Deploy the tested backend commit with `deploy/release-server.sh`. The release
script installs production dependencies and restarts `istari-api.service` and
`istari-worker.service`. Do not replace the live SQLite WAL files.

## Acceptance checks

1. Open `/mb/`, select Builder, and test both a natural-language brief and **Manual search**. Use multiple paraphrases of the same explicit-address request and one pasted Companies House officer appointment link.
2. Confirm the graph name and optional URL key persist, and click the progress strip to inspect the run log.
3. Confirm typed seeds, cycles, people expansion, default-on document enrichment, nearby controls, and limits appear before approval.
4. Change a control, approve the plan, and confirm the run log advances without duplicate work.
   Planning output should show public model and registry status, never hidden chain-of-thought.
5. Open the generated graph and verify that entity-address edges point to the specific researched entities named by their evidence.
6. Check repeated edge tooltips, graph selection, deletion confirmation, duplicate resolution, selected-subgraph questions, and immutable version URLs.
7. Check that a promoted person becomes the active top-level seed identity and can be restored without losing merged edges.
8. On a generated graph, set a node as an enrichment centre, apply a viewer filter, and confirm **Enrich** creates the next immutable version without changing the prior version. Repeat with relationship expansion off to test missing-PDF-only backfill.
9. Check both systemd service logs after the run and treat any 5xx response as a failed acceptance test.

## Recovery

Jobs are resumable. The continuous worker claims durable SQLite queue items; the
browser only observes them by SSE or polling. Restart the worker rather than
deleting queue state. Redeploy the last known-good backend commit if a release
fails, and redeploy the matching Netlify commit if the proxy contract changes.

The archived VPS backup under `server_backups/` is local recovery material. It
is deliberately excluded from Git and is not part of the active deployment.

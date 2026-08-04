# Istari handoff

## Source of truth

- Frontend and local CLI: `https://github.com/awm-miller/istari`, branch `main`.
- Discovery backend: `https://github.com/awm-miller/istari-juicer-sites`, branch `main`.
- Production frontend: `https://projectistari.netlify.app`.
- Production backend: `https://istari-juicer.alexmiller3146.chatgpt.site`.
- Sites project ID: `appgprj_6a634f96ceec8191b73dc194d003265c`.

The Netlify site is the only public application surface. The Sites root returns
404, and its API and generated graphs reject requests that do not include the
server-side proxy token.

## Service boundary

Netlify owns the viewer, Builder, graph-specific functions, password gate, and
15-minute background job pump. ChatGPT Sites owns natural-language planning, registry
adapters, the durable work queue, graph checkpoints, immutable graph versions,
and generated graph responses.

The Netlify proxy must preserve the requested path and add
`X-Istari-Proxy-Token`. The same token must be stored as
`ISTARI_SITES_PROXY_TOKEN` on Netlify and `ISTARI_PROXY_TOKEN` on Sites.

## Environment

Netlify requires:

- `ISTARI_SITES_ORIGIN`
- `ISTARI_SITES_PROXY_TOKEN`
- `OPENROUTER_API_KEY`
- `OPENROUTER_RESOLUTION_MODEL`

Sites requires:

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

Deploy the tested backend commit through ChatGPT Sites using the project ID in
`.openai/hosting.json`. Save a Sites version from that exact pushed commit, then
deploy the saved version. Do not deploy an uncommitted archive.

## Acceptance checks

1. Open `/mb/`, select Builder, and submit a natural-language brief.
2. Confirm researched subjects, exact entities, addresses, evidence links, and editable operations appear before approval.
3. Change an operation or limit, approve the plan, and confirm stdout advances without duplicate work.
   Planning output should show model passes and public web or registry tool status, never hidden chain-of-thought.
4. Open the generated graph and verify that entity-address edges point to the specific researched entities named by their evidence.
5. Check repeated edge tooltips, graph selection, duplicate resolution, selected-subgraph questions, and immutable version URLs.
6. Check Sites worker logs after the run and treat any 5xx response as a failed acceptance test.

## Recovery

Jobs are resumable. Browser polling and the Netlify scheduled pump can acquire
a short lease and continue the queue. Do not restart a case by deleting D1
state. Redeploy the last known-good saved Sites version if a backend release
fails, and redeploy the matching Netlify commit if the proxy contract changes.

The archived VPS backup under `server_backups/` is local recovery material. It
is deliberately excluded from Git and is not part of the active deployment.

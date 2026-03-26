# Project Istari

CLI pipeline for linking people to England & Wales charities and companies.

Given one or more **seed names**, Istari searches UK public registries, resolves entity matches with AI, expands the network of connected organisations and people, and screens results against the OFAC sanctions list — producing a ranked, explorable network graph.

## Architecture

![Pipeline Architecture](docs/architecture.svg)

### Pipeline stages

| Stage | What happens |
|---|---|
| **Step 1 — Seed Expansion** | Generate name variants from the seed, search the Charity Commission API, Companies House API, and web (Serper). A rules-first entity-resolution layer uses Gemini / OpenAI only for ambiguous candidates. |
| **Step 2 — Org Expansion** | Walk linked charities and companies outward from Step 1 matches. An address-pivot search finds additional organisations registered at the same addresses. |
| **Step 2b — PDF Enrichment** | Download charity/company PDFs (annual reports, accounts) and extract structured data via OpenDataLoader + Gemini, including connection phrasing and explanatory detail for mentions. |
| **Step 3 — People Expansion** | Discover trustees, directors, secretaries, and officers for every scoped organisation. Rank people by the number and weight of their connections. |
| **Step 4 — OFAC Screening** | Screen the ranked people list against the US Treasury OFAC SDN sanctions list. |

### Data sources & services

- **Charity Commission for England & Wales** — charity search, details, linked entities
- **Companies House** — officer search, company profiles, appointments
- **Gemini / OpenAI** — entity resolution, PDF extraction, name-variant generation
- **Serper** — web search for supplementary evidence
- **OFAC SDN list** — sanctions screening

### Storage & output

- **SQLite** — all entities, relationships, resolution decisions, and run metadata
- **Flask web UI** — serves an interactive network graph at `localhost:5000`
- **JSON export** — graph payload for the Netlify viewer
- **Graph rebuild** — merges person aliases and equivalent addresses across runs so shared edges collapse onto common nodes

## Quick start

```bash
# Install
pip install -e .

# Set API keys in .env
cp .env.example .env

# Initialise the database
python -m src.cli init-db

# Run the full pipeline for a seed name
python -m src.cli run-name "Jane Smith"

# Or run multiple seeds with overlap analysis
python -m src.cli run-seeds "Jane Smith" "John Doe"

# Launch the web UI
python -m src.cli web-ui

# Rebuild the combined graph from all saved runs
python scripts/rebuild_graph.py
```

## CLI commands

| Command | Description |
|---|---|
| `init-db` | Create the SQLite schema |
| `step1-seed NAME` | Expand a single seed name |
| `step2-orgs RUN_ID` | Expand connected organisations |
| `pdf-enrich RUN_ID` | Enrich from charity/company PDFs |
| `step3-people RUN_ID` | Expand connected people |
| `step4-ofac RUN_ID` | OFAC sanctions screening |
| `run-name NAME` | Full pipeline for one seed |
| `run-seeds NAME [NAME ...]` | Full pipeline per seed + overlap |
| `rank` | Rank people by connections |
| `export-network --run-id ID` | Export graph as JSON |
| `web-ui` | Launch the Flask web UI |
| `healthcheck` | Check API keys and tooling |

# Project Istari

Project Istari starts a person and asks:

"Which charities, companies, people, watchlists, and news hits are connected to them?"

It does this by searching UK public registries, deciding which hits are really about the same person, expanding outward through organisations and officers or trustees, then rebuilding everything into one combined graph that can be reviewed in a browser. The best bit of this project is the database viewer which can be adapted quite flexibly into other data sources. I'm not wild about the other stuff -- it's a work in progress.

## Architecture

![Project Istari pipeline](docs/architecture_dark.png)

Open the detailed version: [`docs/architecture.svg`](docs/architecture.svg)

## What the system does

 Istari makes a list of likely spelling variants for that person's name. It then searches the Charity Commission and Companies House for possible matches.

Next, it decides which search hits are really about the same person. Easy cases are handled by rules. Hard cases can go to an AI model. Once a hit is accepted, the linked organisation becomes part of that seed's run.

After that, Istari starts expanding outward from the organisations it has found.

It looks for:

- linked charities from Charity Commission
- a company and a charity with the same normalised name
- other organisations at the same address
- extra organisations mentioned in PDFs and filings

Then it expands the people attached to those organisations.

That means:

- trustees for charities
- officers, directors, and similar roles for companies

Then it keeps looping only through organisations.

In practice, that means newly discovered trustees or officers are kept as graph people and used for ranking, sanctions, and review, but they do not become fresh external search seeds inside the same run.

So the real discovery loop is:

1. find initial organisations from the seed
2. expand connected organisations
3. expand people from those organisations
4. run PDF enrichment across in-scope organisations
5. repeat organisation and address expansion until the run stops growing or hits its round limit

That is the core of the pipeline.

## What happens during discovery

### 1. Seed search

The system starts with a seed name and searches the UK registries for likely matches.

In the standard CLI pipeline, the main sources are:

- Charity Commission
- Companies House

Some extra discovery paths depend on API keys or feature flags being enabled.

### 2. Entity resolution

Search results are messy. The same person can appear under many spellings, and different people can share similar names.

Istari scores each candidate and decides whether it is:

- a real match
- a maybe
- not the same person

Only real matches automatically drive the network outward.

### 3. Organisation expansion

Once an organisation is in scope, Istari tries to find nearby organisations in several ways:

- a charity linked to that charity
- the same organisation appearing in the other registry
- another organisation at the same address
- an organisation mentioned in a filing or annual report PDF

This is how the graph starts to spread out from one initial seed.

### 4. People expansion

For every in-scope organisation, Istari pulls the people attached to it.

That usually means:

- trustees for charities
- officers and directors for companies

These people are stored as graph nodes and role edges.

### 5. PDF enrichment

Once an organisation is already in scope and already has people linked to it, Istari can run PDF enrichment over filings, annual reports, and similar documents for that organisation.

This can add:

- extra person evidence
- resolved organisation mentions
- unresolved organisation mentions that stay visible as low-confidence review nodes



### 4. Ranking

At the end of a run, people are ranked by how strongly they connect into the discovered organisation network.

In simple terms, people rise higher when they connect to more important or more numerous organisations in that seed's run.

### 7. Sanctions screening

The ranked people are screened against sanctions data.

Those results are saved so they can be reused later instead of recomputed every time.

Once you have multiple runs, Istari rebuilds one combined graph from the latest run for each seed.

This rebuild stage is separate from discovery.

During rebuild, the system:

1. picks the latest run for each seed
2. merges duplicate people across runs
3. merges shared organisations and addresses
4. refreshes or reuses sanctions data
5. attaches Egypt judgments hits
6. attaches adverse-media or negative-news hits
7. builds the optional open-letters and low-confidence-node overlays
8. writes the viewer outputs

The viewer also supports two separate review overlays.

`Open letters` is the existing mapping-derived review layer. It covers signatory data, open-letter style evidence, and similar spreadsheet-imported links that should stay visually separate from the main graph.

`Low confidence nodes` are unresolved PDF-extracted organisation mentions. These are cases where the PDF extraction found an organisation name, but the pipeline could not confidently resolve it to a Charity Commission or Companies House record. They stay visible as review nodes instead of being dropped entirely.

Both overlays are built separately and exported as their own JSON payloads so they can be turned on and off independently in the viewer.

## What gets written out

After rebuild, the project writes:

- the main HTML viewer
- the main graph JSON
- the open-letters overlay JSON
- the low-confidence-nodes overlay JSON
- the address coordinate JSON used by the viewer

These outputs are written into `output/` and copied into `netlify_graph_viewer/` when that folder exists.

## Main data sources

| Source | What it is used for |
|---|---|
| Charity Commission for England and Wales | charity search, trustees, linked charities |
| Companies House | officer search, company records, appointments |
| Serper | web search for some registry discovery and adverse media |
| Gemini / OpenAI | entity resolution, PDF extraction, translation, article classification |
| Local sanctions data | sanctions screening |
| `data/egypt_judgments_screen.json` | curated Egypt judgments annotation during rebuild |
| `data/negative_news.sqlite` | stored adverse-media results |





## Org-anchored runs

`run-orgs` is the trusted-root version of the pipeline.

Use it when you already know that one or more charities or companies are correct, but a person-name seed is too ambiguous. In those cases, `run-name` can drift because it has to resolve the name first. `run-orgs` skips that first person-match step and starts directly from the organisation roots you provide.

That means the run will:

- treat the supplied charities or companies as known-good anchors
- expand outward through the usual organisation discovery flow
- pull trustees, officers, directors, and similar direct people from those anchored organisations
- still rank people and run sanctions screening at the end

This is useful for cases like “the charity cluster is right, but the person name keeps matching the wrong records”.



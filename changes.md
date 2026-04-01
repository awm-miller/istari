## Recent Work Summary

This file now describes the recent graph-viewer and Netlify work that landed across the latest commits, rather than the original scratch task list.

## Low-Confidence Mapping Overlay

- Added a separate low-confidence import pipeline for Mapping spreadsheets, backed by `src/mapping_low_confidence.py`.
- Low-confidence links and unmatched entities are exported into `graph-data-low-confidence.json`.
- The viewer can load that overlay on demand instead of mixing it directly into the main merged graph.
- Rich low-confidence evidence and tooltip support were explored in later iterations, but the current viewer behavior has been intentionally simplified back toward the last stable interaction model.

## Viewer Interaction Improvements

- Added persistent manual merge support for addresses, people, and identities through the browser context menu.
- Added right-click actions to open relevant external registry pages for nodes when registry metadata exists.
- Added connection-analysis and evidence-opening support in the viewer.
- Added address map support for the currently visible address nodes.
- Added a ranked side panel showing the top scored visible identity/person nodes.

## Low-Confidence Viewer Stability Rollback

- The low-confidence overlay previously expanded into a much larger full overlay network and then into a WebGL/Pixi renderer experiment.
- That newer renderer path caused hangs and browser crashes when enabling the overlay.
- The current implementation was rolled back to the older, more stable overlap-first rendering model.
- In the current model, the viewer primarily shows low-confidence links that intersect the main graph.
- Low-confidence organisation hubs can be expanded via right click to reveal their linked low-confidence people.

## Current Low-Confidence Behavior

- The low-confidence toggle is off by default.
- Enabling it loads the imported low-confidence overlay data.
- The default view favors overlap with the main graph rather than drawing the entire low-confidence network at once.
- Low-confidence organisation expansion is available through the node context menu.
- Tooltips now explain why a low-confidence node or edge is visible when it comes from overlap or expansion.

## Filters and UI

- Added a dedicated low-confidence toggle in the viewer controls.
- Added a filter for `Other organisation` so it behaves like the other node-type filters.
- Existing filters remain available for identities, charities, companies, addresses, and people.

## Evidence and Netlify Functions

- Added `netlify/functions/evidence-file.js` so evidence documents can be opened from the deployed viewer.
- Updated `netlify/functions/merge-overrides.js` so identity merges are handled alongside address and person merges.
- The deployed site continues to use Netlify Functions for viewer-side actions that need server support.

## Deployment

- Netlify CLI is now part of the repo tooling.
- The project is linked to `https://projectistari.netlify.app`.
- Recent production deployments were run from the current working tree after the low-confidence stability rollback.

## Current State

- `main` includes the stable overlap-only low-confidence viewer rollback.
- `main` also includes the evidence helper, organisation filter, and remaining viewer polish from the latest commit set.
- The present focus is stability first, then iterating forward from the known-good renderer behavior rather than from the failed WebGL rewrite.

## Sanctions Screening

- Added a baked sanctions-screening pass that stores results in `person_sanctions` instead of relying on live viewer-time checks.
- The current screening combines OFAC SDN, the UK Sanctions List, France `Direction Generale du Tresor`, and a one-time live Germany `Finanzsanktionsliste` lookup.
- Overlapping hits are deduplicated before storage so repeated EU-style results from France and Germany collapse into one stored match set.
- Graph export now reads sanctions from the database and marks person nodes with `sanctioned: true`, plus a tooltip warning line naming the matched source set.
- The combined graph was smoke-tested from a copied database and then fully baked on `data/charity_links.sanctions-test-combined.sqlite`.
- That copied combined bake screened `2047` ranked people and persisted `36` sanctioned hits before the combined graph was rendered from the copied database.
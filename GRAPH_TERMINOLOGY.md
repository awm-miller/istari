# Graph Terminology

This file is a quick reference for the terms used in the generated graph view in `scripts/consolidate_and_graph.py`.

## Lanes

- `lane 1`: `Identity`
  Seed-linked identity / alias nodes. These are the upstream people we care about when tracing from a searched org or address.
- `lane 2`: `Organisations`
  Companies, charities, and other org-like entities.
- `lane 3`: `Addresses`
  Shared or registered addresses linked to organisations.
- `lane 4`: `People`
  Non-seed people pulled in from org records.

`lane 0` seed nodes exist in the data model but are normally hidden in the graph UI.

## Direction

- `upstream`
  Moving from an organisation or address toward `Identity` nodes.
  Example: `identity -> organisation` is an upstream relationship when viewed from the organisation.
- `downstream`
  Moving from an `Identity` or `Organisation` toward addresses and lane-4 people.
  Example: `organisation -> address` and `organisation -> person`.

In search mode, we now treat upstream and downstream differently depending on the view:

- normal search: show both direct and indirect upstream links, plus downstream context
- indirect-orgs view: show only upstream identity context for qualifying orgs

## Node Types

- `seed`
  Original searched name. Usually hidden in the UI.
- `seed_alias`
  The lane-1 identity node associated with a seed.
- `organisation`
  A company, charity, or similar entity in lane 2.
- `address`
  An address node in lane 3.
- `person`
  A lane-4 individual from organisation records.

## Edge Types

- `role`
  Direct connection between an individual and an organisation.
  This can connect:
  - `Identity -> Organisation`
  - `Person -> Organisation`
- `address_link`
  Direct connection between an organisation and an address.
- `org_link`
  Direct connection between two organisations.
  Used for merged or related organisations and important for indirect traversal.
- `hidden_connection`
  A derived dashed edge shown only in search/focus-style views.
  It represents a real multi-hop path whose intermediate nodes are hidden or not shown in that view.

## Direct vs Indirect

- `direct connection`
  A real edge that exists in `allEdges`.
  Example: `Identity --role--> Organisation`.
- `indirect connection`
  A connection discovered by traversing multiple real edges.
  Example: `Identity -> OrgA -> shared address -> OrgB`.

Indirect connections are mainly surfaced through `findBridgeConnections()`.

## Search View

`searchOrFocusMode` is the special rendering mode used for:

- text search
- the indirect-orgs checkbox view

In this mode, the graph can show:

- a narrowed set of matched nodes
- direct visible edges
- derived dashed `hidden_connection` edges for indirect relationships

## Indirect Orgs View

The `reveal indirectly connected orgs` checkbox is now treated as a dedicated filtered view:

- it finds organisations where `2+` active individuals are connected indirectly
- indirect means reachable through `org_link` and/or shared-address paths
- it filters the graph down to those orgs
- it shows upstream identity context only
- it keeps downstream addresses and lane-4 people hidden

This view is intended for discovery. If you want to inspect one org in full, use normal search from the full graph view.

## Generic Indirect Org Example

A typical indirectly connected organisation looks like this:

- Identity A has a direct role at Org A
- Identity B has a direct role at Org B
- Org A and Org B connect to Org C through `org_link` and/or shared-address paths
- Identity A and Identity B do not have direct role edges to Org C itself

That is the canonical pattern for an `indirectly connected organisation`.

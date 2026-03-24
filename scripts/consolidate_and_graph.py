"""Post-processing script: consolidate alias people in a run and produce
a self-contained HTML network graph with 4-lane vertical layout.

Lanes:
  0  Seed name
  1  Consolidated seed-person aliases
  2  Organisations
  3  Expanded people (from org records)

Usage:
    python scripts/consolidate_and_graph.py <run_id> [--out graph.html]
"""
from __future__ import annotations

import argparse
import html
import json
import sys
import webbrowser
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import load_settings
from src.ofac.screening import OFACScreener
from src.resolution.features import person_name_similarity
from src.search.queries import (
    _FUZZY_SWAPS,
    _SPELLING_CLUSTERS,
    normalize_name,
)
from src.storage.repository import Repository

# ---------------------------------------------------------------------------
# Alias detection
# ---------------------------------------------------------------------------

_CLUSTER_CANONICAL: dict[str, str] = {}
for _cluster in _SPELLING_CLUSTERS:
    canonical = sorted(_cluster)[0]
    for member in _cluster:
        _CLUSTER_CANONICAL[member] = canonical


def _canonical_token(token: str) -> str:
    return _CLUSTER_CANONICAL.get(token, token)


def _fuzzy_surname_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if _canonical_token(a) == _canonical_token(b):
        return True
    for old, new in _FUZZY_SWAPS:
        if a.replace(old, new) == b or b.replace(old, new) == a:
            return True
    if len(a) > 3 and len(b) > 3 and person_name_similarity(a, b) >= 0.75:
        return True
    return False


def _alias_tokens(name: str) -> list[str]:
    if "," in name:
        parts = name.split(",", 1)
        surname_raw = normalize_name(parts[0])
        given_raw = normalize_name(parts[1])
        tokens = given_raw.split() + surname_raw.split()
    else:
        tokens = normalize_name(name).split()
    return [_canonical_token(t) for t in tokens if t]


def are_aliases(name_a: str, name_b: str) -> bool:
    tokens_a = _alias_tokens(name_a)
    tokens_b = _alias_tokens(name_b)
    if not tokens_a or not tokens_b:
        return False
    surname_a = tokens_a[-1]
    surname_b = tokens_b[-1]
    if not _fuzzy_surname_match(surname_a, surname_b):
        return False
    given_a = set(tokens_a[:-1])
    given_b = set(tokens_b[:-1])
    if not given_a or not given_b:
        return bool(given_a) == bool(given_b)
    return given_a.issubset(given_b) or given_b.issubset(given_a)


def _load_screener() -> OFACScreener:
    screener = OFACScreener()
    sdn_path = Path(__file__).resolve().parents[1] / "data" / "sdn.csv"
    if sdn_path.exists():
        screener.load_csv(sdn_path)
    return screener


_screener: OFACScreener | None = None


def get_screener() -> OFACScreener:
    global _screener
    if _screener is None:
        _screener = _load_screener()
    return _screener


def is_sanctioned(name: str) -> bool:
    return len(get_screener().screen_name(name)) > 0


class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        self._parent.setdefault(x, x)
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]
            x = self._parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _row_str(row, key: str) -> str:
    try:
        return str(row[key] or "")
    except (IndexError, KeyError):
        return ""


def _role_phrase(edge) -> str:
    phrase = _row_str(edge, "relationship_phrase").strip()
    if "named as a trustee" in phrase.lower():
        return "is a trustee of"
    if phrase:
        return phrase
    rt = _row_str(edge, "role_type").lower()
    if "trustee" in rt:
        return "is a trustee of"
    if "director" in rt:
        return "is a director of"
    if "secretary" in rt:
        return "is a secretary of"
    if "accountant" in rt or "examiner" in rt or "auditor" in rt:
        return "is listed in governance/finance docs for"
    return "is linked to"


def _role_key(edge) -> tuple[int, str]:
    return (int(edge["organisation_id"]), _role_phrase(edge))


def _tag_sanctioned_nodes(nodes: list[dict]) -> None:
    for node in nodes:
        if node.get("kind") not in ("seed", "seed_alias", "person"):
            continue
        names = [node["label"], *(node.get("aliases") or [])]
        if any(is_sanctioned(n) for n in names):
            node["sanctioned"] = True
            warning = "\u26a0\ufe0f <strong>SANCTIONED (OFAC SDN)</strong>"
            tooltip_lines = list(node.get("tooltip_lines") or [])
            if not tooltip_lines or tooltip_lines[0] != warning:
                node["tooltip_lines"] = [warning, *tooltip_lines]


def consolidate_run(run_id: int) -> dict:
    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()

    ranked = repository.get_ranked_people_for_run(run_id, limit=500)
    raw_edges = repository.get_run_network_edges(run_id)
    address_rows = repository.get_run_address_edges(run_id)
    run_row = repository.get_run(run_id)
    seed_name = str(run_row["seed_name"]) if run_row else "Seed"

    people = [
        {
            "person_id": int(r["id"]),
            "name": str(r["canonical_name"]),
            "org_count": int(r["organisation_count"]),
            "role_count": int(r["role_count"]),
            "score": float(r["weighted_organisation_score"]),
        }
        for r in ranked
    ]

    # --- alias grouping ---
    uf = UnionFind()
    for i, a in enumerate(people):
        for j in range(i + 1, len(people)):
            if are_aliases(a["name"], people[j]["name"]):
                uf.union(i, j)

    groups: dict[int, list[int]] = {}
    for i in range(len(people)):
        groups.setdefault(uf.find(i), []).append(i)

    person_id_to_group_id: dict[int, str] = {}
    consolidated: list[dict] = []
    for _root, members in groups.items():
        entries = [people[i] for i in members]
        all_names = sorted(set(e["name"] for e in entries), key=lambda n: len(n))
        label = max(all_names, key=len)
        pid_set = {e["person_id"] for e in entries}
        group_id = f"person:{min(pid_set)}"

        org_ids: set[int] = set()
        role_keys: set[tuple[int, str]] = set()
        total_weight = 0.0
        for edge in raw_edges:
            if int(edge["person_id"]) in pid_set:
                org_ids.add(int(edge["organisation_id"]))
                role_keys.add(_role_key(edge))
                total_weight += float(edge["edge_weight"] or 0)

        consolidated.append({
            "group_id": group_id,
            "label": label,
            "aliases": sorted(set(e["name"] for e in entries)),
            "person_ids": sorted(pid_set),
            "org_count": len(org_ids),
            "role_count": len(role_keys),
            "score": round(total_weight, 4),
            "is_seed_alias": are_aliases(seed_name, label),
        })
        for pid in pid_set:
            person_id_to_group_id[pid] = group_id

    consolidated.sort(key=lambda c: (-c["score"], -c["org_count"], c["label"]))

    seed_aliases = [c for c in consolidated if c["is_seed_alias"]]
    expanded_people = [c for c in consolidated if not c["is_seed_alias"]]

    # --- build edges with full metadata ---
    org_map: dict[int, dict] = {}
    for edge in raw_edges:
        oid = int(edge["organisation_id"])
        if oid not in org_map:
            org_map[oid] = {
                "id": f"org:{oid}",
                "label": str(edge["organisation_name"]),
                "registry_type": str(edge["registry_type"] or ""),
                "registry_number": str(edge["registry_number"] or ""),
            }

    address_map: dict[int, dict] = {}
    org_addresses: dict[str, list[dict]] = defaultdict(list)
    for row in address_rows:
        aid = int(row["address_id"])
        if aid not in address_map:
            address_map[aid] = {
                "id": f"addr:{aid}",
                "label": str(row["address_label"] or ""),
                "postcode": str(row["postcode"] or ""),
                "country": str(row["country"] or ""),
            }
        org_addresses[f"org:{int(row['organisation_id'])}"].append(
            {
                "address_id": aid,
                "label": str(row["address_label"] or ""),
                "phrase": str(row["relationship_phrase"] or "is registered at"),
                "source": str(row["source"] or ""),
            }
        )

    # person→org edges with role detail
    person_org_edges: list[dict] = []
    seen_po: set[tuple[str, str, str]] = set()
    # also track per-person their roles for tooltip
    person_roles: dict[str, list[dict]] = defaultdict(list)
    # track per-org their people for tooltip
    org_people: dict[str, list[dict]] = defaultdict(list)

    for edge in raw_edges:
        pid = int(edge["person_id"])
        gid = person_id_to_group_id.get(pid)
        if not gid:
            continue
        org_id = f"org:{int(edge['organisation_id'])}"
        role_type = str(edge["role_type"] or "link")
        role_label = str(edge["role_label"] or "")
        phrase = _role_phrase(edge)
        key = (gid, org_id, phrase)
        if key not in seen_po:
            seen_po.add(key)
            person_org_edges.append({
                "source": gid,
                "target": org_id,
                "role_type": role_type,
                "role_label": role_label,
                "phrase": phrase,
                "source_provider": str(edge["source"] or ""),
                "confidence": str(edge["confidence_class"] or ""),
                "weight": float(edge["edge_weight"] or 0.35),
            })
        group_entry = next((c for c in consolidated if c["group_id"] == gid), None)
        person_label = group_entry["label"] if group_entry else ""
        org_label = org_map[int(edge["organisation_id"])]["label"] if int(edge["organisation_id"]) in org_map else ""
        person_roles[gid].append({
            "phrase": phrase,
            "org": org_label,
            "role_type": role_type,
            "role_label": role_label,
        })
        org_people[org_id].append({
            "person": person_label,
            "phrase": phrase,
            "role_type": role_type,
        })

    # --- person↔person shared-org connections ---
    org_to_group_ids: dict[str, set[str]] = defaultdict(set)
    for edge in raw_edges:
        pid = int(edge["person_id"])
        gid = person_id_to_group_id.get(pid)
        if gid:
            org_to_group_ids[f"org:{int(edge['organisation_id'])}"].add(gid)

    shared_org_edges: list[dict] = []
    seen_pp: set[tuple[str, str]] = set()
    for org_id, group_ids in org_to_group_ids.items():
        glist = sorted(group_ids)
        for i, ga in enumerate(glist):
            for gb in glist[i + 1:]:
                pair = (min(ga, gb), max(ga, gb))
                if pair in seen_pp:
                    continue
                seen_pp.add(pair)
                org_label = org_map.get(int(org_id.split(":")[1]), {}).get("label", org_id)
                shared_org_edges.append({
                    "source": ga,
                    "target": gb,
                    "shared_org": org_label,
                    "shared_org_id": org_id,
                })

    # Deduplicate person roles for tooltips
    for gid in person_roles:
        seen = set()
        deduped = []
        for r in person_roles[gid]:
            key = (r["phrase"], r["org"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        person_roles[gid] = deduped

    for oid in org_people:
        seen = set()
        deduped = []
        for r in org_people[oid]:
            key = (r["person"], r["phrase"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        org_people[oid] = deduped

    for oid in org_addresses:
        seen = set()
        deduped = []
        for r in org_addresses[oid]:
            key = (r["label"], r["phrase"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        org_addresses[oid] = deduped

    # --- assemble nodes ---
    nodes: list[dict] = []

    nodes.append({
        "id": "seed",
        "label": seed_name,
        "kind": "seed",
        "lane": 0,
        "tooltip_lines": [f"Seed: {seed_name}"],
    })

    for entry in seed_aliases:
        roles = person_roles.get(entry["group_id"], [])
        tooltip = [f"<strong>{entry['label']}</strong>"]
        if len(entry["aliases"]) > 1:
            tooltip.append(f"Aliases: {', '.join(entry['aliases'])}")
        tooltip.append(f"{entry['org_count']} orgs, {entry['role_count']} roles, score {entry['score']}")
        for r in roles[:15]:
            tooltip.append(f"  {r['phrase']} <em>{r['org']}</em>")
        nodes.append({
            "id": entry["group_id"],
            "label": entry["label"],
            "kind": "seed_alias",
            "lane": 1,
            "aliases": entry["aliases"],
            "org_count": entry["org_count"],
            "role_count": entry["role_count"],
            "score": entry["score"],
            "tooltip_lines": tooltip,
        })

    for oid, info in org_map.items():
        people_list = org_people.get(info["id"], [])
        tooltip = [f"<strong>{info['label']}</strong>"]
        tooltip.append(f"{info['registry_type']} {info['registry_number']}")
        addresses = org_addresses.get(info["id"], [])
        if addresses:
            tooltip.append(f"{len(addresses)} linked addresses:")
            for addr in addresses[:5]:
                tooltip.append(f"  {addr['phrase']} <em>{addr['label']}</em>")
        tooltip.append(f"{len(people_list)} linked people:")
        for p in people_list[:20]:
            tooltip.append(f"  {p['person']} {p['phrase']}")
        nodes.append({
            "id": info["id"],
            "label": info["label"],
            "kind": "organisation",
            "lane": 2,
            "registry_type": info["registry_type"],
            "registry_number": info["registry_number"],
            "people_count": len(people_list),
            "tooltip_lines": tooltip,
        })

    for aid, info in address_map.items():
        tooltip = [f"<strong>{info['label']}</strong>"]
        if info["postcode"]:
            tooltip.append(f"Postcode: {info['postcode']}")
        if info["country"]:
            tooltip.append(f"Country: {info['country']}")
        nodes.append(
            {
                "id": info["id"],
                "label": info["label"],
                "kind": "address",
                "lane": 3,
                "postcode": info["postcode"],
                "country": info["country"],
                "tooltip_lines": tooltip,
            }
        )

    for entry in expanded_people:
        roles = person_roles.get(entry["group_id"], [])
        tooltip = [f"<strong>{entry['label']}</strong>"]
        if len(entry["aliases"]) > 1:
            tooltip.append(f"Aliases: {', '.join(entry['aliases'])}")
        tooltip.append(f"{entry['org_count']} orgs, {entry['role_count']} roles, score {entry['score']}")
        for r in roles[:15]:
            tooltip.append(f"  {r['phrase']} <em>{r['org']}</em>")
        nodes.append({
            "id": entry["group_id"],
            "label": entry["label"],
            "kind": "person",
            "lane": 4,
            "aliases": entry.get("aliases", []),
            "org_count": entry["org_count"],
            "role_count": entry["role_count"],
            "score": entry["score"],
            "tooltip_lines": tooltip,
        })

    # --- assemble edges ---
    graph_edges: list[dict] = []

    for entry in seed_aliases:
        graph_edges.append({
            "source": "seed",
            "target": entry["group_id"],
            "kind": "alias",
            "tooltip": f"{seed_name} = {entry['label']}",
        })

    for pe in person_org_edges:
        person_node = next((n for n in nodes if n["id"] == pe["source"]), None)
        org_node = next((n for n in nodes if n["id"] == pe["target"]), None)
        p_label = person_node["label"] if person_node else pe["source"]
        o_label = org_node["label"] if org_node else pe["target"]
        graph_edges.append({
            "source": pe["source"],
            "target": pe["target"],
            "kind": "role",
            "role_type": pe["role_type"],
            "role_label": pe["role_label"],
            "phrase": pe["phrase"],
            "source_provider": pe["source_provider"],
            "confidence": pe["confidence"],
            "weight": pe["weight"],
            "tooltip": f"{p_label} {pe['phrase']} {o_label}",
        })

    for row in address_rows:
        graph_edges.append(
            {
                "source": f"org:{int(row['organisation_id'])}",
                "target": f"addr:{int(row['address_id'])}",
                "kind": "address_link",
                "role_type": "organisation_address",
                "role_label": "registered_address",
                "phrase": str(row["relationship_phrase"] or "is registered at"),
                "source_provider": str(row["source"] or ""),
                "confidence": "high",
                "weight": 0.8,
                "tooltip": (
                    f"{str(row['organisation_name'] or '')} "
                    f"{str(row['relationship_phrase'] or 'is registered at')} "
                    f"{str(row['address_label'] or '')}"
                ).replace("  ", " "),
            }
        )

    for se in shared_org_edges:
        a_node = next((n for n in nodes if n["id"] == se["source"]), None)
        b_node = next((n for n in nodes if n["id"] == se["target"]), None)
        a_label = a_node["label"] if a_node else se["source"]
        b_label = b_node["label"] if b_node else se["target"]
        graph_edges.append({
            "source": se["source"],
            "target": se["target"],
            "kind": "shared_org",
            "shared_org": se["shared_org"],
            "tooltip": f"{a_label} & {b_label} share org: {se['shared_org']}",
        })

    _tag_sanctioned_nodes(nodes)
    return {
        "seed_name": seed_name,
        "run_id": run_id,
        "consolidated": consolidated,
        "nodes": nodes,
        "edges": graph_edges,
    }


# ---------------------------------------------------------------------------
# Multi-seed merge
# ---------------------------------------------------------------------------

def consolidate_multi_run(run_ids: list[int]) -> dict:
    """Merge multiple runs into one graph while keeping per-seed identities.

    Layout:
      lane 0: seeds (one per run)
      lane 1: identity nodes / seed aliases (kept per run)
      lane 2: deduplicated organisations
      lane 3: deduplicated expanded people (merged only by alias matching)
    """
    runs = [consolidate_run(rid) for rid in run_ids]
    seed_names = [r["seed_name"] for r in runs]

    def _dedupe_edges(items: list[dict], key_fields: tuple[str, ...]) -> list[dict]:
        seen: set[tuple] = set()
        out: list[dict] = []
        for item in items:
            key = tuple(item.get(field) for field in key_fields)
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _merged_person_match(left: dict, right: dict) -> bool:
        left_names = [left["label"], *(left.get("aliases") or [])]
        right_names = [right["label"], *(right.get("aliases") or [])]
        for left_name in left_names:
            for right_name in right_names:
                if are_aliases(left_name, right_name):
                    return True
        return False

    # Build per-run maps and gather lane-3 people for cross-run merging.
    run_contexts: list[dict] = []
    person_entries: list[dict] = []
    address_nodes: dict[str, dict] = {}
    org_address_edges: list[dict] = []
    for seed_index, run in enumerate(runs):
        node_map: dict[str, dict] = {str(node["id"]): node for node in run["nodes"]}
        identity_ids = {str(node["id"]) for node in run["nodes"] if node["kind"] == "seed_alias"}
        person_ids = {str(node["id"]) for node in run["nodes"] if node["kind"] == "person"}
        for node in run["nodes"]:
            if node["kind"] == "address":
                address_id = str(node["id"])
                if address_id not in address_nodes:
                    address_nodes[address_id] = {
                        **node,
                        "id": address_id,
                        "lane": 3,
                    }
            if node["kind"] != "person":
                continue
            person_entries.append({
                "run_id": int(run["run_id"]),
                "seed_index": seed_index,
                "seed_name": run["seed_name"],
                "orig_id": str(node["id"]),
                "label": str(node["label"]),
                "aliases": list(node.get("aliases") or []),
                "tooltip_lines": list(node.get("tooltip_lines") or []),
            })
        run_contexts.append({
            "run": run,
            "seed_index": seed_index,
            "node_map": node_map,
            "identity_ids": identity_ids,
            "person_ids": person_ids,
        })

    # Merge stage-4 people across runs using the existing alias matcher.
    person_uf = UnionFind()
    for i, left in enumerate(person_entries):
        for j in range(i + 1, len(person_entries)):
            if _merged_person_match(left, person_entries[j]):
                person_uf.union(i, j)

    merged_person_groups: dict[int, list[dict]] = {}
    for i, entry in enumerate(person_entries):
        merged_person_groups.setdefault(person_uf.find(i), []).append(entry)

    person_entry_to_merged_id: dict[tuple[int, str], str] = {}
    merged_person_nodes: dict[str, dict] = {}
    merged_person_orgs: dict[str, set[str]] = defaultdict(set)
    merged_person_role_keys: dict[str, set[tuple[str, str]]] = defaultdict(set)
    merged_person_score: dict[str, float] = defaultdict(float)
    merged_person_roles: dict[str, list[dict]] = defaultdict(list)
    merged_person_seeds: dict[str, set[str]] = defaultdict(set)

    for group_index, entries in enumerate(merged_person_groups.values(), start=1):
        merged_id = f"merged_person:{group_index}"
        all_aliases = sorted({
            str(name)
            for entry in entries
            for name in [entry["label"], *entry.get("aliases", [])]
            if str(name).strip()
        })
        label = max(all_aliases, key=len) if all_aliases else entries[0]["label"]
        for entry in entries:
            person_entry_to_merged_id[(int(entry["run_id"]), str(entry["orig_id"]))] = merged_id
            merged_person_seeds[merged_id].add(str(entry["seed_name"]))
        merged_person_nodes[merged_id] = {
            "id": merged_id,
            "label": label,
            "kind": "person",
            "lane": 4,
            "aliases": all_aliases,
            "org_count": 0,
            "role_count": 0,
            "score": 0.0,
            "shared": len({entry["seed_name"] for entry in entries}) > 1,
            "tooltip_lines": [],
        }

    all_identity_names: set[str] = set()
    for run in runs:
        all_identity_names.add(run["seed_name"])
        for node in run["nodes"]:
            if node["kind"] == "seed_alias":
                all_identity_names.add(str(node["label"]))
                for alias in node.get("aliases", []):
                    all_identity_names.add(str(alias))

    pruned_ids: set[str] = set()
    for mid, mnode in list(merged_person_nodes.items()):
        names = [mnode["label"], *(mnode.get("aliases") or [])]
        if any(are_aliases(n, idn) for n in names for idn in all_identity_names):
            pruned_ids.add(mid)
    for mid in pruned_ids:
        del merged_person_nodes[mid]

    nodes: list[dict] = []
    edges: list[dict] = []
    org_nodes: dict[str, dict] = {}
    org_people: dict[str, list[dict]] = defaultdict(list)
    org_identities: dict[str, list[dict]] = defaultdict(list)
    org_seed_names: dict[str, set[str]] = defaultdict(set)
    identity_meta: dict[str, dict] = {}

    # Keep seeds and identities separate per run.
    for context in run_contexts:
        run = context["run"]
        seed_index = context["seed_index"]
        seed_id = f"seed:{run['run_id']}"
        nodes.append({
            "id": seed_id,
            "label": run["seed_name"],
            "kind": "seed",
            "lane": 0,
            "seed_index": seed_index,
            "seed_name": run["seed_name"],
            "tooltip_lines": [f"Seed: {run['seed_name']}"],
        })

        for node in run["nodes"]:
            if node["kind"] != "seed_alias":
                continue
            identity_id = f"identity:{run['run_id']}:{node['id']}"
            identity_label = str(node["label"])
            identity_node = {
                **node,
                "id": identity_id,
                "lane": 1,
                "seed_index": seed_index,
                "seed_name": run["seed_name"],
                "shared": False,
            }
            nodes.append(identity_node)
            identity_meta[identity_id] = identity_node
            edges.append({
                "source": seed_id,
                "target": identity_id,
                "kind": "alias",
                "tooltip": f"{run['seed_name']} = {identity_label}",
            })

    # Shared organisations plus merged org/people edges.
    identity_org_edges: list[dict] = []
    org_person_edges: list[dict] = []
    for context in run_contexts:
        run = context["run"]
        node_map = context["node_map"]
        identity_ids = context["identity_ids"]
        person_ids = context["person_ids"]

        for node in run["nodes"]:
            if node["kind"] != "organisation":
                continue
            org_id = str(node["id"])
            org_seed_names[org_id].add(str(run["seed_name"]))
            if org_id not in org_nodes:
                org_nodes[org_id] = {
                    **node,
                    "id": org_id,
                    "lane": 2,
                    "shared": False,
                }

        for edge in run["edges"]:
            if edge.get("kind") == "address_link":
                org_id = str(edge["source"])
                address_id = str(edge["target"])
                if org_id in org_nodes and address_id in address_nodes:
                    org_address_edges.append(
                        {
                            "source": org_id,
                            "target": address_id,
                            "kind": "address_link",
                            "role_type": "organisation_address",
                            "role_label": "registered_address",
                            "phrase": edge.get("phrase", ""),
                            "source_provider": edge.get("source_provider", ""),
                            "confidence": edge.get("confidence", ""),
                            "weight": float(edge.get("weight") or 0.8),
                            "tooltip": edge.get("tooltip", ""),
                        }
                    )
                continue
            if edge.get("kind") != "role":
                continue
            source_id = str(edge["source"])
            target_id = str(edge["target"])
            source_node = node_map.get(source_id)
            target_node = node_map.get(target_id)
            if source_node is None or target_node is None:
                continue

            org_node = source_node if source_node.get("kind") == "organisation" else target_node if target_node.get("kind") == "organisation" else None
            person_node = source_node if source_id in identity_ids or source_id in person_ids else target_node if target_id in identity_ids or target_id in person_ids else None
            person_orig_id = source_id if person_node is source_node else target_id if person_node is target_node else None
            if org_node is None or person_node is None or person_orig_id is None:
                continue

            org_id = str(org_node["id"])
            org_seed_names[org_id].add(str(run["seed_name"]))

            if person_orig_id in identity_ids:
                identity_id = f"identity:{run['run_id']}:{person_orig_id}"
                identity_org_edges.append({
                    "source": identity_id,
                    "target": org_id,
                    "kind": "role",
                    "role_type": edge.get("role_type", ""),
                    "role_label": edge.get("role_label", ""),
                    "phrase": edge.get("phrase", ""),
                    "source_provider": edge.get("source_provider", ""),
                    "confidence": edge.get("confidence", ""),
                    "weight": float(edge.get("weight") or 0.35),
                    "tooltip": edge.get("tooltip", ""),
                })
                org_identities[org_id].append({
                    "identity": identity_meta[identity_id]["label"],
                    "seed": run["seed_name"],
                    "phrase": edge.get("phrase", ""),
                })
                continue

            if person_orig_id in person_ids:
                merged_person_id = person_entry_to_merged_id.get((int(run["run_id"]), person_orig_id))
                if not merged_person_id or merged_person_id not in merged_person_nodes:
                    continue
                phrase = str(edge.get("phrase", "") or "")
                merged_person_orgs[merged_person_id].add(org_id)
                merged_person_role_keys[merged_person_id].add((org_id, phrase))
                merged_person_score[merged_person_id] += float(edge.get("weight") or 0.35)
                org_person_edges.append({
                    "source": org_id,
                    "target": merged_person_id,
                    "kind": "role",
                    "role_type": edge.get("role_type", ""),
                    "role_label": edge.get("role_label", ""),
                    "phrase": phrase,
                    "source_provider": edge.get("source_provider", ""),
                    "confidence": edge.get("confidence", ""),
                    "weight": float(edge.get("weight") or 0.35),
                    "tooltip": edge.get("tooltip", ""),
                })
                person_label = merged_person_nodes[merged_person_id]["label"]
                org_people[org_id].append({
                    "person": person_label,
                    "phrase": phrase,
                })
                merged_person_roles[merged_person_id].append({
                    "phrase": phrase,
                    "org": org_nodes[org_id]["label"],
                })

    identity_org_edges = _dedupe_edges(identity_org_edges, ("source", "target", "phrase"))
    org_person_edges = _dedupe_edges(org_person_edges, ("source", "target", "phrase"))

    for org_id, node in org_nodes.items():
        node["shared"] = len(org_seed_names.get(org_id, set())) > 1
        identities = org_identities.get(org_id, [])
        people = org_people.get(org_id, [])
        tooltip = [f"<strong>{node['label']}</strong>"]
        if node.get("registry_type") or node.get("registry_number"):
            tooltip.append(f"{node.get('registry_type', '')} {node.get('registry_number', '')}".strip())
        if len(org_seed_names.get(org_id, set())) > 1:
            tooltip.append(f"Seen under: {', '.join(sorted(org_seed_names[org_id]))}")
        if identities:
            tooltip.append(f"{len(identities)} linked identities:")
            for row in identities[:10]:
                tooltip.append(f"  {row['seed']}: {row['identity']} {row['phrase']}")
        if people:
            tooltip.append(f"{len(people)} linked people:")
            for row in people[:12]:
                tooltip.append(f"  {row['person']} {row['phrase']}")
        node["tooltip_lines"] = tooltip
        nodes.append(node)

    for node in address_nodes.values():
        nodes.append(node)

    merged_people_consolidated: list[dict] = []
    for merged_id, node in merged_person_nodes.items():
        node["org_count"] = len(merged_person_orgs.get(merged_id, set()))
        node["role_count"] = len(merged_person_role_keys.get(merged_id, set()))
        node["score"] = round(float(merged_person_score.get(merged_id, 0.0)), 4)
        tooltip = [f"<strong>{node['label']}</strong>"]
        if len(node.get("aliases") or []) > 1:
            tooltip.append(f"Aliases: {', '.join(node['aliases'])}")
        tooltip.append(f"{node['org_count']} orgs, {node['role_count']} roles, score {node['score']}")
        if len(merged_person_seeds.get(merged_id, set())) > 1:
            tooltip.append(f"Appears under: {', '.join(sorted(merged_person_seeds[merged_id]))}")
        seen_lines: set[tuple[str, str]] = set()
        for row in merged_person_roles.get(merged_id, []):
            key = (str(row["phrase"]), str(row["org"]))
            if key in seen_lines:
                continue
            seen_lines.add(key)
            tooltip.append(f"  {row['phrase']} <em>{row['org']}</em>")
            if len(seen_lines) >= 15:
                break
        node["tooltip_lines"] = tooltip
        nodes.append(node)
        merged_people_consolidated.append({
            "group_id": merged_id,
            "label": node["label"],
            "aliases": list(node.get("aliases") or []),
            "person_ids": [],
            "org_count": node["org_count"],
            "role_count": node["role_count"],
            "score": node["score"],
            "is_seed_alias": False,
        })

    edges.extend(identity_org_edges)
    edges.extend(org_address_edges)
    edges.extend(org_person_edges)

    consolidated: list[dict] = []
    for context in run_contexts:
        run = context["run"]
        for entry in run["consolidated"]:
            if not entry["is_seed_alias"]:
                continue
            consolidated.append({
                **entry,
                "group_id": f"identity:{run['run_id']}:{entry['group_id']}",
                "seed_name": run["seed_name"],
            })
    consolidated.extend(merged_people_consolidated)
    consolidated.sort(key=lambda c: (-float(c["score"]), -int(c["org_count"]), c["label"]))

    _tag_sanctioned_nodes(nodes)
    return {
        "seed_name": "Istari",
        "run_id": "+".join(str(r) for r in run_ids),
        "seed_names": seed_names,
        "runs": runs,
        "consolidated": consolidated,
        "nodes": nodes,
        "edges": edges,
    }


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def render_html(data: dict) -> str:
    nodes_json = json.dumps(data["nodes"], ensure_ascii=False).replace("</", "<\\/")
    edges_json = json.dumps(data["edges"], ensure_ascii=False).replace("</", "<\\/")
    seed_name = html.escape(data["seed_name"])
    run_id = data["run_id"]
    is_multi_seed = sum(1 for n in data["nodes"] if n["kind"] == "seed") > 1
    identity_dropdown_html = (
        '  <div class="org-dropdown" id="identity-dropdown">\n'
        '    <button class="org-dropdown-btn" id="identity-dropdown-btn">Identities &#9662;</button>\n'
        '    <div class="org-dropdown-menu" id="identity-dropdown-menu"></div>\n'
        "  </div>\n"
        if is_multi_seed else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Istari</title>
<style>
:root {{
  --bg: #0c0e14; --surface: #141820; --border: #1e2430;
  --text: #d0d4dc; --text-dim: #6b7385; --text-bright: #f0f2f5;
  --red: #e55561; --amber: #d4a017; --blue: #58a6ff; --green: #3fb950;
  --purple: #b382f0; --shared: #445577;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); overflow: hidden; height: 100vh; }}
.topbar {{
  display: flex; align-items: center; gap: 16px; padding: 12px 20px;
  background: var(--surface); border-bottom: 1px solid var(--border); z-index: 20; position: relative;
}}
.topbar h1 {{ font-size: 16px; font-weight: 700; color: var(--text-bright); white-space: nowrap; }}
.topbar .stats {{ font-size: 12px; color: var(--text-dim); white-space: nowrap; }}
.search-box {{
  margin-left: auto; display: flex; align-items: center; gap: 6px;
}}
.search-box input {{
  width: 220px; padding: 6px 12px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--bg); color: var(--text); font-size: 13px; outline: none;
}}
.search-box input:focus {{ border-color: var(--blue); }}
.toggle {{
  display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-dim);
  cursor: pointer; white-space: nowrap; user-select: none;
}}
.toggle input {{ accent-color: var(--blue); cursor: pointer; }}
.toggle input:checked + span {{ color: var(--blue); }}
.org-dropdown {{ position: relative; margin-left: 8px; }}
.org-dropdown-btn {{
  background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
  color: var(--text); font-size: 12px; padding: 5px 12px; cursor: pointer; white-space: nowrap;
}}
.org-dropdown-btn:hover {{ border-color: var(--blue); }}
.org-dropdown-menu {{
  display: none; position: absolute; top: 100%; left: 0; margin-top: 4px;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 8px 0; min-width: 280px; max-height: 360px; overflow-y: auto; z-index: 50;
  box-shadow: 0 8px 24px rgba(0,0,0,0.5);
}}
.org-dropdown-menu.open {{ display: block; }}
.org-dropdown-menu label {{
  display: flex; align-items: center; gap: 8px; padding: 4px 14px; font-size: 11.5px;
  color: var(--text); cursor: pointer; white-space: nowrap;
}}
.org-dropdown-menu label:hover {{ background: rgba(255,255,255,0.04); }}
.org-dropdown-menu input {{ accent-color: var(--green); cursor: pointer; }}
.search-box .clear-btn {{
  background: none; border: none; color: var(--text-dim); cursor: pointer; font-size: 16px; padding: 2px 6px;
}}
.lane-labels {{
  position: absolute; left: 8px; top: 56px; z-index: 15; pointer-events: none;
}}
.lane-label {{
  font-size: 11px; font-weight: 700; color: var(--text-dim); letter-spacing: 0.5px;
  text-transform: uppercase; position: absolute; left: 0; transform: translateY(-50%);
  background: var(--bg); padding: 2px 8px; border-radius: 4px;
}}
.legend {{
  position: absolute; top: 56px; right: 12px; z-index: 15;
  display: flex; flex-direction: column; gap: 5px; background: var(--surface);
  border: 1px solid var(--border); border-radius: 8px; padding: 10px 14px; font-size: 11px;
}}
.legend .row {{ display: flex; align-items: center; gap: 6px; }}
.legend .dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
#graph {{ width: 100vw; height: calc(100vh - 48px); overflow: hidden; }}
.tooltip {{
  position: fixed; background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 12px 16px; font-size: 12px; line-height: 1.6; pointer-events: none; z-index: 100;
  max-width: 380px; box-shadow: 0 8px 24px rgba(0,0,0,0.6); display: none;
}}
.tooltip strong {{ color: var(--text-bright); }}
.tooltip em {{ color: var(--green); font-style: normal; }}
.tooltip .dim {{ color: var(--text-dim); }}
.focus-panel {{
  position: fixed; top: 60px; right: 12px; z-index: 200;
  background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
  padding: 18px 22px; font-size: 12.5px; line-height: 1.7;
  max-width: 420px; max-height: calc(100vh - 100px); overflow-y: auto;
  box-shadow: 0 12px 40px rgba(0,0,0,0.7); display: none;
}}
.focus-panel h3 {{ margin: 0 0 8px; font-size: 15px; color: var(--text-bright); }}
.focus-panel .close-btn {{
  position: absolute; top: 8px; right: 12px; background: none; border: none;
  color: var(--text-dim); font-size: 20px; cursor: pointer; line-height: 1;
}}
.focus-panel .close-btn:hover {{ color: var(--text-bright); }}
.focus-panel .section {{ margin-top: 10px; }}
.focus-panel .section-title {{ font-weight: 700; color: var(--blue); font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
.focus-panel .conn {{ padding: 3px 0; color: var(--text); }}
.focus-panel .conn em {{ color: var(--green); font-style: normal; }}
.focus-panel .conn .dim {{ color: var(--text-dim); }}
svg text {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }}
</style>
</head>
<body>
<div class="topbar">
  <h1>Istari</h1>
{identity_dropdown_html}  <div class="org-dropdown" id="people-dropdown">
    <button class="org-dropdown-btn" id="people-dropdown-btn">People &#9662;</button>
    <div class="org-dropdown-menu" id="people-dropdown-menu"></div>
  </div>
  <label class="toggle">
    <input id="stage3-multi-org-only" type="checkbox" />
    <span>only show individuals connected to 2+ organisations</span>
  </label>
  <label class="toggle">
    <input id="org-multi-person-only" type="checkbox" />
    <span>only show organisations connected to 2+ individuals</span>
  </label>
  <div class="search-box">
    <input id="search" type="search" placeholder="Filter by name..." autocomplete="off" />
    <button class="clear-btn" id="clear-search">&times;</button>
  </div>
</div>
<div id="graph"></div>
<div class="lane-labels" id="lane-labels"></div>
<div class="legend">
  <div class="row"><span class="dot" style="background:var(--red)"></span> Seed</div>
  <div class="row"><span class="dot" style="background:var(--amber)"></span> Seed identity</div>
  <div class="row"><span class="dot" style="background:var(--green)"></span> Organisation</div>
  <div class="row"><span class="dot" style="background:var(--purple)"></span> Address</div>
  <div class="row"><span class="dot" style="background:var(--blue)"></span> Person</div>
  <div class="row"><span class="dot" style="background:#ff2222;border:2px solid #ff2222"></span> Sanctioned (OFAC)</div>
</div>
<div class="tooltip" id="tooltip"></div>
<div class="focus-panel" id="focus-panel"><button class="close-btn" id="focus-close">&times;</button><div id="focus-content"></div></div>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<script>
const allNodes = {nodes_json};
const allEdges = {edges_json}.filter(e => e.kind !== "shared_org" && e.kind !== "cross_seed");
const multiSeed = allNodes.filter(n => n.kind === "seed").length > 1;

const container = document.getElementById("graph");
const tooltipEl = document.getElementById("tooltip");
const searchInput = document.getElementById("search");
const clearBtn = document.getElementById("clear-search");
const laneLabelsDiv = document.getElementById("lane-labels");
const stage3MultiOrgToggle = document.getElementById("stage3-multi-org-only");
const orgMultiPersonToggle = document.getElementById("org-multi-person-only");

const W = container.clientWidth;
const H = container.clientHeight;

const LANE_Y = {{ 0: 80, 1: 260, 2: 520, 3: 760, 4: 980 }};
const LANE_NAMES = {{ 0: "Seed", 1: "Identity", 2: "Organisations", 3: "Addresses", 4: "People" }};

const svg = d3.select(container).append("svg")
  .attr("width", "100%").attr("height", "100%")
  .style("display", "block");
const gRoot = svg.append("g");
const zoom = d3.zoom().scaleExtent([0.05, 6]).on("zoom", (e) => gRoot.attr("transform", e.transform));
svg.call(zoom);
svg.on("dblclick.zoom", null);

const nodeById = new Map(allNodes.map(n => [n.id, n]));
const edgesByNodeId = new Map();
allEdges.forEach(e => {{
  if (!edgesByNodeId.has(e.source)) edgesByNodeId.set(e.source, []);
  if (!edgesByNodeId.has(e.target)) edgesByNodeId.set(e.target, []);
  edgesByNodeId.get(e.source).push(e);
  edgesByNodeId.get(e.target).push(e);
}});

// -- build identity and people dropdowns --
const identityNodes = allNodes
  .filter(n => n.lane === 1)
  .sort((a, b) => (a.seed_index - b.seed_index) || a.label.localeCompare(b.label));
const selectedIdentities = new Set(identityNodes.map(n => n.id));

const peopleNodes = allNodes.filter(n => n.lane === 4).sort((a, b) => a.label.localeCompare(b.label));
const selectedPeople = new Set(peopleNodes.map(n => n.id));

const identityDropdownBtn = document.getElementById("identity-dropdown-btn");
const identityDropdownMenu = document.getElementById("identity-dropdown-menu");
const peopleDropdownBtn = document.getElementById("people-dropdown-btn");
const peopleDropdownMenu = document.getElementById("people-dropdown-menu");

// per-person org count for multi-org filter
const personOrgIds = new Map();
allEdges.filter(e => e.kind === "role").forEach(e => {{
  const personId = nodeById.get(e.source)?.kind === "organisation" ? e.target : e.source;
  const orgId = nodeById.get(e.source)?.kind === "organisation" ? e.source : e.target;
  const personNode = nodeById.get(personId);
  const orgNode = nodeById.get(orgId);
  if (personNode?.lane !== 4 || orgNode?.kind !== "organisation") return;
  if (!personOrgIds.has(personId)) personOrgIds.set(personId, new Set());
  personOrgIds.get(personId).add(orgId);
}});

// per-organisation individual count for org filter
const orgPersonIds = new Map();
allEdges.filter(e => e.kind === "role").forEach(e => {{
  const sourceNode = nodeById.get(e.source);
  const targetNode = nodeById.get(e.target);
  const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
  const personNode = sourceNode?.kind === "organisation" ? targetNode : sourceNode;
  if (!orgNode || !personNode) return;
  if (personNode.lane !== 1 && personNode.lane !== 4) return;
  if (!orgPersonIds.has(orgNode.id)) orgPersonIds.set(orgNode.id, new Set());
  orgPersonIds.get(orgNode.id).add(personNode.id);
}});

function activeIdentityIds() {{
  if (!multiSeed) return new Set(identityNodes.map(n => n.id));
  return new Set(identityNodes.filter(n => selectedIdentities.has(n.id)).map(n => n.id));
}}

function candidateOrgIdsForSelectedIdentities() {{
  const activeIds = activeIdentityIds();
  const orgIdentityIds = new Map();
  allEdges.forEach(e => {{
    if (e.kind !== "role") return;
    const sourceNode = nodeById.get(e.source);
    const targetNode = nodeById.get(e.target);
    const identityNode = sourceNode?.lane === 1 ? sourceNode : targetNode?.lane === 1 ? targetNode : null;
    const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
    if (!identityNode || !orgNode) return;
    if (!activeIds.has(identityNode.id)) return;
    if (!orgIdentityIds.has(orgNode.id)) orgIdentityIds.set(orgNode.id, new Set());
    orgIdentityIds.get(orgNode.id).add(identityNode.id);
  }});
  return new Set(
    [...orgIdentityIds.entries()]
      .map(([orgId]) => orgId)
  );
}}

function visiblePeopleList() {{
  const candidateOrgIds = candidateOrgIdsForSelectedIdentities();
  return peopleNodes.filter(n => {{
    if (stage3MultiOrgToggle?.checked && (n.org_count || 0) < 2) return false;
    const myOrgs = personOrgIds.get(n.id) || new Set();
    return [...myOrgs].some(orgId => candidateOrgIds.has(orgId));
  }});
}}

function visibleAddressIdsForVisibleOrgs(visibleOrgs) {{
  const visibleAddresses = new Set();
  allEdges.forEach(e => {{
    if (e.kind !== "address_link") return;
    const sourceNode = nodeById.get(e.source);
    const targetNode = nodeById.get(e.target);
    const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
    const addressNode = sourceNode?.kind === "address" ? sourceNode : targetNode?.kind === "address" ? targetNode : null;
    if (!orgNode || !addressNode) return;
    if (!visibleOrgs.has(orgNode.id)) return;
    visibleAddresses.add(addressNode.id);
  }});
  return visibleAddresses;
}}

function orgIdsForVisiblePeople(visiblePeople) {{
  const orgIds = new Set();
  visiblePeople.forEach(personId => {{
    const myOrgs = personOrgIds.get(personId) || new Set();
    [...myOrgs].forEach(orgId => orgIds.add(orgId));
  }});
  return orgIds;
}}

function buildIdentityDropdown() {{
  if (!identityDropdownMenu || !identityDropdownBtn) return;
  identityDropdownMenu.innerHTML = "";

  const allRow = document.createElement("label");
  allRow.style.fontWeight = "600";
  allRow.style.borderBottom = "1px solid var(--border)";
  allRow.style.paddingBottom = "6px";
  allRow.style.marginBottom = "4px";
  const allCb = document.createElement("input");
  allCb.type = "checkbox";
  allCb.checked = identityNodes.every(n => selectedIdentities.has(n.id));
  allCb.addEventListener("change", () => {{
    if (allCb.checked) identityNodes.forEach(n => selectedIdentities.add(n.id));
    else selectedIdentities.clear();
    buildIdentityDropdown();
    buildPeopleDropdown();
    applyFilter();
  }});
  allRow.appendChild(allCb);
  allRow.appendChild(document.createTextNode(" All identities"));
  identityDropdownMenu.appendChild(allRow);

  identityNodes.forEach(identity => {{
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selectedIdentities.has(identity.id);
    cb.addEventListener("change", () => {{
      if (cb.checked) selectedIdentities.add(identity.id);
      else selectedIdentities.delete(identity.id);
      buildPeopleDropdown();
      applyFilter();
    }});
    label.appendChild(cb);
    label.appendChild(document.createTextNode(` ${{identity.seed_name}}: ${{identity.label}}`));
    identityDropdownMenu.appendChild(label);
  }});
}}

function buildPeopleDropdown() {{
  peopleDropdownMenu.innerHTML = "";
  const visList = visiblePeopleList();

  const allRow = document.createElement("label");
  allRow.style.fontWeight = "600";
  allRow.style.borderBottom = "1px solid var(--border)";
  allRow.style.paddingBottom = "6px";
  allRow.style.marginBottom = "4px";
  const allCb = document.createElement("input");
  allCb.type = "checkbox";
  allCb.checked = visList.every(n => selectedPeople.has(n.id));
  allCb.addEventListener("change", () => {{
    if (allCb.checked) visList.forEach(n => selectedPeople.add(n.id));
    else visList.forEach(n => selectedPeople.delete(n.id));
    buildPeopleDropdown();
    applyFilter();
  }});
  allRow.appendChild(allCb);
  allRow.appendChild(document.createTextNode(" All people"));
  peopleDropdownMenu.appendChild(allRow);

  visList.forEach(person => {{
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selectedPeople.has(person.id);
    cb.addEventListener("change", () => {{
      if (cb.checked) selectedPeople.add(person.id);
      else selectedPeople.delete(person.id);
      applyFilter();
    }});
    label.appendChild(cb);
    label.appendChild(document.createTextNode(" " + person.label));
    peopleDropdownMenu.appendChild(label);
  }});
}}
buildIdentityDropdown();
buildPeopleDropdown();

identityDropdownBtn?.addEventListener("click", (e) => {{
  e.stopPropagation();
  identityDropdownMenu.classList.toggle("open");
}});
peopleDropdownBtn.addEventListener("click", (e) => {{
  e.stopPropagation();
  peopleDropdownMenu.classList.toggle("open");
}});
document.addEventListener("click", () => {{
  identityDropdownMenu?.classList.remove("open");
  peopleDropdownMenu.classList.remove("open");
}});
identityDropdownMenu?.addEventListener("click", (e) => e.stopPropagation());
peopleDropdownMenu.addEventListener("click", (e) => e.stopPropagation());

// -- measure text widths via hidden canvas --
const measureCtx = document.createElement("canvas").getContext("2d");
function textWidth(text, fs) {{
  measureCtx.font = `${{fs}}px 'Segoe UI', system-ui, sans-serif`;
  return measureCtx.measureText(text).width;
}}

function nodeLabel(d) {{
  return d.label;
}}
function fontSize(d) {{
  if (d.kind === "seed") return 13;
  if (d.kind === "seed_alias") return 12;
  return 10.5;
}}
function pillHeight(d) {{ return fontSize(d) + 12; }}
function pillWidth(d) {{ return textWidth(nodeLabel(d), fontSize(d)) + 18; }}

function nodeColor(d) {{
  if (d.sanctioned) return "#ff2222";
  if (d.kind === "seed") return "var(--red)";
  if (d.kind === "seed_alias") return "var(--amber)";
  if (d.kind === "organisation") return "var(--green)";
  if (d.kind === "address") return "var(--purple)";
  return "var(--blue)";
}}
function edgeStroke(d) {{
  if (d.kind === "alias") return "var(--amber)";
  if (d.kind === "address_link") return "var(--purple)";
  const rt = (d.role_type || "").toLowerCase();
  if (rt.includes("trustee")) return "var(--blue)";
  if (rt.includes("director")) return "var(--purple)";
  if (rt.includes("secretary")) return "#0ea5e9";
  return "#2a3040";
}}

// -- position nodes --
function layoutRow(list, yTop, xMin, xMax) {{
  if (list.length === 0) return 0;
  const spacing = 16;
  const rowGap = 18;
  const pad = 18;
  const usableMin = xMin + pad;
  const usableMax = xMax - pad;
  const maxRowW = Math.max(120, usableMax - usableMin);

  const rows = [];
  let currentRow = [];
  let currentWidth = 0;
  list.forEach(n => {{
    const pw = pillWidth(n);
    const nextWidth = currentRow.length ? currentWidth + spacing + pw : pw;
    if (currentRow.length && nextWidth > maxRowW) {{
      rows.push(currentRow);
      currentRow = [n];
      currentWidth = pw;
    }} else {{
      currentRow.push(n);
      currentWidth = nextWidth;
    }}
  }});
  if (currentRow.length) rows.push(currentRow);

  const maxPillH = Math.max(...rows.flat().map(pillHeight));
  const rowStep = maxPillH + rowGap;

  rows.forEach((row, rowIndex) => {{
    const rowW = row.reduce((sum, n) => sum + pillWidth(n), 0) + spacing * (row.length - 1);
    let cx = usableMin + Math.max(0, (maxRowW - rowW) / 2);
    const rowY = yTop + rowIndex * rowStep;
    row.forEach(n => {{
      const pw = pillWidth(n);
      n.x = cx + pw / 2;
      n.y = rowY;
      cx += pw + spacing;
    }});
  }});
  return rows.length * rowStep;
}}

const LANE_GAP = 50;

function positionNodes() {{
  const visible = allNodes.filter(n => n._visible !== false);
  let curY = 60;
  if (!multiSeed) {{
    const laneKeys = [0, 1, 2, 3, 4];
    laneKeys.forEach(lane => {{
      const list = visible.filter(n => n.lane === lane);
      LANE_Y[lane] = curY;
      const h = layoutRow(list, curY, 0, W);
      curY += Math.max(h, 30) + LANE_GAP;
    }});
  }} else {{
    LANE_Y[0] = curY;
    const seedH = layoutRow(visible.filter(n => n.kind === "seed"), curY, 0, W);
    curY += Math.max(seedH, 30) + LANE_GAP;

    LANE_Y[1] = curY;
    const idH = layoutRow(visible.filter(n => n.lane === 1), curY, 0, W);
    curY += Math.max(idH, 30) + LANE_GAP;

    LANE_Y[2] = curY;
    const orgH = layoutRow(visible.filter(n => n.kind === "organisation"), curY, 0, W);
    curY += Math.max(orgH, 30) + LANE_GAP;

    LANE_Y[3] = curY;
    const addrH = layoutRow(visible.filter(n => n.kind === "address"), curY, 0, W);
    curY += Math.max(addrH, 30) + LANE_GAP;

    LANE_Y[4] = curY;
    layoutRow(visible.filter(n => n.lane === 4), curY, 0, W);
  }}
}}
positionNodes();

// -- edges layer --
const edgeGroup = gRoot.append("g");
const roleLine = edgeGroup.selectAll("line.role-edge").data(allEdges).join("line")
  .attr("class", "role-edge")
  .attr("stroke", edgeStroke)
  .attr("stroke-width", d => d.kind === "alias" ? 2.5 : 1.4 + (d.weight || 0) * 1.5)
  .attr("stroke-opacity", d => d.kind === "alias" ? 0.8 : 0.45);

// -- nodes layer: pill groups --
const nodeGroup = gRoot.append("g");
const pills = nodeGroup.selectAll("g.pill").data(allNodes).join("g")
  .attr("class", "pill")
  .style("cursor", "pointer");

pills.append("rect")
  .attr("rx", d => pillHeight(d) / 2)
  .attr("ry", d => pillHeight(d) / 2)
  .attr("width", pillWidth)
  .attr("height", pillHeight)
  .attr("fill", nodeColor)
  .attr("fill-opacity", d => d.sanctioned ? 0.35 : 0.18)
  .attr("stroke", nodeColor)
  .attr("stroke-width", d => d.sanctioned ? 2.5 : 1.2)
  .attr("stroke-opacity", d => d.sanctioned ? 1.0 : 0.7);

pills.append("text")
  .text(nodeLabel)
  .attr("font-size", fontSize)
  .attr("font-weight", d => (d.kind === "seed" || d.kind === "seed_alias") ? 600 : 400)
  .attr("fill", d => (d.kind === "seed" || d.kind === "seed_alias") ? "var(--text-bright)" : "var(--text)")
  .attr("text-anchor", "middle")
  .attr("dominant-baseline", "central")
  .attr("x", d => pillWidth(d) / 2)
  .attr("y", d => pillHeight(d) / 2)
  .style("pointer-events", "none");

function updatePositions() {{
  roleLine
    .attr("x1", d => nodeById.get(d.source)?.x ?? 0)
    .attr("y1", d => nodeById.get(d.source)?.y ?? 0)
    .attr("x2", d => nodeById.get(d.target)?.x ?? 0)
    .attr("y2", d => nodeById.get(d.target)?.y ?? 0);
  pills.attr("transform", d => `translate(${{d.x - pillWidth(d) / 2}},${{d.y - pillHeight(d) / 2}})`);
}}
updatePositions();

// -- lane labels --
function updateLaneLabels() {{
  const t = d3.zoomTransform(svg.node());
  laneLabelsDiv.innerHTML = "";
  Object.entries(LANE_NAMES).forEach(([lane, name]) => {{
    const screenY = LANE_Y[lane] * t.k + t.y + 48;
    const el = document.createElement("div");
    el.className = "lane-label";
    el.style.top = screenY + "px";
    el.textContent = name;
    laneLabelsDiv.appendChild(el);
  }});
}}
updateLaneLabels();
svg.on("zoom.labels", null);
zoom.on("zoom.labels", updateLaneLabels);
svg.call(zoom).on("zoom", (e) => {{
  gRoot.attr("transform", e.transform);
  updateLaneLabels();
}});

// -- tooltip --
function showTooltip(event, lines) {{
  tooltipEl.innerHTML = lines.join("<br>");
  tooltipEl.style.display = "block";
  positionTooltip(event);
}}
function positionTooltip(event) {{
  const pad = 14;
  let x = event.clientX + pad;
  let y = event.clientY - 10;
  const rect = tooltipEl.getBoundingClientRect();
  if (x + rect.width > window.innerWidth - 10) x = event.clientX - rect.width - pad;
  if (y + rect.height > window.innerHeight - 10) y = window.innerHeight - rect.height - 10;
  tooltipEl.style.left = x + "px";
  tooltipEl.style.top = y + "px";
}}
function hideTooltip() {{ tooltipEl.style.display = "none"; }}

pills
  .on("mouseover", (event, d) => showTooltip(event, d.tooltip_lines || [d.label]))
  .on("mousemove", (event) => positionTooltip(event))
  .on("mouseout", hideTooltip);

roleLine
  .on("mouseover", (event, d) => showTooltip(event, [d.tooltip || "link"]))
  .on("mousemove", positionTooltip)
  .on("mouseout", hideTooltip)
  .style("cursor", "default")
  .style("pointer-events", "stroke");

// -- drag --
const drag = d3.drag()
  .on("start", (event, d) => {{ d._dragging = true; }})
  .on("drag", (event, d) => {{
    d.x = event.x; d.y = event.y;
    updatePositions();
  }})
  .on("end", (event, d) => {{ d._dragging = false; }});
pills.call(drag);

// -- focus panel (double-click) --
const focusPanel = document.getElementById("focus-panel");
const focusContent = document.getElementById("focus-content");
const focusClose = document.getElementById("focus-close");
let focusedNodeId = null;

function closeFocusPanel() {{
  focusPanel.style.display = "none";
  focusedNodeId = null;
  pills.attr("opacity", d => d._visible ? 1 : 0);
  pills.attr("display", d => d._visible ? null : "none");
  roleLine.attr("stroke-opacity", d => d.kind === "alias" ? 0.8 : 0.45);
  roleLine.attr("display", d => {{
    const s = nodeById.get(d.source);
    const t = nodeById.get(d.target);
    return (s?._visible && t?._visible) ? null : "none";
  }});
}}
focusClose.addEventListener("click", closeFocusPanel);

pills.on("dblclick", (event, d) => {{
  event.stopPropagation();
  focusedNodeId = d.id;

  const edges = edgesByNodeId.get(d.id) || [];
  const directRoles = [];
  const seenDirectConnections = new Set();
  edges
    .filter(e => e.kind === "role" || e.kind === "alias" || e.kind === "address_link")
    .forEach(e => {{
      const otherId = e.source === d.id ? e.target : e.source;
      const key = `${{e.kind}}:${{otherId}}`;
      if (seenDirectConnections.has(key)) return;
      seenDirectConnections.add(key);
      directRoles.push(e);
    }});

  let html = `<h3>${{d.label}}</h3>`;
  if (d.aliases && d.aliases.length > 1) {{
    html += `<div class="dim">Aliases: ${{d.aliases.join(", ")}}</div>`;
  }}
  if (d.org_count) html += `<div class="dim">${{d.org_count}} orgs, ${{d.role_count}} roles, score ${{d.score}}</div>`;

  if (directRoles.length) {{
    html += `<div class="section"><div class="section-title">Direct connections</div>`;
    directRoles.forEach(e => {{
      const otherId = e.source === d.id ? e.target : e.source;
      const other = nodeById.get(otherId);
      html += `<div class="conn">${{e.tooltip || (other ? other.label : otherId)}}</div>`;
    }});
    html += `</div>`;
  }}

  focusContent.innerHTML = html;
  focusPanel.style.display = "block";

  const connectedIds = new Set(edges.map(e => e.source === d.id ? e.target : e.source));
  connectedIds.add(d.id);

  pills.attr("display", n => {{
    if (connectedIds.has(n.id)) return null;
    return n._visible ? null : "none";
  }});

  pills.attr("opacity", n => {{
    if (connectedIds.has(n.id)) return 1;
    if (!n._visible) return 0;
    return 0.12;
  }});
  roleLine.attr("display", e => {{
    if (e.source === d.id || e.target === d.id) return null;
    const s = nodeById.get(e.source);
    const t = nodeById.get(e.target);
    return (s?._visible && t?._visible) ? null : "none";
  }});
  roleLine.attr("stroke-opacity", e => {{
    if (e.source === d.id || e.target === d.id) return 0.8;
    return 0.04;
  }});
}});

svg.on("dblclick.focus", () => {{
  if (focusedNodeId) closeFocusPanel();
}});

// -- search / filter --
let searchTerm = "";

function applyFilter() {{
  const q = searchTerm.toLowerCase();

  allNodes.forEach(n => {{ n._visible = false; }});

  const activeIdentities = activeIdentityIds();
  const candidateOrgIds = candidateOrgIdsForSelectedIdentities();
  const visibleOrgs = new Set(
    [...candidateOrgIds].filter(orgId => {{
      if (!orgMultiPersonToggle?.checked) return true;
      return (orgPersonIds.get(orgId)?.size || 0) >= 2;
    }})
  );
  const visiblePeople = new Set();

  allNodes.filter(n => n.lane === 4).forEach(n => {{
    let visible = selectedPeople.has(n.id);
    if (visible && stage3MultiOrgToggle?.checked) visible = (n.org_count || 0) >= 2;
    const myOrgs = personOrgIds.get(n.id) || new Set();
    if (visible) visible = [...myOrgs].some(orgId => visibleOrgs.has(orgId));
    if (visible && q) {{
      const nameMatch = n.label.toLowerCase().includes(q);
      const aliasMatch = (n.aliases || []).some(a => a.toLowerCase().includes(q));
      visible = nameMatch || aliasMatch;
    }}
    n._visible = visible;
    if (visible) visiblePeople.add(n.id);
  }});

  let addressOrgIds = visibleOrgs;
  if (stage3MultiOrgToggle?.checked) {{
    addressOrgIds = new Set(
      [...orgIdsForVisiblePeople(visiblePeople)].filter(orgId => visibleOrgs.has(orgId))
    );
  }}
  const visibleAddresses = visibleAddressIdsForVisibleOrgs(addressOrgIds);

  allNodes.filter(n => n.kind === "organisation").forEach(n => {{ n._visible = visibleOrgs.has(n.id); }});
  allNodes.filter(n => n.kind === "address").forEach(n => {{ n._visible = visibleAddresses.has(n.id); }});

  const visibleAliases = new Set();
  allEdges.forEach(e => {{
    if (e.kind !== "role") return;
    const sourceNode = nodeById.get(e.source);
    const targetNode = nodeById.get(e.target);
    const aliasNode = sourceNode?.lane === 1 ? sourceNode : targetNode?.lane === 1 ? targetNode : null;
    const orgNode = sourceNode?.kind === "organisation" ? sourceNode : targetNode?.kind === "organisation" ? targetNode : null;
    if (!aliasNode || !orgNode) return;
    if (!activeIdentities.has(aliasNode.id)) return;
    if (!visibleOrgs.has(orgNode.id)) return;
    visibleAliases.add(aliasNode.id);
  }});
  allNodes.filter(n => n.lane === 1).forEach(n => {{ n._visible = visibleAliases.has(n.id); }});

  const visibleSeeds = new Set();
  allEdges.forEach(e => {{
    if (e.kind !== "alias") return;
    const sourceNode = nodeById.get(e.source);
    const targetNode = nodeById.get(e.target);
    const seedNode = sourceNode?.kind === "seed" ? sourceNode : targetNode?.kind === "seed" ? targetNode : null;
    const aliasNode = sourceNode?.lane === 1 ? sourceNode : targetNode?.lane === 1 ? targetNode : null;
    if (!seedNode || !aliasNode) return;
    if (!visibleAliases.has(aliasNode.id)) return;
    visibleSeeds.add(seedNode.id);
  }});
  allNodes.filter(n => n.kind === "seed").forEach(n => {{ n._visible = visibleSeeds.has(n.id); }});

  positionNodes();
  updatePositions();

  pills.attr("display", d => d._visible ? null : "none");
  roleLine.attr("display", d => {{
    const s = nodeById.get(d.source);
    const t = nodeById.get(d.target);
    return (s?._visible && t?._visible) ? null : "none";
  }});

  if (identityDropdownBtn && identityNodes.length) {{
    const selectedCount = identityNodes.filter(n => selectedIdentities.has(n.id)).length;
    const totalCount = identityNodes.length;
    identityDropdownBtn.textContent = selectedCount === totalCount
      ? "Identities \u25BE"
      : `${{selectedCount}}/${{totalCount}} identities \u25BE`;
  }}
  const visList = visiblePeopleList();
  const selCount = visList.filter(n => selectedPeople.has(n.id)).length;
  const totalCount = visList.length;
  peopleDropdownBtn.textContent = selCount === totalCount
    ? "People \u25BE"
    : `${{selCount}}/${{totalCount}} people \u25BE`;
}}

searchInput.addEventListener("input", () => {{
  searchTerm = searchInput.value.trim();
  applyFilter();
}});
stage3MultiOrgToggle?.addEventListener("change", () => {{
  buildPeopleDropdown();
  applyFilter();
}});
orgMultiPersonToggle?.addEventListener("change", () => {{
  buildPeopleDropdown();
  applyFilter();
}});
clearBtn.addEventListener("click", () => {{
  searchInput.value = "";
  searchTerm = "";
  applyFilter();
  searchInput.focus();
}});

applyFilter();

// initial zoom to fit
const allX = allNodes.filter(n => n._visible).map(n => n.x);
const allY = allNodes.filter(n => n._visible).map(n => n.y);
const bounds = {{
  x0: Math.min(...allX) - 60, x1: Math.max(...allX) + 60,
  y0: Math.min(...allY) - 40, y1: Math.max(...allY) + 40,
}};
const bw = bounds.x1 - bounds.x0;
const bh = bounds.y1 - bounds.y0;
const scale = Math.min(W / bw, H / bh, 1.5) * 0.85;
const tx = (W - bw * scale) / 2 - bounds.x0 * scale;
const ty = (H - bh * scale) / 2 - bounds.y0 * scale;
svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
</script>
</body>
</html>"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consolidate aliases and generate network graph.",
    )
    parser.add_argument("run_ids", type=int, nargs="+", help="One or more run IDs")
    parser.add_argument(
        "--out",
        default=None,
        help="Output HTML path (default: output/run_<ids>_graph.html)",
    )
    args = parser.parse_args()

    if len(args.run_ids) == 1:
        data = consolidate_run(args.run_ids[0])
    else:
        data = consolidate_multi_run(args.run_ids)

    print(f"\n{'='*70}")
    print(f"  Consolidated ranking (run {data['run_id']})")
    print(f"{'='*70}\n")
    for rank, entry in enumerate(data["consolidated"], 1):
        aliases = ", ".join(entry["aliases"])
        print(f"  {rank:>3}.  {entry['label']}")
        if len(entry["aliases"]) > 1:
            print(f"        aliases: {aliases}")
        print(f"        orgs={entry['org_count']}  roles={entry['role_count']}  score={entry['score']}")
        print()

    id_slug = "+".join(str(r) for r in args.run_ids)
    out_path = args.out or f"output/run_{id_slug}_graph.html"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(render_html(data), encoding="utf-8")
    print(f"Graph written to {out_path}")

    try:
        webbrowser.open(Path(out_path).resolve().as_uri())
    except Exception:
        pass


if __name__ == "__main__":
    main()

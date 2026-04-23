from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NETLIFY_ROOT = PROJECT_ROOT / "netlify_graph_viewer"
OUTPUT_ROOT = PROJECT_ROOT / "output"
REDIRECTS_PATH = NETLIFY_ROOT / "_redirects"
ROOT_INDEX_PATH = NETLIFY_ROOT / "index.html"

SOURCE_TO_TARGET = {
    "latest_graph.html": "index.html",
    "graph-data.json": "graph-data.json",
    "graph-data-open-letters.json": "graph-data-open-letters.json",
    "graph-data-low-confidence-nodes.json": "graph-data-low-confidence-nodes.json",
    "graph-data-low-confidence.json": "graph-data-low-confidence.json",
    "address-coordinates.json": "address-coordinates.json",
}

HONORIFICS = {
    "mr",
    "mrs",
    "ms",
    "miss",
    "dr",
    "prof",
    "professor",
    "sheikh",
    "shaykh",
    "shaikh",
    "sir",
}


@dataclass(frozen=True)
class GraphBundle:
    key: str
    title: str
    database_candidates: tuple[str, ...]
    merge_database_candidates: tuple[str, ...] = ()

    def resolve_database_path(self) -> Path:
        for relative_path in self.database_candidates:
            candidate = PROJECT_ROOT / relative_path
            if candidate.exists():
                return candidate
        joined = ", ".join(self.database_candidates)
        raise FileNotFoundError(f"No database found for {self.key!r}; checked: {joined}")

    def resolve_merge_database_paths(self) -> list[Path]:
        paths: list[Path] = []
        for relative_path in self.merge_database_candidates:
            candidate = PROJECT_ROOT / relative_path
            if candidate.exists():
                paths.append(candidate)
        return paths


GRAPH_BUNDLES = (
    GraphBundle("mb", "MB", ("old_dbs/charity_links.filtered_rebuild.sqlite", "data/istari_latest.db")),
    GraphBundle("iums", "IUMS", ("data/iums_uk.db",)),
    GraphBundle(
        "sevenspikes",
        "Seven Spikes",
        ("data/seven_spikes_sandbox.db", "data/sandbox_seven_spikes.db"),
        ("data/seven_spikes_trustees.db",),
    ),
    GraphBundle(
        "expanded-mb-names",
        "Expanded MB Names",
        ("data/expanded_mb_names.db",),
        ("data/mahfuzh_safiee.db",),
    ),
)


def clean_bundle_directory(bundle_dir: Path) -> None:
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)


def copy_bundle_outputs(bundle_dir: Path) -> None:
    for source_name, target_name in SOURCE_TO_TARGET.items():
        source_path = OUTPUT_ROOT / source_name
        if not source_path.exists():
            raise FileNotFoundError(f"Expected rebuild output was not created: {source_path}")
        shutil.copy2(source_path, bundle_dir / target_name)


def write_redirects_file() -> None:
    REDIRECTS_PATH.write_text(
        "/ /mb/ 302\n"
        "/mb /mb/ 301\n"
        "/iums /iums/ 301\n"
        "/sevenspikes /sevenspikes/ 301\n"
        "/expanded-mb-names /expanded-mb-names/ 301\n",
        encoding="utf-8",
    )


def write_root_redirect_index() -> None:
    ROOT_INDEX_PATH.write_text(
        """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="0; url=/mb/">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redirecting to MB</title>
  <script>
    window.location.replace("/mb/");
  </script>
</head>
<body>
  <p>Redirecting to <a href="/mb/">MB</a>...</p>
</body>
</html>
""",
        encoding="utf-8",
    )


def normalize_person_name(value: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", " ", str(value or "").lower().strip())
    tokens = [token for token in cleaned.split() if token and token not in HONORIFICS]
    return " ".join(tokens)


def lookup_existing_id(
    cursor: sqlite3.Cursor,
    table: str,
    where_clause: str,
    params: tuple[object, ...],
) -> int | None:
    row = cursor.execute(f"SELECT id FROM {table} WHERE {where_clause}", params).fetchone()
    return int(row[0]) if row else None


def merge_database_into(target_path: Path, source_path: Path) -> None:
    target = sqlite3.connect(target_path)
    source = sqlite3.connect(source_path)
    target.row_factory = sqlite3.Row
    source.row_factory = sqlite3.Row
    target.execute("PRAGMA foreign_keys = OFF")
    source.execute("PRAGMA foreign_keys = OFF")

    run_ids: dict[int, int] = {}
    organisation_ids: dict[int, int] = {}
    address_ids: dict[int, int] = {}
    person_ids: dict[int, int] = {}
    evidence_ids: dict[int, int] = {}
    candidate_ids: dict[int, int] = {}

    try:
        for row in source.execute("SELECT * FROM runs ORDER BY id"):
            cursor = target.execute(
                """
                INSERT INTO runs (seed_name, creativity_level, created_at)
                VALUES (?, ?, ?)
                """,
                (row["seed_name"], row["creativity_level"], row["created_at"]),
            )
            run_ids[int(row["id"])] = int(cursor.lastrowid)

        for row in source.execute("SELECT * FROM input_names ORDER BY id"):
            target.execute(
                "INSERT INTO input_names (run_id, input_name) VALUES (?, ?)",
                (run_ids[int(row["run_id"])], row["input_name"]),
            )

        for row in source.execute("SELECT * FROM name_variants ORDER BY id"):
            target.execute(
                """
                INSERT INTO name_variants (run_id, variant_name, strategy, creativity_level)
                VALUES (?, ?, ?, ?)
                """,
                (run_ids[int(row["run_id"])], row["variant_name"], row["strategy"], row["creativity_level"]),
            )

        for row in source.execute("SELECT * FROM organisations ORDER BY id"):
            existing_id = lookup_existing_id(
                target,
                "organisations",
                "registry_type = ? AND registry_number = ? AND suffix = ?",
                (row["registry_type"], row["registry_number"], row["suffix"]),
            )
            if existing_id is None:
                cursor = target.execute(
                    """
                    INSERT INTO organisations (
                        registry_type, registry_number, suffix, organisation_number, name, status, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["registry_type"],
                        row["registry_number"],
                        row["suffix"],
                        row["organisation_number"],
                        row["name"],
                        row["status"],
                        row["metadata_json"],
                    ),
                )
                existing_id = int(cursor.lastrowid)
            organisation_ids[int(row["id"])] = existing_id

        for row in source.execute("SELECT * FROM addresses ORDER BY id"):
            existing_id = lookup_existing_id(
                target,
                "addresses",
                "normalized_key = ?",
                (row["normalized_key"],),
            )
            if existing_id is None:
                cursor = target.execute(
                    """
                    INSERT INTO addresses (label, normalized_key, postcode, country, metadata_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (row["label"], row["normalized_key"], row["postcode"], row["country"], row["metadata_json"]),
                )
                existing_id = int(cursor.lastrowid)
            address_ids[int(row["id"])] = existing_id

        for row in source.execute("SELECT * FROM people ORDER BY id"):
            existing_id = lookup_existing_id(
                target,
                "people",
                "identity_key = ?",
                (row["identity_key"],),
            )
            if existing_id is None:
                cursor = target.execute(
                    "INSERT INTO people (canonical_name, identity_key) VALUES (?, ?)",
                    (row["canonical_name"], row["identity_key"]),
                )
                existing_id = int(cursor.lastrowid)
            person_ids[int(row["id"])] = existing_id

        for row in source.execute("SELECT * FROM identities ORDER BY id"):
            target.execute(
                """
                INSERT OR IGNORE INTO identities (canonical_name, source_run_id, source_person_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    row["canonical_name"],
                    run_ids.get(int(row["source_run_id"])) if row["source_run_id"] is not None else None,
                    row["source_person_name"],
                    row["created_at"],
                ),
            )

        for row in source.execute("SELECT * FROM organisation_addresses ORDER BY id"):
            target.execute(
                """
                INSERT OR IGNORE INTO organisation_addresses (
                    organisation_id, address_id, source, relationship_phrase, metadata_json
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    organisation_ids[int(row["organisation_id"])],
                    address_ids[int(row["address_id"])],
                    row["source"],
                    row["relationship_phrase"],
                    row["metadata_json"],
                ),
            )

        for row in source.execute("SELECT * FROM run_organisations ORDER BY id"):
            target.execute(
                """
                INSERT OR IGNORE INTO run_organisations (run_id, organisation_id, stage, source, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_ids[int(row["run_id"])],
                    organisation_ids[int(row["organisation_id"])],
                    row["stage"],
                    row["source"],
                    row["metadata_json"],
                ),
            )

        for row in source.execute("SELECT * FROM run_org_processing ORDER BY id"):
            target.execute(
                """
                INSERT OR IGNORE INTO run_org_processing (run_id, organisation_id, stage, metadata_json, processed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_ids[int(row["run_id"])],
                    organisation_ids[int(row["organisation_id"])],
                    row["stage"],
                    row["metadata_json"],
                    row["processed_at"],
                ),
            )

        for row in source.execute("SELECT * FROM person_org_roles ORDER BY id"):
            target.execute(
                """
                INSERT OR IGNORE INTO person_org_roles (
                    person_id, organisation_id, role_type, role_label, relationship_kind, relationship_phrase,
                    source, confidence_class, edge_weight, start_date, end_date, provenance_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    person_ids[int(row["person_id"])],
                    organisation_ids[int(row["organisation_id"])],
                    row["role_type"],
                    row["role_label"],
                    row["relationship_kind"],
                    row["relationship_phrase"],
                    row["source"],
                    row["confidence_class"],
                    row["edge_weight"],
                    row["start_date"],
                    row["end_date"],
                    row["provenance_json"],
                ),
            )

        for row in source.execute("SELECT * FROM person_sanctions ORDER BY person_id"):
            target.execute(
                """
                INSERT OR REPLACE INTO person_sanctions (
                    person_id, is_sanctioned, screened_name, screened_birth_month,
                    screened_birth_year, matches_json, checked_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    person_ids[int(row["person_id"])],
                    row["is_sanctioned"],
                    row["screened_name"],
                    row["screened_birth_month"],
                    row["screened_birth_year"],
                    row["matches_json"],
                    row["checked_at"],
                ),
            )

        for row in source.execute("SELECT * FROM evidence_items ORDER BY id"):
            cursor = target.execute(
                """
                INSERT INTO evidence_items (
                    run_id, source, source_key, title, url, snippet, raw_payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_ids[int(row["run_id"])],
                    row["source"],
                    row["source_key"],
                    row["title"],
                    row["url"],
                    row["snippet"],
                    row["raw_payload_json"],
                    row["created_at"],
                ),
            )
            evidence_ids[int(row["id"])] = int(cursor.lastrowid)

        for row in source.execute("SELECT * FROM candidate_matches ORDER BY id"):
            cursor = target.execute(
                """
                INSERT INTO candidate_matches (
                    run_id, variant_name, candidate_name, organisation_name, registry_type, registry_number,
                    suffix, source, evidence_id, feature_payload_json, score, raw_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_ids[int(row["run_id"])],
                    row["variant_name"],
                    row["candidate_name"],
                    row["organisation_name"],
                    row["registry_type"],
                    row["registry_number"],
                    row["suffix"],
                    row["source"],
                    evidence_ids.get(int(row["evidence_id"])) if row["evidence_id"] is not None else None,
                    row["feature_payload_json"],
                    row["score"],
                    row["raw_payload_json"],
                ),
            )
            candidate_ids[int(row["id"])] = int(cursor.lastrowid)

        for row in source.execute("SELECT * FROM resolution_decisions ORDER BY id"):
            target.execute(
                """
                INSERT INTO resolution_decisions (
                    run_id, candidate_match_id, status, confidence, canonical_name, person_identity_key,
                    explanation, rule_score, alias_status, llm_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_ids[int(row["run_id"])],
                    candidate_ids[int(row["candidate_match_id"])],
                    row["status"],
                    row["confidence"],
                    row["canonical_name"],
                    row["person_identity_key"],
                    row["explanation"],
                    row["rule_score"],
                    row["alias_status"],
                    row["llm_payload_json"],
                ),
            )

        target.commit()
    finally:
        source.close()
        target.close()


def backfill_seed_role_scopes(database_path: Path) -> None:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = OFF")
    try:
        runs = connection.execute(
            "SELECT id, seed_name FROM runs ORDER BY id"
        ).fetchall()
        people = connection.execute(
            "SELECT id, canonical_name FROM people ORDER BY id"
        ).fetchall()
        person_name_to_ids: dict[str, list[int]] = {}
        for row in people:
            normalized_name = normalize_person_name(str(row["canonical_name"] or ""))
            if not normalized_name:
                continue
            person_name_to_ids.setdefault(normalized_name, []).append(int(row["id"]))

        for run in runs:
            seed_name = str(run["seed_name"] or "")
            normalized_seed_name = normalize_person_name(seed_name)
            if not normalized_seed_name:
                continue
            person_ids = person_name_to_ids.get(normalized_seed_name, [])
            if not person_ids:
                continue
            placeholders = ", ".join("?" for _ in person_ids)
            scoped_org_ids = {
                int(row["organisation_id"])
                for row in connection.execute(
                    "SELECT organisation_id FROM run_organisations WHERE run_id = ?",
                    (int(run["id"]),),
                ).fetchall()
            }
            for row in connection.execute(
                f"""
                SELECT DISTINCT organisation_id
                FROM person_org_roles
                WHERE person_id IN ({placeholders})
                """,
                tuple(person_ids),
            ):
                organisation_id = int(row["organisation_id"])
                if organisation_id in scoped_org_ids:
                    continue
                connection.execute(
                    """
                    INSERT OR IGNORE INTO run_organisations(
                        run_id,
                        organisation_id,
                        stage,
                        source,
                        metadata_json
                    ) VALUES(?, ?, ?, ?, ?)
                    """,
                    (
                        int(run["id"]),
                        organisation_id,
                        "step1_seed_match",
                        "merged_seed_role_backfill",
                        json.dumps(
                            {
                                "reason": "seed matched existing person role in merged database",
                                "seed_name": seed_name,
                            }
                        ),
                    ),
                )
                scoped_org_ids.add(organisation_id)
        connection.commit()
    finally:
        connection.close()


def prepare_bundle_database(bundle: GraphBundle) -> Path:
    base_database_path = bundle.resolve_database_path()
    merge_database_paths = bundle.resolve_merge_database_paths()
    if not merge_database_paths:
        return base_database_path

    merged_database_path = OUTPUT_ROOT / f"{bundle.key}-combined.db"
    merged_database_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(base_database_path, merged_database_path)
    for merge_database_path in merge_database_paths:
        print(f"Merging {merge_database_path} into {merged_database_path}...", flush=True)
        merge_database_into(merged_database_path, merge_database_path)
    backfill_seed_role_scopes(merged_database_path)
    return merged_database_path


def build_bundle(bundle: GraphBundle) -> None:
    database_path = prepare_bundle_database(bundle)
    env = os.environ.copy()
    env["DATABASE_PATH"] = str(database_path)
    env["GRAPH_VIEW_TITLE"] = bundle.title
    env["SKIP_SANCTIONS_REFRESH"] = "1"
    print(f"Building {bundle.key} from {database_path}...", flush=True)
    subprocess.run(
        [sys.executable, "scripts/rebuild_graph.py"],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )
    bundle_dir = NETLIFY_ROOT / bundle.key
    clean_bundle_directory(bundle_dir)
    copy_bundle_outputs(bundle_dir)
    print(f"Wrote bundle to {bundle_dir}", flush=True)


def main() -> None:
    NETLIFY_ROOT.mkdir(parents=True, exist_ok=True)
    for bundle in GRAPH_BUNDLES:
        build_bundle(bundle)
    write_redirects_file()
    write_root_redirect_index()
    print("Finished building multi-graph Netlify site.", flush=True)


if __name__ == "__main__":
    main()

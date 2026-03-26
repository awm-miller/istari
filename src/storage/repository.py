from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from src.models import CandidateMatch, EvidenceItem, OrganisationRecord, ResolutionDecision


class Repository:
    def __init__(self, database_path: Path, schema_path: Path) -> None:
        self.database_path = Path(database_path)
        self.schema_path = Path(schema_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def init_db(self) -> None:
        schema = self.schema_path.read_text(encoding="utf-8")
        with self.connect() as connection:
            connection.executescript(schema)
            self._migrate_people_table(connection)
            self._ensure_column(
                connection,
                "person_org_roles",
                "confidence_class",
                "TEXT NOT NULL DEFAULT 'medium'",
            )
            self._ensure_column(
                connection,
                "person_org_roles",
                "edge_weight",
                "REAL NOT NULL DEFAULT 1.0",
            )
            self._ensure_column(
                connection,
                "resolution_decisions",
                "alias_status",
                "TEXT NOT NULL DEFAULT 'none'",
            )
            self._ensure_column(
                connection,
                "resolution_decisions",
                "person_identity_key",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "person_org_roles",
                "relationship_kind",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                connection,
                "person_org_roles",
                "relationship_phrase",
                "TEXT NOT NULL DEFAULT ''",
            )

    def create_run(self, seed_name: str, creativity_level: str) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO runs(seed_name, creativity_level) VALUES(?, ?)",
                (seed_name, creativity_level),
            )
            run_id = int(cursor.lastrowid)
            connection.execute(
                "INSERT INTO input_names(run_id, input_name) VALUES(?, ?)",
                (run_id, seed_name),
            )
            return run_id

    def insert_name_variants(self, run_id: int, variants: list[dict[str, Any]]) -> None:
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO name_variants(run_id, variant_name, strategy, creativity_level)
                VALUES(?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        variant["name"],
                        variant["strategy"],
                        variant["creativity_level"],
                    )
                    for variant in variants
                ],
            )

    def upsert_organisation(self, organisation: OrganisationRecord) -> int:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO organisations(
                    registry_type,
                    registry_number,
                    suffix,
                    organisation_number,
                    name,
                    status,
                    metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(registry_type, registry_number, suffix) DO UPDATE SET
                    organisation_number = excluded.organisation_number,
                    name = excluded.name,
                    status = excluded.status,
                    metadata_json = excluded.metadata_json
                """,
                (
                    organisation.registry_type,
                    organisation.registry_number,
                    organisation.suffix,
                    organisation.organisation_number,
                    organisation.name,
                    organisation.status,
                    json.dumps(organisation.metadata),
                ),
            )
            row = connection.execute(
                """
                SELECT id
                FROM organisations
                WHERE registry_type = ? AND registry_number = ? AND suffix = ?
                """,
                (
                    organisation.registry_type,
                    organisation.registry_number,
                    organisation.suffix,
                ),
            ).fetchone()
            return int(row["id"])

    def upsert_address(
        self,
        *,
        label: str,
        normalized_key: str,
        postcode: str | None = None,
        country: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO addresses(
                    label,
                    normalized_key,
                    postcode,
                    country,
                    metadata_json
                ) VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(normalized_key) DO UPDATE SET
                    label = excluded.label,
                    postcode = excluded.postcode,
                    country = excluded.country,
                    metadata_json = excluded.metadata_json
                """,
                (
                    label,
                    normalized_key,
                    postcode,
                    country,
                    json.dumps(metadata or {}),
                ),
            )
            row = connection.execute(
                """
                SELECT id
                FROM addresses
                WHERE normalized_key = ?
                """,
                (normalized_key,),
            ).fetchone()
            return int(row["id"])

    def link_organisation_address(
        self,
        organisation_id: int,
        address_id: int,
        *,
        source: str,
        relationship_phrase: str = "is registered at",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO organisation_addresses(
                    organisation_id,
                    address_id,
                    source,
                    relationship_phrase,
                    metadata_json
                ) VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(organisation_id, address_id) DO UPDATE SET
                    source = excluded.source,
                    relationship_phrase = excluded.relationship_phrase,
                    metadata_json = excluded.metadata_json
                """,
                (
                    organisation_id,
                    address_id,
                    source,
                    relationship_phrase,
                    json.dumps(metadata or {}),
                ),
            )
            row = connection.execute(
                """
                SELECT id
                FROM organisation_addresses
                WHERE organisation_id = ? AND address_id = ?
                """,
                (organisation_id, address_id),
            ).fetchone()
            return int(row["id"])

    def link_run_organisation(
        self,
        run_id: int,
        organisation_id: int,
        *,
        stage: str,
        source: str,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        payload = metadata or {}
        with self.connect() as connection:
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
                    run_id,
                    organisation_id,
                    stage,
                    source,
                    json.dumps(payload),
                ),
            )
            row = connection.execute(
                """
                SELECT id
                FROM run_organisations
                WHERE run_id = ? AND organisation_id = ? AND stage = ? AND source = ?
                """,
                (run_id, organisation_id, stage, source),
            ).fetchone()
            return int(row["id"])

    def get_run_organisations(
        self,
        run_id: int,
        *,
        stages: list[str] | None = None,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT DISTINCT
                organisations.id,
                organisations.registry_type,
                organisations.registry_number,
                organisations.suffix,
                organisations.organisation_number,
                organisations.name,
                organisations.status,
                organisations.metadata_json,
                run_organisations.stage,
                run_organisations.source,
                run_organisations.metadata_json AS run_metadata_json
            FROM run_organisations
            JOIN organisations
                ON organisations.id = run_organisations.organisation_id
            WHERE run_organisations.run_id = ?
        """
        params: list[Any] = [run_id]
        if stages:
            placeholders = ",".join("?" for _ in stages)
            sql += f" AND run_organisations.stage IN ({placeholders})"
            params.extend(stages)
        sql += " ORDER BY organisations.name ASC, organisations.registry_type ASC, organisations.registry_number ASC"
        with self.connect() as connection:
            return connection.execute(sql, params).fetchall()

    def insert_evidence_item(self, run_id: int, item: EvidenceItem) -> int:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO evidence_items(
                    run_id,
                    source,
                    source_key,
                    title,
                    url,
                    snippet,
                    raw_payload_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    item.source,
                    item.source_key,
                    item.title,
                    item.url,
                    item.snippet,
                    json.dumps(item.raw_payload),
                ),
            )
            row = connection.execute(
                """
                SELECT id
                FROM evidence_items
                WHERE run_id = ? AND source = ? AND source_key = ?
                """,
                (run_id, item.source, item.source_key),
            ).fetchone()
            return int(row["id"])

    def insert_candidate_match(self, run_id: int, item: CandidateMatch) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO candidate_matches(
                    run_id,
                    variant_name,
                    candidate_name,
                    organisation_name,
                    registry_type,
                    registry_number,
                    suffix,
                    source,
                    evidence_id,
                    feature_payload_json,
                    score,
                    raw_payload_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    item.name_variant,
                    item.candidate_name,
                    item.organisation_name,
                    item.registry_type,
                    item.registry_number,
                    item.suffix,
                    item.source,
                    item.evidence_id,
                    json.dumps(item.feature_payload),
                    item.score,
                    json.dumps(item.raw_payload),
                ),
            )
            return int(cursor.lastrowid)

    def get_candidate_matches(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM candidate_matches
                WHERE run_id = ?
                ORDER BY score DESC, id ASC
                """,
                (run_id,),
            ).fetchall()

    def get_unresolved_candidate_matches(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT candidate_matches.*
                FROM candidate_matches
                LEFT JOIN resolution_decisions
                    ON resolution_decisions.candidate_match_id = candidate_matches.id
                WHERE candidate_matches.run_id = ?
                  AND resolution_decisions.id IS NULL
                ORDER BY candidate_matches.score DESC, candidate_matches.id ASC
                """,
                (run_id,),
            ).fetchall()

    def insert_resolution_decision(
        self,
        run_id: int,
        candidate_match_id: int,
        decision: ResolutionDecision,
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO resolution_decisions(
                    run_id,
                    candidate_match_id,
                    status,
                    confidence,
                    canonical_name,
                    person_identity_key,
                    explanation,
                    rule_score,
                    alias_status,
                    llm_payload_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    candidate_match_id,
                    decision.status,
                    decision.confidence,
                    decision.canonical_name,
                    decision.person_identity_key,
                    decision.explanation,
                    decision.rule_score,
                    decision.alias_status,
                    json.dumps(decision.llm_payload),
                ),
            )
            return int(cursor.lastrowid)

    def upsert_person(self, canonical_name: str, identity_key: str | None = None) -> int:
        cleaned_name = " ".join(str(canonical_name).split()).strip()
        cleaned_key = " ".join(str(identity_key or cleaned_name).split()).strip()
        if not cleaned_name:
            raise ValueError("Person name is required.")
        if not cleaned_key:
            raise ValueError("Person identity key is required.")
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO people(canonical_name, identity_key)
                VALUES(?, ?)
                ON CONFLICT(identity_key) DO UPDATE SET
                    canonical_name = excluded.canonical_name
                """,
                (cleaned_name, cleaned_key),
            )
            row = connection.execute(
                "SELECT id FROM people WHERE identity_key = ?",
                (cleaned_key,),
            ).fetchone()
            return int(row["id"])

    def upsert_identity(
        self,
        canonical_name: str,
        *,
        source_run_id: int | None = None,
        source_person_name: str | None = None,
    ) -> int:
        cleaned_name = " ".join(str(canonical_name).split()).strip()
        if not cleaned_name:
            raise ValueError("Identity name is required.")
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO identities(canonical_name, source_run_id, source_person_name)
                VALUES(?, ?, ?)
                ON CONFLICT(canonical_name) DO UPDATE SET
                    source_run_id = COALESCE(identities.source_run_id, excluded.source_run_id),
                    source_person_name = COALESCE(identities.source_person_name, excluded.source_person_name)
                """,
                (cleaned_name, source_run_id, source_person_name or cleaned_name),
            )
            row = connection.execute(
                "SELECT id FROM identities WHERE canonical_name = ?",
                (cleaned_name,),
            ).fetchone()
            return int(row["id"])

    def list_identities(self, limit: int = 100) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT id, canonical_name, source_run_id, source_person_name, created_at
                FROM identities
                ORDER BY created_at DESC, canonical_name ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

    def upsert_role(
        self,
        *,
        person_id: int,
        organisation_id: int,
        role_type: str,
        role_label: str,
        relationship_kind: str = "",
        relationship_phrase: str = "",
        source: str,
        confidence_class: str,
        edge_weight: float,
        provenance: dict[str, Any],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO person_org_roles(
                    person_id,
                    organisation_id,
                    role_type,
                    role_label,
                    relationship_kind,
                    relationship_phrase,
                    source,
                    confidence_class,
                    edge_weight,
                    start_date,
                    end_date,
                    provenance_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    person_id,
                    organisation_id,
                    role_type,
                    role_label,
                    relationship_kind,
                    relationship_phrase,
                    source,
                    confidence_class,
                    edge_weight,
                    start_date,
                    end_date,
                    json.dumps(provenance),
                ),
            )

    def get_run(self, run_id: int) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()

    def get_latest_unique_run_ids(self) -> list[int]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT latest.id
                FROM (
                    SELECT MAX(id) AS id
                    FROM runs
                    GROUP BY seed_name
                ) AS latest
                ORDER BY latest.id ASC
                """
            ).fetchall()
            return [int(row["id"]) for row in rows]

    def get_run_variant_names(self, run_id: int) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT variant_name
                FROM name_variants
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchall()
            return [str(row["variant_name"]) for row in rows]

    def get_confirmed_alias_rows(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT
                    candidate_matches.variant_name,
                    candidate_matches.candidate_name,
                    resolution_decisions.canonical_name,
                    resolution_decisions.alias_status
                FROM resolution_decisions
                JOIN candidate_matches
                    ON candidate_matches.id = resolution_decisions.candidate_match_id
                WHERE resolution_decisions.run_id = ?
                  AND resolution_decisions.alias_status = 'confirmed_alias'
                ORDER BY resolution_decisions.id ASC
                """,
                (run_id,),
            ).fetchall()

    def get_ranked_people(self, limit: int = 25) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                WITH org_weights AS (
                    SELECT
                        person_id,
                        organisation_id,
                        MAX(edge_weight) AS organisation_weight
                    FROM person_org_roles
                    GROUP BY person_id, organisation_id
                )
                SELECT
                    people.id,
                    people.canonical_name,
                    COUNT(DISTINCT org_weights.organisation_id) AS organisation_count,
                    COUNT(person_org_roles.id) AS role_count,
                    ROUND(SUM(org_weights.organisation_weight), 4) AS weighted_organisation_score
                FROM people
                JOIN org_weights ON org_weights.person_id = people.id
                JOIN person_org_roles ON person_org_roles.person_id = people.id
                GROUP BY people.id, people.canonical_name
                ORDER BY weighted_organisation_score DESC, organisation_count DESC, role_count DESC, people.canonical_name ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def get_ranked_people_for_run(self, run_id: int, limit: int = 25) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                WITH scoped_orgs AS (
                    SELECT DISTINCT organisation_id
                    FROM run_organisations
                    WHERE run_id = ?
                    UNION
                    SELECT DISTINCT organisations.id AS organisation_id
                    FROM resolution_decisions
                    JOIN candidate_matches
                        ON candidate_matches.id = resolution_decisions.candidate_match_id
                    JOIN organisations
                        ON organisations.registry_type = candidate_matches.registry_type
                       AND organisations.registry_number = candidate_matches.registry_number
                       AND organisations.suffix = candidate_matches.suffix
                    WHERE resolution_decisions.run_id = ?
                      AND resolution_decisions.status = 'match'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM run_organisations
                          WHERE run_id = ?
                      )
                ),
                org_weights AS (
                    SELECT
                        person_org_roles.person_id,
                        person_org_roles.organisation_id,
                        MAX(person_org_roles.edge_weight) AS organisation_weight
                    FROM person_org_roles
                    JOIN scoped_orgs
                        ON scoped_orgs.organisation_id = person_org_roles.organisation_id
                    GROUP BY person_org_roles.person_id, person_org_roles.organisation_id
                )
                SELECT
                    people.id,
                    people.canonical_name,
                    COUNT(DISTINCT org_weights.organisation_id) AS organisation_count,
                    COUNT(person_org_roles.id) AS role_count,
                    ROUND(SUM(org_weights.organisation_weight), 4) AS weighted_organisation_score
                FROM people
                JOIN org_weights ON org_weights.person_id = people.id
                JOIN person_org_roles ON person_org_roles.person_id = people.id
                JOIN scoped_orgs ON scoped_orgs.organisation_id = person_org_roles.organisation_id
                GROUP BY people.id, people.canonical_name
                ORDER BY weighted_organisation_score DESC, organisation_count DESC, role_count DESC, people.canonical_name ASC
                LIMIT ?
                """,
                (run_id, run_id, run_id, limit),
            ).fetchall()

    def get_run_network_edges(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                WITH scoped_orgs AS (
                    SELECT DISTINCT organisation_id
                    FROM run_organisations
                    WHERE run_id = ?
                    UNION
                    SELECT DISTINCT organisations.id AS organisation_id
                    FROM resolution_decisions
                    JOIN candidate_matches
                        ON candidate_matches.id = resolution_decisions.candidate_match_id
                    JOIN organisations
                        ON organisations.registry_type = candidate_matches.registry_type
                       AND organisations.registry_number = candidate_matches.registry_number
                       AND organisations.suffix = candidate_matches.suffix
                    WHERE resolution_decisions.run_id = ?
                      AND resolution_decisions.status = 'match'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM run_organisations
                          WHERE run_id = ?
                      )
                )
                SELECT
                    people.id AS person_id,
                    people.canonical_name AS person_name,
                    organisations.id AS organisation_id,
                    organisations.name AS organisation_name,
                    organisations.registry_type,
                    organisations.registry_number,
                    organisations.suffix,
                    person_org_roles.role_type,
                    person_org_roles.role_label,
                    person_org_roles.relationship_kind,
                    person_org_roles.relationship_phrase,
                    person_org_roles.source,
                    person_org_roles.confidence_class,
                    person_org_roles.edge_weight
                FROM person_org_roles
                JOIN people
                    ON people.id = person_org_roles.person_id
                JOIN organisations
                    ON organisations.id = person_org_roles.organisation_id
                JOIN scoped_orgs
                    ON scoped_orgs.organisation_id = organisations.id
                ORDER BY person_org_roles.edge_weight DESC, people.canonical_name ASC, organisations.name ASC
                """,
                (run_id, run_id, run_id),
            ).fetchall()

    def get_run_scoped_organisations(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                WITH scoped_orgs AS (
                    SELECT DISTINCT organisation_id
                    FROM run_organisations
                    WHERE run_id = ?
                    UNION
                    SELECT DISTINCT organisations.id AS organisation_id
                    FROM resolution_decisions
                    JOIN candidate_matches
                        ON candidate_matches.id = resolution_decisions.candidate_match_id
                    JOIN organisations
                        ON organisations.registry_type = candidate_matches.registry_type
                       AND organisations.registry_number = candidate_matches.registry_number
                       AND organisations.suffix = candidate_matches.suffix
                    WHERE resolution_decisions.run_id = ?
                      AND resolution_decisions.status = 'match'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM run_organisations
                          WHERE run_id = ?
                      )
                )
                SELECT
                    organisations.id,
                    organisations.registry_type,
                    organisations.registry_number,
                    organisations.suffix,
                    organisations.organisation_number,
                    organisations.name,
                    organisations.status,
                    organisations.metadata_json
                FROM organisations
                JOIN scoped_orgs
                    ON scoped_orgs.organisation_id = organisations.id
                ORDER BY organisations.name ASC, organisations.registry_type ASC, organisations.registry_number ASC
                """,
                (run_id, run_id, run_id),
            ).fetchall()

    def get_run_address_edges(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                WITH scoped_orgs AS (
                    SELECT DISTINCT organisation_id
                    FROM run_organisations
                    WHERE run_id = ?
                    UNION
                    SELECT DISTINCT organisations.id AS organisation_id
                    FROM resolution_decisions
                    JOIN candidate_matches
                        ON candidate_matches.id = resolution_decisions.candidate_match_id
                    JOIN organisations
                        ON organisations.registry_type = candidate_matches.registry_type
                       AND organisations.registry_number = candidate_matches.registry_number
                       AND organisations.suffix = candidate_matches.suffix
                    WHERE resolution_decisions.run_id = ?
                      AND resolution_decisions.status = 'match'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM run_organisations
                          WHERE run_id = ?
                      )
                )
                SELECT
                    organisations.id AS organisation_id,
                    organisations.name AS organisation_name,
                    organisations.registry_type,
                    organisations.registry_number,
                    organisations.suffix,
                    addresses.id AS address_id,
                    addresses.label AS address_label,
                    addresses.normalized_key,
                    addresses.postcode,
                    addresses.country,
                    organisation_addresses.source,
                    organisation_addresses.relationship_phrase,
                    organisation_addresses.metadata_json
                FROM organisation_addresses
                JOIN organisations
                    ON organisations.id = organisation_addresses.organisation_id
                JOIN addresses
                    ON addresses.id = organisation_addresses.address_id
                JOIN scoped_orgs
                    ON scoped_orgs.organisation_id = organisations.id
                ORDER BY organisations.name ASC, addresses.label ASC
                """,
                (run_id, run_id, run_id),
            ).fetchall()

    def get_expanded_people_for_run(self, run_id: int, limit: int = 200) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                WITH scoped_orgs AS (
                    SELECT DISTINCT organisation_id
                    FROM run_organisations
                    WHERE run_id = ?
                    UNION
                    SELECT DISTINCT organisations.id AS organisation_id
                    FROM resolution_decisions
                    JOIN candidate_matches
                        ON candidate_matches.id = resolution_decisions.candidate_match_id
                    JOIN organisations
                        ON organisations.registry_type = candidate_matches.registry_type
                       AND organisations.registry_number = candidate_matches.registry_number
                       AND organisations.suffix = candidate_matches.suffix
                    WHERE resolution_decisions.run_id = ?
                      AND resolution_decisions.status = 'match'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM run_organisations
                          WHERE run_id = ?
                      )
                )
                SELECT
                    people.canonical_name AS person_name,
                    organisations.id AS organisation_id,
                    organisations.registry_type,
                    organisations.registry_number,
                    organisations.suffix,
                    organisations.name AS organisation_name,
                    person_org_roles.role_type,
                    person_org_roles.role_label,
                    person_org_roles.source,
                    person_org_roles.edge_weight
                FROM person_org_roles
                JOIN people
                    ON people.id = person_org_roles.person_id
                JOIN organisations
                    ON organisations.id = person_org_roles.organisation_id
                JOIN scoped_orgs
                    ON scoped_orgs.organisation_id = organisations.id
                ORDER BY person_org_roles.edge_weight DESC, people.canonical_name ASC
                LIMIT ?
                """,
                (run_id, run_id, run_id, limit),
            ).fetchall()

    def get_matched_organisations_for_run(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT DISTINCT
                    organisations.id,
                    organisations.registry_type,
                    organisations.registry_number,
                    organisations.suffix,
                    organisations.name
                FROM resolution_decisions
                JOIN candidate_matches
                    ON candidate_matches.id = resolution_decisions.candidate_match_id
                JOIN organisations
                    ON organisations.registry_type = candidate_matches.registry_type
                   AND organisations.registry_number = candidate_matches.registry_number
                   AND organisations.suffix = candidate_matches.suffix
                WHERE resolution_decisions.run_id = ?
                  AND resolution_decisions.status = 'match'
                ORDER BY organisations.name ASC
                """,
                (run_id,),
            ).fetchall()

    def get_overlap_people_for_runs(
        self,
        run_ids: list[int],
        limit: int = 25,
    ) -> list[sqlite3.Row]:
        cleaned_ids = [int(run_id) for run_id in run_ids]
        if not cleaned_ids:
            return []
        placeholders = ",".join("?" for _ in cleaned_ids)
        params: list[Any] = [*cleaned_ids, int(limit)]
        sql = f"""
            WITH matched AS (
                SELECT
                    resolution_decisions.run_id,
                    resolution_decisions.canonical_name,
                    resolution_decisions.person_identity_key,
                    organisations.id AS org_id,
                    resolution_decisions.confidence
                FROM resolution_decisions
                JOIN candidate_matches
                    ON candidate_matches.id = resolution_decisions.candidate_match_id
                JOIN organisations
                    ON organisations.registry_type = candidate_matches.registry_type
                   AND organisations.registry_number = candidate_matches.registry_number
                   AND organisations.suffix = candidate_matches.suffix
                WHERE resolution_decisions.run_id IN ({placeholders})
                  AND resolution_decisions.status = 'match'
            ),
            person_agg AS (
                SELECT
                    matched.person_identity_key,
                    MIN(matched.canonical_name) AS canonical_name,
                    COUNT(DISTINCT matched.run_id) AS seed_count,
                    COUNT(DISTINCT matched.org_id) AS organisation_count,
                    COUNT(*) AS decision_count,
                    ROUND(AVG(matched.confidence), 4) AS avg_resolution_confidence,
                    ROUND(SUM(matched.confidence), 4) AS confidence_sum
                FROM matched
                GROUP BY matched.person_identity_key
            ),
            person_org_weights AS (
                SELECT
                    people.identity_key AS person_identity_key,
                    person_org_roles.organisation_id,
                    MAX(person_org_roles.edge_weight) AS organisation_weight
                FROM people
                JOIN person_org_roles
                    ON person_org_roles.person_id = people.id
                JOIN matched
                    ON matched.person_identity_key = people.identity_key
                   AND matched.org_id = person_org_roles.organisation_id
                GROUP BY people.identity_key, person_org_roles.organisation_id
            ),
            person_weighted AS (
                SELECT
                    person_org_weights.person_identity_key,
                    ROUND(SUM(person_org_weights.organisation_weight), 4) AS weighted_organisation_score
                FROM person_org_weights
                GROUP BY person_org_weights.person_identity_key
            )
            SELECT
                person_agg.canonical_name,
                person_agg.seed_count,
                person_agg.organisation_count,
                person_agg.decision_count,
                person_agg.avg_resolution_confidence,
                person_agg.confidence_sum,
                COALESCE(person_weighted.weighted_organisation_score, 0.0) AS weighted_organisation_score,
                (
                    SELECT GROUP_CONCAT(DISTINCT runs.id)
                    FROM matched
                    JOIN runs ON runs.id = matched.run_id
                    WHERE matched.person_identity_key = person_agg.person_identity_key
                ) AS run_ids,
                (
                    SELECT GROUP_CONCAT(DISTINCT runs.seed_name)
                    FROM matched
                    JOIN runs ON runs.id = matched.run_id
                    WHERE matched.person_identity_key = person_agg.person_identity_key
                ) AS seed_names
            FROM person_agg
            LEFT JOIN person_weighted
                ON person_weighted.person_identity_key = person_agg.person_identity_key
            ORDER BY
                person_agg.seed_count DESC,
                weighted_organisation_score DESC,
                person_agg.confidence_sum DESC,
                person_agg.organisation_count DESC,
                person_agg.canonical_name ASC
            LIMIT ?
        """
        with self.connect() as connection:
            return connection.execute(sql, params).fetchall()

    def get_overlap_organisations_for_runs(
        self,
        run_ids: list[int],
        limit: int = 25,
    ) -> list[sqlite3.Row]:
        cleaned_ids = [int(run_id) for run_id in run_ids]
        if not cleaned_ids:
            return []
        placeholders = ",".join("?" for _ in cleaned_ids)
        params: list[Any] = [*cleaned_ids, int(limit)]
        sql = f"""
            WITH matched AS (
                SELECT
                    resolution_decisions.run_id,
                    organisations.id AS org_id,
                    organisations.name AS organisation_name,
                    organisations.registry_type,
                    organisations.registry_number,
                    organisations.suffix,
                    resolution_decisions.canonical_name,
                    resolution_decisions.person_identity_key,
                    resolution_decisions.confidence
                FROM resolution_decisions
                JOIN candidate_matches
                    ON candidate_matches.id = resolution_decisions.candidate_match_id
                JOIN organisations
                    ON organisations.registry_type = candidate_matches.registry_type
                   AND organisations.registry_number = candidate_matches.registry_number
                   AND organisations.suffix = candidate_matches.suffix
                WHERE resolution_decisions.run_id IN ({placeholders})
                  AND resolution_decisions.status = 'match'
            ),
            org_agg AS (
                SELECT
                    matched.org_id,
                    matched.organisation_name,
                    matched.registry_type,
                    matched.registry_number,
                    matched.suffix,
                    COUNT(DISTINCT matched.run_id) AS seed_count,
                    COUNT(DISTINCT matched.person_identity_key) AS person_count,
                    COUNT(*) AS decision_count,
                    ROUND(AVG(matched.confidence), 4) AS avg_resolution_confidence,
                    ROUND(SUM(matched.confidence), 4) AS confidence_sum
                FROM matched
                GROUP BY
                    matched.org_id,
                    matched.organisation_name,
                    matched.registry_type,
                    matched.registry_number,
                    matched.suffix
            ),
            org_person_weights AS (
                SELECT
                    matched.org_id,
                    people.identity_key AS person_identity_key,
                    MAX(person_org_roles.edge_weight) AS person_weight
                FROM matched
                JOIN people
                    ON people.identity_key = matched.person_identity_key
                JOIN person_org_roles
                    ON person_org_roles.person_id = people.id
                   AND person_org_roles.organisation_id = matched.org_id
                GROUP BY matched.org_id, people.identity_key
            ),
            org_weighted AS (
                SELECT
                    org_person_weights.org_id,
                    ROUND(SUM(org_person_weights.person_weight), 4) AS weighted_organisation_score
                FROM org_person_weights
                GROUP BY org_person_weights.org_id
            )
            SELECT
                org_agg.org_id,
                org_agg.organisation_name,
                org_agg.registry_type,
                org_agg.registry_number,
                org_agg.suffix,
                org_agg.seed_count,
                org_agg.person_count,
                org_agg.decision_count,
                org_agg.avg_resolution_confidence,
                org_agg.confidence_sum,
                COALESCE(org_weighted.weighted_organisation_score, 0.0) AS weighted_organisation_score,
                (
                    SELECT GROUP_CONCAT(DISTINCT runs.id)
                    FROM matched
                    JOIN runs ON runs.id = matched.run_id
                    WHERE matched.org_id = org_agg.org_id
                ) AS run_ids,
                (
                    SELECT GROUP_CONCAT(DISTINCT runs.seed_name)
                    FROM matched
                    JOIN runs ON runs.id = matched.run_id
                    WHERE matched.org_id = org_agg.org_id
                ) AS seed_names
            FROM org_agg
            LEFT JOIN org_weighted
                ON org_weighted.org_id = org_agg.org_id
            ORDER BY
                org_agg.seed_count DESC,
                org_agg.person_count DESC,
                weighted_organisation_score DESC,
                org_agg.confidence_sum DESC,
                org_agg.organisation_name ASC
            LIMIT ?
        """
        with self.connect() as connection:
            return connection.execute(sql, params).fetchall()

    def _migrate_people_table(self, connection: sqlite3.Connection) -> None:
        columns = connection.execute("PRAGMA table_info(people)").fetchall()
        if any(str(column["name"]) == "identity_key" for column in columns):
            return

        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS people_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL,
                    identity_key TEXT NOT NULL UNIQUE
                )
                """
            )
            connection.execute(
                """
                INSERT INTO people_new(id, canonical_name, identity_key)
                SELECT id, canonical_name, canonical_name
                FROM people
                """
            )
            connection.execute("DROP TABLE people")
            connection.execute("ALTER TABLE people_new RENAME TO people")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_people_canonical_name ON people(canonical_name)"
            )
        finally:
            connection.execute("PRAGMA foreign_keys = ON")

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        definition: str,
    ) -> None:
        columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        if any(str(column["name"]) == column_name for column in columns):
            return
        connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
        )

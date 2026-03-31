from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")
_PLAIN_URL_RE = re.compile(r"(?<!\()(?P<url>https?://[^\s)>\]]+)")
LOW_CONFIDENCE_GROUP_MIN_SIZE = 5


def normalize_mapping_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def slugify_mapping_label(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", normalize_mapping_label(value))
    return slug.strip("_") or "node"


def default_mapping_db_path(project_root: Path) -> Path:
    return project_root / "data" / "mapping_links.sqlite"


def extract_evidence_links(text: str) -> list[dict[str, str]]:
    raw_text = str(text or "")
    found: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for title, url in _MARKDOWN_LINK_RE.findall(raw_text):
        key = (url.strip(), title.strip())
        if key in seen:
            continue
        seen.add(key)
        found.append(
            {
                "kind": "markdown_link",
                "title": title.strip() or url.strip(),
                "url": url.strip(),
            }
        )

    masked_text = _MARKDOWN_LINK_RE.sub("", raw_text)
    for match in _PLAIN_URL_RE.finditer(masked_text):
        url = match.group("url").rstrip(".,);:")
        key = (url, "")
        if not url or key in seen:
            continue
        seen.add(key)
        found.append(
            {
                "kind": "plain_url",
                "title": url,
                "url": url,
            }
        )

    return found


class MappingStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def init_db(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS mapping_imports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_dir TEXT NOT NULL,
                    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS mapping_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    import_id INTEGER NOT NULL REFERENCES mapping_imports(id) ON DELETE CASCADE,
                    workbook_name TEXT NOT NULL,
                    sheet_name TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    normalized_label TEXT NOT NULL,
                    entity_type TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    raw_row_json TEXT NOT NULL,
                    UNIQUE(workbook_name, sheet_name, row_number)
                );

                CREATE TABLE IF NOT EXISTS mapping_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    import_id INTEGER NOT NULL REFERENCES mapping_imports(id) ON DELETE CASCADE,
                    workbook_name TEXT NOT NULL,
                    sheet_name TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    from_label TEXT NOT NULL,
                    from_normalized_label TEXT NOT NULL,
                    to_label TEXT NOT NULL,
                    to_normalized_label TEXT NOT NULL,
                    link_type TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    quality TEXT NOT NULL DEFAULT 'low',
                    raw_row_json TEXT NOT NULL,
                    UNIQUE(workbook_name, sheet_name, row_number)
                );

                CREATE TABLE IF NOT EXISTS mapping_evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mapping_link_id INTEGER NOT NULL REFERENCES mapping_links(id) ON DELETE CASCADE,
                    ordinal INTEGER NOT NULL,
                    evidence_kind TEXT NOT NULL DEFAULT 'plain_url',
                    title TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL DEFAULT '',
                    snippet TEXT NOT NULL DEFAULT '',
                    UNIQUE(mapping_link_id, ordinal)
                );

                CREATE TABLE IF NOT EXISTS mapping_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mapping_link_id INTEGER NOT NULL REFERENCES mapping_links(id) ON DELETE CASCADE,
                    endpoint TEXT NOT NULL,
                    run_key TEXT NOT NULL,
                    matched_node_id TEXT NOT NULL,
                    matched_node_label TEXT NOT NULL,
                    match_type TEXT NOT NULL,
                    UNIQUE(mapping_link_id, endpoint, run_key)
                );

                CREATE INDEX IF NOT EXISTS idx_mapping_entities_normalized_label
                    ON mapping_entities(normalized_label);
                CREATE INDEX IF NOT EXISTS idx_mapping_links_from_normalized_label
                    ON mapping_links(from_normalized_label);
                CREATE INDEX IF NOT EXISTS idx_mapping_links_to_normalized_label
                    ON mapping_links(to_normalized_label);
                CREATE INDEX IF NOT EXISTS idx_mapping_matches_run_key
                    ON mapping_matches(run_key);
                """
            )

    def clear_all(self) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM mapping_matches")
            connection.execute("DELETE FROM mapping_evidence")
            connection.execute("DELETE FROM mapping_links")
            connection.execute("DELETE FROM mapping_entities")
            connection.execute("DELETE FROM mapping_imports")

    def create_import(self, source_dir: Path) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO mapping_imports(source_dir) VALUES(?)",
                (str(source_dir),),
            )
            return int(cursor.lastrowid)

    def insert_entity(
        self,
        *,
        import_id: int,
        workbook_name: str,
        sheet_name: str,
        row_number: int,
        label: str,
        entity_type: str,
        description: str,
        raw_row: list[str],
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO mapping_entities(
                    import_id,
                    workbook_name,
                    sheet_name,
                    row_number,
                    label,
                    normalized_label,
                    entity_type,
                    description,
                    raw_row_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    import_id,
                    workbook_name,
                    sheet_name,
                    row_number,
                    label,
                    normalize_mapping_label(label),
                    entity_type,
                    description,
                    json.dumps(raw_row, ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def insert_link(
        self,
        *,
        import_id: int,
        workbook_name: str,
        sheet_name: str,
        row_number: int,
        from_label: str,
        to_label: str,
        link_type: str,
        description: str,
        raw_row: list[str],
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO mapping_links(
                    import_id,
                    workbook_name,
                    sheet_name,
                    row_number,
                    from_label,
                    from_normalized_label,
                    to_label,
                    to_normalized_label,
                    link_type,
                    description,
                    quality,
                    raw_row_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'low', ?)
                """,
                (
                    import_id,
                    workbook_name,
                    sheet_name,
                    row_number,
                    from_label,
                    normalize_mapping_label(from_label),
                    to_label,
                    normalize_mapping_label(to_label),
                    link_type,
                    description,
                    json.dumps(raw_row, ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def insert_evidence(
        self,
        *,
        mapping_link_id: int,
        ordinal: int,
        evidence_kind: str,
        title: str,
        url: str,
        snippet: str,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO mapping_evidence(
                    mapping_link_id,
                    ordinal,
                    evidence_kind,
                    title,
                    url,
                    snippet
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    mapping_link_id,
                    ordinal,
                    evidence_kind,
                    title,
                    url,
                    snippet,
                ),
            )

    def clear_matches(self, run_key: str) -> None:
        with self.connect() as connection:
            connection.execute("DELETE FROM mapping_matches WHERE run_key = ?", (run_key,))

    def insert_match(
        self,
        *,
        mapping_link_id: int,
        endpoint: str,
        run_key: str,
        matched_node_id: str,
        matched_node_label: str,
        match_type: str,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO mapping_matches(
                    mapping_link_id,
                    endpoint,
                    run_key,
                    matched_node_id,
                    matched_node_label,
                    match_type
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    mapping_link_id,
                    endpoint,
                    run_key,
                    matched_node_id,
                    matched_node_label,
                    match_type,
                ),
            )

    def list_entities(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM mapping_entities
                ORDER BY workbook_name, sheet_name, row_number
                """
            ).fetchall()

    def list_links(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM mapping_links
                ORDER BY workbook_name, sheet_name, row_number
                """
            ).fetchall()

    def list_evidence(self) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM mapping_evidence
                ORDER BY mapping_link_id, ordinal
                """
            ).fetchall()

    def list_matches(self, run_key: str) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT *
                FROM mapping_matches
                WHERE run_key = ?
                ORDER BY mapping_link_id, endpoint
                """,
                (run_key,),
            ).fetchall()


def import_mapping_workbooks(mapping_dir: Path, database_path: Path) -> dict[str, int]:
    try:
        from openpyxl import load_workbook
    except ImportError as exc:  # pragma: no cover - guarded by runtime dependency
        raise RuntimeError("openpyxl is required to import Mapping spreadsheets.") from exc

    mapping_path = Path(mapping_dir)
    workbook_paths = sorted(path for path in mapping_path.glob("*.xlsx") if path.is_file())
    store = MappingStore(database_path)
    store.init_db()
    store.clear_all()
    import_id = store.create_import(mapping_path)

    entity_count = 0
    link_count = 0
    evidence_count = 0

    with store.connect() as connection:
        for workbook_path in workbook_paths:
            workbook = load_workbook(workbook_path, read_only=False, data_only=True)
            for worksheet in workbook.worksheets:
                row_iter = worksheet.iter_rows(values_only=True)
                try:
                    header_row = next(row_iter)
                except StopIteration:
                    continue
                headers = [_cell_text(value) for value in header_row]
                entity_cols = _entity_columns(headers)
                link_cols = _link_columns(headers)

                for row_index, raw_row in enumerate(row_iter, start=2):
                    values = [_cell_text(value) for value in raw_row]
                    if not any(values):
                        continue

                    if entity_cols["label"] is not None:
                        label = values[entity_cols["label"]].strip()
                        entity_type = _column_value(values, entity_cols["type"]).strip()
                        description = _column_value(values, entity_cols["description"]).strip()
                        if label:
                            cursor = connection.execute(
                                """
                                INSERT OR IGNORE INTO mapping_entities(
                                    import_id,
                                    workbook_name,
                                    sheet_name,
                                    row_number,
                                    label,
                                    normalized_label,
                                    entity_type,
                                    description,
                                    raw_row_json
                                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    import_id,
                                    workbook_path.name,
                                    worksheet.title,
                                    row_index,
                                    label,
                                    normalize_mapping_label(label),
                                    entity_type,
                                    description,
                                    json.dumps(values, ensure_ascii=False),
                                ),
                            )
                            if int(cursor.rowcount or 0) > 0:
                                entity_count += 1

                    from_label = _column_value(values, link_cols["from"]).strip()
                    to_label = _column_value(values, link_cols["to"]).strip()
                    if from_label and to_label:
                        link_type = _column_value(values, link_cols["type"]).strip()
                        description = _column_value(values, link_cols["description"]).strip()
                        cursor = connection.execute(
                            """
                            INSERT OR IGNORE INTO mapping_links(
                                import_id,
                                workbook_name,
                                sheet_name,
                                row_number,
                                from_label,
                                from_normalized_label,
                                to_label,
                                to_normalized_label,
                                link_type,
                                description,
                                quality,
                                raw_row_json
                            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'low', ?)
                            """,
                            (
                                import_id,
                                workbook_path.name,
                                worksheet.title,
                                row_index,
                                from_label,
                                normalize_mapping_label(from_label),
                                to_label,
                                normalize_mapping_label(to_label),
                                link_type,
                                description,
                                json.dumps(values, ensure_ascii=False),
                            ),
                        )
                        mapping_link_row = connection.execute(
                            """
                            SELECT id
                            FROM mapping_links
                            WHERE workbook_name = ? AND sheet_name = ? AND row_number = ?
                            """,
                            (
                                workbook_path.name,
                                worksheet.title,
                                row_index,
                            ),
                        ).fetchone()
                        if mapping_link_row is None:
                            continue
                        mapping_link_id = int(mapping_link_row["id"])
                        if int(cursor.rowcount or 0) > 0:
                            link_count += 1
                        for ordinal, evidence in enumerate(
                            extract_evidence_links(description), start=1
                        ):
                            evidence_cursor = connection.execute(
                                """
                                INSERT OR REPLACE INTO mapping_evidence(
                                    mapping_link_id,
                                    ordinal,
                                    evidence_kind,
                                    title,
                                    url,
                                    snippet
                                ) VALUES(?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    mapping_link_id,
                                    ordinal,
                                    evidence["kind"],
                                    evidence["title"],
                                    evidence["url"],
                                    description,
                                ),
                            )
                            if int(evidence_cursor.rowcount or 0) > 0:
                                evidence_count += 1

    return {
        "workbook_count": len(workbook_paths),
        "entity_count": entity_count,
        "link_count": link_count,
        "evidence_count": evidence_count,
    }


def build_low_confidence_overlay(
    *,
    main_data: dict[str, Any],
    database_path: Path,
    run_key: str,
) -> dict[str, Any]:
    store = MappingStore(database_path)
    store.init_db()
    records = _resolved_mapping_records(
        store=store,
        main_data=main_data,
        run_key=run_key,
        record_matches=True,
    )

    overlay_nodes: dict[str, dict[str, Any]] = {}
    overlay_edges: list[dict[str, Any]] = []
    matched_link_count = sum(1 for record in records if record["matched"])
    grouped_records = _group_records_for_overlay(records)
    aggregated_group_count = 0

    for group_records in grouped_records:
        if len(group_records) >= LOW_CONFIDENCE_GROUP_MIN_SIZE and _should_aggregate_group(group_records):
            group_id, group_node, group_edge = _build_aggregate_group(group_records)
            overlay_nodes[group_id] = group_node
            target = group_records[0]["target"]
            if not target["matched"]:
                overlay_nodes[target["id"]] = target["node"]
            overlay_edges.append(group_edge)
            aggregated_group_count += 1
            continue
        for record in group_records:
            if not record["source"]["matched"]:
                overlay_nodes[record["source"]["id"]] = record["source"]["node"]
            if not record["target"]["matched"]:
                overlay_nodes[record["target"]["id"]] = record["target"]["node"]
            overlay_edges.append(_raw_overlay_edge(record))

    return {
        "nodes": list(overlay_nodes.values()),
        "edges": overlay_edges,
        "summary": {
            "run_key": run_key,
            "overlay_node_count": len(overlay_nodes),
            "overlay_edge_count": len(overlay_edges),
            "matched_link_count": matched_link_count,
            "aggregated_group_count": aggregated_group_count,
        },
    }


def build_low_confidence_group_details(
    *,
    main_data: dict[str, Any],
    database_path: Path,
    run_key: str,
) -> dict[str, dict[str, Any]]:
    store = MappingStore(database_path)
    store.init_db()
    records = _resolved_mapping_records(
        store=store,
        main_data=main_data,
        run_key=run_key,
        record_matches=False,
    )

    grouped_details: dict[str, dict[str, Any]] = {}
    for group_records in _group_records_for_overlay(records):
        if len(group_records) < LOW_CONFIDENCE_GROUP_MIN_SIZE or not _should_aggregate_group(group_records):
            continue
        group_id, group_node, _group_edge = _build_aggregate_group(group_records)
        detail_nodes: dict[str, dict[str, Any]] = {}
        detail_edges: list[dict[str, Any]] = []
        for record in group_records:
            if not record["source"]["matched"]:
                detail_nodes[record["source"]["id"]] = record["source"]["node"]
            detail_edges.append(_raw_overlay_edge(record))
        grouped_details[group_id] = {
            "group_id": group_id,
            "group_node": group_node,
            "nodes": list(detail_nodes.values()),
            "edges": detail_edges,
            "summary": {
                "member_count": len(group_records),
                "target_id": group_records[0]["target"]["id"],
                "target_label": group_records[0]["target"]["label"],
            },
        }
    return grouped_details


def build_low_confidence_edge_details(*, database_path: Path) -> dict[str, dict[str, Any]]:
    store = MappingStore(database_path)
    store.init_db()

    evidence_by_link_id = _evidence_by_link_id(store.list_evidence())
    detail_by_edge_id: dict[str, dict[str, Any]] = {}
    for link_row in store.list_links():
        link_id = int(link_row["id"])
        description = str(link_row["description"] or "").strip()
        phrase = _mapping_phrase(str(link_row["link_type"] or "").strip())
        evidence_items = _mapping_evidence_items(evidence_by_link_id.get(link_id, []))
        tooltip_lines = [line for line in [str(link_row["link_type"] or "").strip(), description] if line]
        tooltip_lines.append(
            _mapping_import_line(
                workbook_name=str(link_row["workbook_name"]),
                sheet_name=str(link_row["sheet_name"]),
                row_number=int(link_row["row_number"]),
            )
        )
        detail_by_edge_id[_mapping_edge_id(link_id)] = {
            "tooltip": description or f"{link_row['from_label']} {phrase} {link_row['to_label']}",
            "tooltip_lines": tooltip_lines,
            "evidence": evidence_items[0] if evidence_items else None,
            "evidence_items": evidence_items,
        }
    return detail_by_edge_id


def _resolved_mapping_records(
    *,
    store: MappingStore,
    main_data: dict[str, Any],
    run_key: str,
    record_matches: bool,
) -> list[dict[str, Any]]:
    node_index = _main_node_index(main_data)
    entity_by_normalized = _entity_rows_by_normalized(store.list_entities())
    if record_matches:
        store.clear_matches(run_key)

    records: list[dict[str, Any]] = []
    for link_row in store.list_links():
        from_match = _unique_match(node_index, str(link_row["from_normalized_label"]))
        to_match = _unique_match(node_index, str(link_row["to_normalized_label"]))

        if record_matches and from_match:
            store.insert_match(
                mapping_link_id=int(link_row["id"]),
                endpoint="from",
                run_key=run_key,
                matched_node_id=from_match["node_id"],
                matched_node_label=from_match["node_label"],
                match_type=from_match["match_type"],
            )
        if record_matches and to_match:
            store.insert_match(
                mapping_link_id=int(link_row["id"]),
                endpoint="to",
                run_key=run_key,
                matched_node_id=to_match["node_id"],
                matched_node_label=to_match["node_label"],
                match_type=to_match["match_type"],
            )

        source = _resolved_mapping_endpoint(
            label=str(link_row["from_label"]),
            normalized_label=str(link_row["from_normalized_label"]),
            match=from_match,
            entity_row=entity_by_normalized.get(str(link_row["from_normalized_label"])),
        )
        target = _resolved_mapping_endpoint(
            label=str(link_row["to_label"]),
            normalized_label=str(link_row["to_normalized_label"]),
            match=to_match,
            entity_row=entity_by_normalized.get(str(link_row["to_normalized_label"])),
        )
        if source["id"] == target["id"]:
            continue

        link_type = str(link_row["link_type"] or "").strip()
        records.append(
            {
                "link_id": int(link_row["id"]),
                "source": source,
                "target": target,
                "link_type": link_type,
                "phrase": _mapping_phrase(link_type),
                "import_line": _mapping_import_line(
                    workbook_name=str(link_row["workbook_name"]),
                    sheet_name=str(link_row["sheet_name"]),
                    row_number=int(link_row["row_number"]),
                ),
                "matched": bool(from_match or to_match),
            }
        )
    return records


def _main_node_index(main_data: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    node_index: dict[str, list[dict[str, str]]] = {}
    for node in main_data.get("nodes") or []:
        if str(node.get("kind") or "") == "seed":
            continue
        label = str(node.get("label") or "").strip()
        if label:
            node_index.setdefault(normalize_mapping_label(label), []).append(
                {
                    "node_id": str(node["id"]),
                    "node_label": label,
                    "match_type": "exact_label",
                }
            )
        for alias in node.get("aliases") or []:
            alias_text = str(alias or "").strip()
            if not alias_text:
                continue
            node_index.setdefault(normalize_mapping_label(alias_text), []).append(
                {
                    "node_id": str(node["id"]),
                    "node_label": label or alias_text,
                    "match_type": "exact_alias",
                }
            )
    return node_index


def _entity_rows_by_normalized(rows: list[sqlite3.Row]) -> dict[str, sqlite3.Row]:
    entity_by_normalized: dict[str, sqlite3.Row] = {}
    for row in rows:
        entity_by_normalized.setdefault(str(row["normalized_label"]), row)
    return entity_by_normalized


def _resolved_mapping_endpoint(
    *,
    label: str,
    normalized_label: str,
    match: dict[str, str] | None,
    entity_row: sqlite3.Row | None,
) -> dict[str, Any]:
    if match:
        return {
            "id": match["node_id"],
            "label": match["node_label"],
            "matched": True,
            "node": None,
        }
    node_id, node = _overlay_node_payload(
        label=label,
        normalized_label=normalized_label,
        entity_row=entity_row,
    )
    return {
        "id": node_id,
        "label": label,
        "matched": False,
        "node": node,
    }


def _group_records_for_overlay(records: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    singles: list[list[dict[str, Any]]] = []
    for record in records:
        if record["source"]["matched"]:
            singles.append([record])
            continue
        key = _mapping_group_id(record["target"]["id"], record["link_type"])
        grouped.setdefault(key, []).append(record)
    return list(grouped.values()) + singles


def _should_aggregate_group(group_records: list[dict[str, Any]]) -> bool:
    if not group_records:
        return False
    distinct_sources = {record["source"]["id"] for record in group_records}
    if len(distinct_sources) < LOW_CONFIDENCE_GROUP_MIN_SIZE:
        return False
    return True


def _build_aggregate_group(group_records: list[dict[str, Any]]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    first = group_records[0]
    target = first["target"]
    group_id = _mapping_group_id(target["id"], first["link_type"])
    member_count = len(group_records)
    source_nodes = [record["source"]["node"] for record in group_records if record["source"]["node"]]
    person_count = sum(1 for node in source_nodes if str(node.get("kind") or "") == "person")
    organisation_count = sum(1 for node in source_nodes if str(node.get("kind") or "") == "organisation")
    group_kind = "person" if person_count >= organisation_count else "organisation"
    group_lane = 4 if group_kind == "person" else 2
    role_label = first["link_type"] or "linked"
    group_label = _mapping_group_label(role_label, member_count)
    group_node = {
        "id": group_id,
        "label": group_label,
        "kind": group_kind,
        "lane": group_lane,
        "aliases": [],
        "tooltip_lines": [
            f"{member_count} low-confidence {group_label.lower()}",
            f"Linked to {target['label']}",
            "Right-click to expand the individual members.",
        ],
        "is_low_confidence": True,
        "is_low_confidence_group": True,
        "aggregate_member_count": member_count,
        "aggregate_link_type": role_label,
    }
    group_edge = {
        "id": f"{group_id}:edge",
        "source": group_id,
        "target": target["id"],
        "kind": "mapping_group",
        "phrase": first["phrase"],
        "role_type": role_label or "mapping_group",
        "role_label": role_label or "mapping_group",
        "source_provider": "mapping_import",
        "confidence": "low",
        "weight": min(1.6, 0.35 + member_count * 0.01),
        "tooltip": f"{member_count} grouped low-confidence links",
        "tooltip_lines": [
            f"{member_count} grouped low-confidence links",
            f"Linked to {target['label']}",
        ],
        "is_low_confidence": True,
        "low_confidence_group_id": group_id,
    }
    return group_id, group_node, group_edge


def _raw_overlay_edge(record: dict[str, Any]) -> dict[str, Any]:
    role_label = record["link_type"] or "mapping_link"
    return {
        "id": _mapping_edge_id(int(record["link_id"])),
        "source": record["source"]["id"],
        "target": record["target"]["id"],
        "kind": "mapping_link",
        "phrase": record["phrase"],
        "role_type": role_label,
        "role_label": role_label,
        "source_provider": "mapping_import",
        "confidence": "low",
        "weight": 0.2,
        "tooltip": record["import_line"],
        "tooltip_lines": [line for line in [role_label, record["import_line"]] if line],
        "is_low_confidence": True,
        "detail_available": True,
    }


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _column_value(values: list[str], index: int | None) -> str:
    if index is None or index >= len(values):
        return ""
    return values[index]


def _find_header_index(headers: list[str], target: str, *, start: int = 0) -> int | None:
    for index in range(start, len(headers)):
        if headers[index].strip().lower() == target:
            return index
    return None


def _entity_columns(headers: list[str]) -> dict[str, int | None]:
    label_index = _find_header_index(headers, "label")
    if label_index is None:
        return {"label": None, "type": None, "description": None}
    return {
        "label": label_index,
        "type": _find_header_index(headers, "type", start=label_index + 1),
        "description": _find_header_index(headers, "description", start=label_index + 1),
    }


def _link_columns(headers: list[str]) -> dict[str, int | None]:
    from_index = _find_header_index(headers, "from")
    to_index = _find_header_index(headers, "to")
    if from_index is None or to_index is None:
        return {"from": None, "to": None, "type": None, "description": None}
    start = max(from_index, to_index) + 1
    return {
        "from": from_index,
        "to": to_index,
        "type": _find_header_index(headers, "type", start=start),
        "description": _find_header_index(headers, "description", start=start),
    }


def _unique_match(
    node_index: dict[str, list[dict[str, str]]],
    normalized_label: str,
) -> dict[str, str] | None:
    matches = node_index.get(normalized_label, [])
    unique_by_node_id: dict[str, dict[str, str]] = {}
    for match in matches:
        unique_by_node_id.setdefault(match["node_id"], match)
    if len(unique_by_node_id) != 1:
        return None
    return next(iter(unique_by_node_id.values()))


def _infer_overlay_node_kind(entity_type: str) -> tuple[str, int]:
    lowered = str(entity_type or "").strip().lower()
    if "address" in lowered:
        return ("address", 3)
    if lowered in {"individual", "person"} or "individual" in lowered or "person" in lowered:
        return ("person", 4)
    return ("organisation", 2)


def _ensure_overlay_node(
    overlay_nodes: dict[str, dict[str, Any]],
    *,
    label: str,
    normalized_label: str,
    entity_row: sqlite3.Row | None,
) -> str:
    node_id, node = _overlay_node_payload(
        label=label,
        normalized_label=normalized_label,
        entity_row=entity_row,
    )
    if node_id in overlay_nodes:
        return node_id
    overlay_nodes[node_id] = node
    return node_id


def _overlay_node_payload(
    *,
    label: str,
    normalized_label: str,
    entity_row: sqlite3.Row | None,
) -> tuple[str, dict[str, Any]]:
    node_id = f"mapping-node:{slugify_mapping_label(normalized_label or label)}"
    entity_type = str(entity_row["entity_type"] or "").strip() if entity_row else ""
    description = str(entity_row["description"] or "").strip() if entity_row else ""
    kind, lane = _infer_overlay_node_kind(entity_type)
    tooltip_lines = [line for line in [entity_type, description] if line]
    if entity_row is not None:
        tooltip_lines.append(
            f"Imported from {entity_row['workbook_name']} / {entity_row['sheet_name']} / row {entity_row['row_number']}"
        )
    return node_id, {
        "id": node_id,
        "label": label,
        "kind": kind,
        "lane": lane,
        "aliases": [],
        "tooltip_lines": tooltip_lines,
        "is_low_confidence": True,
        "mapping_entity_type": entity_type,
    }


def _mapping_edge_id(link_id: int) -> str:
    return f"mapping-link:{link_id}"


def _mapping_group_id(target_id: str, link_type: str) -> str:
    return f"mapping-group:{slugify_mapping_label(target_id)}:{slugify_mapping_label(link_type or 'mapping_group')}"


def _mapping_import_line(*, workbook_name: str, sheet_name: str, row_number: int) -> str:
    return f"Imported from {workbook_name} / {sheet_name} / row {row_number}"


def _mapping_evidence_items(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [
        {
            "title": str(item["title"] or item["url"] or "Evidence").strip(),
            "document_url": str(item["url"] or "").strip(),
            "page_hint": "",
            "page_number": None,
            "notes": str(item["snippet"] or "").strip(),
        }
        for item in rows
        if str(item["url"] or "").strip()
    ]


def _evidence_by_link_id(rows: list[sqlite3.Row]) -> dict[int, list[sqlite3.Row]]:
    grouped: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(int(row["mapping_link_id"]), []).append(row)
    return grouped


def _mapping_phrase(link_type: str) -> str:
    lowered = str(link_type or "").strip().lower()
    if "signatory" in lowered:
        return "signed"
    if "affiliate" in lowered:
        return "is affiliated with"
    if "sponsor" in lowered:
        return "sponsored"
    if "spokesperson" in lowered:
        return "acted as spokesperson for"
    if "campaign" in lowered:
        return "is linked through"
    return "is linked to"


def _mapping_group_label(link_type: str, member_count: int) -> str:
    lowered = normalize_mapping_label(link_type)
    if "signatory" in lowered:
        noun = "signatories"
    elif "affiliate" in lowered:
        noun = "affiliates"
    elif "partner" in lowered:
        noun = "partners"
    elif "supporter" in lowered:
        noun = "supporters"
    else:
        noun = "linked members"
    return f"{member_count} {noun}"

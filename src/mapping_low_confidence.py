from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")
_PLAIN_URL_RE = re.compile(r"(?<!\()(?P<url>https?://[^\s)>\]]+)")


def _ensure_text_column(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    column_name: str,
    default_value: str = "",
) -> None:
    columns = {
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in columns:
        return
    escaped = str(default_value).replace("'", "''")
    connection.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} TEXT NOT NULL DEFAULT '{escaped}'"
    )


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
            _ensure_text_column(
                connection,
                table_name="mapping_evidence",
                column_name="document_summary",
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
        document_summary: str = "",
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
                    snippet,
                    document_summary
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mapping_link_id,
                    ordinal,
                    evidence_kind,
                    title,
                    url,
                    snippet,
                    document_summary,
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

    entity_rows = store.list_entities()
    entity_by_normalized: dict[str, sqlite3.Row] = {}
    for row in entity_rows:
        entity_by_normalized.setdefault(str(row["normalized_label"]), row)

    evidence_rows = store.list_evidence()
    evidence_by_link_id: dict[int, list[sqlite3.Row]] = {}
    for row in evidence_rows:
        evidence_by_link_id.setdefault(int(row["mapping_link_id"]), []).append(row)

    store.clear_matches(run_key)

    overlay_nodes: dict[str, dict[str, Any]] = {}
    overlay_edges: list[dict[str, Any]] = []
    matched_link_count = 0

    for link_row in store.list_links():
        from_match = _unique_match(node_index, str(link_row["from_normalized_label"]))
        to_match = _unique_match(node_index, str(link_row["to_normalized_label"]))

        if from_match:
            store.insert_match(
                mapping_link_id=int(link_row["id"]),
                endpoint="from",
                run_key=run_key,
                matched_node_id=from_match["node_id"],
                matched_node_label=from_match["node_label"],
                match_type=from_match["match_type"],
            )
        if to_match:
            store.insert_match(
                mapping_link_id=int(link_row["id"]),
                endpoint="to",
                run_key=run_key,
                matched_node_id=to_match["node_id"],
                matched_node_label=to_match["node_label"],
                match_type=to_match["match_type"],
            )

        source_id = from_match["node_id"] if from_match else _ensure_overlay_node(
            overlay_nodes,
            label=str(link_row["from_label"]),
            normalized_label=str(link_row["from_normalized_label"]),
            entity_row=entity_by_normalized.get(str(link_row["from_normalized_label"])),
        )
        target_id = to_match["node_id"] if to_match else _ensure_overlay_node(
            overlay_nodes,
            label=str(link_row["to_label"]),
            normalized_label=str(link_row["to_normalized_label"]),
            entity_row=entity_by_normalized.get(str(link_row["to_normalized_label"])),
        )
        if source_id == target_id:
            continue

        evidence_items = [
            {
                "title": str(item["title"] or item["url"] or "Evidence").strip(),
                "document_url": str(item["url"] or "").strip(),
                "page_hint": "",
                "page_number": None,
                "notes": str(item["document_summary"] or item["snippet"] or "").strip(),
            }
            for item in evidence_by_link_id.get(int(link_row["id"]), [])
            if str(item["url"] or "").strip()
        ]
        summary_text = next(
            (
                str(item["document_summary"] or "").strip()
                for item in evidence_by_link_id.get(int(link_row["id"]), [])
                if str(item["document_summary"] or "").strip()
            ),
            "",
        )
        description = summary_text or str(link_row["description"] or "").strip()
        phrase = _mapping_phrase(str(link_row["link_type"] or "").strip())
        tooltip_lines = [line for line in [str(link_row["link_type"] or "").strip(), description] if line]
        tooltip_lines.append(
            f"Imported from {link_row['workbook_name']} / {link_row['sheet_name']} / row {link_row['row_number']}"
        )
        overlay_edges.append(
            {
                "id": f"mapping-link:{int(link_row['id'])}",
                "source": source_id,
                "target": target_id,
                "kind": "mapping_link",
                "phrase": phrase,
                "role_type": str(link_row["link_type"] or "").strip() or "mapping_link",
                "role_label": str(link_row["link_type"] or "").strip() or "mapping_link",
                "source_provider": "mapping_import",
                "confidence": "low",
                "weight": 0.2,
                "tooltip": description or f"{link_row['from_label']} {phrase} {link_row['to_label']}",
                "tooltip_lines": tooltip_lines,
                "is_low_confidence": True,
                "evidence": evidence_items[0] if evidence_items else None,
                "evidence_items": evidence_items,
            }
        )
        if from_match or to_match:
            matched_link_count += 1

    return {
        "nodes": list(overlay_nodes.values()),
        "edges": overlay_edges,
        "summary": {
            "run_key": run_key,
            "overlay_node_count": len(overlay_nodes),
            "overlay_edge_count": len(overlay_edges),
            "matched_link_count": matched_link_count,
        },
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
    node_id = f"mapping-node:{slugify_mapping_label(normalized_label or label)}"
    if node_id in overlay_nodes:
        return node_id

    entity_type = str(entity_row["entity_type"] or "").strip() if entity_row else ""
    description = str(entity_row["description"] or "").strip() if entity_row else ""
    kind, lane = _infer_overlay_node_kind(entity_type)
    tooltip_lines = [line for line in [entity_type, description] if line]
    if entity_row is not None:
        tooltip_lines.append(
            f"Imported from {entity_row['workbook_name']} / {entity_row['sheet_name']} / row {entity_row['row_number']}"
        )
    overlay_nodes[node_id] = {
        "id": node_id,
        "label": label,
        "kind": kind,
        "lane": lane,
        "aliases": [],
        "tooltip_lines": tooltip_lines,
        "is_low_confidence": True,
        "mapping_entity_type": entity_type,
    }
    return node_id


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

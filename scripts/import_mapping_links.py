from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.mapping_low_confidence import default_mapping_db_path, import_mapping_workbooks


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import local Mapping spreadsheets into a separate low-confidence SQLite database.",
    )
    parser.add_argument(
        "--mapping-dir",
        default="Mapping",
        help="Directory containing the local Mapping workbooks.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Output SQLite path (default: data/mapping_links.sqlite).",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    mapping_dir = Path(args.mapping_dir)
    if not mapping_dir.is_absolute():
        mapping_dir = project_root / mapping_dir
    database_path = Path(args.db) if args.db else default_mapping_db_path(project_root)

    summary = import_mapping_workbooks(mapping_dir=mapping_dir, database_path=database_path)
    print(
        json.dumps(
            {
                "ok": True,
                "mapping_dir": str(mapping_dir),
                "database_path": str(database_path),
                **summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

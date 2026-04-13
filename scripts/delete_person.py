from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import load_settings
from src.storage.repository import Repository


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete a person from the database by canonical name.")
    parser.add_argument("name", help="Canonical name to delete")
    args = parser.parse_args()

    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    result = repository.delete_person_by_name(args.name)
    print(json.dumps({"name": args.name, **result}, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json

from src.charity_commission.client import CharityCommissionClient
from src.config import load_settings
from src.pipeline import step3_expand_connected_people
from src.storage.repository import Repository


def main() -> None:
    parser = argparse.ArgumentParser(description="MVP step 3: expand people for scoped organisations.")
    parser.add_argument("run_id", type=int)
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    result = step3_expand_connected_people(
        repository=repository,
        settings=settings,
        charity_client=CharityCommissionClient(settings),
        run_id=args.run_id,
        limit=args.limit,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

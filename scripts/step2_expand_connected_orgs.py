from __future__ import annotations

import argparse
import json

from src.charity_commission.client import CharityCommissionClient
from src.config import load_settings
from src.pipeline import step2_expand_connected_organisations
from src.storage.repository import Repository


def main() -> None:
    parser = argparse.ArgumentParser(description="MVP step 2: expand connected companies and charities.")
    parser.add_argument("run_id", type=int)
    args = parser.parse_args()

    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    result = step2_expand_connected_organisations(
        repository=repository,
        charity_client=CharityCommissionClient(settings),
        run_id=args.run_id,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

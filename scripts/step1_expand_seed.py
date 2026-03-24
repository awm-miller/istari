from __future__ import annotations

import argparse
import json

from src.charity_commission.client import CharityCommissionClient
from src.config import load_settings
from src.pipeline import step1_expand_seed
from src.resolution.matcher import HybridMatcher
from src.search.provider import build_search_providers
from src.storage.repository import Repository


def main() -> None:
    parser = argparse.ArgumentParser(description="MVP step 1: expand a seed name via registries only.")
    parser.add_argument("name")
    parser.add_argument(
        "--creativity",
        choices=["strict", "balanced", "exploratory"],
        default="balanced",
    )
    args = parser.parse_args()

    settings = load_settings()
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    result = step1_expand_seed(
        repository=repository,
        charity_client=CharityCommissionClient(settings),
        search_providers=build_search_providers(settings, include_web_dork=False),
        matcher=HybridMatcher(settings),
        seed_name=args.name,
        creativity_level=args.creativity,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

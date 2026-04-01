from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import load_settings
from src.mapping_evidence_enrichment import MappingEvidenceEnricher
from src.mapping_low_confidence import default_mapping_db_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-read low-confidence evidence documents with Gemini and improve mapping evidence summaries.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite path for low-confidence mapping data (default: data/mapping_links.sqlite).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of distinct evidence URLs to process.",
    )
    parser.add_argument(
        "--url",
        dest="urls",
        action="append",
        default=[],
        help="Specific evidence URL to process. Repeat for multiple URLs.",
    )
    parser.add_argument(
        "--no-rebuild-graph",
        action="store_true",
        help="Skip running scripts/rebuild_graph.py after enrichment.",
    )
    parser.add_argument(
        "--generate-links",
        action="store_true",
        help="Also generate low-confidence entities and links from the evidence documents.",
    )
    args = parser.parse_args()

    settings = load_settings()
    database_path = Path(args.db) if args.db else default_mapping_db_path(settings.project_root)
    enricher = MappingEvidenceEnricher(settings=settings, database_path=database_path)
    summary = enricher.enrich(
        limit=args.limit,
        only_urls=list(args.urls or []),
        allow_generated_rows=bool(args.generate_links),
    )
    if not args.no_rebuild_graph:
        enricher.rebuild_graph()
    print(
        json.dumps(
            {
                "ok": True,
                "database_path": str(database_path),
                "rebuild_graph": not bool(args.no_rebuild_graph),
                "generate_links": bool(args.generate_links),
                **summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

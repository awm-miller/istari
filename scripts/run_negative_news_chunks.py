from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_settings
from src.negative_news import _negative_news_db_path, load_negative_news_clusters
from src.storage.negative_news_store import NegativeNewsStore
from src.storage.repository import Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run negative-news cluster chunks sequentially with resume support.",
    )
    parser.add_argument("--start-offset", type=int, default=0, help="First cluster offset to process.")
    parser.add_argument("--chunk-size", type=int, default=50, help="Clusters per chunk.")
    parser.add_argument("--stop-offset", type=int, default=-1, help="Optional exclusive upper offset limit.")
    parser.add_argument("--broad-pages", type=int, default=10, help="Broad search pages per alias.")
    parser.add_argument("--org-pages", type=int, default=2, help="Org-qualified search pages per alias.")
    parser.add_argument("--num", type=int, default=10, help="Serper results per page.")
    parser.add_argument("--max-articles", type=int, default=40, help="Max fetched/classified URLs per cluster.")
    parser.add_argument("--max-passes", type=int, default=5, help="Max rerun passes per chunk.")
    return parser


def _chunk_config(
    *,
    offset: int,
    limit: int,
    broad_pages: int,
    org_pages: int,
    num: int,
    max_articles: int,
    run_ids: list[int],
) -> dict[str, object]:
    return {
        "mode": "cluster_batch",
        "offset": int(offset),
        "limit": int(limit),
        "broad_pages": int(broad_pages),
        "org_pages": int(org_pages),
        "num_per_page": int(num),
        "max_extract_chars": 500000,
        "max_articles_per_cluster": int(max_articles),
        "classify": True,
        "run_ids": list(run_ids),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = load_settings(Path.cwd())
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    cluster_source = load_negative_news_clusters(repository, offset=0, limit=1)
    total_available = int(cluster_source.get("total_available") or 0)
    run_ids = list(cluster_source.get("run_ids") or [])
    if total_available <= 0:
        print(json.dumps({"ok": True, "message": "No merged clusters available."}, indent=2))
        return

    stop_offset = total_available if int(args.stop_offset) < 0 else min(int(args.stop_offset), total_available)
    chunk_size = max(1, int(args.chunk_size))

    store = NegativeNewsStore(
        _negative_news_db_path(settings),
        settings.project_root / "src" / "storage" / "negative_news_schema.sql",
    )
    store.init_db()

    for offset in range(max(0, int(args.start_offset)), stop_offset, chunk_size):
        limit = min(chunk_size, stop_offset - offset)
        config = _chunk_config(
            offset=offset,
            limit=limit,
            broad_pages=int(args.broad_pages),
            org_pages=int(args.org_pages),
            num=int(args.num),
            max_articles=int(args.max_articles),
            run_ids=run_ids,
        )
        output_path = settings.project_root / "data" / f"negative_news_clusters_offset{offset}_limit{limit}.json"
        for attempt in range(1, int(args.max_passes) + 1):
            cmd = [
                sys.executable,
                "-m",
                "src.cli",
                "negative-news-clusters",
                "--offset",
                str(offset),
                "--limit",
                str(limit),
                "--broad-pages",
                str(args.broad_pages),
                "--org-pages",
                str(args.org_pages),
                "--num",
                str(args.num),
                "--max-articles",
                str(args.max_articles),
                "--out",
                str(output_path),
            ]
            print(
                json.dumps(
                    {
                        "chunk_offset": offset,
                        "chunk_limit": limit,
                        "attempt": attempt,
                        "command": cmd,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            proc = subprocess.run(cmd, check=False)
            row = store.get_batch_run_by_config(config)
            if row is None:
                if proc.returncode != 0:
                    raise SystemExit(proc.returncode)
                raise RuntimeError(f"Missing negative-news batch row for offset={offset} limit={limit}")
            completed = int(row["completed_clusters"] or 0)
            total_clusters = int(row["total_clusters"] or 0)
            status = str(row["status"] or "")
            print(
                json.dumps(
                    {
                        "chunk_offset": offset,
                        "chunk_limit": limit,
                        "attempt": attempt,
                        "returncode": proc.returncode,
                        "status": status,
                        "completed_clusters": completed,
                        "total_clusters": total_clusters,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if status == "completed" and completed >= total_clusters:
                break
            if attempt >= int(args.max_passes):
                raise RuntimeError(
                    f"Chunk offset={offset} limit={limit} did not complete after {attempt} passes."
                )
            time.sleep(2.0)


if __name__ == "__main__":
    main()

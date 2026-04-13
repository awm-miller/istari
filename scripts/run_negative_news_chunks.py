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
    parser.add_argument("--chunk-size", type=int, default=50, help="Retained for compatibility; processing now isolates one cluster per subprocess.")
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


def _append_skip_log(
    path: Path,
    *,
    offset: int,
    returncode: int,
    attempts: int,
    label: str = "",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "offset": int(offset),
                    "label": str(label),
                    "returncode": int(returncode),
                    "attempts": int(attempts),
                    "skipped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
                ensure_ascii=False,
            )
            + "\n"
        )


def _cluster_label(repository: Repository, *, offset: int) -> str:
    source = load_negative_news_clusters(repository, offset=offset, limit=1)
    clusters = list(source.get("clusters") or [])
    if not clusters:
        return ""
    return str(clusters[0].get("label") or "")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = load_settings(Path.cwd())
    repository = Repository(
        settings.database_path,
        settings.project_root / "src" / "storage" / "schema.sql",
    )
    repository.init_db()
    print(
        json.dumps(
            {
                "stage": "load_cluster_source_start",
                "offset": 0,
                "limit": 1,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    cluster_source = load_negative_news_clusters(repository, offset=0, limit=1)
    print(
        json.dumps(
            {
                "stage": "load_cluster_source_done",
                "offset": 0,
                "limit": 1,
                "total_available": int(cluster_source.get("total_available") or 0),
                "run_ids": len(cluster_source.get("run_ids") or []),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    total_available = int(cluster_source.get("total_available") or 0)
    run_ids = list(cluster_source.get("run_ids") or [])
    if total_available <= 0:
        print(json.dumps({"ok": True, "message": "No merged clusters available."}, indent=2))
        return

    stop_offset = total_available if int(args.stop_offset) < 0 else min(int(args.stop_offset), total_available)

    store = NegativeNewsStore(
        _negative_news_db_path(settings),
        settings.project_root / "src" / "storage" / "negative_news_schema.sql",
    )
    store.init_db()
    skip_log_path = settings.project_root / "data" / "negative_news_skipped_clusters.jsonl"

    for offset in range(max(0, int(args.start_offset)), stop_offset):
        limit = 1
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
        final_returncode = 0
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
                final_returncode = int(proc.returncode or 0)
                print(
                    json.dumps(
                        {
                            "chunk_offset": offset,
                            "chunk_limit": limit,
                            "attempt": attempt,
                            "returncode": proc.returncode,
                            "status": "missing_batch_row",
                            "completed_clusters": 0,
                            "total_clusters": 1,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                if attempt < int(args.max_passes):
                    time.sleep(min(60.0, 2.0 ** attempt))
                    continue
                break
            completed = int(row["completed_clusters"] or 0)
            total_clusters = int(row["total_clusters"] or 0)
            status = str(row["status"] or "")
            final_returncode = int(proc.returncode or 0)
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
            if attempt < int(args.max_passes):
                time.sleep(min(60.0, 2.0 ** attempt))
        else:
            final_returncode = 1

        row = store.get_batch_run_by_config(config)
        completed = int(row["completed_clusters"] or 0) if row is not None else 0
        total_clusters = int(row["total_clusters"] or 1) if row is not None else 1
        status = str(row["status"] or "") if row is not None else ""
        if status == "completed" and completed >= total_clusters:
            continue

        label = _cluster_label(repository, offset=offset)
        _append_skip_log(
            skip_log_path,
            offset=offset,
            returncode=final_returncode,
            attempts=int(args.max_passes),
            label=label,
        )
        print(
            json.dumps(
                {
                    "chunk_offset": offset,
                    "chunk_limit": limit,
                    "status": "skipped",
                    "label": label,
                    "returncode": final_returncode,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path

from src.storage.negative_news_store import NegativeNewsStore


def test_negative_news_store_reuses_existing_batch_run(tmp_path: Path) -> None:
    database_path = tmp_path / "negative_news.sqlite"
    schema_path = Path("c:/Users/Alex/GitHub/project-istari/src/storage/negative_news_schema.sql")
    store = NegativeNewsStore(database_path, schema_path)
    store.init_db()

    config = {
        "mode": "cluster_batch",
        "offset": 0,
        "limit": 50,
        "broad_pages": 10,
        "org_pages": 2,
        "num_per_page": 10,
        "max_extract_chars": 500000,
        "max_articles_per_cluster": 40,
        "classify": True,
        "run_ids": [1, 2, 3],
    }

    run_id_1 = store.get_or_create_batch_run(
        config=config,
        offset_value=0,
        limit_value=50,
        total_clusters=50,
        output_path="data/out.json",
    )
    run_id_2 = store.get_or_create_batch_run(
        config=config,
        offset_value=0,
        limit_value=50,
        total_clusters=50,
        output_path="data/out.json",
    )

    assert run_id_1 == run_id_2
    row = store.get_batch_run(run_id_1)
    assert row is not None
    assert int(row["total_clusters"]) == 50
    assert str(row["status"]) == "running"

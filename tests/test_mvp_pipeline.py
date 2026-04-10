from __future__ import annotations

from src.services.mvp_pipeline import hydrate_cached_sanctions


class _FakeRepository:
    def __init__(self, rows: dict[int, dict[str, object]]) -> None:
        self.rows = rows

    def get_person_sanctions(self, person_ids: list[int]) -> dict[int, dict[str, object]]:
        return {person_id: self.rows[person_id] for person_id in person_ids if person_id in self.rows}


def test_hydrate_cached_sanctions_reuses_matching_cache() -> None:
    repository = _FakeRepository(
        {
            1: {
                "is_sanctioned": True,
                "screened_name": "Alice Example",
                "screened_birth_month": 5,
                "screened_birth_year": 1980,
                "matches": [{"name": "Alice Example"}],
            }
        }
    )
    ranking = [{"person_id": 1, "canonical_name": "Alice Example", "identity_key": "alice:1980-05"}]

    result = hydrate_cached_sanctions(repository, ranking)

    assert result["cached_count"] == 1
    assert result["cached_hit_count"] == 1
    assert result["pending_ranking"] == []
    assert ranking[0]["sanctions_hit"] is True
    assert ranking[0]["ofac_matches"] == [{"name": "Alice Example"}]


def test_hydrate_cached_sanctions_rescreens_changed_identity() -> None:
    repository = _FakeRepository(
        {
            1: {
                "is_sanctioned": False,
                "screened_name": "Alice Example",
                "screened_birth_month": 5,
                "screened_birth_year": 1980,
                "matches": [],
            }
        }
    )
    ranking = [{"person_id": 1, "canonical_name": "Alice Example", "identity_key": "alice:1981-05"}]

    result = hydrate_cached_sanctions(repository, ranking)

    assert result["cached_count"] == 0
    assert result["cached_hit_count"] == 0
    assert result["pending_ranking"] == ranking
    assert "sanctions_hit" not in ranking[0]

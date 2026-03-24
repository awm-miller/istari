from __future__ import annotations

from dataclasses import dataclass

from src.storage.repository import Repository


@dataclass(slots=True)
class RankedPerson:
    person_id: int
    canonical_name: str
    organisation_count: int
    role_count: int
    weighted_organisation_score: float


def rank_people(
    repository: Repository,
    limit: int = 25,
    run_id: int | None = None,
) -> list[RankedPerson]:
    if run_id is None:
        rows = repository.get_ranked_people(limit=limit)
    else:
        rows = repository.get_ranked_people_for_run(run_id=run_id, limit=limit)
    return [
        RankedPerson(
            person_id=int(row["id"]),
            canonical_name=str(row["canonical_name"]),
            organisation_count=int(row["organisation_count"]),
            role_count=int(row["role_count"]),
            weighted_organisation_score=float(row["weighted_organisation_score"]),
        )
        for row in rows
    ]

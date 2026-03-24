from __future__ import annotations

from typing import Any


def company_role_type(officer_role: Any) -> str:
    role = str(officer_role or "").strip().lower()
    if not role:
        return "company_officer"
    return role.replace(" ", "_")


def company_relationship_kind(officer_role: Any) -> str:
    role = str(officer_role or "").strip().lower()
    if "director" in role:
        return "director_of"
    if "secretary" in role:
        return "secretary_of"
    return "company_officer_of"


def company_relationship_phrase(officer_role: Any) -> str:
    role = str(officer_role or "").strip().lower()
    if "director" in role:
        return "is a director of"
    if "secretary" in role:
        return "is a secretary of"
    if role:
        return f"is listed at Companies House as {role} of"
    return "is listed at Companies House as an officer of"

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class NameVariant:
    name: str
    strategy: str
    creativity_level: str


@dataclass(slots=True)
class EvidenceItem:
    source: str
    source_key: str
    title: str
    url: str | None
    snippet: str
    raw_payload: dict[str, Any]


@dataclass(slots=True)
class OrganisationRecord:
    registry_type: str
    registry_number: str
    suffix: int = 0
    organisation_number: int | None = None
    name: str = ""
    status: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CandidateMatch:
    name_variant: str
    candidate_name: str
    organisation_name: str
    registry_type: str | None
    registry_number: str | None
    suffix: int
    source: str
    evidence_id: int | None
    feature_payload: dict[str, Any]
    score: float
    raw_payload: dict[str, Any]


@dataclass(slots=True)
class ResolutionDecision:
    status: str
    confidence: float
    canonical_name: str
    explanation: str
    rule_score: float
    person_identity_key: str = ""
    alias_status: str = "none"
    llm_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PdfSourceDocument:
    organisation_name: str
    document_url: str
    title: str
    source_provider: str
    local_pdf_path: str = ""
    markdown_path: str = ""
    markdown_text: str = ""
    filing_description: str = ""


@dataclass(slots=True)
class PdfExtractedEntity:
    name: str
    entity_type: str
    role_category: str
    role_label: str
    organisation_name: str
    source_document_url: str
    source_page_hint: str = ""
    confidence: float = 0.0
    registry_hint: str = ""
    notes: str = ""


def dataclass_to_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    raise TypeError(f"Unsupported dataclass value: {type(value)!r}")

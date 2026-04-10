"""Unit tests for negative-news helpers (no network)."""

from __future__ import annotations

from src.negative_news import _required_term_match_locations
from src.negative_news import _should_skip_result_url
from src.negative_news import build_mb_queries
from src.negative_news import ExtractionReport
from src.negative_news import extraction_report_summary


def test_extraction_report_summary_flags() -> None:
    ex = ExtractionReport(
        url="https://example.com/a",
        final_url="https://example.com/a",
        content_type="text/html",
        http_status=200,
        raw_bytes=5000,
        text_chars=50,
        non_blank_lines=3,
        truncated_by_cap=False,
        max_extract_chars=500_000,
        title="T",
        text="x" * 50,
        first_preview="x",
        last_preview="x",
    )
    s = extraction_report_summary(ex)
    assert s["low_body_warning"] is True
    assert s["truncation_risk"] is False

    ex2 = ExtractionReport(
        url="https://example.com/b",
        final_url="https://example.com/b",
        content_type="text/html",
        http_status=200,
        raw_bytes=2_000_000,
        text_chars=500_000,
        non_blank_lines=1000,
        truncated_by_cap=True,
        max_extract_chars=500_000,
        title="Long",
        text="y" * 500_000,
        first_preview="y" * 400,
        last_preview="y" * 400,
    )
    s2 = extraction_report_summary(ex2)
    assert s2["low_body_warning"] is False
    assert s2["truncation_risk"] is True


def test_build_mb_queries_are_name_only() -> None:
    queries = build_mb_queries("Bilal Yasin", ["بلال ياسين"])
    assert queries == ['"Bilal Yasin"', '"بلال ياسين"']


def test_build_mb_queries_with_context_term() -> None:
    queries = build_mb_queries(
        "Bilal Yasin",
        ["بلال ياسين"],
        context_terms=["Development and Training Academy"],
    )
    assert queries == [
        '"Bilal Yasin"',
        '"Bilal Yasin" "Development and Training Academy"',
        '"بلال ياسين"',
        '"بلال ياسين" "Development and Training Academy"',
    ]


def test_required_term_match_locations_checks_title_snippet_and_text() -> None:
    matches = _required_term_match_locations(
        ["Development and Training Academy", "International Green Hands"],
        title="Bilal Yasin linked to Development and Training Academy",
        snippet="No org here.",
        extracted_text="International Green Hands appears deeper in the body.",
    )
    assert matches == {
        "Development and Training Academy": ["title"],
        "International Green Hands": ["extracted_text"],
    }


def test_required_term_match_locations_returns_empty_when_org_phrase_absent() -> None:
    matches = _required_term_match_locations(
        ["Development and Training Academy"],
        title="Bilal Yasin lecture",
        snippet="Academic profile and conference abstract.",
        extracted_text="This is about a different person with the same name.",
    )
    assert matches == {}


def test_skip_result_url_filters_social_domains() -> None:
    assert _should_skip_result_url("https://www.facebook.com/foo") is True
    assert _should_skip_result_url("https://www.instagram.com/foo") is True
    assert _should_skip_result_url("https://www.youtube.com/watch?v=1") is True
    assert _should_skip_result_url("https://pk.linkedin.com/in/foo") is True
    assert _should_skip_result_url("https://find-and-update.company-information.service.gov.uk/company/1") is True
    assert _should_skip_result_url("https://register-of-charities.charitycommission.gov.uk/charity-search/-/charity-details/1") is True
    assert _should_skip_result_url("https://twitter.com/foo/status/1") is True
    assert _should_skip_result_url("https://x.com/foo/status/1") is True
    assert _should_skip_result_url("https://www.tiktok.com/@foo/video/1") is True
    assert _should_skip_result_url("https://www.aljazeera.com/news/2026/1/1/example") is False

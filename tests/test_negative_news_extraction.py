"""Unit tests for negative-news helpers (no network)."""

from __future__ import annotations

import scripts.consolidate_and_graph as consolidate_and_graph

from src.negative_news import _collect_search_hits
from src.negative_news import _collect_cluster_search_hits
from src.negative_news import _required_term_match_locations
from src.negative_news import _should_skip_result_url
from src.negative_news import build_cluster_query_specs
from src.negative_news import build_mb_queries
from src.negative_news import ExtractionReport
from src.negative_news import extraction_report_summary
from src.negative_news import load_negative_news_clusters


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


def test_build_cluster_query_specs_uses_broad_and_org_page_limits() -> None:
    specs = build_cluster_query_specs(
        ["Bilal Yasin", "Bilal Khalil Hasan Yasin"],
        ["بلال ياسين", "بلال خليل حسن ياسين"],
        context_terms=["Development and Training Academy"],
        broad_pages=10,
        org_pages=2,
    )
    assert [(spec.query, spec.pages, spec.bucket, spec.language, spec.required_terms) for spec in specs] == [
        ('"Bilal Yasin"', 10, "broad", "english", []),
        ('"Bilal Yasin" "Development and Training Academy"', 2, "org", "english", ["Development and Training Academy"]),
        ('"Bilal Khalil Hasan Yasin"', 10, "broad", "english", []),
        (
            '"Bilal Khalil Hasan Yasin" "Development and Training Academy"',
            2,
            "org",
            "english",
            ["Development and Training Academy"],
        ),
        ('"بلال ياسين"', 10, "broad", "arabic", []),
        ('"بلال ياسين" "Development and Training Academy"', 2, "org", "arabic", ["Development and Training Academy"]),
        ('"بلال خليل حسن ياسين"', 10, "broad", "arabic", []),
        (
            '"بلال خليل حسن ياسين" "Development and Training Academy"',
            2,
            "org",
            "arabic",
            ["Development and Training Academy"],
        ),
    ]


def test_collect_search_hits_dedupes_urls_across_cluster_alias_queries() -> None:
    specs = build_cluster_query_specs(
        ["Bilal Yasin", "Bilal Khalil Hasan Yasin"],
        [],
        context_terms=[],
        broad_pages=1,
        org_pages=1,
    )

    def fake_search(_settings: object, *, query: str, page: int, num: int, cache_dir: object) -> list[dict[str, str]]:
        assert page == 1
        assert num == 10
        assert cache_dir == "cache"
        if query == '"Bilal Yasin"':
            return [{"title": "One", "link": "https://example.com/shared", "snippet": "a"}]
        if query == '"Bilal Khalil Hasan Yasin"':
            return [{"title": "Two", "link": "https://example.com/shared", "snippet": "b"}]
        return []

    hits = _collect_search_hits(
        settings=object(),  # type: ignore[arg-type]
        query_specs=specs,
        num_per_page=10,
        cache_dir="cache",  # type: ignore[arg-type]
        max_articles=None,
        search_func=fake_search,
    )
    assert len(hits) == 1
    assert hits[0]["url"] == "https://example.com/shared"
    assert hits[0]["query"] == '"Bilal Yasin"'


def test_collect_cluster_search_hits_reserves_capacity_for_org_queries() -> None:
    specs = build_cluster_query_specs(
        ["Bilal Yasin", "Bilal Khalil Hasan Yasin"],
        ["بلال ياسين"],
        context_terms=["Development and Training Academy"],
        broad_pages=1,
        org_pages=1,
    )

    def fake_search(_settings: object, *, query: str, page: int, num: int, cache_dir: object) -> list[dict[str, str]]:
        return [
            {
                "title": query,
                "link": f"https://example.com/{abs(hash(query))}",
                "snippet": query,
            }
        ]

    hits = _collect_cluster_search_hits(
        settings=object(),  # type: ignore[arg-type]
        query_specs=specs,
        num_per_page=10,
        cache_dir="cache",  # type: ignore[arg-type]
        max_articles=4,
        search_func=fake_search,
    )
    assert len(hits) == 4
    assert any(hit["bucket"] == "broad" for hit in hits)
    assert any(hit["bucket"] == "org" for hit in hits)


def test_load_negative_news_clusters_uses_combined_merged_people_and_suppresses_stdout(
    monkeypatch: object,
    capsys: object,
) -> None:
    class FakeRepository:
        def get_latest_unique_run_ids(self) -> list[int]:
            return [11, 22]

        def get_organisation_names_for_person_ids(self, person_ids: list[int]) -> list[str]:
            assert person_ids in ([1, 2], [9])
            return ["Org A", "Org B"] if person_ids == [1, 2] else ["Org Z"]

    def fake_consolidate_multi_run(run_ids: list[int]) -> dict[str, object]:
        assert run_ids == [11, 22]
        print("noisy consolidate output")
        return {
            "nodes": [
                {
                    "id": "merged_person:2",
                    "kind": "person",
                    "label": "Bilal Yasin",
                    "aliases": ["Bilal Khalil Hasan Yasin"],
                    "person_ids": [1, 2],
                    "org_count": 4,
                    "role_count": 8,
                    "score": 12.5,
                },
                {
                    "id": "merged_person:3",
                    "kind": "person",
                    "label": "Other Person",
                    "aliases": [],
                    "person_ids": [9],
                    "org_count": 1,
                    "role_count": 2,
                    "score": 3.0,
                },
                {
                    "id": "identity_cluster:1",
                    "kind": "seed_alias",
                    "label": "Ignore Me",
                },
            ]
        }

    monkeypatch.setattr(consolidate_and_graph, "consolidate_multi_run", fake_consolidate_multi_run)
    result = load_negative_news_clusters(FakeRepository(), limit=1)  # type: ignore[arg-type]
    captured = capsys.readouterr()
    assert captured.out == ""
    assert result == {
        "run_ids": [11, 22],
        "total_available": 2,
        "clusters": [
            {
                "cluster_id": "merged_person:2",
                "label": "Bilal Yasin",
                "aliases": ["Bilal Yasin", "Bilal Khalil Hasan Yasin"],
                "person_ids": [1, 2],
                "org_count": 4,
                "role_count": 8,
                "score": 12.5,
                "context_terms": ["Org A", "Org B"],
            }
        ],
    }


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
    assert _should_skip_result_url("https://open.endole.co.uk/insight/company/1-example") is True
    assert _should_skip_result_url("https://www.checkfree.co.uk/Company/03502114/example") is True
    assert _should_skip_result_url("https://www.northdata.com/Haj,+Mohamed+Sheikhadam,+London/nim") is True
    assert _should_skip_result_url("https://www.northdata.de/El-Zayat,Ibrahim/17m3") is True
    assert _should_skip_result_url("https://www.northdata.fr/El-Zayat,Ibrahim/e62") is True
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

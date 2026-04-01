from __future__ import annotations

import csv
import json
import logging
import re
import urllib.parse
import urllib.request
from collections import defaultdict
from html import unescape
from pathlib import Path
from typing import Any

log = logging.getLogger("istari.ofac")

SDN_CSV_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"
UK_SANCTIONS_CSV_URL = "https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.csv"
FR_TRESOR_JSON_URL = (
    "https://gels-avoirs.dgtresor.gouv.fr/ApiPublic/api/v1/publication/"
    "derniere-publication-fichier-json"
)
GERMANY_SEARCH_URL = "https://www.finanz-sanktionsliste.de/fisalis/?lang=en"
DOWNLOAD_TIMEOUT_SECONDS = 90
_SOURCE_PRIORITY = {
    "OFAC SDN": 0,
    "UK Sanctions List": 1,
    "Direction Generale du Tresor": 2,
    "Germany Finanzsanktionsliste": 3,
}

_HONORIFICS = re.compile(
    r"\b(mr|mrs|ms|dr|prof|sir|dame|lord|lady|rev|hon)\b",
    re.IGNORECASE,
)
_NON_ALPHA = re.compile(r"[^a-z\s]")
_MULTI_SPACE = re.compile(r"\s+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_PARTICLE_TOKENS = {"al", "el"}
_TOKEN_CANONICAL = {
    "majid": "majed",
}
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_DOB_TEXT_RE = re.compile(
    r"(?i)\b(?:dob|alt\.?\s*dob|geboren|born)\b[^A-Za-z0-9]{0,8}(?:\d{1,2}\s+)?([A-Za-z]{3,9})[\s,/-]+(\d{4})"
)
_DOB_ISO_RE = re.compile(
    r"(?i)\b(?:dob|alt\.?\s*dob|geboren|born)\b[^0-9]{0,8}(\d{4})[-/](\d{1,2})[-/]\d{1,2}"
)
_DOB_NUMERIC_RE = re.compile(
    r"(?i)\b(?:dob|alt\.?\s*dob|geboren|born)\b[^0-9]{0,8}\d{1,2}[./-](\d{1,2})[./-](\d{4})"
)
_IDENTITY_KEY_DOB_RE = re.compile(r":(\d{4})-(\d{2})$")
_GERMAN_RESULT_RE = re.compile(
    r"<h3><span[^>]*>(?P<score>\d+)%</span>:\s*\((?P<source_id>[^)]+)\)\s*"
    r"(?P<title>.*?)</h3>(?P<body>.*?)(?=<hr\s*/><h3>|<p>\d+\s+Treffer|</main>)",
    re.IGNORECASE | re.DOTALL,
)
_GERMAN_NAME_RE = re.compile(r"Name:\s*([^,.;]+)")


def _normalize(name: str) -> str:
    text = name.lower()
    text = _HONORIFICS.sub(" ", text)
    text = _NON_ALPHA.sub(" ", text)
    return _MULTI_SPACE.sub(" ", text).strip()


def _canonical_token(token: str) -> str:
    return _TOKEN_CANONICAL.get(token, token)


def _expanded_tokens(name: str) -> set[str]:
    tokens: set[str] = set()
    for raw_token in _normalize(name).split():
        if not raw_token:
            continue
        token = _canonical_token(raw_token)
        if token not in _PARTICLE_TOKENS:
            tokens.add(token)
        for particle in _PARTICLE_TOKENS:
            if token.startswith(particle) and len(token) > len(particle) + 2:
                tokens.add(token[len(particle) :])
    return tokens


def _token_set(name: str) -> frozenset[str]:
    return frozenset(_expanded_tokens(name))


def _token_match(query_tokens: frozenset[str], entry_tokens: frozenset[str]) -> str | None:
    if query_tokens == entry_tokens:
        return "exact"
    if len(query_tokens) < 2 or len(entry_tokens) < 2:
        return None
    if query_tokens <= entry_tokens:
        return "subset"
    if entry_tokens <= query_tokens and len(query_tokens - entry_tokens) <= 1:
        return "subset"
    return None


def _strip_html(text: str) -> str:
    cleaned = _HTML_TAG_RE.sub(" ", str(text or ""))
    cleaned = unescape(cleaned)
    return _MULTI_SPACE.sub(" ", cleaned).strip()


def _unique_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _MULTI_SPACE.sub(" ", str(value or "")).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _flatten_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            values.extend(_flatten_strings(item))
        return values
    if isinstance(value, dict):
        values = []
        for item in value.values():
            values.extend(_flatten_strings(item))
        return values
    return [str(value)]


def _extract_entry_birth_month_years(text: str) -> set[tuple[int, int]]:
    values: set[tuple[int, int]] = set()
    lowered = str(text or "").lower()
    for month_name, year in _DOB_TEXT_RE.findall(lowered):
        month = _MONTHS.get(str(month_name).lower())
        if not month:
            continue
        try:
            values.add((month, int(year)))
        except (TypeError, ValueError):
            continue
    for year, month in _DOB_ISO_RE.findall(lowered):
        try:
            month_value = int(month)
            year_value = int(year)
        except (TypeError, ValueError):
            continue
        if 1 <= month_value <= 12:
            values.add((month_value, year_value))
    for month, year in _DOB_NUMERIC_RE.findall(lowered):
        try:
            month_value = int(month)
            year_value = int(year)
        except (TypeError, ValueError):
            continue
        if 1 <= month_value <= 12:
            values.add((month_value, year_value))
    return values


def extract_identity_key_birth_month_year(identity_key: str) -> tuple[int | None, int | None]:
    match = _IDENTITY_KEY_DOB_RE.search(str(identity_key or ""))
    if not match:
        return None, None
    try:
        return int(match.group(2)), int(match.group(1))
    except (TypeError, ValueError):
        return None, None


class OFACScreener:
    """Screen person names against local sanctions lists plus optional Germany lookups."""

    def __init__(self, *, enable_remote_sources: bool = False) -> None:
        self.enable_remote_sources = enable_remote_sources
        self._entries: list[dict[str, Any]] = []
        self._germany_cache: dict[str, list[dict[str, Any]]] = {}

    @property
    def loaded(self) -> bool:
        return len(self._entries) > 0

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    def _download(self, url: str, path: Path) -> None:
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Istari sanctions loader/1.0",
                "Accept": "*/*",
            },
        )
        with urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            path.write_bytes(response.read())

    def _prepare_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        if entry.get("_prepared"):
            return entry
        names = _unique_texts([str(entry.get("name", "")), *(entry.get("aliases") or [])])
        entry["aliases"] = [name for name in names if name != str(entry.get("name", ""))]
        entry["_prepared_names"] = names
        entry["_prepared_norms"] = {_normalize(name) for name in names if _normalize(name)}
        entry["_prepared_tokens"] = [(name, _token_set(name)) for name in names if _token_set(name)]
        entry["birth_month_years"] = set(entry.get("birth_month_years") or set())
        if not entry["birth_month_years"]:
            entry["birth_month_years"] = _extract_entry_birth_month_years(entry.get("remarks", ""))
        entry["_prepared"] = True
        return entry

    def _append_entry(self, entry: dict[str, Any]) -> None:
        self._entries.append(self._prepare_entry(entry))

    def _reset(self) -> None:
        self._entries = []
        self._germany_cache = {}

    def load_csv(self, path: Path) -> None:
        if not path.exists():
            log.warning("OFAC SDN file not found at %s", path)
            return
        self._reset()
        self._load_ofac_csv(path)

    def load_sources(self, data_dir: Path) -> None:
        self._reset()
        ofac_path = data_dir / "sdn.csv"
        uk_path = data_dir / "uk_sanctions.csv"
        fr_path = data_dir / "fr_tresor.json"
        if ofac_path.exists():
            self._load_ofac_csv(ofac_path)
        if uk_path.exists():
            self._load_uk_csv(uk_path)
        if fr_path.exists():
            self._load_france_json(fr_path)

    def ensure_local_sources(self, target_dir: Path) -> None:
        target_dir.mkdir(parents=True, exist_ok=True)
        ofac_path = target_dir / "sdn.csv"
        uk_path = target_dir / "uk_sanctions.csv"
        fr_path = target_dir / "fr_tresor.json"
        if not ofac_path.exists():
            log.info("Downloading OFAC SDN list to %s ...", ofac_path)
            self._download(SDN_CSV_URL, ofac_path)
        if not uk_path.exists():
            log.info("Downloading UK sanctions list to %s ...", uk_path)
            self._download(UK_SANCTIONS_CSV_URL, uk_path)
        if not fr_path.exists():
            log.info("Downloading DG Tresor sanctions list to %s ...", fr_path)
            self._download(FR_TRESOR_JSON_URL, fr_path)
        self.load_sources(target_dir)

    def download_and_load(self, target_dir: Path) -> Path:
        self.ensure_local_sources(target_dir)
        return target_dir / "sdn.csv"

    def _load_ofac_csv(self, path: Path) -> None:
        loaded = 0
        with open(path, encoding="utf-8", errors="replace") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if len(row) < 3:
                    continue
                sdn_type = row[2].strip().strip('"').lower()
                if sdn_type != "individual":
                    continue
                raw_name = row[1].strip().strip('"')
                if not raw_name or raw_name == "-0-":
                    continue
                self._append_entry(
                    {
                        "ent_num": row[0].strip(),
                        "name": raw_name,
                        "aliases": [],
                        "program": row[3].strip().strip('"') if len(row) > 3 else "",
                        "remarks": row[11].strip().strip('"') if len(row) > 11 else "",
                        "source": "OFAC SDN",
                        "source_id": row[0].strip(),
                    }
                )
                loaded += 1
        log.info("Loaded %d OFAC SDN individuals from %s", loaded, path)

    def _load_uk_csv(self, path: Path) -> None:
        with open(path, encoding="utf-8", errors="replace") as fh:
            lines = fh.read().splitlines()
        if not lines:
            return
        data_lines = lines[1:] if lines[0].startswith("Report Date:") else lines
        groups: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in csv.DictReader(data_lines):
            if str(row.get("Designation Type") or "").strip().lower() != "individual":
                continue
            source_id = str(row.get("Unique ID") or row.get("OFSI Group ID") or "").strip()
            if not source_id:
                continue
            groups[source_id].append(row)

        loaded = 0
        for source_id, rows in groups.items():
            names: list[str] = []
            birth_month_years: set[tuple[int, int]] = set()
            remarks_parts: list[str] = []
            program = ""
            for row in rows:
                name_parts = [
                    str(row.get(f"Name {idx}") or "").strip()
                    for idx in range(1, 7)
                ]
                full_name = " ".join(part for part in name_parts if part)
                if full_name:
                    names.append(full_name)
                birth_month_years |= _extract_entry_birth_month_years(
                    f"DOB {str(row.get('D.O.B') or '')}"
                )
                birth_month_years |= _extract_entry_birth_month_years(
                    str(row.get("Other Information") or "")
                )
                remarks_parts.extend(
                    value
                    for value in [
                        row.get("Position"),
                        row.get("Other Information"),
                        row.get("UK Statement of Reasons"),
                    ]
                    if str(value or "").strip()
                )
                if not program:
                    program = str(row.get("Regime Name") or "").strip()
            names = _unique_texts(names)
            if not names:
                continue
            self._append_entry(
                {
                    "ent_num": source_id,
                    "name": names[0],
                    "aliases": names[1:],
                    "program": program,
                    "remarks": " ".join(_unique_texts(remarks_parts)),
                    "source": "UK Sanctions List",
                    "source_id": source_id,
                    "birth_month_years": birth_month_years,
                }
            )
            loaded += 1
        log.info("Loaded %d UK sanctions individuals from %s", loaded, path)

    def _load_france_json(self, path: Path) -> None:
        with open(path, encoding="utf-8", errors="replace") as fh:
            payload = json.load(fh)
        publications = payload.get("Publications") or {}
        details = publications.get("PublicationDetail") or []
        loaded = 0
        for detail in details:
            nature = str(detail.get("Nature") or "").lower()
            if "personne physique" not in nature:
                continue
            surname = str(detail.get("Nom") or "").strip()
            if not surname:
                continue
            first_names: list[str] = []
            aliases: list[str] = []
            remarks_parts: list[str] = []
            birth_month_years: set[tuple[int, int]] = set()
            for item in detail.get("RegistreDetail") or []:
                field_name = str(item.get("TypeChamp") or "").upper()
                values = _flatten_strings(item.get("Valeur"))
                if field_name == "PRENOM":
                    first_names.extend(values)
                elif field_name == "ALIAS":
                    aliases.extend(values)
                elif "NAISS" in field_name:
                    for value in values:
                        birth_month_years |= _extract_entry_birth_month_years(f"DOB {value}")
                else:
                    remarks_parts.extend(values)
            first_names = _unique_texts(first_names)
            aliases = _unique_texts(aliases)
            names = _unique_texts(
                [f"{first_name} {surname}".strip() for first_name in first_names] + [surname] + aliases
            )
            if not names:
                continue
            self._append_entry(
                {
                    "ent_num": str(detail.get("IdRegistre") or "").strip() or surname,
                    "name": names[0],
                    "aliases": names[1:],
                    "program": "Direction Generale du Tresor",
                    "remarks": " ".join(_unique_texts(remarks_parts)),
                    "source": "Direction Generale du Tresor",
                    "source_id": str(detail.get("IdRegistre") or "").strip() or surname,
                    "birth_month_years": birth_month_years,
                }
            )
            loaded += 1
        log.info("Loaded %d DG Tresor individuals from %s", loaded, path)

    def _parse_german_search_results(self, html: str) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for match in _GERMAN_RESULT_RE.finditer(str(html or "")):
            title_text = _strip_html(match.group("title"))
            title_text = re.sub(
                r"^\d+%:\s*\([^)]+\)\s*",
                "",
                title_text,
            ).strip()
            title_text = re.sub(r"\s+-\s+\d{4}$", "", title_text).strip()
            body_text = _strip_html(match.group("body"))
            aliases = _unique_texts(_GERMAN_NAME_RE.findall(body_text))
            birth_month_years = _extract_entry_birth_month_years(body_text)
            entries.append(
                self._prepare_entry(
                    {
                        "ent_num": match.group("source_id").strip(),
                        "name": title_text,
                        "aliases": aliases,
                        "program": "Germany Finanzsanktionsliste",
                        "remarks": body_text,
                        "source": "Germany Finanzsanktionsliste",
                        "source_id": match.group("source_id").strip(),
                        "birth_month_years": birth_month_years,
                    }
                )
            )
        return entries

    def _fetch_german_entries(self, name: str) -> list[dict[str, Any]]:
        query = _normalize(name)
        if not query:
            return []
        cached = self._germany_cache.get(query)
        if cached is not None:
            return cached
        try:
            body = urllib.parse.urlencode({"txtSearch": name, "cmdSearch": "1"}).encode("utf-8")
            request = urllib.request.Request(
                GERMANY_SEARCH_URL,
                data=body,
                headers={
                    "User-Agent": "Istari sanctions loader/1.0",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
                html = response.read().decode("utf-8", "replace")
            entries = self._parse_german_search_results(html)
        except Exception as exc:  # pragma: no cover - network failures are non-deterministic
            log.warning("Germany sanctions lookup failed for %s: %s", name, exc)
            entries = []
        self._germany_cache[query] = entries
        return entries

    def _entry_key(self, entry: dict[str, Any]) -> str:
        source = str(entry.get("source") or "OFAC SDN")
        source_id = str(entry.get("source_id") or entry.get("ent_num") or entry.get("name") or "")
        return f"{source}:{source_id}"

    def _hit_priority(self, hit: dict[str, Any]) -> tuple[int, int]:
        source_rank = _SOURCE_PRIORITY.get(str(hit.get("source") or ""), 99)
        match_rank = 0 if str(hit.get("match_type") or "") == "exact" else 1
        return (source_rank, match_rank)

    def _dedupe_hit_key(self, query_norm: str, hit: dict[str, Any]) -> tuple[str, ...] | None:
        self._prepare_entry(hit)
        births = tuple(sorted(set(hit.get("birth_month_years") or set())))
        prepared_norms = set(hit.get("_prepared_norms") or set())
        if births and query_norm in prepared_norms:
            return ("name-dob", query_norm, str(births))
        source_id = str(hit.get("source_id") or "").strip().upper()
        if source_id.startswith("EU "):
            return ("eu", source_id)
        if str(hit.get("match_type") or "") == "exact" and query_norm in prepared_norms:
            return ("name", query_norm)
        return None

    def _merge_hit(self, primary: dict[str, Any], duplicate: dict[str, Any]) -> dict[str, Any]:
        merged = dict(primary)
        merged_sources = _unique_texts(
            [
                *(primary.get("sources") or [str(primary.get("source") or "")]),
                *(duplicate.get("sources") or [str(duplicate.get("source") or "")]),
            ]
        )
        merged_source_ids = _unique_texts(
            [
                *(primary.get("source_ids") or [str(primary.get("source_id") or "")]),
                *(duplicate.get("source_ids") or [str(duplicate.get("source_id") or "")]),
            ]
        )
        merged["sources"] = [value for value in merged_sources if value]
        merged["source_ids"] = [value for value in merged_source_ids if value]
        merged["birth_month_years"] = set(primary.get("birth_month_years") or set()) | set(
            duplicate.get("birth_month_years") or set()
        )
        remarks = _unique_texts(
            [str(primary.get("remarks") or ""), str(duplicate.get("remarks") or "")]
        )
        merged["remarks"] = " ".join(remarks)
        return merged

    def _dedupe_hits(self, query_norm: str, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        deduped_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
        for hit in hits:
            key = self._dedupe_hit_key(query_norm, hit)
            if key is None:
                hit = self._merge_hit(hit, {})
                deduped.append(hit)
                continue
            existing = deduped_by_key.get(key)
            if existing is None:
                deduped_by_key[key] = self._merge_hit(hit, {})
                continue
            if self._hit_priority(hit) < self._hit_priority(existing):
                deduped_by_key[key] = self._merge_hit(hit, existing)
            else:
                deduped_by_key[key] = self._merge_hit(existing, hit)
        deduped.extend(deduped_by_key.values())
        deduped.sort(key=self._hit_priority)
        return deduped

    def _match_entry(self, query_norm: str, query_tokens: frozenset[str], entry: dict[str, Any]) -> dict[str, Any] | None:
        self._prepare_entry(entry)
        best_match: tuple[str, str] | None = None
        for candidate_name in entry.get("_prepared_names") or []:
            candidate_norm = _normalize(candidate_name)
            if candidate_norm and candidate_norm == query_norm:
                best_match = ("exact", candidate_name)
                break
            candidate_tokens = _token_set(candidate_name)
            match_type = _token_match(query_tokens, candidate_tokens)
            if not match_type:
                continue
            if best_match is None or best_match[0] != "exact":
                best_match = (match_type, candidate_name)
        if best_match is None:
            return None
        return {
            "match_type": best_match[0],
            "matched_name": best_match[1],
            **entry,
        }

    def screen_name(
        self,
        name: str,
        *,
        birth_month: int | None = None,
        birth_year: int | None = None,
    ) -> list[dict[str, Any]]:
        """Check a single name against sanctions lists."""
        query_norm = _normalize(name)
        query_tokens = _token_set(name)
        if not query_norm:
            return []

        candidate_entries = list(self._entries)
        if self.enable_remote_sources:
            candidate_entries.extend(self._fetch_german_entries(name))

        hits_by_key: dict[str, dict[str, Any]] = {}
        for entry in candidate_entries:
            hit = self._match_entry(query_norm, query_tokens, entry)
            if not hit:
                continue
            key = self._entry_key(hit)
            existing = hits_by_key.get(key)
            if existing and existing.get("match_type") == "exact":
                continue
            hits_by_key[key] = hit

        hits = list(hits_by_key.values())
        hits = self._dedupe_hits(query_norm, hits)
        if birth_month is None or birth_year is None:
            return hits

        verified_hits = []
        for entry in hits:
            entry_births = set(entry.get("birth_month_years") or set())
            if (birth_month, birth_year) not in entry_births:
                continue
            verified_hits.append(
                {
                    **entry,
                    "matched_birth_month": birth_month,
                    "matched_birth_year": birth_year,
                }
            )
        return verified_hits

    def screen_names(
        self,
        names: list[str],
        *,
        birth_month_years: dict[str, tuple[int | None, int | None]] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        results: dict[str, list[dict[str, Any]]] = {}
        for name in names:
            birth_month, birth_year = (birth_month_years or {}).get(name, (None, None))
            hits = self.screen_name(name, birth_month=birth_month, birth_year=birth_year)
            if hits:
                results[name] = hits
        return results

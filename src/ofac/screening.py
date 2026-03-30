from __future__ import annotations

import csv
import logging
import re
import urllib.request
from pathlib import Path
from typing import Any

log = logging.getLogger("istari.ofac")

SDN_CSV_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"

_HONORIFICS = re.compile(
    r"\b(mr|mrs|ms|dr|prof|sir|dame|lord|lady|rev|hon)\b",
    re.IGNORECASE,
)
_NON_ALPHA = re.compile(r"[^a-z\s]")
_MULTI_SPACE = re.compile(r"\s+")
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
    r"(?i)\b(?:dob|alt\.?\s*dob)\b[^A-Za-z0-9]{0,8}(?:\d{1,2}\s+)?([A-Za-z]{3,9})[\s,/-]+(\d{4})"
)
_DOB_ISO_RE = re.compile(
    r"(?i)\b(?:dob|alt\.?\s*dob)\b[^0-9]{0,8}(\d{4})[-/](\d{1,2})[-/]\d{1,2}"
)
_IDENTITY_KEY_DOB_RE = re.compile(r":(\d{4})-(\d{2})$")


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


def _token_match(query_tokens: frozenset[str], sdn_tokens: frozenset[str]) -> str | None:
    if query_tokens == sdn_tokens:
        return "exact"
    if len(query_tokens) < 2 or len(sdn_tokens) < 2:
        return None
    if query_tokens <= sdn_tokens:
        return "subset"
    # Allow one extra token in the query for middle names,
    # e.g. "Majed Khalil Al-Zeer" vs "AL-ZEER, Majed".
    if sdn_tokens <= query_tokens and len(query_tokens - sdn_tokens) <= 1:
        return "subset"
    return None


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
    """Loads the OFAC SDN list and screens person names against it."""

    def __init__(self) -> None:
        self._entries: list[dict[str, str]] = []
        self._normalized_names: set[str] = set()
        self._token_sets: dict[frozenset[str], str] = {}
        self._entry_birth_month_years: dict[str, set[tuple[int, int]]] = {}

    @property
    def loaded(self) -> bool:
        return len(self._entries) > 0

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    def load_csv(self, path: Path) -> None:
        if not path.exists():
            log.warning("SDN file not found at %s", path)
            return

        self._entries = []
        self._normalized_names = set()
        self._token_sets = {}
        self._entry_birth_month_years = {}

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

                entry = {
                    "ent_num": row[0].strip(),
                    "name": raw_name,
                    "program": row[3].strip().strip('"') if len(row) > 3 else "",
                    "remarks": row[11].strip().strip('"') if len(row) > 11 else "",
                }
                self._entries.append(entry)
                norm = _normalize(raw_name)
                self._normalized_names.add(norm)
                tokens = _token_set(raw_name)
                if len(tokens) >= 2:
                    self._token_sets[tokens] = raw_name
                self._entry_birth_month_years[entry["ent_num"]] = _extract_entry_birth_month_years(
                    entry.get("remarks", "")
                )

        log.info("Loaded %d OFAC SDN individuals from %s", len(self._entries), path)

    def download_and_load(self, target_dir: Path) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / "sdn.csv"
        log.info("Downloading OFAC SDN list to %s ...", path)
        urllib.request.urlretrieve(SDN_CSV_URL, path)
        self.load_csv(path)
        return path

    def screen_name(
        self,
        name: str,
        *,
        birth_month: int | None = None,
        birth_year: int | None = None,
    ) -> list[dict[str, Any]]:
        """Check a single name against the SDN list.

        Returns a list of matching SDN entries (empty if no match).
        Uses exact-normalized match and token-set match (order-independent)
        to handle 'LAST, First' vs 'First Last' variations.
        """
        if not self._entries:
            return []

        norm = _normalize(name)
        tokens = _token_set(name)
        hits: list[dict[str, Any]] = []
        seen_ent: set[str] = set()

        if norm in self._normalized_names:
            for entry in self._entries:
                if _normalize(entry["name"]) == norm and entry["ent_num"] not in seen_ent:
                    seen_ent.add(entry["ent_num"])
                    hits.append({"match_type": "exact", **entry})

        if len(tokens) >= 2:
            for sdn_tokens, sdn_raw in self._token_sets.items():
                match_type = _token_match(tokens, sdn_tokens)
                if match_type:
                    for entry in self._entries:
                        if entry["name"] == sdn_raw and entry["ent_num"] not in seen_ent:
                            seen_ent.add(entry["ent_num"])
                            hits.append({"match_type": match_type, **entry})

        if birth_month is None or birth_year is None:
            return []

        verified_hits = []
        for entry in hits:
            entry_births = self._entry_birth_month_years.get(str(entry.get("ent_num") or ""), set())
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
        """Screen multiple names. Returns a dict mapping name -> list of hits."""
        results: dict[str, list[dict[str, Any]]] = {}
        for name in names:
            birth_month, birth_year = (birth_month_years or {}).get(name, (None, None))
            hits = self.screen_name(name, birth_month=birth_month, birth_year=birth_year)
            if hits:
                results[name] = hits
        return results

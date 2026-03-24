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


def _normalize(name: str) -> str:
    text = name.lower()
    text = _HONORIFICS.sub(" ", text)
    text = _NON_ALPHA.sub(" ", text)
    return _MULTI_SPACE.sub(" ", text).strip()


def _token_set(name: str) -> frozenset[str]:
    return frozenset(_normalize(name).split())


class OFACScreener:
    """Loads the OFAC SDN list and screens person names against it."""

    def __init__(self) -> None:
        self._entries: list[dict[str, str]] = []
        self._normalized_names: set[str] = set()
        self._token_sets: dict[frozenset[str], str] = {}

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

        log.info("Loaded %d OFAC SDN individuals from %s", len(self._entries), path)

    def download_and_load(self, target_dir: Path) -> Path:
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / "sdn.csv"
        log.info("Downloading OFAC SDN list to %s ...", path)
        urllib.request.urlretrieve(SDN_CSV_URL, path)
        self.load_csv(path)
        return path

    def screen_name(self, name: str) -> list[dict[str, Any]]:
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
                if tokens == sdn_tokens or (tokens <= sdn_tokens and len(tokens) >= 2):
                    for entry in self._entries:
                        if entry["name"] == sdn_raw and entry["ent_num"] not in seen_ent:
                            seen_ent.add(entry["ent_num"])
                            match_type = "exact" if tokens == sdn_tokens else "subset"
                            hits.append({"match_type": match_type, **entry})

        return hits

    def screen_names(self, names: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Screen multiple names. Returns a dict mapping name -> list of hits."""
        results: dict[str, list[dict[str, Any]]] = {}
        for name in names:
            hits = self.screen_name(name)
            if hits:
                results[name] = hits
        return results

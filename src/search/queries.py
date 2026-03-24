from __future__ import annotations

import re
from itertools import product

from src.models import NameVariant


NICKNAMES = {
    "alexander": {"alex"},
    "alexandra": {"alex"},
    "william": {"will", "bill"},
    "robert": {"rob", "bob"},
    "michael": {"mike"},
    "james": {"jim"},
    "elizabeth": {"liz", "beth"},
    "katherine": {"kate", "katie"},
    "catherine": {"kate", "katie"},
    "christopher": {"chris"},
    "margaret": {"maggie", "meg"},
}

HONORIFICS = {"mr", "mrs", "ms", "miss", "dr", "sir", "lady", "prof"}

# Clusters of first-name spellings that refer to the same person.
# Every entry in a cluster is reachable from every other entry.
_SPELLING_CLUSTERS: list[set[str]] = [
    {"mohammed", "mohammad", "muhammad", "mohamed", "muhammed", "mohamad"},
    {"ahmed", "ahmad"},
    {"hussein", "husein", "hussain", "hossein"},
    {"mustafa", "mostafa", "mustapha"},
    {"yusuf", "youssef", "yousef", "yousuf", "yosef", "joseph"},
    {"abdel", "abdul", "abd"},
    {"ali", "aly"},
    {"omar", "umar"},
    {"ibrahim", "ebrahim"},
    {"ismail", "ismaeel", "ismael"},
    {"khalid", "khaled"},
    {"tariq", "tarek", "tareq"},
    {"steven", "stephen"},
    {"geoffrey", "jeffrey"},
    {"jon", "john"},
    {"ann", "anne"},
    {"phillip", "philip"},
    {"grey", "gray"},
    {"stuart", "stewart"},
    {"alan", "allan", "allen"},
    {"brian", "bryan"},
    {"neil", "neal"},
    {"sean", "shaun", "shawn"},
    {"teresa", "theresa"},
    {"rachel", "rachael"},
    {"leigh", "lee"},
    {"nicholas", "nicolas"},
    {"carl", "karl"},
    {"marc", "mark"},
]

# Build a fast lookup: lowercase name -> set of alternatives (excluding itself)
SPELLING_ALTERNATIVES: dict[str, set[str]] = {}
for _cluster in _SPELLING_CLUSTERS:
    for _name in _cluster:
        SPELLING_ALTERNATIVES.setdefault(_name, set()).update(_cluster - {_name})

# Substring pairs that are commonly swapped in transliterations and misspellings.
# Each pair is tried in both directions.  Ordered longest-first so longer
# patterns get a chance before shorter ones consume the same characters.
_FUZZY_SWAPS: list[tuple[str, str]] = [
    ("sch", "sh"),
    ("ph", "f"),
    ("ck", "k"),
    ("ou", "u"),
    ("ee", "i"),
    ("ei", "i"),
    ("ie", "i"),
    ("ar", "er"),
    ("er", "ar"),
    ("ah", "a"),
    ("ll", "l"),
    ("tt", "t"),
    ("ss", "s"),
    ("mm", "m"),
    ("nn", "n"),
    ("rr", "r"),
    ("ff", "f"),
    ("bb", "b"),
    ("dd", "d"),
    ("pp", "p"),
    ("y", "i"),
    ("i", "y"),
    ("c", "k"),
    ("k", "c"),
    ("s", "z"),
    ("z", "s"),
]


def normalize_name(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^\w\s-]", " ", value)
    tokens = [token for token in value.split() if token and token not in HONORIFICS]
    return " ".join(tokens)


_REPEATED_GIVEN_NAME_TOKENS = {
    "mohammed",
    "mohammad",
    "muhammad",
    "mohamed",
    "muhammed",
    "mohamad",
}


def is_low_information_person_name(value: str) -> bool:
    normalized = normalize_name(value)
    tokens = normalized.split()
    if not tokens:
        return True
    if len(tokens) == 1:
        return True
    if len(set(tokens)) == 1:
        return True
    if len(tokens) == 2 and tokens[0] in _REPEATED_GIVEN_NAME_TOKENS and tokens[1] in _REPEATED_GIVEN_NAME_TOKENS:
        return True
    return False


_SINGLE_CHAR_SWAPS = {s for s, _ in _FUZZY_SWAPS if len(s) == 1}


def _fuzzy_token_variants(token: str, max_variants: int = 6) -> list[str]:
    """Apply common character swaps to *one* name token and return up to
    *max_variants* unique alternative spellings (not including the original)."""
    results: set[str] = set()
    for old, new in _FUZZY_SWAPS:
        if len(results) >= max_variants:
            break
        idx = token.find(old)
        while idx != -1 and len(results) < max_variants:
            # Skip single-char swaps at word start -- they produce junk
            # like "Zteven" from the s->z rule.
            if idx == 0 and old in _SINGLE_CHAR_SWAPS:
                idx = token.find(old, idx + 1)
                continue
            candidate = token[:idx] + new + token[idx + len(old):]
            if candidate != token and len(candidate) > 1:
                results.add(candidate)
            idx = token.find(old, idx + 1)
    return list(results)


def _spelling_and_fuzzy_alternatives(token: str) -> list[str]:
    """Return known spelling alternatives plus fuzzy swaps for a token."""
    alts: set[str] = set()
    alts.update(SPELLING_ALTERNATIVES.get(token, set()))
    alts.update(_fuzzy_token_variants(token))
    alts.discard(token)
    return sorted(alts)


def generate_name_variants(name: str, creativity_level: str) -> list[NameVariant]:
    normalized = normalize_name(name)
    tokens = normalized.split()
    if not tokens:
        return []

    variants: list[NameVariant] = []
    seen: set[str] = set()

    def add_variant(value: str, strategy: str) -> None:
        clean = " ".join(value.split())
        if not clean:
            return
        key = clean.lower()
        if key in seen:
            return
        seen.add(key)
        variants.append(
            NameVariant(
                name=clean.title(),
                strategy=strategy,
                creativity_level=creativity_level,
            )
        )

    add_variant(normalized, "normalized")

    first = tokens[0]
    last = tokens[-1]
    middle = tokens[1:-1]

    add_variant(f"{first} {last}", "first_last")
    if middle:
        initials = " ".join(token[0] for token in middle)
        add_variant(f"{first} {initials} {last}", "middle_initials")
    add_variant(f"{first[0]} {last}", "first_initial_last")

    if creativity_level in {"balanced", "exploratory"}:
        if middle:
            add_variant(f"{first} {last}", "drop_middle_names")
        add_variant(f"{last}, {first}", "surname_first")

        for nickname in NICKNAMES.get(first, set()):
            add_variant(f"{nickname} {last}", "nickname_first_name")
            add_variant(f"{nickname[0]} {last}", "nickname_initial_last")

        # Alternative spellings of first name (known clusters)
        for alt_first in SPELLING_ALTERNATIVES.get(first, set()):
            add_variant(f"{alt_first} {last}", "alt_spelling_first")

        # Slightly more creative balanced generation:
        # add fuzzy last-name and first-name swaps in a capped way.
        for alt_last in _fuzzy_token_variants(last, max_variants=4):
            add_variant(f"{first} {alt_last}", "fuzzy_last_balanced")
        for alt_first in _fuzzy_token_variants(first, max_variants=3):
            add_variant(f"{alt_first} {last}", "fuzzy_first_balanced")

        # Add "first middle last" compressed forms if middle exists.
        if middle:
            add_variant(f"{first} {middle[0]} {last}", "first_single_middle_last")
            add_variant(f"{first[0]} {middle[0]} {last}", "first_middle_initial_last")

    if creativity_level == "exploratory":
        add_variant(f"{first[0]}. {last}", "dotted_initial")
        if len(tokens) >= 2:
            add_variant(" ".join(token[0] for token in tokens), "all_initials")

        # Fuzzy variants: apply character-level swaps to first and last name
        # independently, then combine the cross-product (capped).
        first_alts = [first] + _spelling_and_fuzzy_alternatives(first)
        last_alts = [last] + _fuzzy_token_variants(last)
        count = 0
        max_fuzzy = 20
        for f_alt, l_alt in product(first_alts, last_alts):
            if f_alt == first and l_alt == last:
                continue
            add_variant(f"{f_alt} {l_alt}", "fuzzy_spelling")
            count += 1
            if count >= max_fuzzy:
                break

    return variants


def build_dork_queries(variant: NameVariant) -> list[str]:
    q = f'"{variant.name}"'
    return [
        f'{q} site:register-of-charities.charitycommission.gov.uk',
        f'{q} "charity commission" trustee',
        f'{q} site:find-and-update.company-information.service.gov.uk',
        f'{q} "companies house" director OR secretary',
        f'{q} "land registry" "title register"',
        f'{q} site:gov.uk "search property information land registry"',
        f'{q} charity trustee OR director',
    ]

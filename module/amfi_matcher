"""
Match user's scheme names to canonical AMFI scheme codes.

Without an AMFI code, nothing else in the system works — no VRO lookup,
no factsheet, no NAV history. This module is the entry point.

Strategy:
  1. Pull AMFI's master daily NAV file (covers every Indian MF scheme)
  2. Normalize names by stripping noise (Plan/Direct/Growth/IDCW/punctuation)
  3. Fuzzy match using difflib (good enough, no extra deps)
  4. Prefer Direct Growth variants when picking among multiple matches
"""
from __future__ import annotations
import re
import difflib
from typing import List, Dict, Optional

from . import vr_scraper

# Words to drop when comparing scheme names
NOISE_WORDS = {
    "fund", "scheme", "the", "of", "an", "a",
    "open", "ended", "openended",
    "plan", "option",
    "regular", "direct",
    "growth", "idcw", "dividend", "reinvestment", "payout",
}


def _normalize(name: str) -> str:
    """Lowercase, remove punctuation, drop noise words, collapse spaces."""
    if not name:
        return ""
    s = name.lower()
    s = re.sub(r"[-_/\.\(\),]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    tokens = [t for t in s.split() if t and t not in NOISE_WORDS]
    return " ".join(tokens)


def find_best_match(scheme_name: str, threshold: float = 0.65) -> Optional[Dict]:
    """
    Best AMFI match for a user-typed scheme name. Returns dict with
    scheme_code, scheme_name, isin_growth, isin_div, fund_house, similarity.
    Returns None if no match crosses threshold.
    """
    if not scheme_name:
        return None

    try:
        master = vr_scraper.fetch_amfi_scheme_master()
    except Exception:
        return None

    target = _normalize(scheme_name)
    if not target:
        return None

    best = None  # tuple of (score, raw_ratio, scheme_dict)

    for s in master:
        candidate_norm = _normalize(s["scheme_name"])
        if not candidate_norm:
            continue
        ratio = difflib.SequenceMatcher(None, target, candidate_norm).ratio()
        if ratio < threshold:
            continue

        score = ratio
        name_lower = s["scheme_name"].lower()
        # Prefer Direct + Growth (default user intent)
        if "direct" in name_lower:
            score += 0.05
        if "growth" in name_lower:
            score += 0.05
        # Penalize IDCW / Dividend variants for default match
        if "idcw" in name_lower or "dividend" in name_lower:
            score -= 0.10
        # Strong bonus if the target string is contained in the candidate's tokens
        target_tokens = set(target.split())
        cand_tokens = set(candidate_norm.split())
        if target_tokens.issubset(cand_tokens):
            score += 0.10

        if best is None or score > best[0]:
            best = (score, ratio, s)

    if not best:
        return None

    score, ratio, scheme = best
    return {
        "scheme_code": scheme["scheme_code"],
        "scheme_name": scheme["scheme_name"],
        "isin_growth": scheme.get("isin_growth"),
        "isin_div": scheme.get("isin_div"),
        "fund_house": scheme.get("fund_house"),
        "similarity": round(ratio, 3),
        "score": round(score, 3),
    }


def batch_match(scheme_names: List[str]) -> Dict[str, Optional[Dict]]:
    """
    Match many scheme names. Pre-fetches AMFI master once for speed.
    Returns {original_name: match_dict_or_None}.
    """
    try:
        vr_scraper.fetch_amfi_scheme_master()  # warm cache
    except Exception:
        pass
    return {name: find_best_match(name) for name in set(scheme_names) if name}

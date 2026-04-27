"""
AMFI scheme name matcher — works without any network access.

Priority order:
  1. Bundled CSV  (data/scheme_master.csv) — always available, no network
  2. Live AMFI fetch (amfiindia.com)        — used when available, updates cache
  3. mfapi.in search                        — backup live source

The bundled CSV covers ~190 popular schemes across all major AMCs
including JioBlackRock (very new). Run scripts/update_scheme_master.py
to refresh it (GitHub Actions does this monthly).
"""
from __future__ import annotations
import re
import csv
import difflib
import logging
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

DATA_DIR   = Path(__file__).resolve().parent.parent / "data"
MASTER_CSV = DATA_DIR / "scheme_master.csv"
CACHE_DIR  = DATA_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Words stripped before fuzzy comparison
NOISE = {
    "fund","scheme","the","of","an","a",
    "open","ended","openended",
    "plan","option",
    "regular","direct",
    "growth","idcw","dividend","reinvestment","payout",
    "nfo",
}


def _norm(name: str) -> str:
    """Lowercase, remove punctuation, drop noise words, collapse spaces."""
    s = name.lower()
    s = re.sub(r"[-_/\.\(\),&]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    tokens = [t for t in s.split() if t and t not in NOISE]
    return " ".join(tokens)


def _score(target_norm: str, candidate: Dict) -> float:
    """Return a float score (higher = better match)."""
    cand_norm = _norm(candidate["scheme_name"])
    ratio = difflib.SequenceMatcher(None, target_norm, cand_norm).ratio()
    if ratio < 0.55:
        return 0.0

    score = ratio
    cname = candidate["scheme_name"].lower()

    # Prefer Direct Growth
    if "direct" in cname: score += 0.06
    if "growth" in cname: score += 0.06
    # Penalise IDCW / Dividend
    if "idcw" in cname or "dividend" in cname: score -= 0.12

    # Strong bonus: all target tokens present in candidate
    t_tokens = set(target_norm.split())
    c_tokens = set(cand_norm.split())
    if t_tokens and t_tokens.issubset(c_tokens):
        score += 0.15

    return score


# ─────────────────────────────────────────────
# Master loading
# ─────────────────────────────────────────────

def _load_bundled() -> List[Dict]:
    """Load the CSV bundled with the repo. Always works offline."""
    if not MASTER_CSV.exists():
        log.warning(f"Bundled master CSV not found at {MASTER_CSV}")
        return []
    schemes = []
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("scheme_code") and row.get("scheme_name"):
                schemes.append(row)
    log.info(f"Loaded {len(schemes)} schemes from bundled CSV")
    return schemes


def _load_live() -> List[Dict]:
    """
    Try to fetch full master from AMFI. Returns [] on any failure.
    Cached daily so we only hit AMFI once per day maximum.
    """
    import datetime, json, requests

    today = datetime.date.today().isoformat()
    cache_f = CACHE_DIR / f"amfi_live_{today}.json"
    if cache_f.exists():
        try:
            return json.loads(cache_f.read_text(encoding="utf-8"))
        except Exception:
            pass

    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    try:
        r = requests.get(url, timeout=12,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
    except Exception as e:
        log.info(f"Live AMFI fetch failed (offline?): {e}")
        return []

    schemes = []
    current_amc = None
    for line in r.text.splitlines():
        line = line.strip()
        if not line or line.startswith("Scheme Code"):
            continue
        if "|" not in line:
            if line.endswith("Mutual Fund"):
                current_amc = line
            continue
        parts = line.split("|")
        if len(parts) < 4:
            continue
        schemes.append({
            "scheme_code": parts[0].strip(),
            "isin_growth":  parts[1].strip() or "",
            "isin_div":     parts[2].strip() or "",
            "scheme_name":  parts[3].strip(),
            "fund_house":   current_amc or "",
        })

    if schemes:
        cache_f.write_text(json.dumps(schemes), encoding="utf-8")
        log.info(f"Fetched {len(schemes)} schemes from AMFI live")
    return schemes


def _get_master() -> List[Dict]:
    """Bundled CSV first; merge with live data if available."""
    bundled = _load_bundled()
    live    = _load_live()

    if not live:
        return bundled  # offline mode — bundled only

    # Merge: live is authoritative, bundled fills gaps for very new schemes
    # that may not yet appear in live (usually this doesn't happen).
    live_codes = {s["scheme_code"] for s in live}
    extra = [s for s in bundled if s["scheme_code"] not in live_codes]
    return live + extra


# ─────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────

def find_best_match(scheme_name: str, threshold: float = 0.62) -> Optional[Dict]:
    """
    Best AMFI match for a user-typed scheme name.
    Returns dict with scheme_code, scheme_name, isin_growth, isin_div,
    fund_house, similarity.  Returns None if below threshold.
    """
    if not scheme_name or not scheme_name.strip():
        return None

    master  = _get_master()
    if not master:
        log.error("Scheme master is empty — check data/scheme_master.csv")
        return None

    target_norm = _norm(scheme_name)
    if not target_norm:
        return None

    best_score = 0.0
    best       = None

    for candidate in master:
        s = _score(target_norm, candidate)
        if s > best_score:
            best_score = s
            best       = candidate

    if best is None or best_score < threshold:
        log.info(f"No match for '{scheme_name}' (best score {best_score:.2f})")
        return None

    # Compute clean similarity ratio for display
    ratio = difflib.SequenceMatcher(
        None, target_norm, _norm(best["scheme_name"])
    ).ratio()

    return {
        "scheme_code": best["scheme_code"],
        "scheme_name": best["scheme_name"],
        "isin_growth": best.get("isin_growth") or None,
        "isin_div":    best.get("isin_div")    or None,
        "fund_house":  best.get("fund_house")  or None,
        "similarity":  round(ratio, 3),
        "score":       round(best_score, 3),
    }


def batch_match(names: List[str]) -> Dict[str, Optional[Dict]]:
    """Match many names in one call. Pre-loads master once."""
    master = _get_master()   # warm cache
    return {n: find_best_match(n) for n in set(names) if n}

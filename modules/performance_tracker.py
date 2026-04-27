"""
Performance comparison: scheme vs category average vs benchmark/index.

We use mfapi.in's NAV history (free) to compute returns over standard windows.
Category average is computed across all schemes the user has imported in the same
sub-category — for proper category coverage, run a one-time bulk download of all
peer scheme NAVs first.
"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from statistics import mean
import logging

from . import database as db
from . import vr_scraper

log = logging.getLogger(__name__)


PERIODS = {
    "1M": 30,
    "3M": 90,
    "6M": 182,
    "1Y": 365,
    "3Y": 365 * 3,
    "5Y": 365 * 5,
}


def compute_returns(nav_history: List[Dict], as_of: Optional[date] = None) -> Dict[str, float]:
    """
    Returns CAGR for periods >= 1Y, absolute return for shorter periods.
    Expects nav_history sorted DESCENDING by date (newest first), as mfapi.in returns it.
    """
    if not nav_history:
        return {}

    as_of = as_of or date.today()
    nav_by_date = {}
    for item in nav_history:
        try:
            d = datetime.strptime(item["date"], "%Y-%m-%d").date() \
                if isinstance(item["date"], str) else item["date"]
            nav_by_date[d] = item["nav"]
        except (ValueError, KeyError):
            continue

    if not nav_by_date:
        return {}

    # current NAV: nearest date <= as_of
    current_date = max(d for d in nav_by_date if d <= as_of)
    current_nav = nav_by_date[current_date]

    results = {}
    for period_label, days in PERIODS.items():
        target_date = current_date - timedelta(days=days)
        # find nearest available NAV on/before target_date
        candidates = [d for d in nav_by_date if d <= target_date]
        if not candidates:
            continue
        past_date = max(candidates)
        past_nav = nav_by_date[past_date]
        if past_nav <= 0:
            continue
        if days >= 365:
            years = days / 365
            ret = ((current_nav / past_nav) ** (1 / years) - 1) * 100
        else:
            ret = ((current_nav / past_nav) - 1) * 100
        results[period_label] = round(ret, 2)
    return results


def compute_for_scheme(scheme_code: str) -> Dict:
    """Return full performance picture for a single scheme."""
    nav_history = vr_scraper.get_nav_history(scheme_code)
    if not nav_history:
        return {"error": f"No NAV history for {scheme_code}"}
    scheme_returns = compute_returns(nav_history)

    scheme_row = db.get_scheme(scheme_code)
    sub_category = scheme_row["sub_category"] if scheme_row else None

    category_avg = compute_category_average(sub_category) if sub_category else {}

    today = date.today().isoformat()
    for period, ret in scheme_returns.items():
        cat_ret = category_avg.get(period)
        db.save_performance(
            scheme_code, today, period,
            ret, cat_ret if cat_ret is not None else 0, 0
        )

    return {
        "scheme_code": scheme_code,
        "scheme_name": scheme_row["scheme_name"] if scheme_row else None,
        "sub_category": sub_category,
        "scheme_returns": scheme_returns,
        "category_average": category_avg,
        "benchmark_returns": {},  # populate when you wire in an index data source
        "as_of": today,
    }


def compute_category_average(sub_category: str) -> Dict[str, float]:
    """
    Average returns across all peers in the same sub-category that we have data for.
    For meaningful category averages, you need NAV history for all peers — see
    scripts/seed_category_peers.py for a one-time bulk loader.
    """
    with db.get_conn() as conn:
        peers = conn.execute(
            "SELECT scheme_code FROM schemes WHERE sub_category = ?", (sub_category,)
        ).fetchall()

    if len(peers) < 2:
        return {}

    period_returns = {p: [] for p in PERIODS}
    for p in peers:
        history = vr_scraper.get_nav_history(p["scheme_code"])
        if not history:
            continue
        rets = compute_returns(history)
        for period, ret in rets.items():
            period_returns[period].append(ret)

    return {p: round(mean(v), 2) for p, v in period_returns.items() if v}

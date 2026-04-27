"""
Compare two factsheets to surface:
  - New stocks bought
  - Stocks fully sold (exited)
  - Stocks where weight increased / decreased (rebalancing)
  - Sector weight changes (current month vs previous month)

This is the comparison layer the user asked for in points 1 and 2.
"""
from __future__ import annotations
from typing import Dict, List, Tuple

from . import database as db


def compare_factsheets(scheme_code: str) -> Dict:
    """
    Compare the two most recent factsheets for a scheme.
    Returns a dict with new_buys, exited, weight_changes, sector_changes.
    If only one factsheet exists, returns that one as the baseline.
    """
    latest_two = db.get_latest_two_factsheets(scheme_code)

    if not latest_two:
        return {"error": "No factsheets stored for this scheme. Run a refresh first."}

    if len(latest_two) == 1:
        return {
            "scheme_code": scheme_code,
            "current_date": latest_two[0]["factsheet_date"],
            "previous_date": None,
            "message": "Only one factsheet on file — comparison needs at least two.",
            "new_buys": [],
            "exited": [],
            "weight_changes": [],
            "sector_changes": [],
        }

    current, previous = latest_two[0], latest_two[1]

    cur_holdings = {h["stock_name"]: h for h in db.get_holdings(current["id"])}
    prev_holdings = {h["stock_name"]: h for h in db.get_holdings(previous["id"])}

    cur_sectors = {s["sector"]: s["percentage"] for s in db.get_sectors(current["id"])}
    prev_sectors = {s["sector"]: s["percentage"] for s in db.get_sectors(previous["id"])}

    new_buys = []
    weight_changes = []
    for name, h in cur_holdings.items():
        if name not in prev_holdings:
            new_buys.append({
                "stock_name": name,
                "sector": h["sector"],
                "percentage": h["percentage"],
            })
        else:
            old_pct = prev_holdings[name]["percentage"]
            new_pct = h["percentage"]
            change = new_pct - old_pct
            if abs(change) >= 0.05:  # ignore changes < 5 bps (likely rounding)
                weight_changes.append({
                    "stock_name": name,
                    "sector": h["sector"],
                    "old_percentage": round(old_pct, 2),
                    "new_percentage": round(new_pct, 2),
                    "change": round(change, 2),
                })

    exited = [
        {
            "stock_name": name,
            "sector": h["sector"],
            "previous_percentage": h["percentage"],
        }
        for name, h in prev_holdings.items()
        if name not in cur_holdings
    ]

    weight_changes.sort(key=lambda x: abs(x["change"]), reverse=True)
    new_buys.sort(key=lambda x: x["percentage"], reverse=True)
    exited.sort(key=lambda x: x["previous_percentage"], reverse=True)

    sector_changes = []
    all_sectors = set(cur_sectors) | set(prev_sectors)
    for s in all_sectors:
        old = prev_sectors.get(s, 0)
        new = cur_sectors.get(s, 0)
        change = new - old
        if abs(change) < 0.01:
            continue
        sector_changes.append({
            "sector": s,
            "old_percentage": round(old, 2),
            "new_percentage": round(new, 2),
            "change": round(change, 2),
        })
    sector_changes.sort(key=lambda x: abs(x["change"]), reverse=True)

    return {
        "scheme_code": scheme_code,
        "current_date": current["factsheet_date"],
        "previous_date": previous["factsheet_date"],
        "new_buys": new_buys,
        "exited": exited,
        "weight_changes": weight_changes,
        "sector_changes": sector_changes,
        "summary": {
            "stocks_added": len(new_buys),
            "stocks_exited": len(exited),
            "rebalanced_holdings": len(weight_changes),
            "sectors_changed": len(sector_changes),
        },
    }


def compare_all_held_schemes() -> List[Dict]:
    """Run comparison for every scheme the user holds."""
    return [compare_factsheets(s["scheme_code"]) for s in db.list_held_schemes()]

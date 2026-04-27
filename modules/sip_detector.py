"""
SIP detection — robust version.

The problem with simple "same date every month" detection:
  - SIPs scheduled on the 5th run on the 6th if 5th is Sunday
  - First instalment after NFO sometimes lands on a different day
  - Amounts can vary by ±1 paisa due to rounding
  - Lump sum on the SIP date gets mixed in with SIP debits
  - Folio may get reassigned mid-stream

Algorithm:
  1. Group purchase transactions by (folio_no, scheme_name_raw).
  2. Within each group, cluster transactions by amount (median ± 5%).
  3. For each amount-cluster, check chronological intervals.
  4. SIP candidate if: 3+ consecutive transactions with intervals in [25, 35] days,
     and the day-of-month falls within ±3 days of a stable mode.
  5. Mark transactions as SIP, save SIP record, compute next-expected date.

This catches the standard cases AND lets through legitimate SIPs that drift
because of weekends, while still excluding lumpsums and mismatched amounts.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import median, mode, StatisticsError
from typing import List, Dict, Tuple
import calendar

from . import database as db


SIP_MIN_OCCURRENCES = 3            # need at least 3 hits to call something a SIP
SIP_MIN_INTERVAL_DAYS = 25         # roughly monthly
SIP_MAX_INTERVAL_DAYS = 35
SIP_DAY_TOLERANCE = 3              # day-of-month can drift this much (weekends/holidays)
SIP_AMOUNT_TOLERANCE_PCT = 0.05    # 5% — covers SIP step-ups and rounding
ACTIVE_SIP_GRACE_DAYS = 45         # if last instalment > 45 days old, mark stopped


# --------------------------- helpers ---------------------------

def _parse_date(d) -> date:
    if isinstance(d, date):
        return d
    if isinstance(d, datetime):
        return d.date()
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


def _amounts_close(a: float, b: float, tol_pct: float = SIP_AMOUNT_TOLERANCE_PCT) -> bool:
    if a == 0 or b == 0:
        return False
    return abs(a - b) / max(abs(a), abs(b)) <= tol_pct


def _safe_mode(values):
    """mode() raises if there's no unique mode. Return most common, ties broken by smallest."""
    if not values:
        return None
    counts = defaultdict(int)
    for v in values:
        counts[v] += 1
    return min(values, key=lambda v: (-counts[v], v))


def _next_expected_sip_date(last_date: date, sip_day: int) -> date:
    """Compute the next expected SIP date given the day-of-month."""
    year, month = last_date.year, last_date.month
    # advance one month
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    last_day_of_month = calendar.monthrange(year, month)[1]
    return date(year, month, min(sip_day, last_day_of_month))


# --------------------------- core detection ---------------------------

def _cluster_by_amount(txns: List[Dict]) -> List[List[Dict]]:
    """
    Greedy clustering: for each transaction, try to put it in an existing
    cluster whose median amount is within tolerance. Otherwise start a new cluster.
    """
    clusters: List[List[Dict]] = []
    for t in txns:
        placed = False
        for cluster in clusters:
            cluster_median = median(x["amount"] for x in cluster)
            if _amounts_close(t["amount"], cluster_median):
                cluster.append(t)
                placed = True
                break
        if not placed:
            clusters.append([t])
    return clusters


def _is_sip_chain(cluster: List[Dict]) -> Tuple[bool, Dict]:
    """
    Given a chronologically-sorted cluster of similar-amount transactions,
    decide if it's a SIP. Returns (is_sip, metadata).
    """
    if len(cluster) < SIP_MIN_OCCURRENCES:
        return False, {}

    cluster = sorted(cluster, key=lambda t: t["transaction_date"])

    intervals = []
    for prev, curr in zip(cluster[:-1], cluster[1:]):
        d1 = _parse_date(prev["transaction_date"])
        d2 = _parse_date(curr["transaction_date"])
        intervals.append((d2 - d1).days)

    monthly_intervals = [
        i for i in intervals
        if SIP_MIN_INTERVAL_DAYS <= i <= SIP_MAX_INTERVAL_DAYS
    ]
    if len(monthly_intervals) < SIP_MIN_OCCURRENCES - 1:
        return False, {}

    # day-of-month consistency check
    days_of_month = [_parse_date(t["transaction_date"]).day for t in cluster]
    sip_day_guess = _safe_mode(days_of_month)
    if sip_day_guess is None:
        return False, {}

    matching_days = sum(
        1 for d in days_of_month if abs(d - sip_day_guess) <= SIP_DAY_TOLERANCE
        # handle month-end wrap (e.g., 31st vs 1st)
        or abs(d - sip_day_guess) >= 28 - SIP_DAY_TOLERANCE
    )
    if matching_days < SIP_MIN_OCCURRENCES:
        return False, {}

    last_date = _parse_date(cluster[-1]["transaction_date"])
    today = date.today()
    is_active = (today - last_date).days <= ACTIVE_SIP_GRACE_DAYS

    confidence = matching_days / len(cluster)

    return True, {
        "occurrences": len(cluster),
        "sip_amount": round(median(t["amount"] for t in cluster), 2),
        "sip_day": sip_day_guess,
        "start_date": _parse_date(cluster[0]["transaction_date"]).isoformat(),
        "last_seen_date": last_date.isoformat(),
        "next_expected_date": _next_expected_sip_date(last_date, sip_day_guess).isoformat(),
        "status": "active" if is_active else "stopped",
        "confidence": round(confidence, 2),
        "txn_ids": [t["id"] for t in cluster],
    }


def detect_sips() -> Dict:
    """
    Run SIP detection across all stored transactions.
    Idempotent — safe to run after every fresh CAS import.
    """
    rows = db.get_transactions_for_sip_detection()

    # group by (folio_no, scheme_name_raw)
    groups: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
    for r in rows:
        key = (r["folio_no"] or "", r["scheme_name_raw"] or "")
        groups[key].append({
            "id": r["id"],
            "folio_no": r["folio_no"],
            "scheme_code": r["scheme_code"],
            "scheme_name_raw": r["scheme_name_raw"],
            "transaction_date": r["transaction_date"],
            "amount": r["amount"],
        })

    sips_found = 0
    txns_marked = 0

    for (folio, scheme_name), txns in groups.items():
        if len(txns) < SIP_MIN_OCCURRENCES:
            continue
        clusters = _cluster_by_amount(txns)
        for cluster in clusters:
            is_sip, meta = _is_sip_chain(cluster)
            if not is_sip:
                continue

            sip_record = {
                "folio_no": folio,
                "scheme_code": cluster[0].get("scheme_code"),
                "scheme_name_raw": scheme_name,
                "sip_amount": meta["sip_amount"],
                "sip_day": meta["sip_day"],
                "start_date": meta["start_date"],
                "last_seen_date": meta["last_seen_date"],
                "next_expected_date": meta["next_expected_date"],
                "occurrences": meta["occurrences"],
                "status": meta["status"],
                "confidence": meta["confidence"],
            }
            sip_id = db.upsert_sip(sip_record)
            db.link_transactions_to_sip(sip_id, meta["txn_ids"])
            sips_found += 1
            txns_marked += len(meta["txn_ids"])

    return {"sips_found": sips_found, "transactions_marked": txns_marked}


# --------------------------- diagnostics ---------------------------

def explain_grouping(folio_no: str, scheme_name: str) -> Dict:
    """
    Diagnostic helper — given a folio + scheme, return the cluster analysis
    so you can see WHY a SIP was/wasn't detected. Useful when a user reports
    'this is clearly a SIP, why didn't you catch it'.
    """
    rows = db.get_transactions_for_sip_detection()
    matching = [
        {
            "id": r["id"],
            "folio_no": r["folio_no"],
            "scheme_code": r["scheme_code"],
            "scheme_name_raw": r["scheme_name_raw"],
            "transaction_date": r["transaction_date"],
            "amount": r["amount"],
        }
        for r in rows
        if (r["folio_no"] or "") == folio_no and (r["scheme_name_raw"] or "") == scheme_name
    ]

    if not matching:
        return {"error": "No transactions found for this folio + scheme"}

    clusters = _cluster_by_amount(matching)
    cluster_info = []
    for i, c in enumerate(clusters):
        is_sip, meta = _is_sip_chain(c)
        cluster_info.append({
            "cluster_index": i,
            "txn_count": len(c),
            "amount_range": (min(t["amount"] for t in c), max(t["amount"] for t in c)),
            "is_sip": is_sip,
            "metadata": meta if is_sip else None,
            "transactions": [(t["transaction_date"], t["amount"]) for t in c],
        })

    return {
        "folio_no": folio_no,
        "scheme_name": scheme_name,
        "total_txns": len(matching),
        "clusters": cluster_info,
    }

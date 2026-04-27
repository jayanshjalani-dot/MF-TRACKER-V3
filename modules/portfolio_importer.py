"""
Portfolio importer — supports:
  1. CAMS / KFintech Consolidated Account Statement (CAS) PDFs
  2. CSV / Excel transaction exports

KEY FIX (Apr 2026): CSV imports now auto-match scheme names against the AMFI
master and create scheme records. Without this, the schemes table stays empty
and every other page shows "no portfolio".

Also includes reconcile_unmatched() — fixes already-imported transactions that
have NULL scheme_code without requiring a fresh import.
"""
from __future__ import annotations
import io
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd

from . import database as db
from . import amfi_matcher


# =========================================================================
# CAS PDF (CAMS / KFintech)
# =========================================================================

def import_cas_pdf(file_bytes: bytes, password: str) -> Dict[str, Any]:
    try:
        import casparser
    except ImportError:
        raise RuntimeError("casparser is not installed. Run: pip install casparser")

    bio = io.BytesIO(file_bytes)
    parsed = casparser.read_cas_pdf(bio, password, output="dict")

    rows: List[Dict[str, Any]] = []
    schemes_seen: List[Dict[str, Any]] = []

    for folio in parsed.get("folios", []):
        folio_no = folio.get("folio")
        for scheme in folio.get("schemes", []):
            scheme_name = scheme.get("scheme")
            isin = scheme.get("isin")
            amfi = scheme.get("amfi")  # AMFI scheme code — canonical key

            if amfi:
                schemes_seen.append({
                    "scheme_code": str(amfi),
                    "scheme_name": scheme_name,
                    "isin_growth": isin,
                    "fund_house": _extract_amc(scheme_name),
                })

            for txn in scheme.get("transactions", []):
                rows.append({
                    "folio_no": folio_no,
                    "scheme_code": str(amfi) if amfi else None,
                    "scheme_name_raw": scheme_name,
                    "transaction_date": _parse_date(txn.get("date")),
                    "transaction_type": _normalize_txn_type(txn.get("type") or txn.get("description", "")),
                    "amount": float(txn.get("amount") or 0),
                    "units": float(txn.get("units")) if txn.get("units") else None,
                    "nav": float(txn.get("nav")) if txn.get("nav") else None,
                    "source_file": "cas_pdf",
                })

    seen_codes = set()
    for s in schemes_seen:
        if s["scheme_code"] in seen_codes:
            continue
        seen_codes.add(s["scheme_code"])
        existing = db.get_scheme(s["scheme_code"])
        if not existing:
            db.upsert_scheme({
                "scheme_code": s["scheme_code"],
                "scheme_name": s["scheme_name"],
                "isin_growth": s.get("isin_growth"),
                "fund_house": s.get("fund_house"),
                "vr_code": None, "isin_div": None, "category": None,
                "sub_category": None, "objective": None, "benchmark": None,
                "expense_ratio": None, "aum": None,
            })

    inserted = db.insert_transactions(rows)
    return {
        "schemes_found": len(seen_codes),
        "transactions_in_file": len(rows),
        "transactions_inserted": inserted,
        "duplicates_skipped": len(rows) - inserted,
    }


# =========================================================================
# CSV / Excel — with auto AMFI matching
# =========================================================================

def import_csv(file_bytes: bytes, column_map: Dict[str, str]) -> Dict[str, Any]:
    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
    except Exception:
        df = pd.read_excel(io.BytesIO(file_bytes))

    for k in ("transaction_date", "scheme_name", "amount"):
        if k not in column_map or column_map[k] not in df.columns:
            raise ValueError(f"Missing required column mapping: {k}")

    rows = []
    for _, r in df.iterrows():
        try:
            amount = float(r[column_map["amount"]])
        except (ValueError, TypeError):
            continue
        if amount <= 0:
            continue
        rows.append({
            "folio_no": _safe_str(r, column_map.get("folio_no")),
            "scheme_code": _safe_str(r, column_map.get("scheme_code")),
            "scheme_name_raw": str(r[column_map["scheme_name"]]).strip(),
            "transaction_date": _parse_date(r[column_map["transaction_date"]]),
            "transaction_type": _normalize_txn_type(
                _safe_str(r, column_map.get("transaction_type")) or "Purchase"
            ),
            "amount": amount,
            "units": _safe_float(r, column_map.get("units")),
            "nav": _safe_float(r, column_map.get("nav")),
            "source_file": "csv_import",
        })

    # Auto-match scheme names → AMFI codes
    unique_names = sorted({r["scheme_name_raw"] for r in rows if not r.get("scheme_code")})
    matches = amfi_matcher.batch_match(unique_names)

    matched_count = 0
    unmatched = []
    match_details = {}

    for name, match in matches.items():
        if match:
            matched_count += 1
            match_details[name] = match
            db.upsert_scheme({
                "scheme_code": match["scheme_code"],
                "scheme_name": match["scheme_name"],
                "isin_growth": match.get("isin_growth"),
                "isin_div": match.get("isin_div"),
                "fund_house": match.get("fund_house"),
                "vr_code": None, "category": None, "sub_category": None,
                "objective": None, "benchmark": None,
                "expense_ratio": None, "aum": None,
            })
        else:
            unmatched.append(name)

    for r in rows:
        if not r.get("scheme_code") and r["scheme_name_raw"] in match_details:
            r["scheme_code"] = match_details[r["scheme_name_raw"]]["scheme_code"]

    inserted = db.insert_transactions(rows)

    return {
        "transactions_in_file": len(rows),
        "transactions_inserted": inserted,
        "duplicates_skipped": len(rows) - inserted,
        "schemes_matched": matched_count,
        "schemes_unmatched": len(unmatched),
        "match_details": match_details,
        "unmatched_names": unmatched,
    }


# =========================================================================
# Reconcile — fix already-imported transactions that have NULL scheme_code
# =========================================================================

def reconcile_unmatched() -> Dict[str, Any]:
    """
    Find transactions where scheme_code is NULL, try to match each unique
    scheme name against AMFI, create scheme records, and update transactions.
    Use this after importing data with the old (broken) flow.
    """
    with db.get_conn() as conn:
        unmatched_names = [
            r["scheme_name_raw"] for r in conn.execute(
                """
                SELECT DISTINCT scheme_name_raw FROM transactions
                WHERE (scheme_code IS NULL OR scheme_code = '' OR scheme_code = 'None')
                  AND scheme_name_raw IS NOT NULL
                """
            ).fetchall()
        ]

    if not unmatched_names:
        return {"matched": 0, "still_unmatched": [], "match_details": {}}

    matches = amfi_matcher.batch_match(unmatched_names)

    matched_count = 0
    still_unmatched = []
    match_details = {}

    for name, match in matches.items():
        if not match:
            still_unmatched.append(name)
            continue
        match_details[name] = match
        matched_count += 1

        db.upsert_scheme({
            "scheme_code": match["scheme_code"],
            "scheme_name": match["scheme_name"],
            "isin_growth": match.get("isin_growth"),
            "isin_div": match.get("isin_div"),
            "fund_house": match.get("fund_house"),
            "vr_code": None, "category": None, "sub_category": None,
            "objective": None, "benchmark": None,
            "expense_ratio": None, "aum": None,
        })

        with db.get_conn() as conn:
            conn.execute(
                """
                UPDATE transactions SET scheme_code = ?
                WHERE scheme_name_raw = ?
                  AND (scheme_code IS NULL OR scheme_code = '' OR scheme_code = 'None')
                """,
                (match["scheme_code"], name),
            )
            conn.commit()

    return {
        "matched": matched_count,
        "still_unmatched": still_unmatched,
        "match_details": match_details,
    }


# =========================================================================
# Helpers
# =========================================================================

def _safe_str(row, col):
    if not col or col == "—":
        return None
    val = row.get(col)
    if pd.isna(val):
        return None
    return str(val).strip()


def _safe_float(row, col):
    if not col or col == "—":
        return None
    val = row.get(col)
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_date(d) -> str:
    if isinstance(d, datetime):
        return d.date().isoformat()
    if hasattr(d, "isoformat"):
        return d.isoformat()[:10]
    s = str(d).strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%y",
                "%Y/%m/%d", "%d %b %Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return pd.to_datetime(s, dayfirst=True).date().isoformat()


def _normalize_txn_type(raw: str) -> str:
    if not raw:
        return "Purchase"
    r = raw.lower()
    if any(k in r for k in ["sip", "systematic"]):
        return "SIP"
    if any(k in r for k in ["redempt", "sell", "sale", "swp", "switch out"]):
        return "Redemption"
    if any(k in r for k in ["dividend", "idcw"]):
        return "Dividend"
    if any(k in r for k in ["switch in", "switch_in"]):
        return "Switch-In"
    if any(k in r for k in ["purchase", "buy", "subscript", "investment"]):
        return "Purchase"
    return "Purchase"


def _extract_amc(name: str) -> Optional[str]:
    if not name:
        return None
    known = ["HDFC", "ICICI", "SBI", "Axis", "Kotak", "Nippon", "DSP", "Mirae",
             "Aditya", "Franklin", "UTI", "Tata", "Quant", "PPFAS", "Parag",
             "Edelweiss", "Invesco", "L&T", "Sundaram", "Canara", "IDFC",
             "Bandhan", "Motilal", "Baroda", "PGIM", "Mahindra", "JM",
             "WhiteOak", "HSBC", "Navi", "Quantum", "ITI", "Helios",
             "Old Bridge", "Samco", "360 ONE", "Trust", "Union", "Zerodha",
             "Jio BlackRock", "JioBlackRock", "Bank of India"]
    for k in known:
        if name.lower().startswith(k.lower()):
            return k
    return name.split()[0] if name else None

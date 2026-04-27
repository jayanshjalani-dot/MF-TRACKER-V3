"""
Data fetcher for fund details, factsheets, NAVs.

Sources:
  - AMFI for canonical scheme master + daily NAV (no ToS issues)
  - mfapi.in for full historical NAV per scheme (free, community-run)
  - Value Research for portfolio (holdings, sectors), category, manager, objective

Hardened version (Apr 2026): VR HTML changes break selectors. This module
tries multiple extraction strategies per field and gracefully degrades when
something can't be parsed. Cached aggressively to be polite.
"""
from __future__ import annotations
import time
import re
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests
from bs4 import BeautifulSoup

from . import database as db

log = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

VR_RATE_LIMIT_SEC = 3.0
VR_BASE = "https://www.valueresearchonline.com"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_last_vr_request = 0.0


def _http_get(url: str, throttle: bool = True, timeout: int = 30) -> Optional[requests.Response]:
    global _last_vr_request
    if throttle:
        elapsed = time.time() - _last_vr_request
        if elapsed < VR_RATE_LIMIT_SEC:
            time.sleep(VR_RATE_LIMIT_SEC - elapsed)
        _last_vr_request = time.time()
    try:
        r = requests.get(
            url, timeout=timeout,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IN,en;q=0.9",
            }
        )
        if r.status_code == 200:
            return r
        log.warning(f"GET {url} → {r.status_code}")
    except requests.RequestException as e:
        log.warning(f"GET {url} failed: {e}")
    return None


# =====================================================================
# AMFI master + NAVs
# =====================================================================

def fetch_amfi_scheme_master() -> List[Dict[str, Any]]:
    """AMFI's daily pipe-delimited NAV file — covers every Indian MF scheme."""
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    cache_file = CACHE_DIR / f"amfi_master_{date.today().isoformat()}.txt"

    if cache_file.exists():
        text = cache_file.read_text(encoding="utf-8")
    else:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        text = r.text
        cache_file.write_text(text, encoding="utf-8")

    schemes = []
    current_amc = None
    current_category = None

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("Scheme Code"):
            continue
        if "|" not in line:
            if line.endswith("Mutual Fund"):
                current_amc = line
            else:
                current_category = line
            continue
        parts = line.split("|")
        if len(parts) < 6:
            continue
        try:
            schemes.append({
                "scheme_code": parts[0].strip(),
                "isin_growth": parts[1].strip() or None,
                "isin_div": parts[2].strip() or None,
                "scheme_name": parts[3].strip(),
                "nav": float(parts[4].strip()) if parts[4].strip() not in ("N.A.", "") else None,
                "nav_date": parts[5].strip(),
                "fund_house": current_amc,
                "category_raw": current_category,
            })
        except (ValueError, IndexError):
            continue
    return schemes


def get_nav_history(scheme_code: str) -> List[Dict[str, Any]]:
    """Full NAV history from mfapi.in. Cached daily."""
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    cache_file = CACHE_DIR / f"nav_{scheme_code}_{date.today().isoformat()}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())

    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning(f"NAV fetch failed for {scheme_code}: {e}")
        return []

    history = []
    for item in data.get("data", []):
        try:
            history.append({
                "date": datetime.strptime(item["date"], "%d-%m-%Y").date().isoformat(),
                "nav": float(item["nav"]),
            })
        except (ValueError, KeyError):
            continue
    cache_file.write_text(json.dumps(history))
    return history


# =====================================================================
# Value Research
# =====================================================================

def find_vr_code(scheme_name: str, isin: str = None) -> Optional[str]:
    """
    Search VR for a scheme. Tries ISIN first (more accurate), then name.
    Returns VR code (numeric).
    """
    safe_key = re.sub(r"[^a-z0-9]+", "_", (scheme_name or "").lower())[:80]
    cache_file = CACHE_DIR / f"vr_search_{safe_key}.json"
    if cache_file.exists():
        cached = json.loads(cache_file.read_text())
        if cached.get("vr_code"):
            return cached["vr_code"]

    queries = []
    if isin:
        queries.append(isin)
    if scheme_name:
        # remove noise that may not be in VR's title
        cleaned = re.sub(
            r"\b(Direct|Regular|Plan|Growth|IDCW|Dividend|Reinvestment|Payout|Option)\b",
            "", scheme_name, flags=re.I
        ).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        queries.append(cleaned)
        queries.append(scheme_name)

    vr_code = None
    for q in queries:
        if not q:
            continue
        url = f"{VR_BASE}/funds/search/?q={requests.utils.quote(q)}"
        r = _http_get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        # Find first link matching /funds/<numeric_id>/
        for link in soup.find_all("a", href=True):
            m = re.search(r"/funds/(\d+)/?", link["href"])
            if m:
                vr_code = m.group(1)
                break
        if vr_code:
            break

    cache_file.write_text(json.dumps({
        "vr_code": vr_code, "queried": scheme_name, "isin": isin
    }))
    return vr_code


def fetch_vr_fund_page(vr_code: str) -> Optional[BeautifulSoup]:
    cache_file = CACHE_DIR / f"vr_fund_{vr_code}_{date.today().isoformat()}.html"
    if cache_file.exists():
        return BeautifulSoup(cache_file.read_text(encoding="utf-8"), "html.parser")
    r = _http_get(f"{VR_BASE}/funds/{vr_code}/")
    if not r:
        return None
    cache_file.write_text(r.text, encoding="utf-8")
    return BeautifulSoup(r.text, "html.parser")


def fetch_vr_portfolio_page(vr_code: str) -> Optional[BeautifulSoup]:
    cache_file = CACHE_DIR / f"vr_portfolio_{vr_code}_{date.today().isoformat()}.html"
    if cache_file.exists():
        return BeautifulSoup(cache_file.read_text(encoding="utf-8"), "html.parser")
    r = _http_get(f"{VR_BASE}/funds/{vr_code}/portfolio/")
    if not r:
        return None
    cache_file.write_text(r.text, encoding="utf-8")
    return BeautifulSoup(r.text, "html.parser")


# --------------------------- Field extractors ---------------------------

def _label_value(soup: BeautifulSoup, *labels) -> Optional[str]:
    """
    Find label-value pairs. VR uses many layouts:
      <span>Category</span><span>Equity: Mid Cap</span>
      <dt>Category</dt><dd>Equity: Mid Cap</dd>
      <td>Category</td><td>Equity: Mid Cap</td>
    """
    label_pattern = re.compile(
        r"^\s*(?:" + "|".join(re.escape(l) for l in labels) + r")\s*:?\s*$",
        re.I
    )
    for el in soup.find_all(string=label_pattern):
        parent = el.parent
        if not parent:
            continue
        # Try next sibling
        sib = parent.find_next_sibling()
        if sib and sib.get_text(strip=True):
            return sib.get_text(strip=True)
        # Try parent's next sibling
        if parent.parent:
            for s in parent.parent.find_all(string=True):
                t = s.strip()
                if not t or label_pattern.match(t):
                    continue
                return t
    return None


def _parse_managers(soup: BeautifulSoup) -> List[str]:
    text = soup.get_text(" ", strip=True)
    # "Fund Manager: Rajeev Thakkar (since Apr 2013), Raunak Onkar"
    m = re.search(
        r"Fund Manager[s]?\s*:?\s*([A-Z][\w\s.,&/()\-]{3,300}?)"
        r"(?=\s+(?:Inception|Benchmark|Launch|Asset|AUM|Expense|Category|$))",
        text
    )
    if not m:
        return []
    raw = m.group(1)
    # Strip "(since ...)" annotations
    raw = re.sub(r"\([^)]*\)", "", raw)
    parts = re.split(r"\s*(?:,|\band\b|/|;)\s*", raw)
    return [p.strip() for p in parts if 3 < len(p.strip()) < 80 and p.strip()[0].isupper()]


def _parse_objective(soup: BeautifulSoup) -> Optional[str]:
    for h in soup.find_all(["h2", "h3", "h4", "strong"]):
        if "objective" in h.get_text(strip=True).lower():
            sib = h.find_next_sibling()
            if sib:
                t = sib.get_text(strip=True)
                if 30 < len(t) < 2000:
                    return t
    return None


def _to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"-?\d+\.?\d*", s.replace(",", ""))
    return float(m.group()) if m else None


def _parse_aum(s: Optional[str]) -> Optional[float]:
    """'Rs 12,345 Cr' → 1234500 (lakhs)."""
    if not s:
        return None
    val = _to_float(s)
    if val is None:
        return None
    return val * 100 if "cr" in s.lower() else val


def parse_fund_details(vr_code: str) -> Optional[Dict[str, Any]]:
    soup = fetch_vr_fund_page(vr_code)
    if not soup:
        return None

    name_el = soup.select_one("h1") or soup.select_one(".fund-name")

    return {
        "vr_code": vr_code,
        "scheme_name": name_el.get_text(strip=True) if name_el else None,
        "category": _label_value(soup, "Category"),
        "sub_category": _label_value(soup, "Sub-category", "Sub Category", "Sub-Category"),
        "fund_house": _label_value(soup, "Fund house", "Fund House", "AMC"),
        "benchmark": _label_value(soup, "Benchmark"),
        "fund_managers": _parse_managers(soup),
        "objective": _parse_objective(soup),
        "expense_ratio": _to_float(_label_value(soup, "Expense ratio", "Expense Ratio")),
        "aum": _parse_aum(_label_value(soup, "Fund size", "AUM")),
    }


# --------------------------- Portfolio extraction ---------------------------

def _extract_factsheet_date(soup: BeautifulSoup) -> Optional[str]:
    text = soup.get_text(" ", strip=True)
    for pattern, fmt in [
        (r"[Aa]s on\s+(\d{1,2}[-\s][A-Za-z]{3}[-\s]\d{2,4})", "%d-%b-%Y"),
        (r"[Pp]ortfolio\s+date\s*:?\s*(\d{1,2}[-\s/][A-Za-z]{3}[-\s/]\d{2,4})", "%d-%b-%Y"),
        (r"(\d{1,2}\s+[A-Za-z]{3,9},?\s+\d{4})", "%d %B %Y"),
    ]:
        m = re.search(pattern, text)
        if not m:
            continue
        raw = re.sub(r"\s+", "-", m.group(1)).rstrip(",")
        for f in (fmt, "%d-%b-%y", "%d-%B-%Y"):
            try:
                return datetime.strptime(raw, f).date().isoformat()
            except ValueError:
                continue
    # Fallback: 1st of current month
    return date.today().replace(day=1).isoformat()


def _find_table_after(soup: BeautifulSoup, header_keywords: List[str]):
    """Find the first <table> that comes after a heading containing any keyword."""
    pattern = re.compile("|".join(header_keywords), re.I)
    for h in soup.find_all(["h1", "h2", "h3", "h4", "h5", "div", "p", "span"]):
        if pattern.search(h.get_text(strip=True)):
            t = h.find_next("table")
            if t:
                return t
    return None


def _parse_table(table) -> List[List[str]]:
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    return rows


def _extract_holdings(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    table = _find_table_after(soup, [
        "equity holding", "top holding", "portfolio holding",
        "stock holding", "holdings"
    ])
    if not table:
        return []

    rows = _parse_table(table)
    if len(rows) < 2:
        return []

    header = [c.lower() for c in rows[0]]
    holdings = []
    for r in rows[1:]:
        if len(r) < 2:
            continue
        # last cell with a numeric value is the % allocation
        pct = None
        for cell in reversed(r):
            v = _to_float(cell)
            if v is not None:
                pct = v
                break
        if pct is None or pct > 100 or pct < 0:
            continue
        stock_name = r[0].strip()
        if not stock_name or stock_name.lower() in ("total", "cash", "others", "net"):
            continue
        sector = None
        # Try to find a sector column heuristically
        if len(r) >= 3:
            # the cell that's not the stock name and not the % cell, prefer middle ones
            for idx, cell in enumerate(r[1:-1], start=1):
                cell_low = cell.lower()
                if not _to_float(cell) and 2 < len(cell) < 60 and not cell_low.startswith("rs"):
                    sector = cell
                    break
        holdings.append({
            "stock_name": stock_name,
            "sector": sector,
            "asset_type": "Equity",
            "percentage": pct,
        })
    return holdings


def _extract_sectors(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    table = _find_table_after(soup, [
        "sector allocation", "sector break", "sectoral", "industry allocation"
    ])
    if not table:
        return []

    rows = _parse_table(table)
    sectors = []
    for r in rows[1:] if len(rows) > 1 else rows:
        if len(r) < 2:
            continue
        pct = None
        for cell in reversed(r):
            v = _to_float(cell)
            if v is not None:
                pct = v
                break
        if pct is None or pct > 100 or pct < 0:
            continue
        sector_name = r[0].strip()
        if not sector_name or sector_name.lower() in ("total", "others"):
            continue
        sectors.append({"sector": sector_name, "percentage": pct})
    return sectors


def parse_portfolio(vr_code: str) -> Dict[str, Any]:
    soup = fetch_vr_portfolio_page(vr_code)
    if not soup:
        return {"holdings": [], "sectors": [], "factsheet_date": None}
    return {
        "factsheet_date": _extract_factsheet_date(soup),
        "holdings": _extract_holdings(soup),
        "sectors": _extract_sectors(soup),
    }


# =====================================================================
# Orchestrator — refresh one scheme end-to-end
# =====================================================================

def refresh_scheme(scheme_code: str) -> Dict[str, Any]:
    """
    Fetches: VR code (if missing), fund details, fund managers, factsheet
    (holdings + sectors). Triggers alerts via DB layer.
    """
    scheme_row = db.get_scheme(scheme_code)
    if not scheme_row:
        return {"error": f"Scheme {scheme_code} not in DB"}

    vr_code = scheme_row["vr_code"]
    if not vr_code:
        vr_code = find_vr_code(scheme_row["scheme_name"], scheme_row["isin_growth"])
        if not vr_code:
            return {"error": f"VRO code not found for '{scheme_row['scheme_name']}'"}

    details = parse_fund_details(vr_code)
    if not details:
        return {"error": "VRO fund page could not be loaded"}

    db.upsert_scheme({
        "scheme_code": scheme_code,
        "vr_code": vr_code,
        "isin_growth": scheme_row["isin_growth"],
        "isin_div": scheme_row["isin_div"],
        "scheme_name": details.get("scheme_name") or scheme_row["scheme_name"],
        "category": details.get("category"),
        "sub_category": details.get("sub_category"),
        "fund_house": details.get("fund_house") or scheme_row["fund_house"],
        "objective": details.get("objective"),
        "benchmark": details.get("benchmark"),
        "expense_ratio": details.get("expense_ratio"),
        "aum": details.get("aum"),
    })

    if details.get("fund_managers"):
        db.update_fund_managers(scheme_code, details["fund_managers"])

    portfolio = parse_portfolio(vr_code)
    factsheet_id = None
    if portfolio["holdings"] or portfolio["sectors"]:
        factsheet_id = db.save_factsheet(
            scheme_code, portfolio["factsheet_date"],
            portfolio["holdings"], portfolio["sectors"],
            source="valueresearch", raw=portfolio,
        )

    return {
        "scheme_code": scheme_code,
        "vr_code": vr_code,
        "category": details.get("category"),
        "sub_category": details.get("sub_category"),
        "fund_managers": details.get("fund_managers"),
        "factsheet_id": factsheet_id,
        "factsheet_date": portfolio["factsheet_date"],
        "holdings_count": len(portfolio["holdings"]),
        "sectors_count": len(portfolio["sectors"]),
    }

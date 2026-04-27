"""
Pull Google News headlines for each held scheme via the RSS endpoint.
No API key required, but Google rate-limits if you hammer it — we cache aggressively.
"""
from __future__ import annotations
import re
from datetime import datetime, date
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List, Dict
from urllib.parse import quote

import feedparser

from . import database as db

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_news_for_scheme(scheme_code: str, scheme_name: str,
                          fund_house: str = None, limit: int = 20) -> int:
    """
    Search Google News for the scheme. Returns count of new items inserted.
    The query strategy: scheme name OR (fund house + key fund characteristic)
    to maximize relevant hits.
    """
    queries = _build_queries(scheme_name, fund_house)
    items_to_save = []

    for q in queries:
        url = f"https://news.google.com/rss/search?q={quote(q)}&hl=en-IN&gl=IN&ceid=IN:en"
        feed = feedparser.parse(url)

        for entry in feed.entries[:limit]:
            published_at = None
            if hasattr(entry, "published"):
                try:
                    published_at = parsedate_to_datetime(entry.published).isoformat()
                except (TypeError, ValueError):
                    pass

            items_to_save.append({
                "scheme_code": scheme_code,
                "title": entry.get("title", "").strip(),
                "link": entry.get("link", "").strip(),
                "source": _extract_source(entry),
                "published_at": published_at,
                "summary": _clean_html(entry.get("summary", "")[:500]),
            })

    return db.save_news_items(items_to_save)


def fetch_news_for_all_held() -> Dict:
    """Fetch news for every held scheme."""
    held = db.list_held_schemes()
    total_inserted = 0
    failures = []
    for s in held:
        try:
            inserted = fetch_news_for_scheme(
                s["scheme_code"], s["scheme_name"], s["fund_house"]
            )
            total_inserted += inserted
        except Exception as e:
            failures.append({"scheme": s["scheme_name"], "error": str(e)})
    return {
        "schemes_processed": len(held),
        "items_inserted": total_inserted,
        "failures": failures,
    }


def _build_queries(scheme_name: str, fund_house: str = None) -> List[str]:
    """
    Build 1-2 targeted queries. Too many queries = rate limit problems.
    Strip 'Direct', 'Growth', 'Plan', etc. from scheme name to broaden hits.
    """
    cleaned = re.sub(
        r"\b(direct|growth|plan|regular|idcw|dividend|reinvestment|payout|option)\b",
        "",
        scheme_name,
        flags=re.I,
    ).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    queries = [f'"{cleaned}"']
    if fund_house and fund_house.lower() not in cleaned.lower():
        queries.append(f'"{fund_house}" mutual fund')
    return queries


def _extract_source(entry) -> str:
    if hasattr(entry, "source") and hasattr(entry.source, "title"):
        return entry.source.title
    if hasattr(entry, "title") and " - " in entry.title:
        return entry.title.rsplit(" - ", 1)[-1]
    return ""


def _clean_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s).strip()

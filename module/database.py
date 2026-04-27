"""
Database layer for the MF Tracker.
Uses SQLite for portability — works locally and on Streamlit Cloud.
For production, you can swap to Postgres by changing the connection string.
"""
import sqlite3
import json
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mf_tracker.db"


SCHEMA = """
CREATE TABLE IF NOT EXISTS schemes (
    scheme_code TEXT PRIMARY KEY,
    vr_code TEXT,
    isin_growth TEXT,
    isin_div TEXT,
    scheme_name TEXT NOT NULL,
    category TEXT,
    sub_category TEXT,
    fund_house TEXT,
    objective TEXT,
    benchmark TEXT,
    expense_ratio REAL,
    aum REAL,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fund_managers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_code TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    start_date DATE,
    end_date DATE,
    is_current INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (scheme_code) REFERENCES schemes(scheme_code)
);

CREATE TABLE IF NOT EXISTS factsheets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_code TEXT NOT NULL,
    factsheet_date DATE NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    raw_json TEXT,
    UNIQUE(scheme_code, factsheet_date),
    FOREIGN KEY (scheme_code) REFERENCES schemes(scheme_code)
);

CREATE TABLE IF NOT EXISTS holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    factsheet_id INTEGER NOT NULL,
    stock_name TEXT NOT NULL,
    sector TEXT,
    asset_type TEXT,
    percentage REAL,
    FOREIGN KEY (factsheet_id) REFERENCES factsheets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_holdings_factsheet ON holdings(factsheet_id);

CREATE TABLE IF NOT EXISTS sector_allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    factsheet_id INTEGER NOT NULL,
    sector TEXT NOT NULL,
    percentage REAL NOT NULL,
    FOREIGN KEY (factsheet_id) REFERENCES factsheets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sector_factsheet ON sector_allocations(factsheet_id);

CREATE TABLE IF NOT EXISTS performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_code TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    period TEXT NOT NULL,
    scheme_return REAL,
    category_avg REAL,
    benchmark_return REAL,
    UNIQUE(scheme_code, as_of_date, period)
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folio_no TEXT,
    scheme_code TEXT,
    scheme_name_raw TEXT,
    transaction_date DATE NOT NULL,
    transaction_type TEXT,
    amount REAL,
    units REAL,
    nav REAL,
    is_sip INTEGER DEFAULT 0,
    sip_id INTEGER,
    source_file TEXT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sip_id) REFERENCES sips(id)
);

CREATE INDEX IF NOT EXISTS idx_txn_folio_scheme ON transactions(folio_no, scheme_code);
CREATE INDEX IF NOT EXISTS idx_txn_date ON transactions(transaction_date);

CREATE TABLE IF NOT EXISTS sips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folio_no TEXT,
    scheme_code TEXT,
    scheme_name_raw TEXT,
    sip_amount REAL,
    sip_day INTEGER,
    start_date DATE,
    last_seen_date DATE,
    next_expected_date DATE,
    occurrences INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    confidence REAL DEFAULT 1.0,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT NOT NULL,
    scheme_code TEXT,
    title TEXT,
    description TEXT,
    old_value TEXT,
    new_value TEXT,
    severity TEXT DEFAULT 'info',
    is_read INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_unread ON alerts(is_read, created_at);

CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_code TEXT,
    title TEXT NOT NULL,
    link TEXT UNIQUE,
    source TEXT,
    published_at TIMESTAMP,
    summary TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def init_db():
    """Create the database file and run migrations."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


@contextmanager
def get_conn():
    """Context manager — gives a connection with row_factory + foreign keys on."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


# --------------------------- Schemes ---------------------------

def upsert_scheme(scheme: Dict[str, Any]):
    """Insert or update a scheme record. Logs alerts on category/objective/manager change."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM schemes WHERE scheme_code = ?", (scheme["scheme_code"],)
        ).fetchone()

        if existing:
            # Detect changes that warrant alerts
            for field, alert_type, label in [
                ("category", "category_change", "Category"),
                ("sub_category", "subcategory_change", "Sub-category"),
                ("objective", "objective_change", "Objective"),
            ]:
                old = existing[field]
                new = scheme.get(field)
                if old and new and old.strip() != new.strip():
                    _create_alert(
                        conn,
                        alert_type=alert_type,
                        scheme_code=scheme["scheme_code"],
                        title=f"{label} changed: {existing['scheme_name']}",
                        description=f"{label} changed from '{old}' to '{new}'",
                        old_value=old,
                        new_value=new,
                        severity="warning",
                    )

        conn.execute(
            """
            INSERT INTO schemes (scheme_code, vr_code, isin_growth, isin_div, scheme_name,
                                 category, sub_category, fund_house, objective, benchmark,
                                 expense_ratio, aum, last_updated)
            VALUES (:scheme_code, :vr_code, :isin_growth, :isin_div, :scheme_name,
                    :category, :sub_category, :fund_house, :objective, :benchmark,
                    :expense_ratio, :aum, :last_updated)
            ON CONFLICT(scheme_code) DO UPDATE SET
                vr_code = excluded.vr_code,
                scheme_name = excluded.scheme_name,
                category = excluded.category,
                sub_category = excluded.sub_category,
                fund_house = excluded.fund_house,
                objective = excluded.objective,
                benchmark = excluded.benchmark,
                expense_ratio = excluded.expense_ratio,
                aum = excluded.aum,
                last_updated = excluded.last_updated
            """,
            {
                "scheme_code": scheme["scheme_code"],
                "vr_code": scheme.get("vr_code"),
                "isin_growth": scheme.get("isin_growth"),
                "isin_div": scheme.get("isin_div"),
                "scheme_name": scheme.get("scheme_name"),
                "category": scheme.get("category"),
                "sub_category": scheme.get("sub_category"),
                "fund_house": scheme.get("fund_house"),
                "objective": scheme.get("objective"),
                "benchmark": scheme.get("benchmark"),
                "expense_ratio": scheme.get("expense_ratio"),
                "aum": scheme.get("aum"),
                "last_updated": datetime.utcnow().isoformat(),
            },
        )
        conn.commit()


def get_scheme(scheme_code: str) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM schemes WHERE scheme_code = ?", (scheme_code,)
        ).fetchone()


def list_held_schemes() -> List[sqlite3.Row]:
    """Schemes the user actually holds (have transactions)."""
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT s.* FROM schemes s
            WHERE s.scheme_code IN (
                SELECT DISTINCT scheme_code FROM transactions WHERE scheme_code IS NOT NULL
            )
            ORDER BY s.scheme_name
            """
        ).fetchall()


# --------------------------- Fund Managers ---------------------------

def update_fund_managers(scheme_code: str, current_managers: List[str]):
    """
    Compares current managers vs DB. If new ones added or old ones removed,
    creates an alert and updates the manager records.
    """
    with get_conn() as conn:
        existing_rows = conn.execute(
            "SELECT manager_name FROM fund_managers WHERE scheme_code = ? AND is_current = 1",
            (scheme_code,),
        ).fetchall()
        existing = {r["manager_name"] for r in existing_rows}
        current = set(current_managers)

        if not existing and current:
            # First time seeing this scheme — just record managers, no alert
            for m in current:
                conn.execute(
                    "INSERT INTO fund_managers (scheme_code, manager_name, start_date, is_current) VALUES (?, ?, ?, 1)",
                    (scheme_code, m, datetime.utcnow().date().isoformat()),
                )
            conn.commit()
            return

        added = current - existing
        removed = existing - current

        if added or removed:
            scheme = conn.execute(
                "SELECT scheme_name FROM schemes WHERE scheme_code = ?", (scheme_code,)
            ).fetchone()
            scheme_name = scheme["scheme_name"] if scheme else scheme_code

            desc_parts = []
            if added:
                desc_parts.append(f"Added: {', '.join(added)}")
            if removed:
                desc_parts.append(f"Removed: {', '.join(removed)}")

            _create_alert(
                conn,
                alert_type="manager_change",
                scheme_code=scheme_code,
                title=f"Fund manager change: {scheme_name}",
                description=" | ".join(desc_parts),
                old_value=", ".join(sorted(existing)),
                new_value=", ".join(sorted(current)),
                severity="warning",
            )

            today = datetime.utcnow().date().isoformat()
            for m in removed:
                conn.execute(
                    "UPDATE fund_managers SET is_current = 0, end_date = ? WHERE scheme_code = ? AND manager_name = ? AND is_current = 1",
                    (today, scheme_code, m),
                )
            for m in added:
                conn.execute(
                    "INSERT INTO fund_managers (scheme_code, manager_name, start_date, is_current) VALUES (?, ?, ?, 1)",
                    (scheme_code, m, today),
                )
            conn.commit()


# --------------------------- Factsheets ---------------------------

def save_factsheet(scheme_code: str, factsheet_date: str, holdings: List[Dict],
                   sectors: List[Dict], source: str = "valueresearch",
                   raw: Optional[Dict] = None) -> int:
    """Save a factsheet snapshot. Returns the factsheet_id."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO factsheets (scheme_code, factsheet_date, source, raw_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scheme_code, factsheet_date) DO UPDATE SET
                fetched_at = CURRENT_TIMESTAMP,
                raw_json = excluded.raw_json
            RETURNING id
            """,
            (scheme_code, factsheet_date, source, json.dumps(raw) if raw else None),
        )
        factsheet_id = cur.fetchone()["id"]

        # Replace holdings and sectors for this factsheet
        conn.execute("DELETE FROM holdings WHERE factsheet_id = ?", (factsheet_id,))
        conn.execute("DELETE FROM sector_allocations WHERE factsheet_id = ?", (factsheet_id,))

        for h in holdings:
            conn.execute(
                """
                INSERT INTO holdings (factsheet_id, stock_name, sector, asset_type, percentage)
                VALUES (?, ?, ?, ?, ?)
                """,
                (factsheet_id, h.get("stock_name"), h.get("sector"),
                 h.get("asset_type", "Equity"), h.get("percentage", 0)),
            )

        for s in sectors:
            conn.execute(
                "INSERT INTO sector_allocations (factsheet_id, sector, percentage) VALUES (?, ?, ?)",
                (factsheet_id, s.get("sector"), s.get("percentage", 0)),
            )

        conn.commit()
        return factsheet_id


def get_latest_two_factsheets(scheme_code: str) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM factsheets WHERE scheme_code = ?
            ORDER BY factsheet_date DESC LIMIT 2
            """,
            (scheme_code,),
        ).fetchall()


def get_holdings(factsheet_id: int) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM holdings WHERE factsheet_id = ? ORDER BY percentage DESC",
            (factsheet_id,),
        ).fetchall()


def get_sectors(factsheet_id: int) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM sector_allocations WHERE factsheet_id = ? ORDER BY percentage DESC",
            (factsheet_id,),
        ).fetchall()


# --------------------------- Transactions / SIPs ---------------------------

def insert_transactions(rows: List[Dict[str, Any]]) -> int:
    """Bulk insert transactions. Skips duplicates by (folio, scheme, date, amount, units)."""
    inserted = 0
    with get_conn() as conn:
        for r in rows:
            dup = conn.execute(
                """
                SELECT 1 FROM transactions
                WHERE folio_no = ? AND scheme_name_raw = ?
                  AND transaction_date = ? AND ABS(amount - ?) < 0.01
                  AND ABS(COALESCE(units, 0) - COALESCE(?, 0)) < 0.001
                """,
                (r.get("folio_no"), r.get("scheme_name_raw"), r["transaction_date"],
                 r["amount"], r.get("units")),
            ).fetchone()
            if dup:
                continue
            conn.execute(
                """
                INSERT INTO transactions (folio_no, scheme_code, scheme_name_raw, transaction_date,
                                          transaction_type, amount, units, nav, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (r.get("folio_no"), r.get("scheme_code"), r.get("scheme_name_raw"),
                 r["transaction_date"], r.get("transaction_type", "Purchase"),
                 r["amount"], r.get("units"), r.get("nav"), r.get("source_file")),
            )
            inserted += 1
        conn.commit()
    return inserted


def get_transactions_for_sip_detection() -> List[sqlite3.Row]:
    """Pull all purchase transactions, ordered, for SIP detection."""
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT id, folio_no, scheme_code, scheme_name_raw, transaction_date, amount, units
            FROM transactions
            WHERE transaction_type IN ('Purchase', 'SIP', 'Buy', 'P', NULL)
              AND amount > 0
            ORDER BY folio_no, scheme_name_raw, transaction_date
            """
        ).fetchall()


def upsert_sip(sip: Dict[str, Any]) -> int:
    """Insert or update a detected SIP. Returns sip_id."""
    with get_conn() as conn:
        existing = conn.execute(
            """
            SELECT id FROM sips
            WHERE folio_no = ? AND scheme_name_raw = ?
              AND ABS(sip_amount - ?) < 1.0 AND sip_day = ?
            """,
            (sip["folio_no"], sip["scheme_name_raw"], sip["sip_amount"], sip["sip_day"]),
        ).fetchone()

        if existing:
            sip_id = existing["id"]
            conn.execute(
                """
                UPDATE sips SET last_seen_date = ?, next_expected_date = ?,
                                occurrences = ?, status = ?, confidence = ?
                WHERE id = ?
                """,
                (sip["last_seen_date"], sip["next_expected_date"], sip["occurrences"],
                 sip["status"], sip["confidence"], sip_id),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO sips (folio_no, scheme_code, scheme_name_raw, sip_amount, sip_day,
                                  start_date, last_seen_date, next_expected_date, occurrences,
                                  status, confidence)
                VALUES (:folio_no, :scheme_code, :scheme_name_raw, :sip_amount, :sip_day,
                        :start_date, :last_seen_date, :next_expected_date, :occurrences,
                        :status, :confidence)
                RETURNING id
                """,
                sip,
            )
            sip_id = cur.fetchone()["id"]
        conn.commit()
        return sip_id


def link_transactions_to_sip(sip_id: int, txn_ids: List[int]):
    with get_conn() as conn:
        conn.executemany(
            "UPDATE transactions SET is_sip = 1, sip_id = ? WHERE id = ?",
            [(sip_id, tid) for tid in txn_ids],
        )
        conn.commit()


def list_active_sips() -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM sips WHERE status = 'active' ORDER BY scheme_name_raw"
        ).fetchall()


def list_all_sips() -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM sips ORDER BY status, scheme_name_raw"
        ).fetchall()


# --------------------------- Alerts ---------------------------

def _create_alert(conn, alert_type: str, scheme_code: Optional[str], title: str,
                  description: str, old_value: str = None, new_value: str = None,
                  severity: str = "info"):
    conn.execute(
        """
        INSERT INTO alerts (alert_type, scheme_code, title, description, old_value, new_value, severity)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (alert_type, scheme_code, title, description, old_value, new_value, severity),
    )


def list_alerts(unread_only: bool = False, limit: int = 200) -> List[sqlite3.Row]:
    with get_conn() as conn:
        q = "SELECT * FROM alerts"
        if unread_only:
            q += " WHERE is_read = 0"
        q += " ORDER BY created_at DESC LIMIT ?"
        return conn.execute(q, (limit,)).fetchall()


def mark_alert_read(alert_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET is_read = 1 WHERE id = ?", (alert_id,))
        conn.commit()


def mark_all_alerts_read():
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET is_read = 1")
        conn.commit()


# --------------------------- News ---------------------------

def save_news_items(items: List[Dict[str, Any]]) -> int:
    inserted = 0
    with get_conn() as conn:
        for item in items:
            try:
                conn.execute(
                    """
                    INSERT INTO news_items (scheme_code, title, link, source, published_at, summary)
                    VALUES (:scheme_code, :title, :link, :source, :published_at, :summary)
                    """,
                    item,
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass  # duplicate link
        conn.commit()
    return inserted


def list_news(scheme_code: Optional[str] = None, limit: int = 100) -> List[sqlite3.Row]:
    with get_conn() as conn:
        if scheme_code:
            return conn.execute(
                "SELECT * FROM news_items WHERE scheme_code = ? ORDER BY published_at DESC LIMIT ?",
                (scheme_code, limit),
            ).fetchall()
        return conn.execute(
            "SELECT * FROM news_items ORDER BY published_at DESC LIMIT ?", (limit,)
        ).fetchall()


# --------------------------- Performance ---------------------------

def save_performance(scheme_code: str, as_of_date: str, period: str,
                     scheme_return: float, category_avg: float, benchmark_return: float):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO performance (scheme_code, as_of_date, period, scheme_return, category_avg, benchmark_return)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(scheme_code, as_of_date, period) DO UPDATE SET
                scheme_return = excluded.scheme_return,
                category_avg = excluded.category_avg,
                benchmark_return = excluded.benchmark_return
            """,
            (scheme_code, as_of_date, period, scheme_return, category_avg, benchmark_return),
        )
        conn.commit()


def get_performance(scheme_code: str) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM performance WHERE scheme_code = ? ORDER BY as_of_date DESC, period",
            (scheme_code,),
        ).fetchall()

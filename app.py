"""
MF Portfolio Tracker — main dashboard.
"""
import streamlit as st
import pandas as pd

from modules import database as db

st.set_page_config(
    page_title="MF Portfolio Tracker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

db.init_db()

st.title("📊 Mutual Fund Portfolio Tracker")
st.caption("Monthly factsheet diffs · sector tracking · fund manager alerts · news.")

# -------- KPIs --------
held_schemes = db.list_held_schemes()
active_sips = db.list_active_sips()
unread_alerts = db.list_alerts(unread_only=True, limit=500)

with db.get_conn() as conn:
    txn_count = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
    orphan_count = conn.execute(
        """
        SELECT COUNT(DISTINCT scheme_name_raw) AS c FROM transactions
        WHERE scheme_code IS NULL OR scheme_code = '' OR scheme_code = 'None'
        """
    ).fetchone()["c"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Schemes Held", len(held_schemes))
c2.metric("Active SIPs", len(active_sips))
c3.metric("Unread Alerts", len(unread_alerts),
          delta="!" if unread_alerts else None,
          delta_color="inverse" if unread_alerts else "off")
c4.metric("Transactions", txn_count)

st.divider()

# -------- Orphan-transactions warning (the bug from the screenshots) --------
if orphan_count > 0 and txn_count > 0:
    st.error(
        f"⚠️ **{orphan_count} schemes have transactions but no AMFI scheme code.** "
        f"This is why other pages show 'no portfolio'. "
        f"Go to **Import Portfolio → Fix existing data** and click Reconcile."
    )

# -------- Empty state --------
if not held_schemes and txn_count == 0:
    st.info(
        "👋 **Welcome!** Get started:\n\n"
        "1. **Import Portfolio** — upload CAS PDF or CSV (we auto-match scheme names to AMFI)\n"
        "2. **Refresh Data** — fetch VRO codes, factsheets, holdings, sectors, news in one click\n"
        "3. Explore **Holdings Changes**, **Sector Analysis**, **Performance**, **News**, **Alerts**"
    )
    st.stop()

if not held_schemes and txn_count > 0:
    # User has transactions but they're all unmatched
    st.warning(
        "Transactions are imported but no scheme records exist yet. "
        "Go to **Import Portfolio → Fix existing data** and click Reconcile."
    )
    st.stop()

# -------- Portfolio table --------
st.subheader("Your Portfolio")

portfolio_rows = []
with db.get_conn() as conn:
    for s in held_schemes:
        invested = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN transaction_type IN ('Purchase','SIP','Switch-In')
                                  THEN amount ELSE 0 END), 0) -
                COALESCE(SUM(CASE WHEN transaction_type = 'Redemption'
                                  THEN amount ELSE 0 END), 0) AS net
            FROM transactions WHERE scheme_code = ?
            """, (s["scheme_code"],)
        ).fetchone()["net"]

        units = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN transaction_type IN ('Purchase','SIP','Switch-In')
                                  THEN units ELSE 0 END), 0) -
                COALESCE(SUM(CASE WHEN transaction_type = 'Redemption'
                                  THEN units ELSE 0 END), 0) AS u
            FROM transactions WHERE scheme_code = ?
            """, (s["scheme_code"],)
        ).fetchone()["u"]

        # latest NAV from factsheet, fallback to last transaction NAV
        latest_nav = conn.execute(
            "SELECT nav FROM transactions WHERE scheme_code = ? AND nav IS NOT NULL "
            "ORDER BY transaction_date DESC LIMIT 1",
            (s["scheme_code"],)
        ).fetchone()
        nav_val = latest_nav["nav"] if latest_nav else None

        current_value = (units * nav_val) if (nav_val and units) else None

        portfolio_rows.append({
            "Scheme": s["scheme_name"],
            "Category": s["sub_category"] or s["category"] or "—",
            "AMC": s["fund_house"] or "—",
            "Units": round(units, 3) if units else 0,
            "Invested (₹)": round(invested, 2),
            "Current Value (₹)": round(current_value, 2) if current_value else None,
            "VRO Code": s["vr_code"] or "—",
        })

df = pd.DataFrame(portfolio_rows)
total_invested = df["Invested (₹)"].sum()
total_value = df["Current Value (₹)"].sum() if df["Current Value (₹)"].notna().any() else None

c1, c2, c3 = st.columns(3)
c1.metric("Total Invested", f"₹{total_invested:,.0f}")
if total_value:
    c2.metric("Current Value", f"₹{total_value:,.0f}",
              delta=f"₹{total_value - total_invested:,.0f}")
    c3.metric("Returns", f"{((total_value/total_invested - 1) * 100):.2f}%")

st.dataframe(df, use_container_width=True, hide_index=True)

# Show schemes that haven't been refreshed yet
unrefreshed = [s for s in held_schemes if not s["vr_code"]]
if unrefreshed:
    st.warning(
        f"📡 **{len(unrefreshed)} schemes haven't been refreshed yet.** "
        f"Their VRO code, category, holdings, and sectors are not loaded. "
        f"Go to **Refresh Data** and click ⚡ Fetch EVERYTHING."
    )

if unread_alerts:
    st.subheader(f"🔔 Recent Alerts ({len(unread_alerts)} unread)")
    for a in unread_alerts[:5]:
        emoji = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(a["severity"], "•")
        st.warning(f"{emoji} **{a['title']}** — {a['description']}")
    if len(unread_alerts) > 5:
        st.caption(f"+ {len(unread_alerts) - 5} more on the Alerts page")

st.divider()
st.caption(
    "Sources: AMFI · mfapi.in · Value Research · Google News RSS. "
    "Factsheets typically published 7-10 days after month-end."
)

import streamlit as st
import pandas as pd

from modules import database as db
from modules import performance_tracker

st.set_page_config(page_title="Performance", page_icon="⚡", layout="wide")
st.title("⚡ Scheme Performance vs Category")

held = db.list_held_schemes()
if not held:
    st.warning("No portfolio imported yet.")
    st.stop()

chosen = st.selectbox(
    "Scheme",
    options=[s["scheme_code"] for s in held],
    format_func=lambda c: next(s["scheme_name"] for s in held if s["scheme_code"] == c),
)

if st.button("Compute Returns", type="primary"):
    with st.spinner("Fetching NAVs and computing..."):
        result = performance_tracker.compute_for_scheme(chosen)

    if "error" in result:
        st.error(result["error"])
        st.stop()

    st.caption(f"As of {result['as_of']} · Sub-category: {result['sub_category'] or '—'}")

    rows = []
    for period in ["1M", "3M", "6M", "1Y", "3Y", "5Y"]:
        scheme_ret = result["scheme_returns"].get(period)
        cat_ret = result["category_average"].get(period)
        if scheme_ret is None:
            continue
        diff = (scheme_ret - cat_ret) if cat_ret is not None else None
        rows.append({
            "Period": period,
            "Scheme (%)": scheme_ret,
            "Category Avg (%)": cat_ret,
            "Excess Return (%)": diff,
        })

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Scheme (%)": st.column_config.NumberColumn(format="%.2f"),
                "Category Avg (%)": st.column_config.NumberColumn(format="%.2f"),
                "Excess Return (%)": st.column_config.NumberColumn(format="%+.2f"),
            },
        )
        if not result["category_average"]:
            st.info(
                "💡 Category averages need NAV data for peer schemes. "
                "Run `python scripts/seed_category_peers.py` from your terminal "
                "to bulk-fetch NAVs for all peers in your held categories."
            )
    else:
        st.info("Not enough NAV history to compute returns yet.")

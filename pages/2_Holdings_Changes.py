import streamlit as st
import pandas as pd

from modules import database as db
from modules import factsheet_comparator

st.set_page_config(page_title="Holdings Changes", page_icon="📈", layout="wide")
st.title("📈 Holdings Changes — Month over Month")

st.caption(
    "Compares the latest factsheet vs the previous one for each scheme you hold. "
    "Make sure you've run **Refresh Data** at least twice (one month apart) for "
    "comparisons to be available."
)

held = db.list_held_schemes()
if not held:
    st.warning("No portfolio imported yet. Go to **Import Portfolio** first.")
    st.stop()

scheme_names = {s["scheme_code"]: s["scheme_name"] for s in held}
chosen = st.selectbox(
    "Pick a scheme",
    options=list(scheme_names.keys()),
    format_func=lambda code: scheme_names[code],
)

if not chosen:
    st.stop()

result = factsheet_comparator.compare_factsheets(chosen)

if "error" in result:
    st.error(result["error"])
    st.stop()

if result.get("message"):
    st.info(result["message"])
    st.caption(f"Current factsheet date: {result['current_date']}")
    st.stop()

st.success(
    f"Comparing **{result['current_date']}** (current) vs **{result['previous_date']}** (previous)"
)

s = result["summary"]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Stocks Added", s["stocks_added"])
c2.metric("Stocks Exited", s["stocks_exited"])
c3.metric("Holdings Rebalanced", s["rebalanced_holdings"])
c4.metric("Sectors Changed", s["sectors_changed"])

st.divider()

tab_buys, tab_exits, tab_rebal = st.tabs(["🟢 New Buys", "🔴 Exited", "🔁 Rebalanced"])

with tab_buys:
    if result["new_buys"]:
        df = pd.DataFrame(result["new_buys"])
        df.columns = ["Stock", "Sector", "% of Portfolio"]
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No new positions added this period.")

with tab_exits:
    if result["exited"]:
        df = pd.DataFrame(result["exited"])
        df.columns = ["Stock", "Sector", "Was % of Portfolio"]
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No positions exited this period.")

with tab_rebal:
    if result["weight_changes"]:
        df = pd.DataFrame(result["weight_changes"])
        df.columns = ["Stock", "Sector", "Old %", "New %", "Change"]
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Change": st.column_config.NumberColumn(format="%+.2f"),
            },
        )
    else:
        st.info("No meaningful weight changes (>0.05%).")

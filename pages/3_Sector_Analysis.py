import streamlit as st
import pandas as pd
import altair as alt

from modules import database as db
from modules import factsheet_comparator

st.set_page_config(page_title="Sector Analysis", page_icon="🏢", layout="wide")
st.title("🏢 Sector Allocation — Month over Month")

held = db.list_held_schemes()
if not held:
    st.warning("No portfolio imported yet.")
    st.stop()

scheme_names = {s["scheme_code"]: s["scheme_name"] for s in held}
chosen = st.selectbox(
    "Pick a scheme",
    options=list(scheme_names.keys()),
    format_func=lambda code: scheme_names[code],
)

result = factsheet_comparator.compare_factsheets(chosen)
if "error" in result or result.get("message"):
    st.info(result.get("message") or result.get("error"))
    st.stop()

st.caption(
    f"Comparing **{result['current_date']}** vs **{result['previous_date']}**"
)

if not result["sector_changes"]:
    st.info("No sector weight changes detected this period.")
    st.stop()

df = pd.DataFrame(result["sector_changes"])

# Bar chart of changes
chart = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("change:Q", title="Change in Allocation (%)"),
        y=alt.Y("sector:N", sort="-x", title="Sector"),
        color=alt.condition(
            alt.datum.change > 0,
            alt.value("#22c55e"),  # green for increase
            alt.value("#ef4444"),  # red for decrease
        ),
        tooltip=["sector", "old_percentage", "new_percentage", "change"],
    )
    .properties(height=max(300, 28 * len(df)))
)
st.altair_chart(chart, use_container_width=True)

st.dataframe(
    df.rename(columns={
        "sector": "Sector",
        "old_percentage": "Previous %",
        "new_percentage": "Current %",
        "change": "Change",
    }),
    use_container_width=True,
    hide_index=True,
    column_config={
        "Change": st.column_config.NumberColumn(format="%+.2f"),
    },
)

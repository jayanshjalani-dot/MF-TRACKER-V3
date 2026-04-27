import streamlit as st
import pandas as pd

from modules import database as db

st.set_page_config(page_title="Alerts", page_icon="🔔", layout="wide")
st.title("🔔 Alerts")

st.caption(
    "Auto-generated when a held scheme's fund manager, category, sub-category, "
    "or objective changes between data refreshes."
)

show_unread = st.toggle("Show unread only", value=True)

alerts = db.list_alerts(unread_only=show_unread, limit=500)

c1, c2 = st.columns([1, 4])
if c1.button("Mark all read"):
    db.mark_all_alerts_read()
    st.rerun()

if not alerts:
    st.success("✅ No unread alerts.")
    st.stop()

# Group by alert_type for cleaner display
by_type = {}
for a in alerts:
    by_type.setdefault(a["alert_type"], []).append(a)

type_labels = {
    "manager_change": "👤 Fund Manager Changes",
    "category_change": "📂 Category Changes",
    "subcategory_change": "📁 Sub-category Changes",
    "objective_change": "🎯 Objective Changes",
}

for atype, items in by_type.items():
    st.subheader(type_labels.get(atype, atype))
    for a in items:
        with st.container(border=True):
            cols = st.columns([6, 1])
            with cols[0]:
                st.markdown(f"**{a['title']}**")
                st.write(a["description"])
                if a["old_value"] or a["new_value"]:
                    st.caption(f"Was: `{a['old_value']}` → Now: `{a['new_value']}`")
                st.caption(f"_{a['created_at']}_")
            with cols[1]:
                if not a["is_read"]:
                    if st.button("✓", key=f"read_{a['id']}", help="Mark as read"):
                        db.mark_alert_read(a["id"])
                        st.rerun()

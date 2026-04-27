import streamlit as st
import pandas as pd

from modules import database as db
from modules import sip_detector

st.set_page_config(page_title="SIPs", page_icon="🔁", layout="wide")
st.title("🔁 SIPs Detected")

c1, c2 = st.columns([1, 4])
if c1.button("Re-run detection", type="primary"):
    with st.spinner("Analysing transactions..."):
        result = sip_detector.detect_sips()
    st.success(f"Found {result['sips_found']} SIPs · marked {result['transactions_marked']} txns")

show_all = st.toggle("Include stopped SIPs", value=False)

sips = db.list_all_sips() if show_all else db.list_active_sips()

if not sips:
    st.info("No SIPs detected. Re-run detection or import more transactions.")
    st.stop()

rows = []
for s in sips:
    rows.append({
        "Status": s["status"],
        "Folio": s["folio_no"],
        "Scheme": s["scheme_name_raw"],
        "SIP Amount (₹)": s["sip_amount"],
        "SIP Day": s["sip_day"],
        "Started": s["start_date"],
        "Last Instalment": s["last_seen_date"],
        "Next Expected": s["next_expected_date"],
        "Occurrences": s["occurrences"],
        "Confidence": s["confidence"],
    })

df = pd.DataFrame(rows)
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "SIP Amount (₹)": st.column_config.NumberColumn(format="₹%.2f"),
        "Confidence": st.column_config.ProgressColumn(min_value=0, max_value=1.0, format="%.0f%%"),
    },
)

st.caption(
    "💡 **About detection:** A series qualifies as a SIP if there are ≥3 transactions "
    "of similar amount (±5%) at roughly monthly intervals (25-35 days), with a stable "
    "day-of-month (±3 days for weekend/holiday drift). "
    "If a SIP you know exists isn't here, use the **SIP Diagnostics** tab on the Import page."
)

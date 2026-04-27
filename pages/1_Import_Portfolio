import streamlit as st
import pandas as pd
import io

from modules import database as db
from modules import portfolio_importer
from modules import sip_detector
from modules import vr_scraper
from modules import news_aggregator

st.set_page_config(page_title="Import Portfolio", page_icon="📥", layout="wide")
st.title("📥 Import Portfolio")

tab_pdf, tab_csv, tab_reconcile, tab_diag = st.tabs(
    ["CAS PDF", "CSV / Excel", "🔧 Fix existing data", "🔍 SIP Diagnostics"]
)

# =====================================================================
# CAS PDF
# =====================================================================
with tab_pdf:
    st.markdown(
        "Upload your **CAS** from CAMS or KFintech.\n\n"
        "- CAMS: https://www.camsonline.com → Investor Services → Mailback → CAS\n"
        "- KFintech: https://mfs.kfintech.com → Investor Services → CAS\n\n"
        "Pick the **detailed** statement, set a password."
    )
    pdf_file = st.file_uploader("CAS PDF", type=["pdf"])
    password = st.text_input("PDF Password", type="password")

    if st.button("Import PDF", type="primary", disabled=not (pdf_file and password)):
        with st.spinner("Parsing..."):
            try:
                result = portfolio_importer.import_cas_pdf(pdf_file.read(), password)
                st.success(
                    f"✅ Imported {result['transactions_inserted']} transactions / "
                    f"{result['schemes_found']} schemes "
                    f"({result['duplicates_skipped']} duplicates skipped)"
                )
                with st.spinner("Detecting SIPs..."):
                    sip_result = sip_detector.detect_sips()
                st.info(f"🔁 Found {sip_result['sips_found']} SIPs")
                st.session_state["just_imported"] = True
            except Exception as e:
                st.error(f"Import failed: {e}")


# =====================================================================
# CSV / Excel
# =====================================================================
with tab_csv:
    st.markdown(
        "Upload your transaction CSV or Excel. **You don't need an AMFI scheme code column** — "
        "we'll match each scheme name to AMFI's master file automatically."
    )
    csv_file = st.file_uploader("CSV / Excel", type=["csv", "xls", "xlsx"], key="csv_up")

    if csv_file:
        raw = csv_file.read()
        try:
            df = pd.read_csv(io.BytesIO(raw))
        except Exception:
            df = pd.read_excel(io.BytesIO(raw))

        st.write("**Preview:**", df.head(5))
        cols = ["—"] + list(df.columns)

        with st.form("col_map"):
            st.markdown("**Required**")
            c1, c2, c3 = st.columns(3)
            map_date = c1.selectbox("Transaction date *", cols, index=0)
            map_scheme = c2.selectbox("Scheme name *", cols, index=0)
            map_amount = c3.selectbox("Amount *", cols, index=0)

            st.markdown("**Optional** (improves SIP detection accuracy)")
            c4, c5, c6 = st.columns(3)
            map_folio = c4.selectbox("Folio number", cols, index=0)
            map_units = c5.selectbox("Units", cols, index=0)
            map_nav = c6.selectbox("NAV", cols, index=0)
            c7, c8 = st.columns(2)
            map_type = c7.selectbox("Transaction type", cols, index=0)
            map_code = c8.selectbox("AMFI scheme code (skip — we auto-match)", cols, index=0)

            submitted = st.form_submit_button("Import & Match Schemes", type="primary")

        if submitted:
            if "—" in (map_date, map_scheme, map_amount):
                st.error("Date, scheme name, and amount are required")
            else:
                column_map = {
                    "transaction_date": map_date,
                    "scheme_name": map_scheme,
                    "amount": map_amount,
                }
                if map_folio != "—": column_map["folio_no"] = map_folio
                if map_units != "—": column_map["units"] = map_units
                if map_nav != "—": column_map["nav"] = map_nav
                if map_type != "—": column_map["transaction_type"] = map_type
                if map_code != "—": column_map["scheme_code"] = map_code

                with st.spinner("Importing and matching schemes against AMFI..."):
                    try:
                        result = portfolio_importer.import_csv(raw, column_map)
                    except Exception as e:
                        st.error(f"Import failed: {e}")
                        st.stop()

                st.success(
                    f"✅ {result['transactions_inserted']} transactions imported · "
                    f"{result['duplicates_skipped']} duplicates skipped"
                )

                if result["match_details"]:
                    st.subheader(f"✓ Matched {result['schemes_matched']} schemes")
                    match_rows = []
                    for raw_name, m in result["match_details"].items():
                        match_rows.append({
                            "Your name": raw_name,
                            "Matched to (AMFI)": m["scheme_name"],
                            "Code": m["scheme_code"],
                            "Confidence": m["similarity"],
                        })
                    match_df = pd.DataFrame(match_rows)
                    st.dataframe(
                        match_df, use_container_width=True, hide_index=True,
                        column_config={
                            "Confidence": st.column_config.ProgressColumn(
                                min_value=0, max_value=1.0, format="%.0f%%"
                            ),
                        },
                    )

                if result["unmatched_names"]:
                    st.warning(
                        f"⚠️ Could not match: {', '.join(result['unmatched_names'])}\n\n"
                        "These schemes won't appear in the dashboard. Check spelling, or "
                        "they may be too new for AMFI master."
                    )

                with st.spinner("Detecting SIPs..."):
                    sip_result = sip_detector.detect_sips()
                st.info(f"🔁 Found {sip_result['sips_found']} SIPs")

                st.session_state["just_imported"] = True
                st.rerun()


# =====================================================================
# Reconcile — fix already-imported broken data
# =====================================================================
with tab_reconcile:
    st.markdown(
        "If you imported a CSV before this fix and your dashboard shows **No portfolio**, "
        "click below. This finds all transactions with missing scheme codes and matches them "
        "to AMFI's master."
    )

    with db.get_conn() as conn:
        unmatched = conn.execute(
            """
            SELECT scheme_name_raw, COUNT(*) AS n
            FROM transactions
            WHERE scheme_code IS NULL OR scheme_code = '' OR scheme_code = 'None'
            GROUP BY scheme_name_raw
            ORDER BY n DESC
            """
        ).fetchall()

    if unmatched:
        st.warning(f"Found {len(unmatched)} unique scheme names with missing codes:")
        for row in unmatched:
            st.write(f"• {row['scheme_name_raw']} ({row['n']} transactions)")

        if st.button("🔧 Reconcile now", type="primary"):
            with st.spinner("Matching against AMFI master..."):
                result = portfolio_importer.reconcile_unmatched()

            st.success(f"✅ Matched {result['matched']} schemes")

            if result["match_details"]:
                rows = []
                for raw_name, m in result["match_details"].items():
                    rows.append({
                        "Your name": raw_name,
                        "Matched to": m["scheme_name"],
                        "Code": m["scheme_code"],
                        "Confidence": m["similarity"],
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            if result["still_unmatched"]:
                st.error(
                    "Still unmatched (couldn't auto-resolve): "
                    + ", ".join(result["still_unmatched"])
                )

            st.info("Now go to **🔄 Refresh Data** to fetch factsheets, holdings, and sectors.")
            st.session_state["just_reconciled"] = True
    else:
        st.success("✓ All transactions have scheme codes — nothing to reconcile.")


# =====================================================================
# After-import quick action: full data refresh
# =====================================================================
if st.session_state.get("just_imported") or st.session_state.get("just_reconciled"):
    held = db.list_held_schemes()
    if held:
        st.divider()
        st.subheader("🚀 Next step: fetch fund details for all schemes")
        st.caption(
            f"You hold {len(held)} schemes. Click below to fetch VRO codes, "
            f"factsheets, holdings, sector allocations, and news in one go. "
            f"Estimated: ~{len(held) * 8} seconds (rate-limited at 3s/request)."
        )
        if st.button("⚡ Fetch all fund data now", type="primary", use_container_width=True):
            progress = st.progress(0)
            status = st.empty()
            log_box = st.container()

            for i, s in enumerate(held):
                status.write(f"Fetching **{s['scheme_name']}**...")
                try:
                    r = vr_scraper.refresh_scheme(s["scheme_code"])
                    with log_box:
                        if "error" in r:
                            st.warning(f"⚠️ {s['scheme_name']}: {r['error']}")
                        else:
                            st.success(
                                f"✓ {s['scheme_name']}: "
                                f"{r['holdings_count']} holdings, "
                                f"{r['sectors_count']} sectors"
                            )
                except Exception as e:
                    with log_box:
                        st.error(f"✗ {s['scheme_name']}: {e}")
                progress.progress((i + 1) / len(held))

            status.write("Fetching news...")
            try:
                news_result = news_aggregator.fetch_news_for_all_held()
                with log_box:
                    st.success(f"📰 {news_result['items_inserted']} news articles fetched")
            except Exception as e:
                with log_box:
                    st.warning(f"News fetch issue: {e}")

            status.write("✅ Done!")
            st.session_state["just_imported"] = False
            st.session_state["just_reconciled"] = False


# =====================================================================
# SIP diagnostics
# =====================================================================
with tab_diag:
    st.markdown(
        "If a SIP wasn't detected, inspect the clustering here to see why."
    )
    with db.get_conn() as conn:
        groups = conn.execute(
            """
            SELECT folio_no, scheme_name_raw, COUNT(*) AS n,
                   MIN(transaction_date) AS first_date, MAX(transaction_date) AS last_date
            FROM transactions
            WHERE amount > 0
            GROUP BY folio_no, scheme_name_raw
            HAVING n >= 2
            ORDER BY n DESC
            """
        ).fetchall()

    if not groups:
        st.info("Import some transactions first")
    else:
        choice = st.selectbox(
            "Folio + scheme to diagnose",
            options=range(len(groups)),
            format_func=lambda i: f"{groups[i]['folio_no']} | {groups[i]['scheme_name_raw']} ({groups[i]['n']} txns)"
        )
        if st.button("Diagnose"):
            g = groups[choice]
            st.json(sip_detector.explain_grouping(g["folio_no"], g["scheme_name_raw"]))

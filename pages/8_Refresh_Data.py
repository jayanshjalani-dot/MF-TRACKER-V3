"""
One-click full refresh — fetches everything for every held scheme:
  1. Find Value Research code (cached)
  2. Pull category, sub-category, fund manager, objective, benchmark, expense ratio, AUM
  3. Pull latest factsheet → holdings + sector allocation
  4. Compute performance vs category
  5. Fetch Google News
  6. Auto-generate alerts on any change

This is what makes Holdings Changes, Sector Analysis, News, etc. show real data.
"""
import time
import streamlit as st

from modules import database as db
from modules import vr_scraper
from modules import news_aggregator
from modules import performance_tracker
from modules import sip_detector

st.set_page_config(page_title="Refresh Data", page_icon="🔄", layout="wide")
st.title("🔄 Refresh All Fund Data")

held = db.list_held_schemes()

if not held:
    st.warning(
        "No portfolio yet. Go to **Import Portfolio** first.\n\n"
        "If you imported a CSV but the dashboard is empty, open the "
        "**Fix existing data** tab on the Import page and click Reconcile."
    )
    st.stop()

# Show what we're about to refresh
st.caption(f"You hold **{len(held)} schemes**. Estimated time: **{len(held) * 8} seconds**.")

with st.expander("Schemes to refresh", expanded=False):
    for s in held:
        st.write(f"• {s['scheme_name']} (`{s['scheme_code']}`)")

st.divider()

col_a, col_b = st.columns(2)

# =====================================================================
# Full refresh
# =====================================================================
with col_a:
    full_refresh = st.button(
        "⚡ Fetch EVERYTHING (recommended)",
        type="primary", use_container_width=True,
        help="VRO codes, factsheets, holdings, sectors, performance, news — all in one go"
    )

# =====================================================================
# Selective options
# =====================================================================
with col_b:
    with st.popover("Fetch only specific items", use_container_width=True):
        opt_factsheet = st.checkbox("Factsheets (holdings + sectors)", value=True)
        opt_perf = st.checkbox("Performance (returns vs category)", value=True)
        opt_news = st.checkbox("Google News", value=True)
        partial_refresh = st.button("Run selective refresh", type="secondary")

# =====================================================================
# Execute
# =====================================================================
if full_refresh or 'partial_refresh' in locals() and partial_refresh:
    if full_refresh:
        opt_factsheet = opt_perf = opt_news = True

    progress = st.progress(0, text="Starting...")
    log = st.container(height=400)

    total_steps = len(held) * (
        (1 if opt_factsheet else 0) + (1 if opt_perf else 0)
    ) + (1 if opt_news else 0)
    step = 0

    successful_schemes = 0
    failed_schemes = []

    # ----- 1. Factsheets per scheme -----
    if opt_factsheet:
        for i, s in enumerate(held):
            step += 1
            progress.progress(step / total_steps,
                              text=f"[{i+1}/{len(held)}] Factsheet: {s['scheme_name']}")
            try:
                r = vr_scraper.refresh_scheme(s["scheme_code"])
                with log:
                    if "error" in r:
                        st.warning(f"⚠️ **{s['scheme_name']}** — {r['error']}")
                        failed_schemes.append(s['scheme_name'])
                    else:
                        msg = (
                            f"✓ **{s['scheme_name']}** — "
                            f"{r.get('holdings_count', 0)} holdings, "
                            f"{r.get('sectors_count', 0)} sectors"
                        )
                        if r.get('factsheet_date'):
                            msg += f" · factsheet: {r['factsheet_date']}"
                        st.success(msg)
                        successful_schemes += 1
            except Exception as e:
                with log:
                    st.error(f"✗ **{s['scheme_name']}** — {type(e).__name__}: {e}")
                failed_schemes.append(s['scheme_name'])

    # ----- 2. Performance per scheme -----
    if opt_perf:
        for i, s in enumerate(held):
            step += 1
            progress.progress(step / total_steps,
                              text=f"[{i+1}/{len(held)}] Performance: {s['scheme_name']}")
            try:
                r = performance_tracker.compute_for_scheme(s["scheme_code"])
                with log:
                    if "error" in r:
                        st.caption(f"⚠️ Perf {s['scheme_name']}: {r['error']}")
                    else:
                        rets = r.get('scheme_returns', {})
                        if rets:
                            st.caption(
                                f"📊 {s['scheme_name']}: "
                                f"1Y={rets.get('1Y','—')}% · "
                                f"3Y={rets.get('3Y','—')}% · "
                                f"5Y={rets.get('5Y','—')}%"
                            )
            except Exception as e:
                with log:
                    st.caption(f"⚠️ Perf {s['scheme_name']}: {e}")

    # ----- 3. News (single batch call) -----
    if opt_news:
        step += 1
        progress.progress(step / total_steps, text="Fetching news from Google...")
        try:
            r = news_aggregator.fetch_news_for_all_held()
            with log:
                st.info(
                    f"📰 Fetched **{r['items_inserted']}** new articles "
                    f"({len(r.get('failures', []))} fetch failures)"
                )
        except Exception as e:
            with log:
                st.warning(f"News fetch issue: {e}")

    # ----- 4. Re-detect SIPs (in case of new transactions) -----
    sip_result = sip_detector.detect_sips()

    # ----- Summary -----
    progress.progress(1.0, text="✅ Done!")
    st.success(
        f"### ✅ Refresh complete\n"
        f"- {successful_schemes}/{len(held)} schemes refreshed successfully\n"
        f"- {sip_result['sips_found']} SIPs detected\n"
        f"- View **Holdings Changes**, **Sector Analysis**, **News**, "
        f"**Performance** in the sidebar"
    )

    if failed_schemes:
        with st.expander(f"⚠️ {len(failed_schemes)} schemes had issues"):
            for name in failed_schemes:
                st.write(f"• {name}")
            st.caption(
                "Common causes: scheme is too new (no VRO listing yet), "
                "VRO selector changed (HTML structure update), or rate-limit hit. "
                "Re-run after a few minutes."
            )

    if st.button("🔔 Check Alerts page"):
        st.switch_page("pages/6_Alerts.py")

st.divider()

# =====================================================================
# Single-scheme refresh
# =====================================================================
st.subheader("Refresh just one scheme")
scheme_codes = {s["scheme_code"]: s["scheme_name"] for s in held}
chosen = st.selectbox(
    "Pick a scheme",
    options=list(scheme_codes.keys()),
    format_func=lambda c: scheme_codes[c],
)
if st.button("Refresh this one"):
    with st.spinner("Working..."):
        result = vr_scraper.refresh_scheme(chosen)
        perf = performance_tracker.compute_for_scheme(chosen)
    st.json({"refresh": result, "performance": perf})

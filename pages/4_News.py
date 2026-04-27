import streamlit as st
from datetime import datetime

from modules import database as db
from modules import news_aggregator

st.set_page_config(page_title="News", page_icon="📰", layout="wide")
st.title("📰 Fund News")

held = db.list_held_schemes()
if not held:
    st.warning("No portfolio imported yet.")
    st.stop()

with st.sidebar:
    if st.button("🔄 Fetch latest news for all held schemes", use_container_width=True):
        with st.spinner("Fetching from Google News..."):
            result = news_aggregator.fetch_news_for_all_held()
            st.success(
                f"Pulled **{result['items_inserted']}** new articles "
                f"across {result['schemes_processed']} schemes"
            )
            if result["failures"]:
                st.warning(f"{len(result['failures'])} fetches failed")

scheme_filter = st.selectbox(
    "Filter by scheme",
    options=["All"] + [s["scheme_code"] for s in held],
    format_func=lambda code: "All schemes" if code == "All" else next(
        (s["scheme_name"] for s in held if s["scheme_code"] == code), code
    ),
)

news = db.list_news(
    scheme_code=None if scheme_filter == "All" else scheme_filter,
    limit=200,
)

if not news:
    st.info("No news yet. Click **Fetch latest news** in the sidebar.")
    st.stop()

scheme_lookup = {s["scheme_code"]: s["scheme_name"] for s in held}

for item in news:
    scheme_name = scheme_lookup.get(item["scheme_code"], "Unknown scheme")
    pub = item["published_at"][:16].replace("T", " ") if item["published_at"] else ""
    with st.container(border=True):
        st.markdown(f"**[{item['title']}]({item['link']})**")
        meta_parts = [item["source"], pub, scheme_name]
        st.caption(" · ".join([p for p in meta_parts if p]))
        if item["summary"]:
            st.write(item["summary"])

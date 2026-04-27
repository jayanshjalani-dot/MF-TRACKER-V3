"""
Monthly auto-refresh script — invoked by GitHub Actions on the 8th of each month
(after factsheets are typically published).

Runs:
  1. Refreshes fund details + factsheets from VR for all held schemes
  2. Fetches Google News for all held schemes
  3. Re-runs SIP detection (in case there are new transactions in DB)
  4. Computes performance numbers

Commits the updated SQLite DB back to the repo so the deployed Streamlit app
sees fresh data on next page load.
"""
import sys
import time
from pathlib import Path

# Make modules importable when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules import database as db
from modules import vr_scraper
from modules import news_aggregator
from modules import sip_detector
from modules import performance_tracker


def main():
    print("=" * 60)
    print("MF Tracker — monthly refresh")
    print("=" * 60)

    db.init_db()
    held = db.list_held_schemes()
    print(f"Found {len(held)} held schemes")

    if not held:
        print("Nothing to refresh — no portfolio imported.")
        return

    # 1. Refresh factsheets + fund details
    print("\n[1/4] Refreshing factsheets and fund details from VR...")
    for s in held:
        try:
            r = vr_scraper.refresh_scheme(s["scheme_code"])
            print(f"  ✓ {s['scheme_name']}: {r}")
        except Exception as e:
            print(f"  ✗ {s['scheme_name']}: {e}")

    # 2. News
    print("\n[2/4] Fetching news...")
    try:
        r = news_aggregator.fetch_news_for_all_held()
        print(f"  ✓ {r['items_inserted']} new articles, {len(r['failures'])} failures")
    except Exception as e:
        print(f"  ✗ News fetch failed: {e}")

    # 3. SIP detection
    print("\n[3/4] Re-running SIP detection...")
    r = sip_detector.detect_sips()
    print(f"  ✓ {r['sips_found']} SIPs, {r['transactions_marked']} txns marked")

    # 4. Performance
    print("\n[4/4] Computing performance...")
    for s in held:
        try:
            performance_tracker.compute_for_scheme(s["scheme_code"])
            print(f"  ✓ {s['scheme_name']}")
        except Exception as e:
            print(f"  ✗ {s['scheme_name']}: {e}")

    print("\n✅ Monthly refresh complete")


if __name__ == "__main__":
    main()

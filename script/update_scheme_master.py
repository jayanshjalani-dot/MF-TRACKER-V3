"""
Refresh data/scheme_master.csv from AMFI's live NAVAll.txt.
Run by GitHub Actions on the 8th of each month.
Also run manually: python scripts/update_scheme_master.py
"""
import csv
import sys
import requests
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "data" / "scheme_master.csv"

def main():
    print("Fetching AMFI master...")
    try:
        r = requests.get(
            "https://www.amfiindia.com/spages/NAVAll.txt",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
    except Exception as e:
        print(f"FAILED to fetch AMFI: {e}")
        sys.exit(1)

    schemes = []
    current_amc = None
    for line in r.text.splitlines():
        line = line.strip()
        if not line or line.startswith("Scheme Code"):
            continue
        if "|" not in line:
            if line.endswith("Mutual Fund"):
                current_amc = line
            continue
        parts = line.split("|")
        if len(parts) < 4:
            continue
        schemes.append({
            "scheme_code": parts[0].strip(),
            "scheme_name": parts[3].strip(),
            "isin_growth":  parts[1].strip(),
            "isin_div":     parts[2].strip(),
            "fund_house":   current_amc or "",
        })

    if len(schemes) < 1000:
        print(f"Only {len(schemes)} schemes parsed — suspiciously low, aborting")
        sys.exit(1)

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["scheme_code","scheme_name","isin_growth","isin_div","fund_house"]
        )
        writer.writeheader()
        writer.writerows(schemes)

    print(f"✅ Saved {len(schemes)} schemes to {OUT}")

if __name__ == "__main__":
    main()

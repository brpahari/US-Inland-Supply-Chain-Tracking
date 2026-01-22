#!/usr/bin/env python3
"""
barge_monitor.py
Fixes: Corrects unit label to 'barges' (count) to match GTR Figure 10 data.
"""

from __future__ import annotations
import io
import json
import os
import re
from datetime import datetime, timezone
import pandas as pd
import requests

LOCKS27_XLSX_URL = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10.xlsx"
OUT_HIST = "data/history/barge_locks27_weekly.csv"
OUT_STATUS = "data/barge_status.json"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_locks27() -> pd.DataFrame:
    try:
        b = requests.get(LOCKS27_XLSX_URL, timeout=60).content
        # Header hunting logic
        raw = pd.read_excel(io.BytesIO(b), engine="openpyxl", header=None)
        header_idx = 0
        for i, row in raw.head(20).iterrows():
            row_str = " ".join([str(x).lower() for x in row.values])
            if "date" in row_str or "week" in row_str:
                header_idx = i
                break
        
        df = pd.read_excel(io.BytesIO(b), engine="openpyxl", header=header_idx)
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # Find date column
        date_col = next((c for c in df.columns if "date" in c or "week" in c), None)
        # Find total column (It is usually 'Total' representing count of barges)
        total_col = next((c for c in df.columns if "total" in c), None)
        
        if not date_col or not total_col:
            raise ValueError("Columns not found")

        out = df[[date_col, total_col]].copy()
        out.columns = ["week_end_date", "total_barges"]
        
        out["week_end_date"] = pd.to_datetime(out["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        out["total_barges"] = pd.to_numeric(out["total_barges"], errors="coerce")
        out = out.dropna()
        
        return out.sort_values("week_end_date")
    except Exception as e:
        print(f"Barge fetch failed: {e}")
        return pd.DataFrame()

def main() -> int:
    new_data = fetch_locks27()
    
    # Load and Update History
    if os.path.exists(OUT_HIST):
        hist = pd.read_csv(OUT_HIST)
        combined = pd.concat([hist, new_data]).drop_duplicates(subset=["week_end_date"], keep="last")
    else:
        combined = new_data
    
    combined = combined.sort_values("week_end_date")
    os.makedirs(os.path.dirname(OUT_HIST), exist_ok=True)
    combined.to_csv(OUT_HIST, index=False)
    
    # Status JSON
    latest = combined.iloc[-1] if not combined.empty else None
    delta = 0
    if len(combined) >= 5:
        delta = float(combined.iloc[-1]["total_barges"] - combined.iloc[-5]["total_barges"])

    status = {
        "generated_at_utc": utc_now_iso(),
        "locks_27": {
            "week_end_date": str(latest["week_end_date"]) if latest is not None else None,
            "value": float(latest["total_barges"]) if latest is not None else None,
            "delta_4w": delta,
            "unit": "barges" # Correct unit
        }
    }
    
    with open(OUT_STATUS, "w") as f:
        json.dump(status, f, indent=2)
        
    print(f"Barge data updated. Latest: {status['locks_27']['value']}")
    return 0

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
barge_monitor.py
Ingest Locks 27 barge movements and Mississippi River System downbound grain barge per ton rates.
Outputs
  data/history/barge_locks27_weekly.csv
  data/history/barge_rates_weekly.csv
  data/barge_status.json
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd
import requests


LOCKS27_XLSX_URL = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10.xlsx"
# Per ton rates dataset
# Socrata foundry export, no key required for basic download sizes
RATES_ROWS_CSV = "https://agtransport.usda.gov/api/views/7spn-fbua/rows.csv?accessType=DOWNLOAD"

OUT_LOCKS_HIST = "data/history/barge_locks27_weekly.csv"
OUT_RATES_HIST = "data/history/barge_rates_weekly.csv"
OUT_STATUS = "data/barge_status.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_if_changed(path: str, obj: Dict) -> bool:
    new_txt = json.dumps(obj, indent=2, sort_keys=True)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            old_txt = f.read()
        if old_txt == new_txt:
            return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_txt)
    return True


def load_csv(path: str, cols: list) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols)
    return pd.read_csv(path)


def fetch_locks27() -> pd.DataFrame:
    b = requests.get(LOCKS27_XLSX_URL, timeout=60).content
    df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    df.columns = [str(c).strip().lower() for c in df.columns]

    date_col = next((c for c in df.columns if "date" in c), None)
    if not date_col:
        raise RuntimeError("Locks 27 file missing date column")

    # Common columns in the figure files
    rename = {date_col: "week_end_date"}
    if "total" in df.columns:
        rename["total"] = "total_barges"
    if "downbound grain barges" in df.columns:
        rename["downbound grain barges"] = "total_barges"
    df = df.rename(columns=rename)

    if "total_barges" not in df.columns:
        # Fallback, choose the first numeric column after the date
        candidates = [c for c in df.columns if c != "week_end_date"]
        if not candidates:
            raise RuntimeError("Locks 27 file has no metric columns")
        df["total_barges"] = pd.to_numeric(df[candidates[0]], errors="coerce")

    out = df[["week_end_date", "total_barges"]].copy()
    out["week_end_date"] = pd.to_datetime(out["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["total_barges"] = pd.to_numeric(out["total_barges"], errors="coerce")
    out = out.dropna(subset=["week_end_date", "total_barges"])

    out["source_url"] = LOCKS27_XLSX_URL
    out["ingested_at_utc"] = utc_now_iso()
    return out


def fetch_rates() -> pd.DataFrame:
    df = pd.read_csv(RATES_ROWS_CSV)
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Expected fields in this dataset include a week date and a location and a numeric per ton rate
    # Make this resilient by matching by substrings
    week_col = next((c for c in df.columns if "week" in c and "ending" in c), None)
    if not week_col:
        week_col = next((c for c in df.columns if "week" in c), None)

    loc_col = next((c for c in df.columns if "location" in c), None)
    rate_col = next((c for c in df.columns if "per ton" in c or "dollars" in c or "rate" in c), None)

    if not all([week_col, loc_col, rate_col]):
        raise RuntimeError("Rates dataset missing expected columns")

    out = df[[week_col, loc_col, rate_col]].copy()
    out.columns = ["week_end_date", "origin", "rate_usd_per_ton"]

    out["week_end_date"] = pd.to_datetime(out["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["rate_usd_per_ton"] = pd.to_numeric(out["rate_usd_per_ton"], errors="coerce")
    out = out.dropna(subset=["week_end_date", "origin", "rate_usd_per_ton"])

    out["origin"] = out["origin"].astype(str).str.strip()
    out["source_url"] = RATES_ROWS_CSV
    out["ingested_at_utc"] = utc_now_iso()
    return out


def update_history(new_df: pd.DataFrame, path: str, keys: list, cols: list) -> pd.DataFrame:
    hist = load_csv(path, cols)
    combined = pd.concat([hist, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=keys, keep="last")
    combined = combined.sort_values(keys)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    combined.to_csv(path, index=False)
    return combined


def latest_delta_4w(series_df: pd.DataFrame, value_col: str) -> Dict[str, float]:
    series_df = series_df.sort_values("week_end_date")
    latest = float(series_df.iloc[-1][value_col])
    delta = 0.0
    if len(series_df) >= 5:
        delta = latest - float(series_df.iloc[-5][value_col])
    return {"value": latest, "delta_4w": delta}


def main() -> int:
    locks = fetch_locks27()
    rates = fetch_rates()

    locks_cols = ["week_end_date", "total_barges", "source_url", "ingested_at_utc"]
    rates_cols = ["week_end_date", "origin", "rate_usd_per_ton", "source_url", "ingested_at_utc"]

    locks_hist = update_history(locks, OUT_LOCKS_HIST, ["week_end_date"], locks_cols)
    rates_hist = update_history(rates, OUT_RATES_HIST, ["week_end_date", "origin"], rates_cols)

    status = {
        "generated_at_utc": utc_now_iso(),
        "sources": {
            "locks27_xlsx": LOCKS27_XLSX_URL,
            "rates_rows_csv": RATES_ROWS_CSV,
        },
        "locks_27": {},
        "rates": {},
    }

    if not locks_hist.empty:
        latest_week = locks_hist.sort_values("week_end_date").iloc[-1]["week_end_date"]
        sub = locks_hist[locks_hist["week_end_date"] <= latest_week].tail(5)
        status["locks_27"] = {
            "week_end_date": str(latest_week),
            **latest_delta_4w(sub, "total_barges"),
        }

    if not rates_hist.empty:
        # Aggregate across all origins to a single weekly mean for a first pass
        agg = rates_hist.groupby("week_end_date", as_index=False)["rate_usd_per_ton"].mean()
        agg = agg.sort_values("week_end_date")
        latest_week = agg.iloc[-1]["week_end_date"]
        sub = agg.tail(5)
        status["rates"] = {
            "week_end_date": str(latest_week),
            **latest_delta_4w(sub, "rate_usd_per_ton"),
        }

    write_if_changed(OUT_STATUS, status)
    print(f"Updated {OUT_LOCKS_HIST}, {OUT_RATES_HIST}, {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

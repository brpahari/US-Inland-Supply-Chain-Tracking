#!/usr/bin/env python3
"""
barge_monitor.py
Ingest Locks 27 grain barge movements from the GTR datasets xlsx.

Outputs
  data/history/barge_locks27_weekly.csv
  data/barge_status.json
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests


LOCKS27_XLSX_URL = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10.xlsx"

OUT_LOCKS_HIST = "data/history/barge_locks27_weekly.csv"
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


def detect_date_column(df: pd.DataFrame) -> str:
    best_col = None
    best_hits = -1
    for c in df.columns:
        s = pd.to_datetime(df[c], errors="coerce")
        hits = int(s.notna().sum())
        if hits > best_hits:
            best_hits = hits
            best_col = c
    if best_col is None or best_hits < 5:
        raise RuntimeError("Locks 27 file missing a usable date like column")
    return best_col


def detect_total_column(df: pd.DataFrame) -> str:
    cols = [str(c).strip().lower() for c in df.columns]
    for target in ["total tons", "total_tons", "total"]:
        for i, c in enumerate(cols):
            if target == c:
                return df.columns[i]
    for i, c in enumerate(cols):
        if "total" in c and "ton" in c:
            return df.columns[i]
    for i, c in enumerate(cols):
        if "total" in c:
            return df.columns[i]
    raise RuntimeError("Locks 27 file missing a total column")


def fetch_locks27() -> pd.DataFrame:
    b = requests.get(LOCKS27_XLSX_URL, timeout=60).content

    df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    date_col = detect_date_column(df)
    total_col = detect_total_column(df)

    out = df[[date_col, total_col]].copy()
    out.columns = ["week_end_date", "total_tons"]

    out["week_end_date"] = pd.to_datetime(out["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["total_tons"] = pd.to_numeric(out["total_tons"], errors="coerce")

    out = out.dropna(subset=["week_end_date", "total_tons"])

    out["source_url"] = LOCKS27_XLSX_URL
    out["ingested_at_utc"] = utc_now_iso()
    return out


def update_history(new_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["week_end_date", "total_tons", "source_url", "ingested_at_utc"]
    hist = load_csv(OUT_LOCKS_HIST, cols)
    combined = pd.concat([hist, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["week_end_date"], keep="last")
    combined = combined.sort_values(["week_end_date"])
    os.makedirs(os.path.dirname(OUT_LOCKS_HIST), exist_ok=True)
    combined.to_csv(OUT_LOCKS_HIST, index=False)
    return combined


def latest_delta_4w(df: pd.DataFrame, value_col: str) -> Dict[str, float]:
    df = df.sort_values("week_end_date")
    latest = float(df.iloc[-1][value_col])
    delta = 0.0
    if len(df) >= 5:
        delta = latest - float(df.iloc[-5][value_col])
    return {"value": latest, "delta_4w": delta}


def main() -> int:
    locks = fetch_locks27()
    hist = update_history(locks)

    status = {
        "generated_at_utc": utc_now_iso(),
        "sources": {"locks27_xlsx": LOCKS27_XLSX_URL},
        "locks_27": {},
    }

    if not hist.empty:
        latest_week = hist.sort_values("week_end_date").iloc[-1]["week_end_date"]
        tail = hist.tail(5)
        status["locks_27"] = {
            "week_end_date": str(latest_week),
            **latest_delta_4w(tail, "total_tons"),
        }

    write_if_changed(OUT_STATUS, status)
    print(f"Updated {OUT_LOCKS_HIST} and {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

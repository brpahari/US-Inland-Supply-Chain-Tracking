#!/usr/bin/env python3
"""
barge_monitor.py

Locks 27 volume from USDA GTR figure 10.
Writes
  data/history/barge_locks27_weekly.csv
  data/barge_status.json
"""

from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd
import requests


LOCKS27_XLSX_URL = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10.xlsx"
OUT_HIST = "data/history/barge_locks27_weekly.csv"
OUT_STATUS = "data/barge_status.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def norm_header(x: object) -> str:
    s = "" if x is None else str(x)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s


def choose_header_row(raw: pd.DataFrame) -> int:
    for i in range(min(50, len(raw))):
        row = raw.iloc[i].tolist()
        strings = [norm_header(v) for v in row if isinstance(v, str)]
        if len(strings) < 2:
            continue
        blob = " ".join(strings)
        if "date" in blob or "week" in blob or "ending" in blob:
            return i
    return 0


def detect_date_column(df: pd.DataFrame) -> str:
    best = None
    best_hits = -1
    sample_n = min(120, len(df))
    for c in df.columns:
        s = pd.to_datetime(df[c].head(sample_n), errors="coerce")
        hits = int(s.notna().sum())
        if hits > best_hits:
            best_hits = hits
            best = c
    if best is None or best_hits < 5:
        raise RuntimeError("Locks 27 file missing a usable date column")
    return str(best)


def detect_total_column(df: pd.DataFrame, date_col: str) -> str:
    headers = {c: norm_header(c) for c in df.columns}

    for c, h in headers.items():
        if str(c) == date_col:
            continue
        if "total" in h:
            return str(c)

    best = None
    best_hits = -1
    for c in df.columns:
        if str(c) == date_col:
            continue
        vals = pd.to_numeric(df[c], errors="coerce")
        hits = int(vals.notna().sum())
        if hits > best_hits:
            best_hits = hits
            best = c

    if best is None or best_hits < 5:
        raise RuntimeError("Locks 27 file missing a usable numeric total column")
    return str(best)


def fetch_locks27() -> pd.DataFrame:
    b = requests.get(LOCKS27_XLSX_URL, timeout=60).content
    raw = pd.read_excel(io.BytesIO(b), engine="openpyxl", header=None)
    raw = raw.dropna(axis=0, how="all").dropna(axis=1, how="all")

    hdr = choose_header_row(raw)
    df = pd.read_excel(io.BytesIO(b), engine="openpyxl", header=hdr)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df.columns = [str(c).strip() for c in df.columns]

    date_col = detect_date_column(df)
    total_col = detect_total_column(df, date_col)

    out = df[[date_col, total_col]].copy()
    out.columns = ["week_end_date", "total_tons"]

    out["week_end_date"] = pd.to_datetime(out["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["total_tons"] = pd.to_numeric(out["total_tons"], errors="coerce")
    out = out.dropna(subset=["week_end_date", "total_tons"])

    out["source_url"] = LOCKS27_XLSX_URL
    out["ingested_at_utc"] = utc_now_iso()
    return out


def load_hist() -> pd.DataFrame:
    if not os.path.exists(OUT_HIST):
        return pd.DataFrame(columns=["week_end_date", "total_tons", "source_url", "ingested_at_utc"])
    return pd.read_csv(OUT_HIST)


def update_history(new_df: pd.DataFrame) -> pd.DataFrame:
    os.makedirs(os.path.dirname(OUT_HIST), exist_ok=True)
    hist = load_hist()
    combined = pd.concat([hist, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["week_end_date"], keep="last")
    combined = combined.sort_values(["week_end_date"])
    combined.to_csv(OUT_HIST, index=False)
    return combined


def latest_valid_total(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    s = pd.to_numeric(df["total_tons"], errors="coerce")
    ok = df[s.notna()].copy()
    if ok.empty:
        return None
    ok = ok.sort_values("week_end_date")
    return float(ok.iloc[-1]["total_tons"])


def delta_4w(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    df2 = df.copy()
    df2["total_tons"] = pd.to_numeric(df2["total_tons"], errors="coerce")
    df2 = df2.dropna(subset=["total_tons"]).sort_values("week_end_date")
    if len(df2) < 5:
        return None
    return float(df2.iloc[-1]["total_tons"] - df2.iloc[-5]["total_tons"])


def main() -> int:
    locks = fetch_locks27()
    hist = update_history(locks)

    latest = latest_valid_total(hist)
    d4 = delta_4w(hist)

    status: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "sources": {"locks27_xlsx": LOCKS27_XLSX_URL},
        "locks_27": {
            "week_end_date": None,
            "value": latest,
            "delta_4w": d4,
            "unit": "tons",
        },
    }

    if not hist.empty:
        hist2 = hist.copy()
        hist2["total_tons"] = pd.to_numeric(hist2["total_tons"], errors="coerce")
        hist2 = hist2.dropna(subset=["total_tons"]).sort_values("week_end_date")
        if not hist2.empty:
            status["locks_27"]["week_end_date"] = str(hist2.iloc[-1]["week_end_date"])

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"Updated {OUT_HIST} and {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

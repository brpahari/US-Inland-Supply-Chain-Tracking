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
from typing import Dict, Optional, Tuple

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
    out = out.sort_values("week_end_date")

    out["source_url"] = LOCKS27_XLSX_URL
    out["ingested_at_utc"] = utc_now_iso()
    return out


def drop_latest_zero_if_placeholder(d: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    if d.empty:
        return d, None

    df = d.copy().sort_values("week_end_date")
    df["total_tons"] = pd.to_numeric(df["total_tons"], errors="coerce")
    df = df.dropna(subset=["total_tons"])
    if df.empty:
        return df, None

    last_val = float(df.iloc[-1]["total_tons"])
    if last_val != 0.0:
        return df, None

    prev = df.iloc[:-1].tail(8)
    if len(prev) < 5:
        return df, None

    prev_vals = prev["total_tons"].astype(float)
    nonzero = int((prev_vals > 0).sum())
    med = float(prev_vals.median())

    if nonzero >= 7 and med >= 50.0:
        dropped_week = str(df.iloc[-1]["week_end_date"])
        df2 = df.iloc[:-1].copy()
        return df2, f"dropped trailing zero placeholder at {dropped_week}"
    return df, None


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
    return combined


def write_history(df: pd.DataFrame) -> None:
    df.to_csv(OUT_HIST, index=False)


def delta_4w(df: pd.DataFrame) -> Optional[float]:
    d = df.copy()
    d["total_tons"] = pd.to_numeric(d["total_tons"], errors="coerce")
    d = d.dropna(subset=["total_tons"]).sort_values("week_end_date")
    if len(d) < 5:
        return None
    return float(d.iloc[-1]["total_tons"] - d.iloc[-5]["total_tons"])


def main() -> int:
    locks = fetch_locks27()
    locks, note_new = drop_latest_zero_if_placeholder(locks)

    combined = update_history(locks)
    combined, note_hist = drop_latest_zero_if_placeholder(combined)

    write_history(combined)

    latest_week = None
    latest_val = None
    if not combined.empty:
        latest_week = str(combined.iloc[-1]["week_end_date"])
        latest_val = float(pd.to_numeric(combined.iloc[-1]["total_tons"], errors="coerce"))

    status: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "sources": {"locks27_xlsx": LOCKS27_XLSX_URL},
        "locks_27": {
            "week_end_date": latest_week,
            "value": latest_val,
            "delta_4w": delta_4w(combined),
            "unit": "tons",
        },
    }

    note = note_new or note_hist
    if note:
        status["locks_27"]["note"] = note

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"Updated {OUT_HIST} and {OUT_STATUS}")
    if note:
        print(note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

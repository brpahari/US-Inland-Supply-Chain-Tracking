#!/usr/bin/env python3
"""
rail_monitor.py
Ingest STB weekly rail service metrics for UP and BNSF.
Outputs
  data/history/rail_weekly.csv
  data/rail_status.json
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests
from bs4 import BeautifulSoup


STB_LANDING_URL = "https://www.stb.gov/reports-data/rail-service-data/"
OUT_HISTORY = "data/history/rail_weekly.csv"
OUT_STATUS = "data/rail_status.json"

CARRIER_ALIASES = {
    "UP": {"UP", "UNION PACIFIC", "UNION PACIFIC RAILROAD", "UNION PACIFIC R.R.", "UNION PACIFIC RR"},
    "BNSF": {"BNSF", "BNSF RAILWAY", "BNSF RAILWAY CO", "BNSF RAILWAY COMPANY"},
}

REQUIRED_METRICS = ("train_speed_mph", "terminal_dwell_hours")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def read_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def read_bytes(url: str, timeout: int = 60) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def find_latest_stb_xlsx_url() -> str:
    html = read_text(STB_LANDING_URL)
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".xlsx"):
            if href.startswith("/"):
                href = "https://www.stb.gov" + href
            links.append(href)
    if not links:
        raise RuntimeError("Could not find any .xlsx link on STB landing page")
    # If multiple links exist, prefer the one that looks like a consolidated dataset
    # Otherwise take the first
    preferred = [u for u in links if "data" in u.lower() or "ep" in u.lower() or "724" in u.lower()]
    return preferred[0] if preferred else links[0]


def normalize_carrier(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None
    for canon, alias_set in CARRIER_ALIASES.items():
        if s == canon or s in alias_set:
            return canon
    # Sometimes carrier appears as a code inside a longer string
    if "UNION PACIFIC" in s:
        return "UP"
    if "BNSF" in s:
        return "BNSF"
    return None


def try_read_excel_with_headers(xlsx_bytes: bytes, header_candidates: List[int]) -> pd.DataFrame:
    last_err = None
    for hdr in header_candidates:
        try:
            df = pd.read_excel(io.BytesIO(xlsx_bytes), engine="openpyxl", header=hdr)
            if df is not None and len(df.columns) > 1:
                return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to read excel with header candidates. Last error {last_err}")


def find_column(df_cols: List[str], contains_any: Tuple[str, ...]) -> Optional[str]:
    for c in df_cols:
        lc = c.lower().strip()
        for key in contains_any:
            if key in lc:
                return c
    return None


def coerce_week_end_date(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        if pd.notna(v):
            return v
        return None
    except Exception:
        return None


def load_history() -> pd.DataFrame:
    if not os.path.exists(OUT_HISTORY):
        return pd.DataFrame(columns=[
            "week_end_date", "carrier", "train_speed_mph", "terminal_dwell_hours", "source_url", "ingested_at_utc"
        ])
    return pd.read_csv(OUT_HISTORY)


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


def main() -> int:
    xlsx_url = find_latest_stb_xlsx_url()
    xlsx_bytes = read_bytes(xlsx_url)
    source_hash = sha256_bytes(xlsx_bytes)

    df = try_read_excel_with_headers(xlsx_bytes, header_candidates=[0, 1, 2, 3, 4, 5])
    df.columns = [str(c).strip() for c in df.columns]

    cols = list(df.columns)
    date_col = find_column(cols, ("week", "date"))
    carrier_col = find_column(cols, ("railroad", "sub-railroad", "sub railroad", "rr"))
    speed_col = find_column(cols, ("avg train speed", "train speed", "system train speed", "speed"))
    dwell_col = find_column(cols, ("terminal dwell", "term dwell", "dwell"))

    if not all([date_col, carrier_col, speed_col, dwell_col]):
        found = {"date": date_col, "carrier": carrier_col, "speed": speed_col, "dwell": dwell_col}
        raise RuntimeError(f"Missing required columns. Detected mapping {found}. Columns {cols[:40]}")

    work = df[[date_col, carrier_col, speed_col, dwell_col]].copy()
    work.columns = ["week_end_date", "carrier_raw", "train_speed_mph", "terminal_dwell_hours"]

    work["carrier"] = work["carrier_raw"].apply(normalize_carrier)
    work = work.dropna(subset=["carrier"])

    work["week_end_date"] = coerce_week_end_date(work["week_end_date"])
    work = work.dropna(subset=["week_end_date"])

    work["train_speed_mph"] = work["train_speed_mph"].apply(safe_float)
    work["terminal_dwell_hours"] = work["terminal_dwell_hours"].apply(safe_float)
    work = work.dropna(subset=["train_speed_mph"])

    work["source_url"] = xlsx_url
    work["ingested_at_utc"] = utc_now_iso()

    clean = work[["week_end_date", "carrier", "train_speed_mph", "terminal_dwell_hours", "source_url", "ingested_at_utc"]].copy()

    hist = load_history()
    combined = pd.concat([hist, clean], ignore_index=True)
    combined = combined.drop_duplicates(subset=["week_end_date", "carrier"], keep="last")
    combined = combined.sort_values(["week_end_date", "carrier"])

    os.makedirs(os.path.dirname(OUT_HISTORY), exist_ok=True)
    combined.to_csv(OUT_HISTORY, index=False)

    status = {
        "generated_at_utc": utc_now_iso(),
        "source": {"landing": STB_LANDING_URL, "xlsx": xlsx_url, "sha256": source_hash},
        "carriers": {}
    }

    for canon in CARRIER_ALIASES.keys():
        c = combined[combined["carrier"] == canon].sort_values("week_end_date")
        if c.empty:
            continue
        latest = c.iloc[-1]
        tail = c.tail(5)
        metrics = {}
        for m in REQUIRED_METRICS:
            v = float(latest[m])
            delta_4w = 0.0
            if len(tail) >= 5:
                delta_4w = v - float(tail.iloc[0][m])
            metrics[m] = {"value": v, "delta_4w": delta_4w}
        status["carriers"][canon] = {
            "week_end_date": str(latest["week_end_date"]),
            "metrics": metrics
        }

    write_if_changed(OUT_STATUS, status)
    print(f"Updated {OUT_HISTORY} and {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

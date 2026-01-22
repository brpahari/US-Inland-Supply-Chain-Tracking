#!/usr/bin/env python3
"""
rail_monitor.py
Ingest STB weekly rail service metrics for UP and BNSF
Handles wide pivot format where week ending dates are columns

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
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


STB_LANDING_URL = "https://www.stb.gov/reports-data/rail-service-data/"

OUT_HISTORY = "data/history/rail_weekly.csv"
OUT_STATUS = "data/rail_status.json"

CARRIERS = ["UP", "BNSF"]

MEASURE_MATCH = {
    "train_speed_mph": ["train speed", "speed"],
    "terminal_dwell_hours": ["terminal dwell", "dwell"],
}

CARRIER_MATCH = {
    "UP": ["union pacific", "up"],
    "BNSF": ["bnsf"],
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def find_latest_stb_xlsx_url() -> str:
    r = requests.get(STB_LANDING_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".xlsx"):
            if href.startswith("/"):
                href = "https://www.stb.gov" + href
            links.append(href)

    if not links:
        raise RuntimeError("Could not find any xlsx link on STB landing page")

    preferred = [u for u in links if "ep" in u.lower() or "724" in u.lower() or "data" in u.lower()]
    return preferred[0] if preferred else links[0]


def try_read_excel(xlsx_bytes: bytes) -> pd.DataFrame:
    last_err = None
    for hdr in [0, 1, 2, 3, 4, 5, 6]:
        try:
            df = pd.read_excel(io.BytesIO(xlsx_bytes), engine="openpyxl", header=hdr)
            if df is not None and len(df.columns) > 6:
                return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to read excel with header hunting. Last error {last_err}")


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def find_col(df: pd.DataFrame, wants: List[str]) -> Optional[str]:
    cols = list(df.columns)
    for c in cols:
        lc = str(c).strip().lower()
        for w in wants:
            if w in lc:
                return c
    return None


def find_date_columns(df: pd.DataFrame) -> List[str]:
    date_cols = []
    for c in df.columns:
        if isinstance(c, (pd.Timestamp, datetime)):
            date_cols.append(c)
            continue
        s = pd.to_datetime(df[c], errors="coerce")
        # In the wide format, the date columns are headers, not values
        # So we also accept columns whose name parses as a date
        try:
            name_dt = pd.to_datetime(str(c), errors="coerce")
            if pd.notna(name_dt):
                date_cols.append(c)
        except Exception:
            pass
    # Dedup preserve order
    seen = set()
    out = []
    for c in date_cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def carrier_from_row(rr_region: str) -> Optional[str]:
    s = str(rr_region).strip().lower()
    for canon, toks in CARRIER_MATCH.items():
        for t in toks:
            if t in s:
                return canon
    return None


def metric_from_row(measure: str, variable: str, subvar: str) -> Optional[str]:
    blob = " ".join([str(measure), str(variable), str(subvar)]).strip().lower()
    for metric, toks in MEASURE_MATCH.items():
        for t in toks:
            if t in blob:
                return metric
    return None


def load_history() -> pd.DataFrame:
    cols = ["week_end_date", "carrier", "train_speed_mph", "terminal_dwell_hours", "source_url", "ingested_at_utc"]
    if not os.path.exists(OUT_HISTORY):
        return pd.DataFrame(columns=cols)
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
    b = requests.get(xlsx_url, timeout=60).content
    src_hash = sha256_bytes(b)

    df = normalize_cols(try_read_excel(b))

    rr_col = find_col(df, ["railroad", "region"])
    measure_col = find_col(df, ["measure"])
    var_col = find_col(df, ["variable"])
    subvar_col = find_col(df, ["sub-variable", "sub variable", "subvariable"])

    if rr_col is None or measure_col is None or var_col is None or subvar_col is None:
        raise RuntimeError("Could not locate descriptor columns in STB file")

    date_cols = [c for c in df.columns if c not in [rr_col, measure_col, var_col, subvar_col] and "category" not in str(c).lower()]

    # Keep only columns that look like dates
    keep_dates = []
    for c in date_cols:
        dt = pd.to_datetime(str(c), errors="coerce")
        if pd.notna(dt):
            keep_dates.append(c)

    if len(keep_dates) == 0:
        raise RuntimeError("Could not locate week ending date columns in STB file")

    df_small = df[[rr_col, measure_col, var_col, subvar_col] + keep_dates].copy()
    df_small["carrier"] = df_small[rr_col].apply(carrier_from_row)
    df_small["metric"] = df_small.apply(lambda r: metric_from_row(r[measure_col], r[var_col], r[subvar_col]), axis=1)

    df_small = df_small.dropna(subset=["carrier", "metric"])

    long = df_small.melt(
        id_vars=["carrier", "metric"],
        value_vars=keep_dates,
        var_name="week_end_date",
        value_name="value",
    )

    long["week_end_date"] = pd.to_datetime(long["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["week_end_date", "value"])

    pivot = long.pivot_table(
        index=["week_end_date", "carrier"],
        columns="metric",
        values="value",
        aggfunc="last",
    ).reset_index()

    for m in ["train_speed_mph", "terminal_dwell_hours"]:
        if m not in pivot.columns:
            pivot[m] = pd.NA

    pivot["source_url"] = xlsx_url
    pivot["ingested_at_utc"] = utc_now_iso()

    hist = load_history()
    combined = pd.concat([hist, pivot[hist.columns]], ignore_index=True)
    combined = combined.drop_duplicates(subset=["week_end_date", "carrier"], keep="last")
    combined = combined.sort_values(["week_end_date", "carrier"])

    os.makedirs(os.path.dirname(OUT_HISTORY), exist_ok=True)
    combined.to_csv(OUT_HISTORY, index=False)

    status = {
        "generated_at_utc": utc_now_iso(),
        "source": {"landing": STB_LANDING_URL, "xlsx": xlsx_url, "sha256": src_hash},
        "carriers": {},
    }

    for carrier in CARRIERS:
        c = combined[combined["carrier"] == carrier].sort_values("week_end_date")
        if c.empty:
            continue
        latest = c.iloc[-1]
        tail = c.tail(5)

        metrics = {}
        for metric in ["train_speed_mph", "terminal_dwell_hours"]:
            val = latest[metric]
            if pd.isna(val):
                continue
            val = float(val)
            delta_4w = 0.0
            if len(tail) >= 5 and not pd.isna(tail.iloc[0][metric]):
                delta_4w = val - float(tail.iloc[0][metric])
            metrics[metric] = {"value": val, "delta_4w": delta_4w}

        status["carriers"][carrier] = {"week_end_date": str(latest["week_end_date"]), "metrics": metrics}

    write_if_changed(OUT_STATUS, status)
    print(f"Updated {OUT_HISTORY} and {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

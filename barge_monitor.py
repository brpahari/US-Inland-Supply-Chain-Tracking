#!/usr/bin/env python3
"""
rail_monitor.py

STB rail service metrics ingestion.
Writes
  data/history/rail_weekly.csv
  data/rail_status.json

Expected output fields
  week_end_date, carrier, train_speed_mph, terminal_dwell_hours
"""

from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests


STB_XLSX_URL = "https://www.stb.gov/wp-content/uploads/EP-724-Data.xlsx"
OUT_HIST = "data/history/rail_weekly.csv"
OUT_STATUS = "data/rail_status.json"

CARRIERS = ["UP", "BNSF"]
TIMEOUT = 60


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def norm(s: object) -> str:
    x = "" if s is None else str(s)
    x = x.strip().lower()
    x = re.sub(r"\s+", " ", x)
    return x


def fetch_xlsx() -> bytes:
    r = requests.get(STB_XLSX_URL, timeout=TIMEOUT)
    r.raise_for_status()
    return r.content


def read_any_sheet_wide(content: bytes) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    best_df = None
    best_cols = 0

    for name in xls.sheet_names[:12]:
        try:
            raw = pd.read_excel(xls, sheet_name=name)
        except Exception:
            continue
        if raw is None or raw.empty:
            continue
        cols = raw.columns.tolist()
        if len(cols) > best_cols:
            best_cols = len(cols)
            best_df = raw

    if best_df is None:
        raise RuntimeError("Could not read STB workbook")
    return best_df


def detect_week_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for c in df.columns:
        if isinstance(c, (datetime, pd.Timestamp)):
            cols.append(c)
            continue
        s = str(c)
        if re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", s):
            cols.append(c)
    return cols


def melt_wide(df: pd.DataFrame) -> pd.DataFrame:
    # Identify dimension columns
    col_carrier = None
    for c in df.columns:
        if "railroad" in norm(c) or "region" in norm(c):
            col_carrier = c
            break
    if col_carrier is None:
        col_carrier = df.columns[0]

    # Measure columns
    col_measure = None
    for c in df.columns:
        if "measure" in norm(c):
            col_measure = c
            break

    # Week columns
    week_cols = detect_week_columns(df)
    if not week_cols:
        raise RuntimeError("No week date columns found in STB data")

    id_vars = [col_carrier]
    if col_measure is not None:
        id_vars.append(col_measure)

    m = df.melt(id_vars=id_vars, value_vars=week_cols, var_name="week_end_date", value_name="value")

    # Normalize week_end_date
    m["week_end_date"] = pd.to_datetime(m["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    m[col_carrier] = m[col_carrier].astype(str).str.upper().str.strip()
    if col_measure is not None:
        m[col_measure] = m[col_measure].astype(str).str.lower().str.strip()

    m["value"] = pd.to_numeric(m["value"], errors="coerce")
    m = m.dropna(subset=["week_end_date", "value"])

    # Carrier mapping
    def carrier_map(x: str) -> Optional[str]:
      if "BNSF" in x:
          return "BNSF"
      if "UNION PACIFIC" in x or x == "UP":
          return "UP"
      if x in CARRIERS:
          return x
      return None

    m["carrier"] = m[col_carrier].apply(carrier_map)
    m = m.dropna(subset=["carrier"])

    # Metric mapping
    # Use measure column if present, else fallback to nothing
    def metric_map(measure: str) -> Optional[str]:
        s = measure
        if "train speed" in s and "mph" in s:
            return "train_speed_mph"
        if "avg train speed" in s:
            return "train_speed_mph"
        if "terminal dwell" in s or (("dwell" in s) and ("terminal" in s)):
            return "terminal_dwell_hours"
        if "dwell time" in s:
            return "terminal_dwell_hours"
        return None

    if col_measure is None:
        raise RuntimeError("STB sheet missing measure column, cannot map metrics")

    m["metric"] = m[col_measure].apply(metric_map)
    m = m.dropna(subset=["metric"])

    pivot = m.pivot_table(
        index=["week_end_date", "carrier"],
        columns="metric",
        values="value",
        aggfunc="mean",
    ).reset_index()

    if "train_speed_mph" not in pivot.columns:
        pivot["train_speed_mph"] = pd.NA
    if "terminal_dwell_hours" not in pivot.columns:
        pivot["terminal_dwell_hours"] = pd.NA

    pivot["source_url"] = STB_XLSX_URL
    pivot["ingested_at_utc"] = utc_now_iso()
    return pivot


def load_hist() -> pd.DataFrame:
    if not os.path.exists(OUT_HIST):
        return pd.DataFrame(columns=["week_end_date", "carrier", "train_speed_mph", "terminal_dwell_hours", "source_url", "ingested_at_utc"])
    return pd.read_csv(OUT_HIST)


def update_hist(new_df: pd.DataFrame) -> pd.DataFrame:
    os.makedirs(os.path.dirname(OUT_HIST), exist_ok=True)
    hist = load_hist()
    combined = pd.concat([hist, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["week_end_date", "carrier"], keep="last")
    combined = combined.sort_values(["week_end_date", "carrier"])
    combined.to_csv(OUT_HIST, index=False)
    return combined


def metric_delta_4w(df: pd.DataFrame, carrier: str, metric: str) -> Optional[float]:
    d = df[df["carrier"].str.upper() == carrier].copy()
    if d.empty:
        return None
    d[metric] = pd.to_numeric(d[metric], errors="coerce")
    d = d.dropna(subset=[metric]).sort_values("week_end_date")
    if len(d) < 5:
        return None
    return float(d.iloc[-1][metric] - d.iloc[-5][metric])


def latest_value(df: pd.DataFrame, carrier: str, metric: str) -> Optional[float]:
    d = df[df["carrier"].str.upper() == carrier].copy()
    if d.empty:
        return None
    d[metric] = pd.to_numeric(d[metric], errors="coerce")
    d = d.dropna(subset=[metric]).sort_values("week_end_date")
    if d.empty:
        return None
    return float(d.iloc[-1][metric])


def main() -> int:
    content = fetch_xlsx()
    wide = read_any_sheet_wide(content)
    clean = melt_wide(wide)
    hist = update_hist(clean)

    out = {
        "generated_at_utc": utc_now_iso(),
        "source_url": STB_XLSX_URL,
        "carriers": {}
    }

    for c in CARRIERS:
        out["carriers"][c] = {
            "metrics": {
                "train_speed_mph": {
                    "value": latest_value(hist, c, "train_speed_mph"),
                    "delta_4w": metric_delta_4w(hist, c, "train_speed_mph"),
                },
                "terminal_dwell_hours": {
                    "value": latest_value(hist, c, "terminal_dwell_hours"),
                    "delta_4w": metric_delta_4w(hist, c, "terminal_dwell_hours"),
                },
            }
        }

        # latest week for carrier
        d = hist[hist["carrier"].str.upper() == c].copy()
        d = d.sort_values("week_end_date")
        out["carriers"][c]["week_end_date"] = None if d.empty else str(d.iloc[-1]["week_end_date"])

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Updated {OUT_HIST} and {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

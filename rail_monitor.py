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
from typing import Dict, Optional, List, Tuple

import pandas as pd
import requests


STB_RAIL_SERVICE_PAGE = "https://www.stb.gov/reports-data/rail-service-data/"
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


def discover_latest_weekly_xlsx_url() -> str:
    """
    Scrape STB Rail Service Data page for links that look like weekly xlsx files.
    Choose the newest by parsing MM-DD-YY or MM-DD-YYYY in the filename.
    """
    r = requests.get(STB_RAIL_SERVICE_PAGE, timeout=TIMEOUT)
    r.raise_for_status()
    html = r.text

    # Pull all hrefs ending in .xlsx
    hrefs = re.findall(r'href="([^"]+\.xlsx)"', html, flags=re.IGNORECASE)
    if not hrefs:
        raise RuntimeError("No .xlsx links found on STB rail service data page")

    # Normalize to absolute URLs
    urls: List[str] = []
    for h in hrefs:
        if h.startswith("http"):
            urls.append(h)
        elif h.startswith("/"):
            urls.append("https://www.stb.gov" + h)
        else:
            urls.append("https://www.stb.gov/" + h)

    def parse_date_from_url(u: str) -> Optional[datetime]:
        base = u.split("/")[-1]
        base = base.replace("%20", " ")
        m = re.search(r"(\d{2})-(\d{2})-(\d{2,4})", base)
        if not m:
            return None
        mm = int(m.group(1))
        dd = int(m.group(2))
        yy = int(m.group(3))
        if yy < 100:
            yy = 2000 + yy
        try:
            return datetime(yy, mm, dd)
        except Exception:
            return None

    dated: List[Tuple[datetime, str]] = []
    undated: List[str] = []

    for u in urls:
        d = parse_date_from_url(u)
        if d is None:
            undated.append(u)
        else:
            dated.append((d, u))

    if dated:
        dated.sort(key=lambda t: t[0])
        return dated[-1][1]

    # Fallback if nothing parses
    return urls[-1]


def fetch_xlsx(url: str) -> bytes:
    r = requests.get(url, timeout=TIMEOUT)
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


def melt_wide(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    col_carrier = None
    for c in df.columns:
        if "railroad" in norm(c) or "region" in norm(c):
            col_carrier = c
            break
    if col_carrier is None:
        col_carrier = df.columns[0]

    col_measure = None
    for c in df.columns:
        if "measure" in norm(c):
            col_measure = c
            break

    week_cols = detect_week_columns(df)
    if not week_cols:
        raise RuntimeError("No week date columns found in STB data")

    id_vars = [col_carrier]
    if col_measure is not None:
        id_vars.append(col_measure)

    m = df.melt(id_vars=id_vars, value_vars=week_cols, var_name="week_end_date", value_name="value")

    m["week_end_date"] = pd.to_datetime(m["week_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    m[col_carrier] = m[col_carrier].astype(str).str.upper().str.strip()

    if col_measure is None:
        raise RuntimeError("STB sheet missing measure column, cannot map metrics")

    m[col_measure] = m[col_measure].astype(str).str.lower().str.strip()

    m["value"] = pd.to_numeric(m["value"], errors="coerce")
    m = m.dropna(subset=["week_end_date", "value"])

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

    def metric_map(measure: str) -> Optional[str]:
        s = measure
        if "train speed" in s:
            return "train_speed_mph"
        if "terminal dwell" in s:
            return "terminal_dwell_hours"
        if "dwell time" in s and "terminal" in s:
            return "terminal_dwell_hours"
        return None

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

    pivot["source_url"] = source_url
    pivot["ingested_at_utc"] = utc_now_iso()
    return pivot


def load_hist() -> pd.DataFrame:
    if not os.path.exists(OUT_HIST):
        return pd.DataFrame(
            columns=[
                "week_end_date",
                "carrier",
                "train_speed_mph",
                "terminal_dwell_hours",
                "source_url",
                "ingested_at_utc",
            ]
        )
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
    latest_url = discover_latest_weekly_xlsx_url()
    content = fetch_xlsx(latest_url)

    wide = read_any_sheet_wide(content)
    clean = melt_wide(wide, source_url=latest_url)
    hist = update_hist(clean)

    out: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "source_page": STB_RAIL_SERVICE_PAGE,
        "source_url": latest_url,
        "carriers": {},
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

        d = hist[hist["carrier"].str.upper() == c].copy().sort_values("week_end_date")
        out["carriers"][c]["week_end_date"] = None if d.empty else str(d.iloc[-1]["week_end_date"])

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Updated {OUT_HIST} and {OUT_STATUS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

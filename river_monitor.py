#!/usr/bin/env python3
"""
river_monitor.py

USGS river gauge ingestion for Mississippi River

Sites
  St Louis MO 07010000
  Memphis TN 07032000

Writes
  data/river_status.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests


USGS_IV_JSON = "https://waterservices.usgs.gov/nwis/iv/"
TIMEOUT = 45

SITES = {
    "st_louis_mo": {"site_no": "07010000", "label": "Mississippi River at St Louis MO"},
    "memphis_tn": {"site_no": "07032000", "label": "Mississippi River at Memphis TN"},
}

PARAM_GAGE_HEIGHT = "00065"
PARAM_DISCHARGE = "00060"

OUT_STATUS = "data/river_status.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_usgs_iv(
    sites_csv: str,
    param_csv: str,
    start_dt_utc: datetime,
) -> dict:
    params = {
        "format": "json",
        "sites": sites_csv,
        "parameterCd": param_csv,
        "siteStatus": "all",
        "startDT": start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    r = requests.get(USGS_IV_JSON, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def parse_timeseries(payload: dict) -> Dict[Tuple[str, str], List[Tuple[str, float]]]:
    """
    Returns dict keyed by (site_no, parameterCd) with list of (time_iso, value)
    """
    out: Dict[Tuple[str, str], List[Tuple[str, float]]] = {}
    ts_list = payload.get("value", {}).get("timeSeries", []) or []
    for ts in ts_list:
        source_info = ts.get("sourceInfo", {}) or {}
        site_no = None
        for ident in source_info.get("siteCode", []) or []:
            if ident.get("value"):
                site_no = ident.get("value")
                break
        var = ts.get("variable", {}) or {}
        param = var.get("variableCode", [{}])[0].get("value")

        if not site_no or not param:
            continue

        values = ts.get("values", []) or []
        if not values:
            continue

        points: List[Tuple[str, float]] = []
        for v in values[0].get("value", []) or []:
            t = v.get("dateTime")
            x = v.get("value")
            if t is None or x is None:
                continue
            try:
                fx = float(x)
            except Exception:
                continue
            points.append((t, fx))

        if points:
            out[(site_no, param)] = points
    return out


def latest_and_earliest(points: List[Tuple[str, float]]) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[float]]:
    if not points:
        return None, None, None, None
    pts = sorted(points, key=lambda p: p[0])
    e_t, e_v = pts[0]
    l_t, l_v = pts[-1]
    return l_t, l_v, e_t, e_v


def delta_over_window(points: List[Tuple[str, float]]) -> Optional[float]:
    if len(points) < 2:
        return None
    pts = sorted(points, key=lambda p: p[0])
    return float(pts[-1][1] - pts[0][1])


def main() -> int:
    start_dt = datetime.now(timezone.utc) - timedelta(days=7, hours=6)

    site_csv = ",".join([v["site_no"] for v in SITES.values()])
    param_csv = ",".join([PARAM_GAGE_HEIGHT, PARAM_DISCHARGE])

    payload = fetch_usgs_iv(site_csv, param_csv, start_dt)
    series = parse_timeseries(payload)

    out: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "source": {
            "provider": "USGS NWIS Instantaneous Values",
            "endpoint": USGS_IV_JSON,
            "start_dt_utc": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "parameters": {
                "00065": "gage height feet",
                "00060": "discharge cubic feet per second",
            },
        },
        "sites": {},
    }

    for key, meta in SITES.items():
        site_no = meta["site_no"]
        label = meta["label"]

        gh_points = series.get((site_no, PARAM_GAGE_HEIGHT), [])
        q_points = series.get((site_no, PARAM_DISCHARGE), [])

        gh_latest_t, gh_latest_v, gh_earliest_t, gh_earliest_v = latest_and_earliest(gh_points)
        q_latest_t, q_latest_v, q_earliest_t, q_earliest_v = latest_and_earliest(q_points)

        site_obj: Dict[str, object] = {
            "site_no": site_no,
            "label": label,
            "gage_height_ft": {
                "latest_time": gh_latest_t,
                "latest_value": gh_latest_v,
                "earliest_time": gh_earliest_t,
                "earliest_value": gh_earliest_v,
                "delta_7d": delta_over_window(gh_points),
                "series_7d": [{"t": t, "v": v} for (t, v) in sorted(gh_points, key=lambda p: p[0])],
            },
            "discharge_cfs": {
                "latest_time": q_latest_t,
                "latest_value": q_latest_v,
                "earliest_time": q_earliest_t,
                "earliest_value": q_earliest_v,
                "delta_7d": delta_over_window(q_points),
                "series_7d": [{"t": t, "v": v} for (t, v) in sorted(q_points, key=lambda p: p[0])],
            },
        }

        if gh_latest_v is None:
            site_obj["gage_height_ft"]["note"] = "gage height missing in this pull, discharge may still be available"

        out["sites"][key] = site_obj

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUT_STATUS}")
    for k in out["sites"]:
        gh = out["sites"][k]["gage_height_ft"]
        print(k, "gh_latest", gh.get("latest_value"), "points", len(gh.get("series_7d", [])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

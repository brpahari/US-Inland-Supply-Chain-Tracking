#!/usr/bin/env python3
"""
river_monitor.py

USGS river gauge ingestion for Mississippi River
Fixes:
1. Outputs column-oriented JSON (t_utc, v) for dashboard charts.
2. Strictly separates Stage (ft) from Discharge (cfs).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any

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

def fetch_usgs_iv(site_no: str, start_dt_utc: datetime, parameter_cd: Optional[str]) -> dict:
    params = {
        "format": "json",
        "sites": site_no,
        "siteStatus": "all",
        "startDT": start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if parameter_cd:
        params["parameterCd"] = parameter_cd
    
    try:
        r = requests.get(USGS_IV_JSON, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"USGS fetch failed for {site_no}: {e}")
        return {}

def extract_points(ts: dict) -> List[Tuple[str, float]]:
    values = ts.get("values", []) or []
    if not values:
        return []
    arr = values[0].get("value", []) or []
    pts: List[Tuple[str, float]] = []
    for v in arr:
        t = v.get("dateTime")
        x = v.get("value")
        if t is None or x is None:
            continue
        try:
            # USGS time includes offset, keep as string for JS to parse or simple ISO
            pts.append((str(t), float(x)))
        except Exception:
            continue
    return pts

def get_series_stats(points: List[Tuple[str, float]]) -> Dict[str, Any]:
    if not points:
        return {}
    # sort by time
    pts = sorted(points, key=lambda p: p[0])
    
    latest = pts[-1]
    earliest = pts[0]
    
    # Calculate delta
    delta = latest[1] - earliest[1]

    # Columnar format for JS Chart.js performance
    t_utc = [p[0] for p in pts]
    v = [p[1] for p in pts]

    return {
        "latest_time": latest[0],
        "latest_value": latest[1],
        "earliest_time": earliest[0],
        "earliest_value": earliest[1],
        "delta_7d": delta,
        "series_7d": {
            "t_utc": t_utc,
            "v": v
        }
    }

def main() -> int:
    start_dt = datetime.now(timezone.utc) - timedelta(days=7, hours=6)

    out: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "source": {
            "provider": "USGS NWIS Instantaneous Values",
            "endpoint": USGS_IV_JSON,
        },
        "sites": {},
    }

    for key, meta in SITES.items():
        site_no = meta["site_no"]
        print(f"Processing {key} ({site_no})...")
        
        # 1. Fetch Stage and Discharge explicitly
        data = fetch_usgs_iv(site_no, start_dt, f"{PARAM_GAGE_HEIGHT},{PARAM_DISCHARGE}")
        time_series = data.get("value", {}).get("timeSeries", []) or []

        stage_pts = []
        flow_pts = []
        
        for ts in time_series:
            var = ts.get("variable", {})
            code = var.get("variableCode", [{}])[0].get("value")
            name = var.get("variableName", "").lower()
            
            pts = extract_points(ts)
            if not pts:
                continue

            # Strict routing
            if code == PARAM_GAGE_HEIGHT:
                stage_pts = pts
            elif code == PARAM_DISCHARGE:
                flow_pts = pts
            elif "gage height" in name or "stage" in name:
                # Fallback only if code didn't match but name does
                if not stage_pts: stage_pts = pts

        # 2. Build Site Object
        site_obj = {
            "site_no": site_no,
            "label": meta["label"],
            "gage_height_ft": get_series_stats(stage_pts),
            "discharge_cfs": get_series_stats(flow_pts)
        }
        
        # Add metadata for "missing" states
        if not stage_pts:
            site_obj["gage_height_ft"]["note"] = "Primary stage series missing"
        
        out["sites"][key] = site_obj

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUT_STATUS}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

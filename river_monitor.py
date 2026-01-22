#!/usr/bin/env python3
"""
river_monitor.py

USGS NWIS IV monitor for Mississippi River gauges.
Writes a compact 7 day series for charting plus summary stats.

Output
  data/river_status.json
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


USGS_IV_URL = "https://waterservices.usgs.gov/nwis/iv/"
PCODE_GAGE_HEIGHT_FT = "00065"
PCODE_DISCHARGE_CFS = "00060"

SITES = {
    "st_louis_mo": {
        "site_no": "07010000",
        "label": "Mississippi River at St. Louis, MO",
    },
    "memphis_tn": {
        "site_no": "07032000",
        "label": "Mississippi River at Memphis, TN",
    },
}

PERIOD = "P7D"
TIMEOUT = 40
MAX_SERIES_POINTS = 168  # 7 days hourly view


@dataclass
class Point:
    t: datetime
    v: float


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def fetch_iv_json(site_nos: List[str], pcodes: List[str]) -> Dict[str, Any]:
    params = {
        "format": "json",
        "sites": ",".join(site_nos),
        "parameterCd": ",".join(pcodes),
        "siteStatus": "all",
        "period": PERIOD,
    }
    r = requests.get(USGS_IV_URL, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_series(payload: Dict[str, Any]) -> Dict[Tuple[str, str], List[Point]]:
    out: Dict[Tuple[str, str], List[Point]] = {}
    ts_list = payload.get("value", {}).get("timeSeries", [])

    for ts in ts_list:
        src = ts.get("sourceInfo", {})
        site_no = None
        sc = src.get("siteCode", [])
        if sc and isinstance(sc, list):
            site_no = sc[0].get("value")

        var = ts.get("variable", {})
        pcode = None
        vc = var.get("variableCode", [])
        if vc and isinstance(vc, list):
            pcode = vc[0].get("value")

        if not site_no or not pcode:
            continue

        blocks = ts.get("values", [])
        if not blocks:
            continue

        pts: List[Point] = []
        for block in blocks:
            for row in block.get("value", []):
                t_raw = row.get("dateTime")
                v_raw = row.get("value")
                if not t_raw:
                    continue
                v = safe_float(v_raw)
                if v is None:
                    continue
                pts.append(Point(t=parse_time(t_raw), v=v))

        pts.sort(key=lambda p: p.t)
        if pts:
            out[(site_no, pcode)] = pts

    return out


def downsample(points: List[Point], max_points: int) -> List[Point]:
    if len(points) <= max_points:
        return points
    step = (len(points) - 1) / (max_points - 1)
    idxs = []
    for i in range(max_points):
        idxs.append(min(int(round(i * step)), len(points) - 1))
    uniq = []
    seen = set()
    for i in idxs:
        if i not in seen:
            seen.add(i)
            uniq.append(i)
    return [points[i] for i in uniq]


def summarize(points: List[Point]) -> Dict[str, Any]:
    earliest = points[0]
    latest = points[-1]
    delta = latest.v - earliest.v
    days = max((latest.t - earliest.t).total_seconds() / 86400.0, 1e-9)
    slope = delta / days
    return {
        "earliest_utc": earliest.t.isoformat().replace("+00:00", "Z"),
        "earliest_value": earliest.v,
        "latest_utc": latest.t.isoformat().replace("+00:00", "Z"),
        "latest_value": latest.v,
        "delta_7d": delta,
        "slope_per_day": slope,
        "n_points": len(points),
    }


def build_series(points: List[Point]) -> Dict[str, Any]:
    ds = downsample(points, MAX_SERIES_POINTS)
    return {
        "n_points_raw": len(points),
        "n_points": len(ds),
        "t_utc": [p.t.isoformat().replace("+00:00", "Z") for p in ds],
        "v": [p.v for p in ds],
    }


def main() -> int:
    site_nos = [SITES[k]["site_no"] for k in SITES]
    pcodes = [PCODE_GAGE_HEIGHT_FT, PCODE_DISCHARGE_CFS]

    payload = fetch_iv_json(site_nos, pcodes)
    series = extract_series(payload)

    out: Dict[str, Any] = {
        "generated_at_utc": utc_now_iso(),
        "period": PERIOD,
        "source": {
            "provider": "USGS Water Services NWIS IV",
            "endpoint": USGS_IV_URL,
            "parameter_codes": {
                "gage_height_ft": PCODE_GAGE_HEIGHT_FT,
                "discharge_cfs": PCODE_DISCHARGE_CFS,
            },
        },
        "sites": {},
    }

    for key, meta in SITES.items():
        site_no = meta["site_no"]
        site_block: Dict[str, Any] = {
            "site_no": site_no,
            "label": meta["label"],
            "gage_height_ft": None,
            "discharge_cfs": None,
        }

        gh = series.get((site_no, PCODE_GAGE_HEIGHT_FT))
        if gh:
            site_block["gage_height_ft"] = summarize(gh)
            site_block["gage_height_ft"]["series_7d"] = build_series(gh)

        q = series.get((site_no, PCODE_DISCHARGE_CFS))
        if q:
            site_block["discharge_cfs"] = summarize(q)
            site_block["discharge_cfs"]["series_7d"] = build_series(q)

        out["sites"][key] = site_block

    os.makedirs("data", exist_ok=True)
    with open("data/river_status.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print("Wrote data/river_status.json")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.RequestException as e:
        print(f"Network error {e}", file=sys.stderr)
        raise SystemExit(3)
    except Exception as e:
        print(f"Unexpected error {e}", file=sys.stderr)
        raise SystemExit(1)

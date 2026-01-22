#!/usr/bin/env python3
"""
river_monitor.py

Fetch USGS instantaneous values for Mississippi River gauges and compute
simple early warning features.

Outputs
  data/river_status.json

Data source
  USGS Water Services Instantaneous Values (iv) JSON
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

# USGS parameter codes
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

DEFAULT_PERIOD = "P7D"  # last 7 days
DEFAULT_TIMEOUT = 30


@dataclass
class Point:
    t: datetime
    v: float


def _parse_usgs_time(ts: str) -> datetime:
    # Example "2026-01-21T18:30:00.000-06:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def fetch_iv_json(sites: List[str], pcodes: List[str], period: str = DEFAULT_PERIOD) -> Dict[str, Any]:
    params = {
        "format": "json",
        "sites": ",".join(sites),
        "parameterCd": ",".join(pcodes),
        "siteStatus": "all",
        "period": period,
    }
    r = requests.get(USGS_IV_URL, params=params, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_series(payload: Dict[str, Any]) -> Dict[Tuple[str, str], List[Point]]:
    """
    Returns mapping (site_no, pcode) -> sorted list of Points in UTC.
    """
    out: Dict[Tuple[str, str], List[Point]] = {}
    ts_list = payload.get("value", {}).get("timeSeries", [])
    for ts in ts_list:
        source = ts.get("sourceInfo", {})
        site_no = None
        site_codes = source.get("siteCode", [])
        if site_codes and isinstance(site_codes, list):
            site_no = site_codes[0].get("value")
        var = ts.get("variable", {})
        pcode = None
        vcodes = var.get("variableCode", [])
        if vcodes and isinstance(vcodes, list):
            pcode = vcodes[0].get("value")

        if not site_no or not pcode:
            continue

        values_block = ts.get("values", [])
        if not values_block:
            continue

        points: List[Point] = []
        for block in values_block:
            for row in block.get("value", []):
                t_raw = row.get("dateTime")
                v_raw = row.get("value")
                if not t_raw:
                    continue
                v = _safe_float(v_raw)
                if v is None:
                    continue
                points.append(Point(t=_parse_usgs_time(t_raw), v=v))

        points.sort(key=lambda p: p.t)
        if points:
            out[(site_no, pcode)] = points

    return out


def summarize(points: List[Point]) -> Dict[str, Any]:
    """
    Simple features for a 7 day window.
    delta_7d uses the earliest available point in the window.
    """
    latest = points[-1]
    earliest = points[0]
    delta = latest.v - earliest.v
    # A crude slope per day based on the endpoints
    days = max((latest.t - earliest.t).total_seconds() / 86400.0, 1e-9)
    slope_per_day = delta / days
    return {
        "latest_utc": latest.t.isoformat().replace("+00:00", "Z"),
        "latest_value": latest.v,
        "earliest_utc": earliest.t.isoformat().replace("+00:00", "Z"),
        "earliest_value": earliest.v,
        "delta_7d": delta,
        "slope_per_day": slope_per_day,
        "n_points": len(points),
    }


def main() -> int:
    site_nos = [SITES[k]["site_no"] for k in SITES]
    pcodes = [PCODE_GAGE_HEIGHT_FT, PCODE_DISCHARGE_CFS]

    payload = fetch_iv_json(site_nos, pcodes, period=DEFAULT_PERIOD)
    series = extract_series(payload)

    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    out: Dict[str, Any] = {
        "generated_at_utc": now_utc,
        "period": DEFAULT_PERIOD,
        "source": {
            "provider": "USGS Water Services NWIS IV",
            "endpoint": USGS_IV_URL,
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

        q = series.get((site_no, PCODE_DISCHARGE_CFS))
        if q:
            site_block["discharge_cfs"] = summarize(q)

        out["sites"][key] = site_block

    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "river_status.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=False)

    print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as e:
        print(f"HTTP error {e}", file=sys.stderr)
        return_code = 2
        raise SystemExit(return_code)
    except requests.RequestException as e:
        print(f"Network error {e}", file=sys.stderr)
        return_code = 3
        raise SystemExit(return_code)
    except Exception as e:
        print(f"Unexpected error {e}", file=sys.stderr)
        return_code = 1
        raise SystemExit(return_code)

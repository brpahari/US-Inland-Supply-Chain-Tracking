#!/usr/bin/env python3
"""
river_monitor.py

Fetch USGS instantaneous values for Mississippi River gauges and compute
early warning features plus a downsampled 7 day series for visualization.

Outputs
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

DEFAULT_PERIOD = "P7D"
DEFAULT_TIMEOUT = 30

MAX_SERIES_POINTS = 96  # per metric per site, keeps JSON light


@dataclass
class Point:
    t: datetime
    v: float


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_usgs_time(ts: str) -> datetime:
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


def downsample(points: List[Point], max_points: int = MAX_SERIES_POINTS) -> List[Point]:
    if len(points) <= max_points:
        return points

    step = (len(points) - 1) / (max_points - 1)
    keep_idx = []
    for i in range(max_points):
        idx = int(round(i * step))
        keep_idx.append(min(idx, len(points) - 1))

    # de dup in case rounding repeats
    uniq = []
    seen = set()
    for idx in keep_idx:
        if idx not in seen:
            seen.add(idx)
            uniq.append(idx)

    return [points[i] for i in uniq]


def summarize(points: List[Point]) -> Dict[str, Any]:
    latest = points[-1]
    earliest = points[0]
    delta = latest.v - earliest.v
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

    payload = fetch_iv_json(site_nos, pcodes, period=DEFAULT_PERIOD)
    series = extract_series(payload)

    out: Dict[str, Any] = {
        "generated_at_utc": utc_now_iso(),
        "period": DEFAULT_PERIOD,
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
        raise SystemExit(2)
    except requests.RequestException as e:
        print(f"Network error {e}", file=sys.stderr)
        raise SystemExit(3)
    except Exception as e:
        print(f"Unexpected error {e}", file=sys.stderr)
        raise SystemExit(1)

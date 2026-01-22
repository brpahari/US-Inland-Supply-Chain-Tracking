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


def fetch_usgs_iv(site_no: str, start_dt_utc: datetime, parameter_cd: Optional[str]) -> dict:
    params = {
        "format": "json",
        "sites": site_no,
        "siteStatus": "all",
        "startDT": start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if parameter_cd:
        params["parameterCd"] = parameter_cd
    r = requests.get(USGS_IV_JSON, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_site_no(ts: dict) -> Optional[str]:
    source_info = ts.get("sourceInfo", {}) or {}
    for ident in source_info.get("siteCode", []) or []:
        if ident.get("value"):
            return ident.get("value")
    return None


def extract_param_code(ts: dict) -> Optional[str]:
    var = ts.get("variable", {}) or {}
    vc = var.get("variableCode", []) or []
    if not vc:
        return None
    return vc[0].get("value")


def extract_var_name(ts: dict) -> str:
    var = ts.get("variable", {}) or {}
    return str(var.get("variableName") or "")


def extract_unit(ts: dict) -> str:
    var = ts.get("variable", {}) or {}
    u = var.get("unit", {}) or {}
    return str(u.get("unitCode") or "")


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
            fx = float(x)
        except Exception:
            continue
        pts.append((t, fx))
    return pts


def choose_stage_series(time_series: List[dict]) -> Optional[Tuple[str, List[Tuple[str, float]], str]]:
    """
    Returns selected parameter code, points, and a note explaining selection
    """
    candidates: List[Tuple[int, str, str, str, List[Tuple[str, float]]]] = []
    for ts in time_series:
        p = extract_param_code(ts) or ""
        name = extract_var_name(ts).lower()
        unit = extract_unit(ts).lower()
        pts = extract_points(ts)

        if not pts:
            continue

        score = 0

        if "gage height" in name or "stage" in name:
            score += 50
        if unit in ["ft", "feet"]:
            score += 25
        if p == PARAM_GAGE_HEIGHT:
            score += 30

        candidates.append((score, p, name, unit, pts))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    best = candidates[-1]
    score, p, name, unit, pts = best
    note = "auto selected stage series"
    return p, pts, note


def choose_discharge_series(time_series: List[dict]) -> Optional[List[Tuple[str, float]]]:
    for ts in time_series:
        p = extract_param_code(ts)
        if p == PARAM_DISCHARGE:
            pts = extract_points(ts)
            if pts:
                return pts
    for ts in time_series:
        name = extract_var_name(ts).lower()
        unit = extract_unit(ts).lower()
        if "discharge" in name and ("ft3" in unit or "cfs" in unit or unit == "ft3/s"):
            pts = extract_points(ts)
            if pts:
                return pts
    return None


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


def build_site(site_key: str, meta: dict, start_dt: datetime) -> Dict[str, object]:
    site_no = meta["site_no"]
    label = meta["label"]

    gh_points: List[Tuple[str, float]] = []
    q_points: List[Tuple[str, float]] = []
    gh_note: Optional[str] = None
    selected_param: Optional[str] = None

    payload = fetch_usgs_iv(site_no, start_dt, f"{PARAM_GAGE_HEIGHT},{PARAM_DISCHARGE}")
    ts_list = payload.get("value", {}).get("timeSeries", []) or []

    for ts in ts_list:
        p = extract_param_code(ts)
        pts = extract_points(ts)
        if not pts:
            continue
        if p == PARAM_GAGE_HEIGHT:
            gh_points = pts
        if p == PARAM_DISCHARGE:
            q_points = pts

    if not gh_points:
        payload2 = fetch_usgs_iv(site_no, start_dt, None)
        ts_list2 = payload2.get("value", {}).get("timeSeries", []) or []
        chosen = choose_stage_series(ts_list2)
        if chosen:
            selected_param, gh_points, gh_note = chosen

        q_fallback = choose_discharge_series(ts_list2)
        if q_fallback:
            q_points = q_fallback

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

    if gh_note:
        site_obj["gage_height_ft"]["note"] = gh_note
    if selected_param and selected_param != PARAM_GAGE_HEIGHT:
        site_obj["gage_height_ft"]["selected_parameter_cd"] = selected_param
    if not gh_points:
        site_obj["gage_height_ft"]["note"] = "no stage series returned for this site and window"

    return site_obj


def main() -> int:
    start_dt = datetime.now(timezone.utc) - timedelta(days=7, hours=6)

    out: Dict[str, object] = {
        "generated_at_utc": utc_now_iso(),
        "source": {
            "provider": "USGS NWIS Instantaneous Values",
            "endpoint": USGS_IV_JSON,
            "start_dt_utc": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "sites": {},
    }

    for k, meta in SITES.items():
        out["sites"][k] = build_site(k, meta, start_dt)

    os.makedirs("data", exist_ok=True)
    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUT_STATUS}")
    for k in out["sites"]:
        pts = out["sites"][k]["gage_height_ft"].get("series_7d") or []
        lv = out["sites"][k]["gage_height_ft"].get("latest_value")
        print(k, "stage_latest", lv, "points", len(pts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
generate_risk.py

Inputs: data/river_status.json, data/rail_status.json, data/barge_status.json
Outputs: 
  1. data/composite_risk_score.json (Snapshot)
  2. data/history/risk_daily.csv (Time Series)
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

OUT_RISK = "data/composite_risk_score.json"
OUT_RISK_HIST = "data/history/risk_daily.csv"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def load_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def append_risk_history(risk_score: float, risk_level: str, primary_driver: str):
    """Appends to CSV if the last entry is not from today (simple daily dedup)."""
    os.makedirs(os.path.dirname(OUT_RISK_HIST), exist_ok=True)
    
    # Check last entry to avoid duplicate rows for the same timestamp/run
    # For a refined daily feed, we often just check if 'today' is already there.
    # Here we will just append with full timestamp for granularity.
    
    now_ts = utc_now_iso()
    file_exists = os.path.exists(OUT_RISK_HIST)
    
    with open(OUT_RISK_HIST, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp_utc", "risk_score", "risk_level", "primary_driver"])
        
        writer.writerow([now_ts, risk_score, risk_level, primary_driver])

def main() -> int:
    river = load_json("data/river_status.json") or {}
    rail = load_json("data/rail_status.json") or {}
    barge = load_json("data/barge_status.json") or {}

    drivers: List[Dict[str, Any]] = []

    # -- 1. RIVER --
    river_score = 0.0
    delta_7d = 0.0
    latest_stage = None
    stl = river.get("sites", {}).get("st_louis_mo", {})
    gh = stl.get("gage_height_ft", {}) if isinstance(stl, dict) else {}
    if isinstance(gh, dict):
        delta_7d = float(gh.get("delta_7d") or 0.0)
        latest_stage = gh.get("latest_value")

    if delta_7d < -2.0: river_score += 20.0
    if latest_stage is not None and float(latest_stage) < 0.0: river_score += 20.0
    
    drivers.append({
        "name": "river", "score": river_score, 
        "raw": {"delta_7d_ft": delta_7d, "latest_stage_ft": latest_stage}
    })

    # -- 2. RAIL --
    rail_score = 0.0
    up_dwell_delta = 0.0
    up = rail.get("carriers", {}).get("UP", {})
    if isinstance(up, dict):
        dw = up.get("metrics", {}).get("terminal_dwell_hours", {})
        if isinstance(dw, dict):
            up_dwell_delta = float(dw.get("delta_4w") or 0.0)

    if up_dwell_delta > 2.0: rail_score += 30.0
    elif up_dwell_delta > 0.5: rail_score += 15.0

    drivers.append({
        "name": "rail", "score": rail_score, 
        "raw": {"up_dwell_delta_4w_hours": up_dwell_delta}
    })

    # -- 3. BARGE --
    barge_score = 0.0
    locks_delta = 0.0
    locks = barge.get("locks_27", {})
    if isinstance(locks, dict):
        locks_delta = float(locks.get("delta_4w") or 0.0)

    # Logic for COUNT (Barges), not TONS
    if locks_delta < -50.0: barge_score += 30.0
    elif locks_delta < -20.0: barge_score += 15.0

    drivers.append({
        "name": "barge", "score": barge_score, 
        "raw": {"locks27_delta_4w_count": locks_delta}
    })

    # -- COMPOSITE --
    total_score = clamp(river_score + rail_score + barge_score, 0.0, 100.0)
    level = "LOW"
    if total_score > 70: level = "CRITICAL"
    elif total_score > 40: level = "MODERATE"

    primary = "none"
    if drivers:
        primary = max(drivers, key=lambda d: d["score"])["name"]

    out = {
        "generated_at_utc": utc_now_iso(),
        "risk_score": total_score,
        "risk_level": level,
        "primary_driver": primary,
        "drivers": drivers,
    }

    # Write Snapshot
    changed = write_if_changed(OUT_RISK, out)
    
    # Write History (Always append if it's a new run, or logic to dedup)
    # Here we append so we can see the chart grow.
    append_risk_history(total_score, level, primary)

    if changed:
        print(f"Risk Score Updated: {total_score} ({level})")
    else:
        print(f"Risk Score Unchanged: {total_score} ({level})")
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

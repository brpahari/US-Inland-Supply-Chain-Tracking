#!/usr/bin/env python3
"""
generate_risk.py

Inputs
  data/river_status.json
  data/rail_status.json
  data/barge_status.json

Output
  data/composite_risk_score.json
  (Optionally appends to data/history/risk_daily.csv if you add that logic later)

Logic Update:
  - Barge thresholds calibrated for 'Count' (dropping by 20-50 barges) 
    instead of 'Tons' (dropping by 15k-50k tons).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List


OUT_RISK = "data/composite_risk_score.json"


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


def main() -> int:
    river = load_json("data/river_status.json") or {}
    rail = load_json("data/rail_status.json") or {}
    barge = load_json("data/barge_status.json") or {}

    drivers: List[Dict[str, Any]] = []

    # ---------------------------------------------------------
    # 1. RIVER DRIVER (Weight: ~40 pts)
    # ---------------------------------------------------------
    river_score = 0.0
    delta_7d = 0.0
    latest_stage = None
    
    # Navigating the new river_status.json structure
    stl = river.get("sites", {}).get("st_louis_mo", {})
    gh = stl.get("gage_height_ft", {}) if isinstance(stl, dict) else {}
    
    if isinstance(gh, dict):
        delta_7d = float(gh.get("delta_7d") or 0.0)
        latest_stage = gh.get("latest_value")

    # Risk Logic:
    # 1. Fast Drop: > 2ft drop in 7 days is dangerous for loading drafts
    if delta_7d < -2.0:
        river_score += 20.0
    # 2. Low Water: Stage below 0.0 (gauge zero) triggers strict draft reductions
    if latest_stage is not None and float(latest_stage) < 0.0:
        river_score += 20.0

    drivers.append({
        "name": "river",
        "score": river_score,
        "raw": {
            "delta_7d_ft": delta_7d,
            "latest_stage_ft": latest_stage
        }
    })

    # ---------------------------------------------------------
    # 2. RAIL DRIVER (Weight: ~30 pts)
    # ---------------------------------------------------------
    rail_score = 0.0
    up_dwell_delta = 0.0
    
    up = rail.get("carriers", {}).get("UP", {})
    if isinstance(up, dict):
        dw = up.get("metrics", {}).get("terminal_dwell_hours", {})
        if isinstance(dw, dict):
            up_dwell_delta = float(dw.get("delta_4w") or 0.0)

    # Risk Logic:
    # UP Dwell Time rising is a classic congestion signal.
    # +2 hours is a major slowdown. +0.5 hours is a warning.
    if up_dwell_delta > 2.0:
        rail_score += 30.0
    elif up_dwell_delta > 0.5:
        rail_score += 15.0

    drivers.append({
        "name": "rail",
        "score": rail_score,
        "raw": {
            "up_dwell_delta_4w_hours": up_dwell_delta
        }
    })

    # ---------------------------------------------------------
    # 3. BARGE DRIVER (Weight: ~30 pts)
    # ---------------------------------------------------------
    # UPDATED: Thresholds now tuned for "Barge Count" (approx 100-400 range)
    # rather than "Tons" (approx 150k-500k range).
    barge_score = 0.0
    locks_delta = 0.0
    
    locks = barge.get("locks_27", {})
    if isinstance(locks, dict):
        locks_delta = float(locks.get("delta_4w") or 0.0)

    # Risk Logic:
    # A drop of 50 barges/week represents ~15-20% of capacity vanishing.
    if locks_delta < -50.0:
        barge_score += 30.0
    elif locks_delta < -20.0:
        barge_score += 15.0

    drivers.append({
        "name": "barge",
        "score": barge_score,
        "raw": {
            "locks27_delta_4w_count": locks_delta
        }
    })

    # ---------------------------------------------------------
    # COMPOSITE SCORE
    # ---------------------------------------------------------
    total_score = river_score + rail_score + barge_score
    total_score = clamp(total_score, 0.0, 100.0)

    level = "LOW"
    if total_score > 70:
        level = "CRITICAL"
    elif total_score > 40:
        level = "MODERATE"

    # Identify the biggest contributor
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

    if write_if_changed(OUT_RISK, out):
        print(f"Risk Score Updated: {total_score} ({level}) - Driver: {primary}")
    else:
        print(f"Risk Score Unchanged: {total_score} ({level})")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

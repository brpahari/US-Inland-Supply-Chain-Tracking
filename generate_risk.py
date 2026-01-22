#!/usr/bin/env python3
"""
generate_risk.py - Production Version
"""
from __future__ import annotations
import csv
import json
import os
from datetime import datetime, timezone

OUT_RISK = "data/composite_risk_score.json"
OUT_RISK_HIST = "data/history/risk_daily.csv"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def load_json(path):
    if not os.path.exists(path): return {}
    with open(path, "r") as f: return json.load(f)

def append_risk_history(score, level, driver):
    os.makedirs(os.path.dirname(OUT_RISK_HIST), exist_ok=True)
    today_str = utc_now_iso()[:10]
    
    # Check if we already logged today to prevent duplicates
    if os.path.exists(OUT_RISK_HIST):
        with open(OUT_RISK_HIST, "r") as f:
            lines = f.readlines()
            if lines and lines[-1].startswith(today_str):
                return 

    with open(OUT_RISK_HIST, "a", newline="") as f:
        w = csv.writer(f)
        if not os.path.exists(OUT_RISK_HIST) or os.path.getsize(OUT_RISK_HIST) == 0:
            w.writerow(["timestamp_utc", "risk_score", "risk_level", "primary_driver"])
        w.writerow([utc_now_iso(), score, level, driver])

def main():
    river = load_json("data/river_status.json")
    rail = load_json("data/rail_status.json")
    barge = load_json("data/barge_status.json")

    drivers = []
    
    # 1. RIVER (Weight 40)
    r_score = 0
    stl = river.get("sites", {}).get("st_louis_mo", {}).get("gage_height_ft", {})
    delta = float(stl.get("delta_7d") or 0)
    val = stl.get("latest_value")
    
    if delta < -2.0: r_score += 20
    if val is not None and float(val) < 0.0: r_score += 20
    if r_score > 0: drivers.append({"name": "river", "score": r_score})

    # 2. RAIL (Weight 30)
    rr_score = 0
    up = rail.get("carriers", {}).get("UP", {}).get("metrics", {}).get("terminal_dwell_hours", {})
    up_delta = float(up.get("delta_4w") or 0)
    
    if up_delta > 2.0: rr_score += 30
    elif up_delta > 0.5: rr_score += 15
    if rr_score > 0: drivers.append({"name": "rail", "score": rr_score})

    # 3. BARGE (Weight 30)
    b_score = 0
    l27 = barge.get("locks_27", {})
    l27_delta = float(l27.get("delta_4w") or 0)
    
    # COUNT THRESHOLDS (Real Data Logic)
    if l27_delta < -50: b_score += 30
    elif l27_delta < -20: b_score += 15
    if b_score > 0: drivers.append({"name": "barge", "score": b_score})

    # Composite
    total = min(100, r_score + rr_score + b_score)
    level = "LOW"
    if total > 70: level = "CRITICAL"
    elif total > 40: level = "MODERATE"

    primary = "none"
    if drivers:
        primary = max(drivers, key=lambda x: x["score"])["name"]

    out = {
        "generated_at_utc": utc_now_iso(),
        "risk_score": total,
        "risk_level": level,
        "primary_driver": primary,
        "drivers": drivers
    }

    with open(OUT_RISK, "w") as f:
        json.dump(out, f, indent=2)
    
    append_risk_history(total, level, primary)
    print(f"Risk Score: {total} ({level})")

if __name__ == "__main__":
    main()

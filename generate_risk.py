#!/usr/bin/env python3
"""
generate_risk.py

Reads
  data/river_status.json
  data/rail_status.json
  data/barge_status.json

Writes
  data/composite_risk_score.json
  data/history/risk_daily.csv
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


OUT_RISK = "data/composite_risk_score.json"
OUT_RISK_HIST = "data/history/risk_daily.csv"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def append_risk_history(ts_utc: str, risk_score: float, risk_level: str, primary_driver: str) -> None:
    os.makedirs(os.path.dirname(OUT_RISK_HIST), exist_ok=True)
    exists = os.path.exists(OUT_RISK_HIST)
    with open(OUT_RISK_HIST, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["generated_at_utc", "risk_score", "risk_level", "primary_driver"])
        w.writerow([ts_utc, f"{risk_score:.2f}", risk_level, primary_driver])


def main() -> int:
    river = load_json("data/river_status.json") or {}
    rail = load_json("data/rail_status.json") or {}
    barge = load_json("data/barge_status.json") or {}

    drivers = []
    total = 0.0

    # River driver 0 to 40
    river_score = 0.0
    stl = (river.get("sites", {}).get("st_louis_mo", {}) or {}).get("gage_height_ft", {}) or {}
    delta_7d = stl.get("delta_7d")
    latest_stage = stl.get("latest_value")

    if isinstance(delta_7d, (int, float)):
        if delta_7d < -2.0:
            river_score += 20
        if delta_7d < -4.0:
            river_score += 10
    if isinstance(latest_stage, (int, float)):
        if latest_stage < 0.0:
            river_score += 10

    river_score = clamp(river_score, 0.0, 40.0)
    total += river_score
    drivers.append({
        "name": "river",
        "score": river_score,
        "raw": {
            "delta_7d_ft": delta_7d,
            "latest_stage_ft": latest_stage
        }
    })

    # Rail driver 0 to 30
    rail_score = 0.0
    up = (((rail.get("carriers", {}) or {}).get("UP", {}) or {}).get("metrics", {}) or {}).get("terminal_dwell_hours", {}) or {}
    up_dwell_delta = up.get("delta_4w")
    if isinstance(up_dwell_delta, (int, float)):
        if up_dwell_delta > 2.0:
            rail_score += 30
        elif up_dwell_delta > 0.5:
            rail_score += 15
    rail_score = clamp(rail_score, 0.0, 30.0)
    total += rail_score
    drivers.append({
        "name": "rail",
        "score": rail_score,
        "raw": {
            "up_dwell_delta_4w_hours": up_dwell_delta
        }
    })

    # Barge driver 0 to 30
    barge_score = 0.0
    l27 = (barge.get("locks_27", {}) or {})
    l27_delta = l27.get("delta_4w")
    if isinstance(l27_delta, (int, float)):
        if l27_delta < -50000:
            barge_score += 30
        elif l27_delta < -15000:
            barge_score += 15
    barge_score = clamp(barge_score, 0.0, 30.0)
    total += barge_score
    drivers.append({
        "name": "barge",
        "score": barge_score,
        "raw": {
            "locks27_delta_4w_tons": l27_delta
        }
    })

    risk_score = clamp(total, 0.0, 100.0)
    level = "LOW"
    if risk_score > 40:
        level = "MODERATE"
    if risk_score > 70:
        level = "CRITICAL"

    primary = "none"
    if drivers:
        primary = sorted(drivers, key=lambda d: d["score"], reverse=True)[0]["name"]

    out = {
        "generated_at_utc": utc_now_iso(),
        "risk_score": risk_score,
        "risk_level": level,
        "primary_driver": primary,
        "drivers": drivers
    }

    os.makedirs("data", exist_ok=True)
    with open(OUT_RISK, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    append_risk_history(out["generated_at_utc"], risk_score, level, primary)

    print(f"Risk Score {risk_score:.2f} {level} driver {primary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

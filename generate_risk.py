#!/usr/bin/env python3
"""
generate_risk.py
Inputs
  data/river_status.json
  data/rail_status.json
  data/barge_status.json
Output
  data/composite_risk_score.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


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


def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def main() -> int:
    river = load_json("data/river_status.json") or {}
    rail = load_json("data/rail_status.json") or {}
    barge = load_json("data/barge_status.json") or {}

    drivers = []

    # River component
    river_score = 0.0
    delta_7d = 0.0
    latest_stage = None
    stl = river.get("sites", {}).get("st_louis_mo", {})
    gh = stl.get("gage_height_ft", {}) if isinstance(stl, dict) else {}
    if isinstance(gh, dict):
        delta_7d = float(gh.get("delta_7d", 0.0) or 0.0)
        latest_stage = gh.get("latest_value", None)

    # Thresholds are placeholders, tune using history later
    if delta_7d < -2.0:
        river_score += 20.0
    if latest_stage is not None and float(latest_stage) < 0.0:
        river_score += 20.0

    drivers.append({"name": "river", "score": river_score, "raw": {"delta_7d_ft": delta_7d, "latest_stage_ft": latest_stage}})

    # Rail component
    rail_score = 0.0
    up_dwell_delta = 0.0
    up = rail.get("carriers", {}).get("UP", {})
    if isinstance(up, dict):
        dw = up.get("metrics", {}).get("terminal_dwell_hours", {})
        if isinstance(dw, dict):
            up_dwell_delta = float(dw.get("delta_4w", 0.0) or 0.0)

    if up_dwell_delta > 2.0:
        rail_score += 30.0
    elif up_dwell_delta > 0.5:
        rail_score += 15.0

    drivers.append({"name": "rail", "score": rail_score, "raw": {"up_dwell_delta_4w_hours": up_dwell_delta}})

    # Barge component
    barge_score = 0.0
    locks_delta = 0.0
    rates_delta = 0.0
    locks = barge.get("locks_27", {})
    if isinstance(locks, dict):
        locks_delta = float(locks.get("delta_4w", 0.0) or 0.0)

    rates = barge.get("rates", {})
    if isinstance(rates, dict):
        rates_delta = float(rates.get("delta_4w", 0.0) or 0.0)

    # Use a simple divergence heuristic
    # Falling movements plus rising rates is bad
    if locks_delta < -50.0 and rates_delta > 5.0:
        barge_score += 30.0
    elif locks_delta < -25.0:
        barge_score += 15.0

    drivers.append({"name": "barge", "score": barge_score, "raw": {"locks27_delta_4w_barges": locks_delta, "rate_delta_4w_usd_per_ton": rates_delta}})

    score = clip(river_score + rail_score + barge_score, 0.0, 100.0)

    level = "LOW"
    if score > 70:
        level = "CRITICAL"
    elif score > 40:
        level = "MODERATE"

    primary = max(drivers, key=lambda d: d["score"])["name"] if drivers else "none"

    out = {
        "generated_at_utc": utc_now_iso(),
        "risk_score": score,
        "risk_level": level,
        "primary_driver": primary,
        "drivers": drivers,
    }

    write_if_changed(OUT_RISK, out)
    print(f"Risk {score} {level} driver {primary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

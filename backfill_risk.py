#!/usr/bin/env python3
"""
backfill_risk.py
Reconstructs the last 90 days of Risk Scores using:
1. Real historical River data (USGS Daily Values API) with robust fallbacks.
2. Your local Rail/Barge history CSVs.
"""

import pandas as pd
import requests
import os
import csv
from datetime import datetime, timedelta, timezone

# --- CONFIG ---
DAYS_BACK = 90
ST_LOUIS_SITE = "07010000"
OUT_FILE = "data/history/risk_daily.csv"
RAIL_FILE = "data/history/rail_weekly.csv"
BARGE_FILE = "data/history/barge_locks27_weekly.csv"

def fetch_river_history():
    """
    Get daily stage for St. Louis.
    Robustness: Tries Mean (00003), then Min (00002), then Max (00001).
    St. Louis often does not publish Mean stage, only Min/Max.
    """
    print("Fetching USGS river history...")
    url = "https://waterservices.usgs.gov/nwis/dv/"
    
    # Priority: Mean -> Min (Conservative for Low Water) -> Max
    stats_to_try = ["00003", "00002", "00001"]
    
    for stat in stats_to_try:
        params = {
            "format": "json",
            "sites": ST_LOUIS_SITE,
            "period": f"P{DAYS_BACK + 20}D", # Extra buffer
            "parameterCd": "00065", # Gage Height
            "statCd": stat
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                continue
            
            data = r.json()
            # Check if timeSeries list is empty (The cause of previous crash)
            if not data.get('value', {}).get('timeSeries'):
                continue
                
            # Success
            print(f"  Success: Found data using statCd={stat}")
            hist = {}
            ts = data['value']['timeSeries'][0]['values'][0]['value']
            for p in ts:
                # p['value'] is string "12.34", parse to float
                hist[p['dateTime'][:10]] = float(p['value'])
            return hist
            
        except Exception as e:
            print(f"  Error fetching stat {stat}: {e}")
            continue
            
    print("WARNING: Could not fetch river history from USGS (all stats failed). Risk scores will lack river component.")
    return {}

def get_as_of(df, date_str, val_col):
    """Find the latest known value on or before date_str"""
    if df.empty: return 0.0
    # Filter to rows existing by that date
    past = df[df['week_end_date'] <= date_str]
    if past.empty: return float(df.iloc[0][val_col]) # Fallback to oldest
    return float(past.iloc[-1][val_col])

def compute_daily_risk(target_date, river_hist, rail_df, barge_df):
    date_s = target_date.strftime("%Y-%m-%d")
    
    # 1. RIVER INPUTS
    # Current Level
    r_val = river_hist.get(date_s)
    
    # 7-Day Delta
    prev_dt = target_date - timedelta(days=7)
    r_prev = river_hist.get(prev_dt.strftime("%Y-%m-%d"))
    
    if r_val is not None and r_prev is not None:
        r_delta = r_val - r_prev
    else:
        r_delta = 0.0
    
    # 2. RAIL INPUTS (4-week Delta)
    # Look up what we knew on that date vs 28 days prior
    curr_rail = get_as_of(rail_df, date_s, 'terminal_dwell_hours')
    past_rail = get_as_of(rail_df, (target_date - timedelta(days=28)).strftime("%Y-%m-%d"), 'terminal_dwell_hours')
    rail_delta = curr_rail - past_rail

    # 3. BARGE INPUTS (4-week Delta)
    curr_barge = get_as_of(barge_df, date_s, 'total_barges')
    past_barge = get_as_of(barge_df, (target_date - timedelta(days=28)).strftime("%Y-%m-%d"), 'total_barges')
    barge_delta = curr_barge - past_barge

    # --- SCORING LOGIC (Matches generate_risk.py) ---
    drivers = []
    
    # River Score
    r_score = 0
    # Only score river if we successfully fetched data
    if r_val is not None:
        if r_delta < -2.0: r_score += 20
        if r_val < 0.0: r_score += 20
        if r_score > 0: drivers.append(("river", r_score))

    # Rail Score
    rr_score = 0
    if rail_delta > 2.0: rr_score += 30
    elif rail_delta > 0.5: rr_score += 15
    if rr_score > 0: drivers.append(("rail", rr_score))

    # Barge Score (Count Logic)
    b_score = 0
    if barge_delta < -50: b_score += 30
    elif barge_delta < -20: b_score += 15
    if b_score > 0: drivers.append(("barge", b_score))

    total = min(100, r_score + rr_score + b_score)
    
    level = "LOW"
    if total > 70: level = "CRITICAL"
    elif total > 40: level = "MODERATE"

    primary = "none"
    if drivers:
        primary = max(drivers, key=lambda x: x[1])[0]

    return total, level, primary

def main():
    # Load History
    river_hist = fetch_river_history()
    
    rail_df = pd.DataFrame()
    if os.path.exists(RAIL_FILE):
        try:
            rail_df = pd.read_csv(RAIL_FILE)
            # Filter UP only
            if 'carrier' in rail_df.columns:
                rail_df = rail_df[rail_df['carrier'] == 'UP'].sort_values('week_end_date')
        except Exception as e:
            print(f"Warning reading rail file: {e}")

    barge_df = pd.DataFrame()
    if os.path.exists(BARGE_FILE):
        try:
            barge_df = pd.read_csv(BARGE_FILE).sort_values('week_end_date')
            # Ensure column name compatibility (legacy 'tons' vs new 'barges')
            if 'total_barges' not in barge_df.columns and 'total_tons' in barge_df.columns:
                barge_df['total_barges'] = barge_df['total_tons']
        except Exception as e:
             print(f"Warning reading barge file: {e}")

    # Generate
    rows = []
    print(f"Backfilling last {DAYS_BACK} days...")
    
    today = datetime.now()
    for i in range(DAYS_BACK):
        d = today - timedelta(days=(DAYS_BACK - i))
        res = compute_daily_risk(d, river_hist, rail_df, barge_df)
        
        if res:
            score, level, primary = res
            ts = d.replace(hour=12, minute=0).isoformat() + "Z"
            rows.append([ts, score, level, primary])

    # Overwrite CSV
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_utc", "risk_score", "risk_level", "primary_driver"])
        w.writerows(rows)
    
    print(f"Done. Wrote {len(rows)} historical risk points to {OUT_FILE}")

if __name__ == "__main__":
    main()

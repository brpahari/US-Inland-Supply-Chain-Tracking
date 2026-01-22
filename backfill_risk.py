#!/usr/bin/env python3
"""
backfill_risk.py - FIXED
Stops backfilling at 'Yesterday' to avoid conflicting with Live 'Today' data.
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
    print("Fetching USGS river history...")
    url = "https://waterservices.usgs.gov/nwis/dv/"
    stats_to_try = ["00003", "00002", "00001"] # Mean, Min, Max
    
    for stat in stats_to_try:
        params = {
            "format": "json",
            "sites": ST_LOUIS_SITE,
            "period": f"P{DAYS_BACK + 20}D",
            "parameterCd": "00065",
            "statCd": stat
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200: continue
            data = r.json()
            if not data.get('value', {}).get('timeSeries'): continue
                
            hist = {}
            ts = data['value']['timeSeries'][0]['values'][0]['value']
            for p in ts:
                hist[p['dateTime'][:10]] = float(p['value'])
            print(f"  Found {len(hist)} days using stat {stat}")
            return hist
        except: continue
    return {}

def get_as_of(df, date_str, val_col):
    if df.empty: return 0.0
    past = df[df['week_end_date'] <= date_str]
    if past.empty: return float(df.iloc[0][val_col])
    return float(past.iloc[-1][val_col])

def compute_daily_risk(target_date, river_hist, rail_df, barge_df):
    date_s = target_date.strftime("%Y-%m-%d")
    
    # RIVER
    r_val = river_hist.get(date_s)
    prev_dt = target_date - timedelta(days=7)
    r_prev = river_hist.get(prev_dt.strftime("%Y-%m-%d"))
    r_delta = (r_val - r_prev) if (r_val is not None and r_prev is not None) else 0.0
    
    # RAIL
    curr_rail = get_as_of(rail_df, date_s, 'terminal_dwell_hours')
    past_rail = get_as_of(rail_df, (target_date - timedelta(days=28)).strftime("%Y-%m-%d"), 'terminal_dwell_hours')
    rail_delta = curr_rail - past_rail

    # BARGE
    curr_barge = get_as_of(barge_df, date_s, 'total_barges')
    past_barge = get_as_of(barge_df, (target_date - timedelta(days=28)).strftime("%Y-%m-%d"), 'total_barges')
    barge_delta = curr_barge - past_barge

    # SCORE
    drivers = []
    
    r_score = 0
    if r_val is not None:
        if r_delta < -2.0: r_score += 20
        if r_val < 0.0: r_score += 20
        if r_score > 0: drivers.append(("river", r_score))

    rr_score = 0
    if rail_delta > 2.0: rr_score += 30
    elif rail_delta > 0.5: rr_score += 15
    if rr_score > 0: drivers.append(("rail", rr_score))

    b_score = 0
    if barge_delta < -50: b_score += 30
    elif barge_delta < -20: b_score += 15
    if b_score > 0: drivers.append(("barge", b_score))

    total = min(100, r_score + rr_score + b_score)
    level = "LOW"
    if total > 70: level = "CRITICAL"
    elif total > 40: level = "MODERATE"
    primary = max(drivers, key=lambda x: x[1])[0] if drivers else "none"

    return total, level, primary

def main():
    river_hist = fetch_river_history()
    
    # Load support files
    rail_df = pd.DataFrame()
    if os.path.exists(RAIL_FILE):
        try: 
            rail_df = pd.read_csv(RAIL_FILE)
            if 'carrier' in rail_df.columns:
                rail_df = rail_df[rail_df['carrier'] == 'UP'].sort_values('week_end_date')
        except: pass

    barge_df = pd.DataFrame()
    if os.path.exists(BARGE_FILE):
        try:
            barge_df = pd.read_csv(BARGE_FILE).sort_values('week_end_date')
            if 'total_barges' not in barge_df.columns and 'total_tons' in barge_df.columns:
                barge_df['total_barges'] = barge_df['total_tons']
        except: pass

    rows = []
    # Stop at YESTERDAY (Days 1 to 90)
    # Day 0 is Today, we skip it.
    today = datetime.now()
    print(f"Backfilling history (Stopping before today)...")
    
    for i in range(1, DAYS_BACK + 1):
        d = today - timedelta(days=i)
        res = compute_daily_risk(d, river_hist, rail_df, barge_df)
        if res:
            score, level, primary = res
            ts = d.replace(hour=12, minute=0).isoformat() + "Z"
            rows.append([ts, score, level, primary])

    # Sort by date ascending
    rows.sort(key=lambda x: x[0])

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_utc", "risk_score", "risk_level", "primary_driver"])
        w.writerows(rows)
    
    print(f"Backfill complete. {len(rows)} days written (Yesterday and older).")

if __name__ == "__main__":
    main()

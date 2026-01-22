#!/usr/bin/env python3
"""
backfill_risk.py

Generates data/history/risk_daily.csv using REAL historical data.
1. Fetches last 90 days of daily mean river stages from USGS (Archive API).
2. Merges with your existing Rail/Barge history CSVs.
3. Re-calculates the Risk Score for every day in the past 90 days.
"""

import pandas as pd
import requests
import io
import os
import csv
from datetime import datetime, timedelta, timezone

# Configuration
DAYS_BACK = 90
ST_LOUIS_SITE = "07010000"
RAIL_FILE = "data/history/rail_weekly.csv"
BARGE_FILE = "data/history/barge_locks27_weekly.csv"
OUT_FILE = "data/history/risk_daily.csv"

def fetch_usgs_daily_history():
    """Fetches daily mean gage height for St. Louis for last 90 days."""
    end = datetime.now()
    start = end - timedelta(days=DAYS_BACK + 10) # Buffer for rolling calc
    
    url = "https://waterservices.usgs.gov/nwis/dv/"
    params = {
        "format": "json",
        "sites": ST_LOUIS_SITE,
        "startDT": start.strftime("%Y-%m-%d"),
        "endDT": end.strftime("%Y-%m-%d"),
        "parameterCd": "00065", # Gage height
        "statCd": "00003"       # Mean
    }
    print(f"Fetching USGS history from {start.date()}...")
    r = requests.get(url, params=params)
    data = r.json()
    
    # Parse into a simple dict: {date_str: value}
    history = {}
    ts = data['value']['timeSeries'][0]['values'][0]['value']
    for p in ts:
        history[p['dateTime'][:10]] = float(p['value'])
    return history

def get_closest_past_value(df, date_str, value_col):
    """Finds the most recent value in a dataframe prior to date_str."""
    if df.empty: return 0
    # Filter for dates <= current date
    past = df[df['week_end_date'] <= date_str]
    if past.empty:
        # Fallback to earliest if history doesn't go back that far
        return float(df.iloc[0][value_col])
    return float(past.iloc[-1][value_col])

def get_delta_from_series(history, target_date, days_lag):
    """Calculates delta from a dictionary of dates."""
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    prev_dt = target_dt - timedelta(days=days_lag)
    prev_date = prev_dt.strftime("%Y-%m-%d")
    
    curr = history.get(target_date)
    past = history.get(prev_date)
    
    if curr is None or past is None:
        return 0.0
    return curr - past

def compute_risk(river_val, river_delta, rail_delta, barge_delta):
    score = 0
    drivers = []
    
    # River Logic
    if river_delta < -2.0: score += 20
    if river_val < 0.0: score += 20
    if score > 0: drivers.append(("river", score))
    
    # Rail Logic
    r_score = 0
    if rail_delta > 2.0: r_score += 30
    elif rail_delta > 0.5: r_score += 15
    score += r_score
    if r_score > 0: drivers.append(("rail", r_score))

    # Barge Logic (Corrected for Count)
    b_score = 0
    if barge_delta < -50: b_score += 30
    elif barge_delta < -20: b_score += 15
    score += b_score
    if b_score > 0: drivers.append(("barge", b_score))
    
    final_score = min(100, score)
    
    primary = "none"
    if drivers:
        primary = max(drivers, key=lambda x: x[1])[0]
        
    level = "LOW"
    if final_score > 70: level = "CRITICAL"
    elif final_score > 40: level = "MODERATE"
    
    return final_score, level, primary

def main():
    # 1. Load Data
    river_hist = fetch_usgs_daily_history()
    
    rail_df = pd.DataFrame()
    if os.path.exists(RAIL_FILE):
        rail_df = pd.read_csv(RAIL_FILE)
        # Filter for UP only for consistency
        rail_df = rail_df[rail_df['carrier'] == 'UP'].sort_values('week_end_date')

    barge_df = pd.DataFrame()
    if os.path.exists(BARGE_FILE):
        barge_df = pd.read_csv(BARGE_FILE)
        barge_df = barge_df.sort_values('week_end_date')
        # Handle 'total_barges' vs 'total_tons' legacy
        col = 'total_barges' if 'total_barges' in barge_df.columns else 'total_tons'
        barge_df['value'] = barge_df[col]

    # 2. Iterate last 90 days
    output_rows = []
    end_date = datetime.now()
    
    print("Reconstructing risk history...")
    for i in range(DAYS_BACK):
        d = end_date - timedelta(days=DAYS_BACK - i)
        date_str = d.strftime("%Y-%m-%d")
        
        # Get Inputs
        r_val = river_hist.get(date_str, 0)
        r_delta = get_delta_from_series(river_hist, date_str, 7)
        
        # Calculate 4-week deltas for Rail/Barge
        # We look up the value "as of that day" and "as of 28 days prior"
        curr_rail = get_closest_past_value(rail_df, date_str, 'terminal_dwell_hours')
        past_rail = get_closest_past_value(rail_df, (d - timedelta(days=28)).strftime("%Y-%m-%d"), 'terminal_dwell_hours')
        rail_delta = curr_rail - past_rail
        
        curr_barge = get_closest_past_value(barge_df, date_str, 'value')
        past_barge = get_closest_past_value(barge_df, (d - timedelta(days=28)).strftime("%Y-%m-%d"), 'value')
        barge_delta = curr_barge - past_barge
        
        # Compute
        score, level, driver = compute_risk(r_val, r_delta, rail_delta, barge_delta)
        
        # Format ISO timestamp for CSV
        ts = d.replace(hour=12, minute=0, second=0, microsecond=0).isoformat() + "Z"
        output_rows.append([ts, score, level, driver])

    # 3. Write
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_utc", "risk_score", "risk_level", "primary_driver"])
        w.writerows(output_rows)
        
    print(f"Backfill complete. Wrote {len(output_rows)} days to {OUT_FILE}")

if __name__ == "__main__":
    main()

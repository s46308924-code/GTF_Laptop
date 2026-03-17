#
# SCANNER NAME:
# Demand and Supply Trading – HALF YEARLY

import pandas as pd
import os
from datetime import datetime, timedelta
from fyers_data import fetch_historical_data

# ================= USER BUTTONS ===================


DAILY_TF = "1D"
API_DAY_LIMIT = 365
HIST_YEARS = 10          # 8 saal = 16 half-year candles — kafi data

SYMBOL_CSV = "symbols.csv"

SCAN_DATE = datetime.now().strftime("%Y-%m-%d")
MAX_DEMAND_ZONES = 2
MAX_SUPPLY_ZONES = 2

BASE_BODY_PCT = 0.50
LEG_BODY_PCT  = 0.50
MAX_BASE_CANDLES = 3

LEG_IN_SINGLE_MULTIPLIER  = 1.5
LEG_OUT_SINGLE_MULTIPLIER = 2.0

CONT_MIN_CANDLES = 2
CONT_MAX_CANDLES = 3
CONT_BODY_PCT    = 0.50
LEG_IN_CONT_MULTIPLIER  = 1.5
LEG_OUT_CONT_MULTIPLIER = 2.0

RRR = 3

OUTPUT_FOLDER = "output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ACCESS_TOKEN = (yaha se access token read hoga)

import json, os

def find_config():
    current = os.path.dirname(os.path.abspath(__file__))
    while True:
        config_path = os.path.join(current, "config.json")
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                return json.load(f)
        parent = os.path.dirname(current)
        if parent == current:
            raise FileNotFoundError("❌ config.json nahi mila!")
        current = parent

ACCESS_TOKEN = find_config()["access_token"]

# ==================================================
# ================= BASIC HELPERS ==================
# ==================================================

def fyers_symbol(sym):
    sym = sym.strip().upper()
    return "NSE:NIFTY50-INDEX" if sym == "NIFTY50" else f"NSE:{sym}-EQ"

def body_pct(c):
    return abs(c["close"] - c["open"]) / (c["high"] - c["low"] + 1e-9)

def candle_color(c):
    return "GREEN" if c["close"] > c["open"] else "RED"

def candle_range(c):
    return c["high"] - c["low"]

def is_leg(c):
    return body_pct(c) >= LEG_BODY_PCT

def is_base(c):
    return body_pct(c) < BASE_BODY_PCT

# ==================================================
# ============== LOAD SYMBOL LIST ==================
# ==================================================

def load_symbols():
    with open(SYMBOL_CSV) as f:
        return [s.strip() for s in f if s.strip()]

# ==================================================
# ============ DATA FETCH ==========================
# ==================================================

def fetch_data(symbol):
    end = datetime.strptime(SCAN_DATE, "%Y-%m-%d").date()
    start = end - timedelta(days=365 * HIST_YEARS)

    dfs = []
    cur = start

    while cur <= end:
        cur_end = min(cur + timedelta(days=API_DAY_LIMIT), end)
        df = fetch_historical_data(
            symbol,
            DAILY_TF,
            cur.strftime("%Y-%m-%d"),
            cur_end.strftime("%Y-%m-%d"),
            ACCESS_TOKEN
        )
        if df is not None and not df.empty:
            dfs.append(df)
        cur = cur_end + timedelta(days=1)

    if not dfs:
        return None

    df = pd.concat(dfs)
    df = df[~df.index.duplicated()]
    df.sort_index(inplace=True)
    df.index = pd.to_datetime(df.index).tz_localize(None)
    return df

def to_halfyearly(df):
    return df.resample("6ME").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    })

# ==================================================
# ============ STRENGTH CHECK ======================
# ==================================================

def check_single_strength(c, zone_range, mult):
    return candle_range(c) >= zone_range * mult

def check_group_strength(group, zone_range, mult):
    for c in group:
        if body_pct(c) < CONT_BODY_PCT:
            return False
    total = max(c["high"] for c in group) - min(c["low"] for c in group)
    return total >= zone_range * mult

# ==================================================
# ============ FRESHNESS CHECK =====================
# ==================================================

def is_fresh(df, idx, proximal):
    for k in range(idx + 1, len(df)):
        if df.iloc[k]["low"] <= proximal <= df.iloc[k]["high"]:
            return False
    return True

# ==================================================
# ========== MAIN HALF YEARLY SCANNER ==============
# ==================================================

def scan_symbol(df, symbol):

    demand_zones = []
    supply_zones = []

    i = len(df) - 1

    while i >= 5:

        legout = df.iloc[i]
        if not is_leg(legout):
            i -= 1
            continue

        base = []
        j = i - 1
        while j >= 0 and is_base(df.iloc[j]) and len(base) < MAX_BASE_CANDLES:
            base.append(df.iloc[j])
            j -= 1

        if not base or j < 0:
            i -= 1
            continue

        legin = df.iloc[j]
        if not is_leg(legin):
            i -= 1
            continue

        lin = candle_color(legin)
        lout = candle_color(legout)

        if lin == "GREEN" and lout == "GREEN":
            pattern, zone_type = "RBR", "DEMAND"
        elif lin == "RED" and lout == "GREEN":
            pattern, zone_type = "DBR", "DEMAND"
        elif lin == "GREEN" and lout == "RED":
            pattern, zone_type = "RBD", "SUPPLY"
        elif lin == "RED" and lout == "RED":
            pattern, zone_type = "DBD", "SUPPLY"
        else:
            i -= 1
            continue

        base_df = pd.DataFrame(base)

        proximal = (
            base_df[["open","close"]].max(axis=1).max()
            if zone_type == "DEMAND"
            else base_df[["open","close"]].min(axis=1).min()
        )

        wick_sources = [legout] + base
        if pattern in ["DBR", "RBD"]:
            wick_sources.append(legin)

        distal = (
            min(c["low"] for c in wick_sources)
            if zone_type == "DEMAND"
            else max(c["high"] for c in wick_sources)
        )

        zone_range = abs(proximal - distal)

        legin_ok = check_single_strength(legin, zone_range, LEG_IN_SINGLE_MULTIPLIER)
        if not legin_ok:
            group = [df.iloc[k] for k in range(max(j-CONT_MAX_CANDLES+1,0), j+1)]
            legin_ok = check_group_strength(group, zone_range, LEG_IN_CONT_MULTIPLIER)

        legout_ok = check_single_strength(legout, zone_range, LEG_OUT_SINGLE_MULTIPLIER)
        if not legout_ok:
            group = [df.iloc[k] for k in range(i, min(i+CONT_MAX_CANDLES, len(df)))]
            legout_ok = check_group_strength(group, zone_range, LEG_OUT_CONT_MULTIPLIER)

        if not (legin_ok and legout_ok):
            i -= 1
            continue

        if not is_fresh(df, i, proximal):
            i -= 1
            continue

        entry = proximal
        sl = distal
        risk = abs(entry - sl)
        target = entry + risk * RRR if zone_type == "DEMAND" else entry - risk * RRR

        zone = {
            "symbol": symbol,
            "zone_type": zone_type,
            "pattern": pattern,
            "zone_create_date": df.index[i].strftime("%Y-%m-%d"),
            "proximal": round(proximal, 2),
            "distal": round(distal, 2),
            "entry": round(entry, 2),
            "stop_loss": round(sl, 2),
            "target": round(target, 2),
            "timeframe": "HALFYEAR"          # ← HALF YEARLY
        }

        if zone_type == "DEMAND":
            demand_zones.append(zone)
        else:
            supply_zones.append(zone)

        i -= 1

    return demand_zones + supply_zones

# ==================================================
# ================= RUN SCANNER ====================
# ==================================================

def run_scan():

    symbols = load_symbols()
    all_zones = []

    for s in symbols:
        print(f"🔍 Half-Yearly scanning {s}")
        df = fetch_data(fyers_symbol(s))
        if df is None or df.empty:
            continue

        hdf = to_halfyearly(df)
        zones = scan_symbol(hdf, s)
        all_zones.extend(zones)

    if not all_zones:
        print("❌ No half-yearly zones found")
        return

    dfout = pd.DataFrame(all_zones)
    fname = f"halfyearly_zone_scan_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    dfout.to_csv(os.path.join(OUTPUT_FOLDER, fname), index=False)

    print("\n✅ HALF YEARLY ZONE SCAN COMPLETE")

run_scan()

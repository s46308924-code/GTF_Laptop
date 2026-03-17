#
# DEMAND & SUPPLY TRADING – MONTHLY SCANNER 

import pandas as pd
import os
from datetime import datetime, timedelta
from fyers_data import fetch_historical_data

# ===================== USER INPUT ===========================

OUTPUT_DIR   = "output/monthly"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMEFRAME = "1D"
API_LIMIT = 365
HISTORY_YEARS = 10

MAX_DISTANCE_PCT = 20

# BUTTONS SETTING

MONTHLY_BASE_BODY_PCT = 0.50
MONTHLY_LEG_BODY_PCT  = 0.50
MONTHLY_MAX_BASE = 3

MONTHLY_LEG_IN_SINGLE_M  = 1.5
MONTHLY_LEG_OUT_SINGLE_M = 2.0

MONTHLY_CONT_MIN = 2
MONTHLY_CONT_MAX = 3
MONTHLY_CONT_BODY_PCT = 0.50
MONTHLY_LEG_IN_CONT_M  = 1.5
MONTHLY_LEG_OUT_CONT_M = 2.0


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

SYMBOL_CSV   = "symbols.csv"



# ============================================================
# ===================== HELPERS ==============================
# ============================================================

def fyers_symbol(sym):
    return f"NSE:{sym.strip().upper()}-EQ"

def body_pct(c):
    return abs(c["close"] - c["open"]) / (c["high"] - c["low"] + 1e-9)

def candle_color(c):
    return "GREEN" if c["close"] > c["open"] else "RED"

def candle_range(c):
    return c["high"] - c["low"]

def is_leg(c, pct):
    return body_pct(c) >= pct

def is_base(c, pct):
    return body_pct(c) < pct

# ============================================================
# ===================== DATA FETCH ===========================
# ============================================================

def fetch_data(symbol):
    end = datetime.now().date()
    start = end - timedelta(days=HISTORY_YEARS * 365)
    dfs = []
    cur = start

    while cur <= end:
        cur_end = min(cur + timedelta(days=API_LIMIT), end)
        try:
            df = fetch_historical_data(
                symbol,
                TIMEFRAME,
                cur.strftime("%Y-%m-%d"),
                cur_end.strftime("%Y-%m-%d"),
                ACCESS_TOKEN
            )
        except Exception:
            cur = cur_end + timedelta(days=1)
            continue

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

def to_monthly(df):
    return df.resample("ME").agg({
        "open":"first",
        "high":"max",
        "low":"min",
        "close":"last",
        "volume":"sum"
    })

# ============================================================
# ===================== FRESHNESS ============================
# ============================================================

def is_fresh(df, idx, proximal):
    for i in range(idx + 1, len(df)):
        if df.iloc[i]["low"] <= proximal <= df.iloc[i]["high"]:
            return False
    return True

def near_cmp(proximal, cmp):
    return abs(proximal - cmp) / cmp * 100 <= MAX_DISTANCE_PCT

# ============================================================
# ===================== CONTINUOUS CHECK =====================
# ============================================================

def valid_continuous(df, start_idx, direction, zone_range, multiplier):
    candles = []
    i = start_idx
    color = candle_color(df.iloc[i])

    while i >= 0 and candle_color(df.iloc[i]) == color and len(candles) < MONTHLY_CONT_MAX:
        if body_pct(df.iloc[i]) < MONTHLY_CONT_BODY_PCT:
            break
        candles.append(df.iloc[i])
        i -= 1

    if len(candles) < MONTHLY_CONT_MIN:
        return False

    total_range = max(c["high"] for c in candles) - min(c["low"] for c in candles)
    return total_range >= zone_range * multiplier

# ============================================================
# ===================== ZONE SCAN ============================
# ============================================================

def scan_monthly_zones(df):
    zones = []
    i = len(df) - 1

    while i >= 3:
        legout = df.iloc[i]

        if not is_leg(legout, MONTHLY_LEG_BODY_PCT):
            i -= 1
            continue

        base = []
        j = i - 1
        while j >= 0 and is_base(df.iloc[j], MONTHLY_BASE_BODY_PCT) and len(base) < MONTHLY_MAX_BASE:
            base.append(df.iloc[j])
            j -= 1

        if not base or j < 0:
            i -= 1
            continue

        legin = df.iloc[j]
        if not is_leg(legin, MONTHLY_LEG_BODY_PCT):
            i -= 1
            continue

        lin, lout = candle_color(legin), candle_color(legout)

        if lin == "GREEN" and lout == "GREEN":
            pattern, ztype = "RBR", "DEMAND"
        elif lin == "RED" and lout == "GREEN":
            pattern, ztype = "DBR", "DEMAND"
        elif lin == "GREEN" and lout == "RED":
            pattern, ztype = "RBD", "SUPPLY"
        elif lin == "RED" and lout == "RED":
            pattern, ztype = "DBD", "SUPPLY"
        else:
            i -= 1
            continue

        base_df = pd.DataFrame(base)
        proximal = (
            base_df[["open","close"]].max(axis=1).max()
            if ztype == "DEMAND"
            else base_df[["open","close"]].min(axis=1).min()
        )

        wick_sources = []
        if pattern in ["DBR","RBD"]:
            wick_sources.append(legin)
        wick_sources += base + [legout]

        distal = (
            min(c["low"] for c in wick_sources)
            if ztype == "DEMAND"
            else max(c["high"] for c in wick_sources)
        )

        zone_range = abs(proximal - distal)

        legin_ok = (
            candle_range(legin) >= zone_range * MONTHLY_LEG_IN_SINGLE_M or
            valid_continuous(df, j, lin, zone_range, MONTHLY_LEG_IN_CONT_M)
        )

        legout_ok = (
            candle_range(legout) >= zone_range * MONTHLY_LEG_OUT_SINGLE_M or
            valid_continuous(df, i, lout, zone_range, MONTHLY_LEG_OUT_CONT_M)
        )

        if not (legin_ok and legout_ok):
            i -= 1
            continue

        if not is_fresh(df, i, proximal):
            i -= 1
            continue

        zones.append({
            "symbol": None,
            "zone_type": ztype,
            "pattern": pattern,
            "zone_create_date": df.index[i].strftime("%Y-%m-%d"),
            "proximal": round(proximal,2),
            "distal": round(distal,2)
        })

        i -= 1

    return zones

# ============================================================
# ===================== MAIN RUN =============================
# ============================================================

def run():
    symbols = [s.strip() for s in open(SYMBOL_CSV) if s.strip()]
    results = []

    for sym in symbols:
        print("🔍 Monthly scanning:", sym)
        df = fetch_data(fyers_symbol(sym))
        if df is None:
            continue

        cmp = df["close"].iloc[-1]
        mdf = to_monthly(df)

        zones = scan_monthly_zones(mdf)

        for z in zones:
            if near_cmp(z["proximal"], cmp):
                z["symbol"] = sym
                results.append(z)

    if results:
        fname = f"monthly_zones_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        pd.DataFrame(results).to_csv(os.path.join(OUTPUT_DIR, fname), index=False)
        print("✅ MONTHLY CSV CREATED:", fname)
    else:
        print("⚠️ No valid monthly zones")

run()

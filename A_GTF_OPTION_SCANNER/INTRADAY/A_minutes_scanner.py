# MINUTES DEMAND & SUPPLY SCANNER

import pandas as pd
import os, glob
from datetime import datetime, timedelta
from fyers_data import fetch_historical_data

# ========================USER INPUT===========================

DAILY_CSV_DIR  = "output/daily"
HOURLY_CSV_DIR = "output/hourly"
OUTPUT_DIR     = "output/minutes"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMEFRAME = "15"          # 🔒 MINUTES
API_LIMIT = 90            # 🔒 INTRADAY SAFE
HISTORY_YEARS = 2/12
RRR = 3

MAX_DISTANCE_PCT = 10

DAILY_AREA_MULTIPLIER  = 0.5
HOURLY_AREA_MULTIPLIER = 0.5

#  BUTTONS SETTING

MINUTES_BASE_BODY_PCT = 0.50
MINUTES_LEG_BODY_PCT  = 0.50
MINUTES_MAX_BASE = 3

MINUTES_LEG_IN_SINGLE_M  = 1.5
MINUTES_LEG_OUT_SINGLE_M = 2.0

MINUTES_CONT_MIN = 2
MINUTES_CONT_MAX = 3
MINUTES_CONT_BODY_PCT = 0.50
MINUTES_LEG_IN_CONT_M  = 1.5
MINUTES_LEG_OUT_CONT_M = 2.0

# ACCESS_TOKEN = 

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



# ============================================================
# HELPERS
# ============================================================

def fyers_symbol(sym):
    return f"NSE:{sym}-EQ"

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
# DATA FETCH
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

# ============================================================
# FRESHNESS
# ============================================================

def is_fresh(df, idx, proximal):
    for i in range(idx + 1, len(df)):
        if df.iloc[i]["low"] <= proximal <= df.iloc[i]["high"]:
            return False
    return True

def near_cmp(proximal, cmp):
    return abs(proximal - cmp) / cmp * 100 <= MAX_DISTANCE_PCT

# ============================================================
# CONTINUOUS LEG CHECK
# ============================================================

def continuous_leg_ok(df, start_idx, direction, min_c, max_c, body_pct_min, zone_range, mult):
    ref_color = candle_color(df.iloc[start_idx])
    candles = []
    i = start_idx

    while 0 <= i < len(df) and len(candles) < max_c:
        c = df.iloc[i]
        if candle_color(c) != ref_color:
            break
        if body_pct(c) < body_pct_min:
            break
        candles.append(c)
        i += direction

    if len(candles) < min_c:
        return False

    high = max(c["high"] for c in candles)
    low  = min(c["low"] for c in candles)

    return (high - low) >= zone_range * mult

# ============================================================
# ZONE SCAN
# ============================================================

def scan_zones(df):
    zones = []
    i = len(df) - 1

    while i >= 3:
        legout = df.iloc[i]

        if not is_leg(legout, MINUTES_LEG_BODY_PCT):
            i -= 1
            continue

        base = []
        j = i - 1
        while j >= 0 and is_base(df.iloc[j], MINUTES_BASE_BODY_PCT) and len(base) < MINUTES_MAX_BASE:
            base.append(df.iloc[j])
            j -= 1

        if not base or j < 0:
            i -= 1
            continue

        legin = df.iloc[j]
        if not is_leg(legin, MINUTES_LEG_BODY_PCT):
            i -= 1
            continue

        lin = candle_color(legin)
        lout = candle_color(legout)

        if lin == "GREEN" and lout == "GREEN":
            pattern = "RBR"
            ztype = "DEMAND"
        elif lin == "RED" and lout == "GREEN":
            pattern = "DBR"
            ztype = "DEMAND"
        elif lin == "GREEN" and lout == "RED":
            pattern = "RBD"
            ztype = "SUPPLY"
        elif lin == "RED" and lout == "RED":
            pattern = "DBD"
            ztype = "SUPPLY"
        else:
            i -= 1
            continue

        base_df = pd.DataFrame(base)

        if ztype == "DEMAND":
            proximal = base_df[["open","close"]].max(axis=1).max()
        else:
            proximal = base_df[["open","close"]].min(axis=1).min()

        wick_sources = []
        if pattern in ["DBR","RBD"]:
            wick_sources.append(legin)
        wick_sources += base + [legout]

        if ztype == "DEMAND":
            distal = min(c["low"] for c in wick_sources)
        else:
            distal = max(c["high"] for c in wick_sources)

        zone_range = abs(proximal - distal)

        legin_ok = (
            candle_range(legin) >= zone_range * MINUTES_LEG_IN_SINGLE_M or
            continuous_leg_ok(
                df, j, -1,
                MINUTES_CONT_MIN, MINUTES_CONT_MAX,
                MINUTES_CONT_BODY_PCT,
                zone_range,
                MINUTES_LEG_IN_CONT_M
            )
        )

        if not legin_ok:
            i -= 1
            continue

        legout_ok = (
            candle_range(legout) >= zone_range * MINUTES_LEG_OUT_SINGLE_M or
            continuous_leg_ok(
                df, i, +1,
                MINUTES_CONT_MIN, MINUTES_CONT_MAX,
                MINUTES_CONT_BODY_PCT,
                zone_range,
                MINUTES_LEG_OUT_CONT_M
            )
        )

        if not legout_ok:
            i -= 1
            continue

        if not is_fresh(df, i, proximal):
            i -= 1
            continue

        zones.append({
            "type": ztype,
            "pattern": pattern,
            "proximal": proximal,
            "distal": distal,
            "idx": i
        })

        i -= 1

    return zones

# ============================================================
# MAIN RUN
# ============================================================

def run():

    daily_files  = glob.glob(f"{DAILY_CSV_DIR}/*.csv")
    hourly_files = glob.glob(f"{HOURLY_CSV_DIR}/*.csv")

    if not daily_files or not hourly_files:
        print("❌ DAILY / HOURLY CSV not found")
        return

    daily_csv  = max(daily_files, key=os.path.getmtime)
    hourly_csv = max(hourly_files, key=os.path.getmtime)

    ddf = pd.read_csv(daily_csv)
    hdf = pd.read_csv(hourly_csv)

    symbols = sorted(set(ddf["symbol"]).union(set(hdf["symbol"])))
    out = []

    for sym in symbols:
        print("🔍 MINUTES:", sym)
        df = fetch_data(fyers_symbol(sym))
        if df is None:
            continue

        cmp = df["close"].iloc[-1]
        minute_zones = scan_zones(df)

        d_zones = ddf[ddf["symbol"] == sym].to_dict("records")
        h_zones = hdf[hdf["symbol"] == sym].to_dict("records")

        for dz in minute_zones:

            if not near_cmp(dz["proximal"], cmp):
                continue

            if dz["type"] == "SUPPLY":
                if not (dz["proximal"] > cmp and dz["distal"] > cmp):
                    continue
            else:
                if not (dz["proximal"] < cmp and dz["distal"] < cmp):
                    continue

            tags = []
            d_check = dz["distal"]

            for rz in d_zones:
                h = abs(rz["proximal"] - rz["distal"])
                if rz["zone_type"] == "DEMAND":
                    if rz["distal"] <= d_check <= rz["proximal"]:
                        tags.append("DAILY")
                    elif rz["proximal"] < d_check <= rz["proximal"] + h * DAILY_AREA_MULTIPLIER:
                        tags.append("DAILY_AREA")
                else:
                    if rz["proximal"] >= d_check >= rz["distal"]:
                        tags.append("DAILY")
                    elif rz["proximal"] > d_check >= rz["proximal"] - h * DAILY_AREA_MULTIPLIER:
                        tags.append("DAILY_AREA")

            for rz in h_zones:
                h = abs(rz["proximal"] - rz["distal"])
                if rz["zone_type"] == "DEMAND":
                    if rz["distal"] <= d_check <= rz["proximal"]:
                        tags.append("HOURLY")
                    elif rz["proximal"] < d_check <= rz["proximal"] + h * HOURLY_AREA_MULTIPLIER:
                        tags.append("HOURLY_AREA")
                else:
                    if rz["proximal"] >= d_check >= rz["distal"]:
                        tags.append("HOURLY")
                    elif rz["proximal"] > d_check >= rz["proximal"] - h * HOURLY_AREA_MULTIPLIER:
                        tags.append("HOURLY_AREA")

            if not tags:
                continue

            entry = dz["proximal"]
            sl    = dz["distal"]

            if dz["type"] == "DEMAND":
                tgt = entry + (entry - sl) * RRR
            else:
                tgt = entry - (sl - entry) * RRR

            out.append({
                "symbol": sym,
                "zone_type": dz["type"],
                "pattern": dz["pattern"],
                "zone_create_date": df.index[dz["idx"]].strftime("%Y-%m-%d %H:%M"),
                "proximal": round(entry,2),
                "distal": round(sl,2),
                "entry": round(entry,2),
                "stop_loss": round(sl,2),
                "target": round(tgt,2),
                "confluence_tag": "+".join(sorted(set(tags)))
            })

    if out:
        fname = f"minutes_zones_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        pd.DataFrame(out).to_csv(os.path.join(OUTPUT_DIR, fname), index=False)
        print("✅ DONE:", fname)
    else:
        print("⚠️ No valid minute zones")

run()

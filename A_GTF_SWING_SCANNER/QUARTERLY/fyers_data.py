# ==========================================
# FYERS DATA FETCH MODULE (CLEAN VERSION)
# ==========================================

from fyers_apiv3 import fyersModel
import pandas as pd
from datetime import datetime


# ------------------------------------------
# FYERS CLIENT (ACCESS TOKEN REQUIRED)
# ------------------------------------------

def get_fyers_client(access_token):
    return fyersModel.FyersModel(
        client_id=access_token.split(":")[0],
        token=access_token,
        log_path=""
    )


# ------------------------------------------
# FETCH HISTORICAL DATA
# ------------------------------------------

def fetch_historical_data(symbol, timeframe, start_date, end_date, access_token):
    """
    Fetch historical candle data from FYERS
    """

    fyers = get_fyers_client(access_token)

    data = {
        "symbol": symbol,
        "resolution": timeframe,
        "date_format": "1",
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }

    response = fyers.history(data=data)

    if response.get("s") != "ok":
        raise Exception(f"FYERS ERROR: {response}")

    candles = response["candles"]

    df = pd.DataFrame(
        candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")
    df.set_index("timestamp", inplace=True)


    return df

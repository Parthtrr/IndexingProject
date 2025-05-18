from datetime import datetime
import Constant
from data_fetcher import fetch_data
from indexer import index_data
import pandas as pd


def get_nifty_df():
    """
    Fetches and returns Nifty 50 data with columns [Date, Close].
    """
    nifty_symbol = "^NSEI"  # Adjust if your data source uses a different symbol

    nifty_data = fetch_data([nifty_symbol], Constant.startDate, datetime.now().strftime("%Y-%m-%d"))
    if nifty_data is None or nifty_data.empty:
        print("Nifty data not fetched.")
        return None

    if isinstance(nifty_data.columns, pd.MultiIndex):
        try:
            nifty_df = nifty_data.xs(nifty_symbol, axis=1, level=1).copy()
        except KeyError:
            print("Nifty symbol not found in fetched data.")
            return None
    else:
        nifty_cols = [col for col in nifty_data.columns if col.endswith(f"/{nifty_symbol}")]
        if not nifty_cols:
            print("Nifty columns not found.")
            return None
        nifty_df = nifty_data[nifty_cols].copy()
        nifty_df.columns = [col.split("/")[0] for col in nifty_cols]

    nifty_df = nifty_df.reset_index(drop=True)
    if isinstance(nifty_data.columns, pd.MultiIndex):
        nifty_df["Date"] = nifty_data[("Date", "")].values
    else:
        nifty_df["Date"] = nifty_data["Date"].values

    nifty_df["Date"] = pd.to_datetime(nifty_df["Date"])
    nifty_df = nifty_df[["Date", "Close"]].copy()
    return nifty_df


def full_index():
    batch_size = Constant.batch_size
    tickers = Constant.nifty500 + Constant.indices
    nifty_df = get_nifty_df()  # 🔹 Fetch Nifty data once at the beginning

    if nifty_df is None or nifty_df.empty:
        print("Nifty data unavailable. Skipping indexing.")
        return

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        data_df = fetch_data(batch, Constant.startDate, datetime.now().strftime("%Y-%m-%d"))

        if data_df is not None and not data_df.empty:
            if isinstance(data_df.columns, pd.MultiIndex):
                try:
                    date_series = data_df[("Date", "")].copy()
                except KeyError:
                    print("Date column not found in MultiIndex DataFrame")
                    continue
            else:
                date_series = data_df["Date"].copy() if "Date" in data_df.columns else None

            if date_series is None:
                print("Date column is missing, skipping batch")
                continue

            for ticker in batch:
                if isinstance(data_df.columns, pd.MultiIndex):
                    try:
                        ticker_data = data_df.xs(ticker, axis=1, level=1).copy()
                    except KeyError:
                        continue
                else:
                    ticker_cols = [col for col in data_df.columns if col.endswith(f"/{ticker}")]
                    if not ticker_cols:
                        continue
                    ticker_data = data_df[ticker_cols].copy()
                    ticker_data.columns = [col.split("/")[0] for col in ticker_cols]

                ticker_data = ticker_data.reset_index(drop=True)
                ticker_data["Date"] = date_series.values
                ticker_data["Ticker"] = ticker

                index_data("nifty_data_weekly", ticker_data, ticker, nifty_df)  # 🔹 Pass Nifty data in each call

from datetime import datetime
import Constant
from data_fetcher import fetch_data
from indexer import index_data
import pandas as pd


def full_index():
    batch_size = Constant.batch_size  # Define batch size
    tickers = Constant.nifty500  # Get the full list of stocks

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]  # Create a batch of 50 tickers
        data_df = fetch_data(batch, Constant.startDate, datetime.now().strftime("%Y-%m-%d"))

        if data_df is not None and not data_df.empty:  # Ensure data is not empty
            # Extract date from MultiIndex
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

                # Add Date column properly
                ticker_data = ticker_data.reset_index(drop=True)
                ticker_data["Date"] = date_series.values  # Assign the correct date values
                ticker_data["Ticker"] = ticker  # Add Ticker column

                index_data("nifty_data", ticker_data, ticker)  # Index data




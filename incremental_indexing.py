from datetime import datetime, timedelta
import pandas as pd
import Constant
from data_fetcher import fetch_data
from indexer import index_data
from logging_config import get_logger

logger = get_logger(__name__)


def incremental_index(batch_size=50):
    today = datetime.now()
    date_from_index = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    date_to_index = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Starting incremental indexing from {date_from_index} to {date_to_index}")
    nifty500 = Constant.nifty500 + Constant.indices
    for i in range(0, len(nifty500), batch_size):
        batch = nifty500[i:i + batch_size]  # Process tickers in batches
        logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} tickers")

        data_df = fetch_data(batch, date_from_index, date_to_index)

        if data_df is None or data_df.empty:
            logger.warning("No data returned for batch, skipping...")
            continue  # Skip empty batches

        # Extract Date column from MultiIndex
        if isinstance(data_df.columns, pd.MultiIndex):
            try:
                date_series = data_df[("Date", "")].copy()
            except KeyError:
                logger.error("Date column missing in MultiIndex DataFrame, skipping batch")
                continue
        else:
            date_series = data_df["Date"].copy() if "Date" in data_df.columns else None

        if date_series is None:
            logger.error("Date column missing, skipping batch")
            continue

        for ticker in batch:
            logger.info(f"Indexing data for {ticker}")

            # Extract ticker-specific data
            if isinstance(data_df.columns, pd.MultiIndex):
                try:
                    ticker_data = data_df.xs(ticker, axis=1, level=1).copy()
                except KeyError:
                    logger.warning(f"No data found for {ticker}, skipping...")
                    continue
            else:
                ticker_cols = [col for col in data_df.columns if col.endswith(f"/{ticker}")]
                if not ticker_cols:
                    logger.warning(f"No relevant columns found for {ticker}, skipping...")
                    continue
                ticker_data = data_df[ticker_cols].copy()
                ticker_data.columns = [col.split("/")[0] for col in ticker_cols]

            # Add Date & Ticker columns
            ticker_data = ticker_data.reset_index(drop=True)
            ticker_data["Date"] = date_series.values
            ticker_data["Ticker"] = ticker

            # Index the data
            index_data("nifty_data", ticker_data, ticker)

    logger.info("Incremental indexing completed successfully.")

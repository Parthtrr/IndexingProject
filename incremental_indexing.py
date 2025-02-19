from datetime import datetime, timedelta
from Constant import nifty500
from data_fetcher import fetch_data
from indexer import index_data
from logging_config import get_logger

logger = get_logger(__name__)

def incremental_index():
    today = datetime.now()
    date_from_index = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    date_to_index = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Starting incremental indexing from {date_from_index} to {date_to_index}")
    for ticker in nifty500:
        logger.info(f"Incremental Indexing for {ticker}")
        data = fetch_data(ticker, date_from_index, date_to_index)
        if data is not None:
            data["Ticker"] = ticker
            index_data("nifty_data", data,ticker=ticker)

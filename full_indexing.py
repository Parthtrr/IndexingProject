from datetime import datetime

import Constant
from data_fetcher import fetch_data
from indexer import index_data

def full_index():
    for ticker in Constant.nifty500:
        data = fetch_data(ticker, Constant.startDate, datetime.now().strftime("%Y-%m-%d"))
        data["Ticker"] = ticker
        index_data("nifty_data", data, ticker)
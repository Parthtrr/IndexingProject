from data_fetcher import fetch_data
from indexer import index_data

def targeted_index(ticker, start_date, end_date):
    data = fetch_data(ticker, start_date, end_date)
    if data is not None:
        data["Ticker"] = ticker
        index_data("nifty_data", data)
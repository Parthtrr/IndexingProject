from datetime import datetime, timedelta
from Constant import nifty500
from data_fetcher import fetch_data
from indexer import index_data

def incremental_index():
    today = datetime.now()
    if today.hour > 15 or (today.hour == 15 and today.minute >= 30):
        date_to_index = today.strftime("%Y-%m-%d")
    else:
        date_to_index = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    for ticker in nifty500:
        data = fetch_data(ticker, date_to_index, date_to_index)
        data["Ticker"] = ticker
        index_data("nifty_data", data)

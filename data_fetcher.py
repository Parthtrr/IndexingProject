import yfinance as yf

import Constant


def fetch_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date, interval=Constant.interval)
    data.reset_index(inplace=True)
    return data[["Date", "Open", "Close", "High", "Low", "Volume"]]

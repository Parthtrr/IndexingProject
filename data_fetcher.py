import yfinance as yf
from logging_config import get_logger
import Constant

logger = get_logger(__name__)

def fetch_data(ticker, start_date, end_date):
    logger.info(f"Downloading the data for {ticker}")
    data = yf.download(ticker, start=start_date, end=end_date, interval=Constant.interval)
    data.reset_index(inplace=True)
    return data[["Date", "Open", "Close", "High", "Low", "Volume"]]

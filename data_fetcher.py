import yfinance as yf
from logging_config import get_logger
import Constant
import pandas as pd

logger = get_logger(__name__)


def fetch_data(ticker, start_date, end_date):
    try:
        logger.info(f"Downloading the data for {ticker} from {start_date} to {end_date}")
        data = yf.download(ticker, start=start_date, end=end_date, interval=Constant.interval)

        if data.empty:
            logger.warning(f"No data found for {ticker} in the given date range.")
            return None

        data.reset_index(inplace=True)
        return data[["Date", "Open", "Close", "High", "Low", "Volume"]]

    except Exception as e:
        logger.error(f"Error downloading data for {ticker}: {e}")
        return None

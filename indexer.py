from numpy.matlib import empty

from elastic_client import get_es_client
from mappings import index_mapping
from elasticsearch import helpers
import pandas as pd
from logging_config import get_logger
from Constant import rsi_window  # Assuming this is defined as 14 or your preferred period

logger = get_logger(__name__)

def calculate_rsi(data, period):
    data = data.copy()
    data['change'] = data['Close'].diff()
    data['gain'] = data['change'].apply(lambda x: x if x > 0 else 0)
    data['loss'] = data['change'].apply(lambda x: -x if x < 0 else 0)

    data['avg_gain'] = data['gain'].rolling(window=period).mean()
    data['avg_loss'] = data['loss'].rolling(window=period).mean()

    data['rs'] = data['avg_gain'] / data['avg_loss']
    data['rsi'] = 100 - (100 / (1 + data['rs']))

    # Fill the first `period` rows with 0.0
    data['rsi'] = data['rsi'].fillna(0.0)
    return data

def index_data(index_name, data, ticker):
    es = get_es_client()
    logger.info(f"building the indexable object for stock = {ticker}")

    if not es.indices.exists(index=index_name):
        logger.info(f"Index does not exist creating the index with name {index_name}")
        es.indices.create(index=index_name, body=index_mapping)

    # Ensure data is sorted by date before RSI calculation
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data = data.sort_values(by='Date')

    # Calculate RSI
    data = calculate_rsi(data, rsi_window)

    def generate_actions():
        for _, row in data.iterrows():
            date_value = row['Date'].strftime('%Y-%m-%d')
            ticker_value = row['Ticker']

            action = {
                "_op_type": "index",
                "_index": index_name,
                "_id": f"{ticker}_{date_value}",
                "_source": {
                    "ticker": ticker_value,
                    "date": date_value,
                    "open": float(row['Open']) if not pd.isna(row['Open']) else 0.0,
                    "close": float(row['Close']) if not pd.isna(row['Close']) else 0.0,
                    "high": float(row['High']) if not pd.isna(row['High']) else 0.0,
                    "low": float(row['Low']) if not pd.isna(row['Low']) else 0.0,
                    "volume": int(row['Volume']) if not pd.isna(row['Volume']) else 0,
                    "rsi": float(row['rsi']) if not pd.isna(row['rsi']) else 0.0
                }
            }
            yield action

    print(f"Indexing data for {ticker}...")
    success, failed = helpers.bulk(es, generate_actions())
    print(f"Successfully indexed {success} documents.")
    print(f"Failed to index {failed} documents.")

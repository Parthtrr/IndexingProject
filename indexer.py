from numpy.matlib import empty

from elastic_client import get_es_client
from mappings import index_mapping
from elasticsearch import helpers
import pandas as pd
from logging_config import get_logger

logger = get_logger(__name__)

def index_data(index_name, data, ticker):
    es = get_es_client()
    logger.info(f"building the indexable object for stock = {ticker}")
    # Check if the index exists, create if it doesn't
    if not es.indices.exists(index=index_name):
        logger.info(f"Index does not exist creating the index with name {index_name}")
        es.indices.create(index=index_name, body=index_mapping)

    # Prepare bulk indexing actions
    def generate_actions():
        for _, row in data.iterrows():  # Assuming `data` is a pandas DataFrame
            # Ensure the 'Date' column is a datetime object
            date_value = pd.to_datetime(row['Date'], errors='coerce').item().strftime('%Y-%m-%d')
            ticker_value = row['Ticker'].iloc[0] if isinstance(row['Ticker'], pd.Series) else row['Ticker']
            open_value = row['Open'].iloc[0] if isinstance(row['Open'], pd.Series) else row['Open']
            close_value = row['Close'].iloc[0] if isinstance(row['Close'], pd.Series) else row['Close']
            high_value = row['High'].iloc[0] if isinstance(row['High'], pd.Series) else row['High']
            low_value = row['Low'].iloc[0] if isinstance(row['Low'], pd.Series) else row['Low']
            volume_value = row['Volume'].iloc[0] if isinstance(row['Volume'], pd.Series) else row['Volume']

            action = {
                "_op_type": "index",  # Action to perform
                "_index": index_name,  # The index to add the document to
                "_id": f"{ticker}_{date_value}",  # Unique document ID, e.g., ticker + date
                "_source": {
                    "ticker": ticker_value,  # Now using the scalar value of 'Ticker'
                    "date": date_value,
                    "open": float(open_value) if not pd.isna(open_value) else 0.0,  # Convert to float
                    "close": float(close_value) if not pd.isna(close_value) else 0.0,  # Convert to float
                    "high": float(high_value) if not pd.isna(high_value) else 0.0,  # Convert to float
                    "low": float(low_value) if not pd.isna(low_value) else 0.0,  # Convert to float
                    "volume": int(volume_value) if not pd.isna(volume_value) else 0,  # Convert to int
                }
            }
            yield action

    # Perform bulk indexing
    print(f"Indexing data for {ticker}...")
    success, failed = helpers.bulk(es, generate_actions())
    print(f"Successfully indexed {success} documents.")
    print(f"Failed to index {failed} documents.")

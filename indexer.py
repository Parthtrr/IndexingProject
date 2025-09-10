import pandas as pd
from elasticsearch import helpers

from Constant import rsi_window, roc_period, atr_period  # Assuming `roc_period` is defined in your Constant.py
from elastic_client import get_es_client
from logging_config import get_logger
from mappings import index_mapping

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

    data['rsi'] = data['rsi'].fillna(0.0)
    return data

def calculate_atr(data, period):
    data = data.copy()

    data['high_low'] = data['High'] - data['Low']
    data['high_close_prev'] = abs(data['High'] - data['Close'].shift())
    data['low_close_prev'] = abs(data['Low'] - data['Close'].shift())

    data['tr'] = data[['high_low', 'high_close_prev', 'low_close_prev']].max(axis=1)
    data['atr'] = data['tr'].rolling(window=period).mean()

    data['atr'] = data['atr'].fillna(0.0)
    return data


def calculate_roc(data, period):
    """
    Calculate Rate of Change (ROC) for the given data and period.
    """
    data = data.copy()
    data['roc'] = (data['Close'].pct_change(periods=period) * 100)
    data['roc'] = data['roc'].fillna(0.0)  # Fill the first `period` rows with 0.0
    return data


def index_data(index_name, data, ticker, nifty_data=None):
    es = get_es_client()
    logger.info(f"Building the indexable object for stock = {ticker}")

    if not es.indices.exists(index=index_name):
        logger.info(f"Index does not exist. Creating the index with name {index_name}")
        es.indices.create(index=index_name, body=index_mapping)

    # Ensure data is sorted by date before RSI and ROC calculation
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data = data.sort_values(by='Date')

    # Calculate ATR
    data = calculate_atr(data, atr_period)

    # Calculate RSI
    data = calculate_rsi(data, rsi_window)

    # --- New Section: Calculate ROC and Compare with Nifty ROC ---
    # Calculate stock ROC
    data = calculate_roc(data, roc_period)

    # Calculate Nifty ROC if provided
    if nifty_data is not None:
        # Ensure Nifty data is sorted by Date as well
        nifty_data['Date'] = pd.to_datetime(nifty_data['Date'], errors='coerce')
        nifty_data = nifty_data.sort_values(by='Date')

        # Calculate Nifty ROC
        nifty_data = calculate_roc(nifty_data, roc_period)

        # Merge Nifty ROC into stock data based on Date
        nifty_data.rename(columns={'roc': 'roc_nifty'}, inplace=True)
        data = pd.merge(data, nifty_data[['Date', 'roc_nifty']], on='Date', how='left')

    data.fillna({
        'Open': 0.0,
        'Close': 0.0,
        'High': 0.0,
        'Low': 0.0,
        'Volume': 0,
        'rsi': 0.0,
        'roc': 0.0,
        'roc_nifty': 0.0,
        'atr': 0.0
    }, inplace=True)

    # --- End of New Section ---

    def generate_actions():
        for _, row in data.iterrows():
            date_value = row['Date'].strftime('%Y-%m-%d')
            if float(row['Open']) == 0.0:
                continue

            action = {
                "_op_type": "index",
                "_index": index_name,
                "_id": f"{ticker}_{date_value}",
                "_source": {
                    "ticker": ticker,
                    "date": date_value,
                    "open": float(row['Open']),
                    "close": float(row['Close']),
                    "high": float(row['High']),
                    "low": float(row['Low']),
                    "volume": int(row['Volume']),
                    "rsi": float(row['rsi']),
                    "roc": float(row['roc']),
                    "roc_nifty": float(row['roc_nifty']),
                    "atr": float(row['atr'])
                }
            }
            yield action

    try:
        success, _ = helpers.bulk(es, generate_actions(), raise_on_error=True)
        print(f"Successfully indexed {success} documents.")
    except helpers.BulkIndexError as e:
        print(f"{len(e.errors)} document(s) failed to index.")
        for err in e.errors:
            print(err)


def index_data_incremental(index_name, data, ticker, nifty_data=None):
    es = get_es_client()
    logger.info(f"Building the incremental indexable object for stock = {ticker}")

    if not es.indices.exists(index=index_name):
        logger.info(f"Index does not exist. Creating the index with name {index_name}")
        es.indices.create(index=index_name, body=index_mapping)

    # Ensure data is sorted by date before indicator calculations
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data = data.sort_values(by='Date')

    # Calculate indicators
    data = calculate_atr(data, atr_period)
    data = calculate_rsi(data, rsi_window)
    data = calculate_roc(data, roc_period)

    # Handle Nifty ROC
    if nifty_data is not None:
        nifty_data['Date'] = pd.to_datetime(nifty_data['Date'], errors='coerce')
        nifty_data = nifty_data.sort_values(by='Date')
        nifty_data = calculate_roc(nifty_data, roc_period)
        nifty_data.rename(columns={'roc': 'roc_nifty'}, inplace=True)
        data = pd.merge(data, nifty_data[['Date', 'roc_nifty']], on='Date', how='left')

    # Keep only the last 5 rows
    data = data.tail(5)

    def generate_actions():
        for _, row in data.iterrows():
            date_value = row['Date'].strftime('%Y-%m-%d')

            doc = {}
            for field in ["Open", "Close", "High", "Low", "Volume", "rsi", "roc", "roc_nifty", "atr"]:
                value = row[field]
                if isinstance(value, float) and (pd.isna(value) or value == 0.0):
                    continue
                if isinstance(value, int) and value == 0:
                    continue
                doc[field.lower()] = float(value) if isinstance(value, float) else int(value)

            # Always include these fields
            doc["ticker"] = ticker
            doc["date"] = date_value

            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": f"{ticker}_{date_value}",
                "doc": doc,
                "doc_as_upsert": True
            }
            yield action

    try:
        success, _ = helpers.bulk(es, generate_actions(), raise_on_error=True)
        print(f"Successfully indexed {success} documents incrementally.")
    except helpers.BulkIndexError as e:
        print(f"{len(e.errors)} document(s) failed to index.")
        for err in e.errors:
            print(err)

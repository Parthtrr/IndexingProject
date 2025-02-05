index_mapping = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s"
    },
    "mappings": {
        "properties": {
            "date": {"type": "date"},
            "ticker": {"type": "keyword"},
            "open": {"type": "float"},
            "close": {"type": "float"},
            "high": {"type": "float"},
            "low": {"type": "float"},
            "volume": {"type": "long"}
        }
    }
}
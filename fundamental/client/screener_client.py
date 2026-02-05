from fundamental.config.settings import BASE_URL, MAX_RETRIES
from fundamental.utils.retry import retry
from fundamental.utils.logger import get_logger

logger = get_logger(__name__)

class ScreenerClient:

    def __init__(self, http_client):
        self.http = http_client

    @retry(max_retries=MAX_RETRIES)
    def fetch_company_page(self, ticker: str) -> str:
        url = BASE_URL.format(ticker=ticker)
        logger.info(f"Fetching Screener page for {ticker}")
        return self.http.get(url)

from time import sleep
from fundamental.config.settings import REQUEST_DELAY_SEC
from fundamental.models.fundamental import FundamentalData
from fundamental.utils.logger import get_logger

logger = get_logger(__name__)

class FundamentalService:

    def __init__(self, client, parser):
        self.client = client
        self.parser = parser

    def fetch_fundamentals(self, ticker: str) -> FundamentalData:
        logger.info(f"Processing fundamentals for {ticker}")


        try:
            html = self.client.fetch_company_page(ticker)
            parsed = self.parser.parse(html)
            quarterly_metrics = [q["metric"] for q in parsed.get("quarterly", [])]
            sleep(REQUEST_DELAY_SEC)
        except Exception as e:
            logger.error(f"❌ Failed for consolidated {ticker}: {e}")
            html = self.client.fetch_company_page_standalone(ticker)
            parsed = self.parser.parse(html)
            sleep(REQUEST_DELAY_SEC)

        required_metrics = {"Sales", "Net Profit", "EPS in Rs"}

        return FundamentalData(
            quarterly=parsed["quarterly"],
            ratios=parsed["ratios"],
            sector=parsed["sector"],
            market_cap=parsed["market_cap"],
        )

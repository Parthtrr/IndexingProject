import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

class ScreenerParser:

    def parse(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")

        return {
            "quarterly": self._parse_quarterly(soup),
            "ratios": self._parse_ratios(soup),
            "sector": self._parse_sector(soup),
            "market_cap": self._parse_market_cap(soup),
        }

    def _parse_quarterly(self, soup) -> pd.DataFrame:
        table = soup.select_one("section#quarters table")
        if not table:
            raise ValueError("Quarterly results table not found")

        df = pd.read_html(StringIO(str(table)), header=0)[0]
        df.rename(columns={df.columns[0]: "metric"}, inplace=True)

        df["metric"] = (
            df["metric"]
            .str.replace("\xa0", " ", regex=False)
            .str.replace("+", "", regex=False)
            .str.strip()
        )
        return df

    def _parse_ratios(self, soup) -> pd.DataFrame:
        ratios = {}
        for li in soup.select("ul#top-ratios li"):
            name = li.select_one("span.name")
            value = li.select_one("span.number")
            if name and value:
                ratios[name.text.strip()] = value.text.strip()
        return pd.DataFrame(ratios.items(), columns=["Metric", "Value"])

    def _parse_sector(self, soup) -> pd.DataFrame:
        peer_section = soup.select_one("section#peers p.sub")
        if not peer_section:
            return pd.DataFrame(columns=["Category", "Value"])

        links = peer_section.find_all("a")
        keys = ["Broad Sector", "Sector", "Industry Group", "Industry"]
        values = [a.text.strip() for a in links[:4]]

        return pd.DataFrame(zip(keys, values), columns=["Category", "Value"])

    def _parse_market_cap(self, soup) -> float | None:
        for li in soup.select("ul#top-ratios li"):
            name = li.select_one("span.name")
            value = li.select_one("span.number")
            if name and value and name.text.strip() == "Market Cap":
                return float(value.text.replace(",", ""))
        return None

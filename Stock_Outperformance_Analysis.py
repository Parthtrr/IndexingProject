import yfinance as yf
import pandas as pd
import csv
import logging

# Configure logging for cleaner output
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"  # Only print the message, no metadata
)
logger = logging.getLogger(__name__)

# ==========================================================
# Outperforming a Sector (Sectorial Outperformance)
# ==========================================================
# This section compares a stock's performance to its sector ETF.
# ==========================================================

def compare_to_sector(stock_ticker, sector_etf_ticker, period='6mo'):
    stock = yf.Ticker(stock_ticker)
    sector = yf.Ticker(sector_etf_ticker)

    stock_hist = stock.history(period=period)
    sector_hist = sector.history(period=period)

    if stock_hist.empty or sector_hist.empty:
        logger.info(f"Data unavailable for {stock_ticker} or {sector_etf_ticker}. Skipping.")
        return None

    stock_return = (stock_hist['Close'].iloc[-1] / stock_hist['Close'].iloc[0]) - 1
    sector_return = (sector_hist['Close'].iloc[-1] / sector_hist['Close'].iloc[0]) - 1

    try:
        stock_sector = stock.info.get("sector", "Unknown Sector")
    except:
        stock_sector = "Unknown Sector"

    verdict = "Outperformed" if stock_return > sector_return else "Underperformed"

    logger.info("")
    logger.info(f"Sector Comparison - {stock_ticker} vs {sector_etf_ticker}")
    logger.info("------------------------------------------------------")
    logger.info(f"Stock             : {stock_ticker}")
    logger.info(f"Sector            : {stock_sector}")
    logger.info(f"Stock Return      : {stock_return:.2%}")
    logger.info(f"Sector ETF        : {sector_etf_ticker}")
    logger.info(f"Sector Return     : {sector_return:.2%}")
    logger.info(f"{stock_ticker} {verdict.lower()} its sector ({stock_sector})")

    return {
        "Ticker": stock_ticker,
        "Sector": stock_sector,
        "Period": period,
        "Stock Return (%)": round(stock_return * 100, 2),
        "Sector ETF": sector_etf_ticker,
        "Sector Return (%)": round(sector_return * 100, 2),
        "Performance": verdict
    }

# ------------------------------------------------------
# Compare Multiple Stocks Against Sector ETFs
# ------------------------------------------------------

stock_sector_pairs = [
    ("AAPL", "XLK"), ("MSFT", "XLK"), ("GOOGL", "XLK"), ("NVDA", "XLK"), ("ADBE", "XLK"),
    ("JPM", "XLF"), ("BAC", "XLF"), ("WFC", "XLF"), ("C", "XLF"), ("GS", "XLF"),
    ("PFE", "XLV"), ("JNJ", "XLV"), ("MRK", "XLV"), ("UNH", "XLV"), ("ABBV", "XLV"),
    ("XOM", "XLE"), ("CVX", "XLE"), ("SLB", "XLE"), ("COP", "XLE"), ("PSX", "XLE"),
    ("TSLA", "XLY"), ("AMZN", "XLY"), ("HD", "XLY"), ("MCD", "XLY"), ("LOW", "XLY"),
    ("KO", "XLP"), ("PEP", "XLP"), ("WMT", "XLP"), ("PG", "XLP"), ("COST", "XLP"),
    ("VZ", "XLC"), ("T", "XLC"), ("TMUS", "XLC"), ("CHTR", "XLC"), ("CMCSA", "XLC"),
    ("NEM", "XLB"), ("LIN", "XLB"), ("SHW", "XLB"), ("DOW", "XLB"), ("FCX", "XLB"),
    ("DUK", "XLU"), ("NEE", "XLU"), ("SO", "XLU"), ("AEP", "XLU"), ("EXC", "XLU"),
    ("PLD", "XLRE"), ("AMT", "XLRE"), ("CCI", "XLRE"), ("EQIX", "XLRE"), ("PSA", "XLRE")
]

sector_results = []
for stock, etf in stock_sector_pairs:
    res = compare_to_sector(stock, etf)
    if res:
        sector_results.append(res)

sector_csv = "stock_vs_sector_comparison.csv"
with open(sector_csv, mode='w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=sector_results[0].keys())
    writer.writeheader()
    writer.writerows(sector_results)

logger.info("")
logger.info(f"Sector comparison results saved to: {sector_csv}")

# ==========================================================
# Outperforming Its Own History (Historical Outperformance)
# ==========================================================
# This section compares a stock's current return to its historical averages.
# ==========================================================

def compare_to_own_history(stock_ticker, analysis_period='3mo', history_years=5):
    stock = yf.Ticker(stock_ticker)

    recent_hist = stock.history(period=analysis_period)
    if recent_hist.empty:
        return {"Ticker": stock_ticker, "Status": "No recent data"}

    current_return = (recent_hist['Close'].iloc[-1] / recent_hist['Close'].iloc[0]) - 1

    all_hist = stock.history(period=f"{history_years}y")
    if all_hist.empty:
        return {"Ticker": stock_ticker, "Status": "No historical data"}

    monthly_close = all_hist['Close'].resample('ME').last()
    monthly_returns = monthly_close.pct_change()
    avg_monthly_return = monthly_returns.mean()

    months = int(analysis_period.replace("mo", ""))
    expected_return = avg_monthly_return * months

    verdict = "Outperformed" if current_return > expected_return else "Underperformed"

    return {
        "Ticker": stock_ticker,
        "Period": analysis_period,
        "Years of History": history_years,
        "Recent Return (%)": round(current_return * 100, 2),
        "Expected Return (%)": round(expected_return * 100, 2),
        "Result": verdict,
        "Status": "OK"
    }

# ------------------------------------------------------
# Compare Multiple Stocks Against Their Own History
# ------------------------------------------------------

stock_list = list({pair[0] for pair in stock_sector_pairs})

history_results = []
for ticker in stock_list:
    res = compare_to_own_history(ticker, analysis_period="3mo", history_years=5)
    history_results.append(res)

history_csv = "own_history_comparison.csv"
pd.DataFrame(history_results).to_csv(history_csv, index=False)

summary_df = pd.DataFrame(history_results)
logger.info("")
logger.info("Own-History Performance Summary:")
logger.info(summary_df[["Ticker", "Recent Return (%)", "Expected Return (%)", "Result"]].to_string(index=False))
logger.info("")
logger.info(f"Historical comparison results saved to: {history_csv}")

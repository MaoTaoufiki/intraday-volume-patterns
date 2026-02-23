import logging
import time
from pathlib import Path
from datetime import datetime, UTC

import pandas as pd
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


TICKERS: dict[str, list[str]] = {
    "Technology":            ["AAPL", "MSFT", "NVDA", "AMD", "INTC"],
    "Communication":         ["GOOGL", "META", "NFLX", "SNAP", "PINS"],
    "Consumer Discretionary":["AMZN", "TSLA", "NKE", "MCD", "SBUX"],
    "Consumer Staples":      ["PG", "KO", "PEP", "WMT", "COST"],
    "Health Care":           ["JNJ", "UNH", "PFE", "ABBV", "MRNA"],
    "Financials":            ["JPM", "BAC", "GS", "V", "AXP"],
    "Industrials":           ["CAT", "GE", "BA", "UPS", "HON"],
    "Energy":                ["XOM", "CVX", "COP", "SLB", "OXY"],
    "Materials":             ["LIN", "APD", "NEM", "FCX", "ALB"],
    "Utilities":             ["NEE", "DUK", "SO", "AEP", "EXC"],
    "Real Estate":           ["AMT", "PLD", "EQIX", "SPG", "O"],
}

INTERVAL = "5m"       
PERIOD   = "60d"     
OUTPUT_DIR = Path("data")
SLEEP_BETWEEN_TICKERS = 1.0 


def classify_market_cap(market_cap: float | None) -> str:
    if market_cap is None:
        return "unknown"
    if market_cap >= 10_000_000_000:
        return "large"
    if market_cap >= 2_000_000_000:
        return "mid"
    return "small"


def fetch_metadata(ticker: str, sector: str) -> dict:
    try:
        info = yf.Ticker(ticker).info
        market_cap = info.get("marketCap")
        return {
            "ticker":          ticker,
            "company_name":    info.get("longName", ""),
            "sector":          sector,
            "industry":        info.get("industry", ""),
            "market_cap":      market_cap,
            "market_cap_band": classify_market_cap(market_cap),
            "country":         info.get("country", ""),
            "currency":        info.get("currency", "USD"),
            "fetched_at":      datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S'),
        }
    except Exception:
        logger.exception("Failed to fetch metadata for %s", ticker)
        return {}


def fetch_intraday(ticker: str, sector: str, market_cap_band: str) -> pd.DataFrame | None:
    try:
        df = yf.download(
            ticker,
            period=PERIOD,
            interval=INTERVAL,
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            logger.warning("No intraday data returned for %s", ticker)
            return None

        df = df.reset_index()
        
        df.columns = [
            (c[0] if isinstance(c, tuple) else c).lower()
            for c in df.columns
        ]

        time_col = "datetime" if "datetime" in df.columns else "date"
        df = df.rename(columns={time_col: "bar_time"})

        df["bar_time"] = pd.to_datetime(df["bar_time"]).dt.tz_localize(None)

        df["ticker"]          = ticker
        df["sector"]          = sector
        df["market_cap_band"] = market_cap_band
        df["interval"]        = INTERVAL
        df["ingested_at"]     = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')

        df = df[["ticker", "sector", "market_cap_band", "bar_time",
                 "open", "high", "low", "close", "volume", "interval", "ingested_at"]]

        logger.info("Fetched %d rows for %s", len(df), ticker)
        return df

    except Exception:
        logger.exception("Failed to fetch intraday data for %s", ticker)
        return None


def fetch_daily(ticker: str) -> pd.DataFrame | None:
    try:
        df = yf.download(
            ticker,
            period="2y",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            logger.warning("No daily data returned for %s", ticker)
            return None

        df = df.reset_index()
        df.columns = [
            (c[0] if isinstance(c, tuple) else c).lower()
            for c in df.columns
        ]

        time_col = "date" if "date" in df.columns else "datetime"
        df = df.rename(columns={time_col: "trade_date"})
        df["ticker"]      = ticker
        df["ingested_at"] = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')

        df = df[["ticker", "trade_date", "open", "high", "low", "close", "volume", "ingested_at"]]
        return df

    except Exception:
        logger.exception("Failed to fetch daily data for %s", ticker)
        return None


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_metadata:  list[dict]      = []
    all_intraday:  list[pd.DataFrame] = []
    all_daily:     list[pd.DataFrame] = []

    for sector, tickers in TICKERS.items():
        logger.info("── Sector: %s (%d tickers) ──", sector, len(tickers))

        for ticker in tickers:
            logger.info("Processing %s", ticker)

            meta = fetch_metadata(ticker, sector)
            if meta:
                all_metadata.append(meta)
                mkt_band = meta.get("market_cap_band", "unknown")
            else:
                mkt_band = "unknown"

            intraday_df = fetch_intraday(ticker, sector, mkt_band)
            if intraday_df is not None:
                all_intraday.append(intraday_df)

            daily_df = fetch_daily(ticker)
            if daily_df is not None:
                all_daily.append(daily_df)

            time.sleep(SLEEP_BETWEEN_TICKERS)

    if all_metadata:
        meta_path = OUTPUT_DIR / "ticker_metadata.csv"
        pd.DataFrame(all_metadata).to_csv(meta_path, index=False)
        logger.info("Wrote metadata: %s (%d rows)", meta_path, len(all_metadata))

    if all_intraday:
        intraday_path = OUTPUT_DIR / "intraday_bars.csv"
        pd.concat(all_intraday, ignore_index=True).to_csv(intraday_path, index=False)
        logger.info("Wrote intraday bars: %s", intraday_path)

    if all_daily:
        daily_path = OUTPUT_DIR / "daily_bars.csv"
        pd.concat(all_daily, ignore_index=True).to_csv(daily_path, index=False)
        logger.info("Wrote daily bars: %s", daily_path)

    logger.info("Done — output in %s/", OUTPUT_DIR)


if __name__ == "__main__":
    main()
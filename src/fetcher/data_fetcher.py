import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from typing import List, Dict, Any, Optional
import requests
from datetime import datetime
from src.config.settings import settings
from src.database.connection import SessionLocal
from src.database.models import create_tables, stock_prices
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_fetcher")

API_URL = "https://www.alphavantage.co/query"

class StockRow(BaseModel):
    symbol: str
    price_date: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]

def build_params(symbol: str) -> Dict[str, str]:
    return {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": settings.ALPHAVANTAGE_API_KEY,
        "outputsize": "compact"
    }

def fetch_symbol_json(symbol: str) -> Dict[str, Any]:
    params = build_params(symbol)
    logger.info(f"Fetching symbol={symbol}")
    resp = requests.get(API_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "Time Series (Daily)" in data:
        return {"symbol": symbol, "time_series": data["Time Series (Daily)"]}
    if "Note" in data:
        raise RuntimeError(f"API rate limit: {data.get('Note')}")
    if "Error Message" in data:
        raise RuntimeError(f"API error: {data.get('Error Message')}")
    raise RuntimeError("Unexpected API response format")

def parse_time_series_to_rows(symbol: str, ts: Dict[str, Dict[str, str]], limit: int = 5) -> List[StockRow]:
    rows = []
    dates = sorted(ts.keys(), reverse=True)[:limit]
    for date_str in dates:
        values = ts[date_str]
        row = StockRow(
            symbol=symbol,
            price_date=datetime.strptime(date_str, "%Y-%m-%d"),
            open=float(values.get("1. open", 0)),
            high=float(values.get("2. high", 0)),
            low=float(values.get("3. low", 0)),
            close=float(values.get("4. close", 0)),
            volume=int(values.get("5. volume", 0)),
        )
        rows.append(row)
    return rows

def ensure_unique_constraint():
    with SessionLocal() as session:
        session.execute(
            text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'stock_prices_symbol_date_unique'
                ) THEN
                    ALTER TABLE stock_prices
                    ADD CONSTRAINT stock_prices_symbol_date_unique
                    UNIQUE (symbol, price_date);
                END IF;
            END$$;
            """)
        )
        session.commit()

def upsert_rows(rows: List[StockRow]):
    if not rows:
        return
    stmt = insert(stock_prices).values([
        {
            "symbol": r.symbol,
            "price_date": r.price_date.date(),
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume
        } for r in rows
    ])
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements=["symbol", "price_date"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume
        }
    )
    with SessionLocal() as session:
        session.execute(do_update_stmt)
        session.commit()
        logger.info(f"Upserted {len(rows)} rows for {rows[0].symbol}")

def run_pipeline():
    create_tables()
    ensure_unique_constraint()

    symbols = [s.strip() for s in settings.SYMBOLS.split(",") if s.strip()]
    total_rows = 0

    for symbol in symbols:
        try:
            raw = fetch_symbol_json(symbol)
            ts = raw["time_series"]
            rows = parse_time_series_to_rows(symbol, ts, limit=3)
            upsert_rows(rows)
            total_rows += len(rows)
        except Exception as e:
            logger.error(f"Failed for {symbol}: {e}")

    logger.info(f"Pipeline finished. Total rows upserted: {total_rows}")
    return total_rows

if __name__ == "__main__":
    run_pipeline()

from typing import List, Dict, Any, Optional
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from datetime import datetime
from src.utils.logger import get_logger
from src.config.settings import settings
from src.database.connection import SessionLocal
from src.database.models import create_tables, stock_prices
from sqlalchemy import insert
from pydantic import BaseModel, validator

logger = get_logger("data_fetcher")

API_URL = "https://www.alphavantage.co/query"

class TimeSeriesDailyResponse(BaseModel):
    symbol: str
    time_series: Dict[str, Dict[str, str]]

    @validator("time_series", pre=True)
    def find_time_series(cls, v, values):
        if isinstance(v, dict):
            return v
        raise ValueError("Invalid time series format")

class StockRow(BaseModel):
    symbol: str
    price_date: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]

    class Config:
        arbitrary_types_allowed = True

def build_params(symbol: str) -> Dict[str, str]:
    return {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": settings.ALPHAVANTAGE_API_KEY,
        "outputsize": "compact"
    }

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def fetch_symbol_json(symbol: str) -> Dict[str, Any]:
    params = build_params(symbol)
    logger.info("Fetching symbol=%s from AlphaVantage", symbol)
    resp = requests.get(API_URL, params=params, timeout=15)
    if resp.status_code != 200:
        logger.warning("Non-200 response for %s: %s", symbol, resp.status_code)
        resp.raise_for_status()
    data = resp.json()
    for key in ["Time Series (Daily)", "Time Series (60min)", "Time Series (Daily)"]:
        if key in data:
            return {"symbol": symbol, "time_series": data[key]}
    if "Note" in data:
        raise RuntimeError(f"API rate limit or note: {data.get('Note')}")
    if "Error Message" in data:
        raise RuntimeError(f"API error: {data.get('Error Message')}")
    raise RuntimeError("Unexpected API response format")

def parse_time_series_to_rows(symbol: str, ts: Dict[str, Dict[str, str]], limit: int = 5):
    rows = []
    dates = sorted(ts.keys(), reverse=True)[:limit]
    for date_str in dates:
        values = ts[date_str]
        try:
            row = StockRow(
                symbol=symbol,
                price_date=datetime.strptime(date_str, "%Y-%m-%d"),
                open=float(values.get("1. open")) if values.get("1. open") else None,
                high=float(values.get("2. high")) if values.get("2. high") else None,
                low=float(values.get("3. low")) if values.get("3. low") else None,
                close=float(values.get("4. close")) if values.get("4. close") else None,
                volume=int(values.get("5. volume")) if values.get("5. volume") else None,
            )
            rows.append(row)
        except Exception as e:
            logger.warning("Skipping row for %s %s due to parse error: %s", symbol, date_str, e)
    return rows

def upsert_rows(rows: List[StockRow]):
    if not rows:
        logger.info("No rows to upsert")
        return
    stmt = insert(stock_prices).values([
        {
            "symbol": row.symbol,
            "price_date": row.price_date.date(),
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume
        }
        for row in rows
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
        try:
            session.execute(do_update_stmt)
            session.commit()
            logger.info("Upserted %d rows", len(rows))
        except Exception:
            session.rollback()
            logger.exception("Error during upsert")
            raise

def run_pipeline():
    create_tables()
    symbols = [s.strip() for s in settings.SYMBOLS.split(",") if s.strip()]
    logger.info("Pipeline starting for symbols: %s", symbols)
    for symbol in symbols:
        try:
            raw = fetch_symbol_json(symbol)
            ts = raw["time_series"]
            rows = parse_time_series_to_rows(symbol, ts, limit=3)
            upsert_rows(rows)
        except Exception as e:
            logger.exception("Failed pipeline for %s: %s", symbol, e)

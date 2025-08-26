import sys
from src.config.settings import settings
from sqlalchemy import text
from src.database.connection import engine
import requests
from src.utils.logger import get_logger

logger = get_logger("health_check")

def check_db():
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT 1"))
            logger.info("DB OK, result=%s", res.scalar())
    except Exception as e:
        logger.exception("DB health check failed")
        return False
    return True

def check_api():
    if not settings.ALPHAVANTAGE_API_KEY:
        logger.warning("No AlphaVantage API key set")
        return False
    resp = requests.get("https://www.alphavantage.co/query", params={
        "function": "TIME_SERIES_DAILY",
        "symbol": "AAPL",
        "apikey": settings.ALPHAVANTAGE_API_KEY,
        "outputsize": "compact"
    }, timeout=10)
    logger.info("API health status_code=%s", resp.status_code)
    return resp.status_code == 200

if __name__ == "__main__":
    ok_db = check_db()
    ok_api = check_api()
    if ok_db and ok_api:
        logger.info("All health checks passed")
        sys.exit(0)
    logger.error("Some checks failed: db=%s api=%s", ok_db, ok_api)
    sys.exit(2)

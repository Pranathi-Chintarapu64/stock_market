from sqlalchemy import Table, Column, String, Date, Numeric, BigInteger, TIMESTAMP, MetaData, insert
from src.database.connection import engine

metadata = MetaData()

stock_prices = Table(
    "stock_prices",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("price_date", Date, primary_key=True),
    Column("open", Numeric),
    Column("high", Numeric),
    Column("low", Numeric),
    Column("close", Numeric),
    Column("volume", BigInteger),
    Column("fetched_at", TIMESTAMP),
)

def create_tables():
    metadata.create_all(bind=engine)

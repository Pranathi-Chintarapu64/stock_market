CREATE TABLE IF NOT EXISTS stock_prices (
  symbol TEXT NOT NULL,
  price_date DATE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (symbol, price_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(price_date DESC);

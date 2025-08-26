import pytest
from src.fetcher.data_fetcher import parse_time_series_to_rows, StockRow

@pytest.fixture
def sample_ts():
    return {
        "2025-08-01": {
            "1. open": "100.0",
            "2. high": "105.0",
            "3. low": "99.0",
            "4. close": "104.0",
            "5. volume": "1000000"
        },
        "2025-07-31": {
            "1. open": "98.0",
            "2. high": "101.0",
            "3. low": "97.0",
            "4. close": "100.5",
            "5. volume": "800000"
        }
    }

def test_parse_time_series_to_rows(sample_ts):
    rows = parse_time_series_to_rows("TEST", sample_ts, limit=2)
    assert len(rows) == 2
    assert rows[0].symbol == "TEST"
    assert rows[0].close == 104.0

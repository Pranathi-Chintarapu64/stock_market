from src.database.connection import engine

def test_engine_url():
    assert engine is not None
    assert hasattr(engine, "url")

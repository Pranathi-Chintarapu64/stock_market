from typing import Callable, TypeVar
import functools
from src.utils.logger import get_logger

T = TypeVar("T")
logger = get_logger("error_handler")

def safe_execute(fn: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            logger.exception("Unhandled exception in %s: %s", fn.__name__, exc)
            raise
    return wrapped

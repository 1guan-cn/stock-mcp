"""Data 层 — 融资融券。"""

from stock_service.data._cache import fetch_with_cache
from stock_service.data.adapters import tushare as ts
from stock_service.data.store import margin as margin_store
from stock_service.models import DataType


def get_margin(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """获取融资融券数据（增量缓存）。"""
    return fetch_with_cache(
        symbol,
        DataType.MARGIN,
        start_date,
        end_date,
        fetch_fn=ts.get_margin,
        save_fn=margin_store.save_margin,
        query_fn=margin_store.get_cached_margin,
    )

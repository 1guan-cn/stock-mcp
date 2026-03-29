"""Data 层 — 财务报表。"""

from stock_service.data._cache import fetch_with_cache
from stock_service.data.adapters import tushare as ts
from stock_service.data.store import finance as finance_store


def get_reports(
    symbol: str, report_type: str, start_period: str, end_period: str
) -> list[dict]:
    """获取财务报表（增量缓存）。"""
    data_type = f"finance_{report_type}"

    return fetch_with_cache(
        symbol, data_type, start_period, end_period,
        fetch_fn=lambda s, a, b: ts.get_financial(s, report_type, a, b),
        save_fn=lambda s, data: finance_store.save_reports(s, report_type, data),
        query_fn=lambda s, a, b: finance_store.get_cached_reports(s, report_type, a, b),
        exclude_today=False,
    )

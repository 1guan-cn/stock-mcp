"""Data 层 — 日线行情 + 复权因子 + 实时行情。"""

from stock_service.data._cache import fetch_with_cache
from stock_service.data.adapters import akshare as ak
from stock_service.data.adapters import tushare as ts
from stock_service.data.store import quote as quote_store
from stock_service.models import AssetType, DataType
from stock_service.models.quote import DailyBar


def get_daily(
    symbol: str, asset_type: str, start_date: str, end_date: str
) -> list[DailyBar]:
    """获取日线数据（自动缓存，按 asset_type 分派）。"""
    if asset_type == AssetType.INDEX:
        return fetch_with_cache(
            symbol, DataType.INDEX_DAILY, start_date, end_date,
            fetch_fn=lambda s, a, b: ts.get_index_daily(s, a, b),
            save_fn=quote_store.save_index_bars,
            query_fn=quote_store.get_cached_index_bars,
        )
    elif asset_type == AssetType.FUND:
        return fetch_with_cache(
            symbol, DataType.FUND_DAILY, start_date, end_date,
            fetch_fn=lambda s, a, b: ts.get_fund_daily(s, a, b),
            save_fn=quote_store.save_fund_bars,
            query_fn=quote_store.get_cached_fund_bars,
        )
    else:
        return fetch_with_cache(
            symbol, DataType.STOCK_DAILY, start_date, end_date,
            fetch_fn=lambda s, a, b: ts.get_stock_daily(s, a, b),
            save_fn=quote_store.save_stock_bars,
            query_fn=quote_store.get_cached_stock_bars,
        )


def get_realtime(code: str, asset_type: str) -> dict | None:
    """获取实时行情（不缓存，直接透传 Adapter）。

    Args:
        code: 纯代码，A股为数字如 "000001"，美股为 ticker 如 "AAPL"
        asset_type: "stock"/"fund"/"index"/"hk"/"us"
    """
    return ak.get_realtime_quote(code, asset_type)


def get_adj_factors(
    symbol: str, start_date: str, end_date: str, *, is_fund: bool = False
) -> dict[str, float]:
    """获取复权因子（自动缓存）。"""
    return fetch_with_cache(
        symbol, DataType.ADJ_FACTOR, start_date, end_date,
        fetch_fn=lambda s, a, b: ts.get_adj_factor(s, a, b, is_fund=is_fund),
        save_fn=quote_store.save_adj_factors,
        query_fn=quote_store.get_cached_adj_factors,
    )

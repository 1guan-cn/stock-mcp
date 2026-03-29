"""Data 层 — 股票/基金/指数列表管理。"""

from datetime import datetime, timedelta, timezone

from stock_service.data.adapters import tushare as ts
from stock_service.data.store import listing as listing_store
from stock_service.models.listing import FundInfo, IndexInfo, SearchItem, StockInfo

_CACHE_TTL = timedelta(hours=24)


def _need_refresh(table: str) -> bool:
    last = listing_store.get_last_updated(table)
    if last is None:
        return True
    return datetime.now(timezone.utc) - last > _CACHE_TTL


def ensure_data() -> None:
    """确保列表数据已加载（24h TTL 刷新）。"""
    if _need_refresh("stock_list"):
        listing_store.save_stocks(ts.get_stock_list())
    if _need_refresh("fund_list"):
        listing_store.save_funds(ts.get_fund_list())
    if _need_refresh("index_list"):
        listing_store.save_indexes(ts.get_index_list())


def get_stocks(
    *,
    market: str | None = None,
    industry: str | None = None,
    area: str | None = None,
    keyword: str | None = None,
) -> list[StockInfo]:
    if _need_refresh("stock_list"):
        listing_store.save_stocks(ts.get_stock_list())
    return listing_store.get_stocks(market=market, industry=industry, area=area, keyword=keyword)


def get_funds(
    *,
    fund_type: str | None = None,
    management: str | None = None,
    keyword: str | None = None,
) -> list[FundInfo]:
    if _need_refresh("fund_list"):
        listing_store.save_funds(ts.get_fund_list())
    return listing_store.get_funds(fund_type=fund_type, management=management, keyword=keyword)


def get_indexes(*, keyword: str | None = None) -> list[IndexInfo]:
    if _need_refresh("index_list"):
        listing_store.save_indexes(ts.get_index_list())
    return listing_store.get_indexes(keyword=keyword)


def search(keyword: str) -> list[SearchItem]:
    ensure_data()
    return listing_store.search(keyword)


def resolve_symbols(
    *, code: str | None = None, market: str | None = None, industry: str | None = None
) -> list[tuple[str, str, str]]:
    return listing_store.resolve_symbols(code=code, market=market, industry=industry)

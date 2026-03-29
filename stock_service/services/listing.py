"""Service 层 — 列表查询。薄委托到 Data 层。"""

from stock_service.data import listing as listing_data
from stock_service.models.listing import FundInfo, IndexInfo, SearchItem, StockInfo


def get_stock_list(
    *,
    market: str | None = None,
    industry: str | None = None,
    area: str | None = None,
    keyword: str | None = None,
) -> list[StockInfo]:
    return listing_data.get_stocks(market=market, industry=industry, area=area, keyword=keyword)


def get_fund_list(
    *,
    fund_type: str | None = None,
    management: str | None = None,
    keyword: str | None = None,
) -> list[FundInfo]:
    return listing_data.get_funds(fund_type=fund_type, management=management, keyword=keyword)


def get_index_list(
    *,
    keyword: str | None = None,
) -> list[IndexInfo]:
    return listing_data.get_indexes(keyword=keyword)


def search(keyword: str) -> list[SearchItem]:
    return listing_data.search(keyword)

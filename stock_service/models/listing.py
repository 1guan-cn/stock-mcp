from pydantic import BaseModel

from stock_service.models import AssetType


class StockInfo(BaseModel):
    symbol: str
    name: str
    area: str | None = None
    industry: str | None = None
    market: str | None = None
    list_date: str | None = None


class FundInfo(BaseModel):
    symbol: str
    name: str
    fund_type: str | None = None
    management: str | None = None
    list_date: str | None = None


class IndexInfo(BaseModel):
    symbol: str
    name: str
    fullname: str | None = None
    market: str | None = None
    publisher: str | None = None
    category: str | None = None
    list_date: str | None = None


class StockListResponse(BaseModel):
    total: int
    items: list[StockInfo]


class FundListResponse(BaseModel):
    total: int
    items: list[FundInfo]


class IndexListResponse(BaseModel):
    total: int
    items: list[IndexInfo]


class SearchItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType


class SearchResponse(BaseModel):
    total: int
    items: list[SearchItem]

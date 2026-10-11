from enum import StrEnum


class AssetType(StrEnum):
    STOCK = "stock"
    FUND = "fund"
    INDEX = "index"
    HK = "hk"
    US = "us"
    GLOBAL_INDEX = "global_index"


class AdjustType(StrEnum):
    QFQ = "qfq"
    HFQ = "hfq"
    NONE = "none"


class DetailLevel(StrEnum):
    SIMPLE = "simple"
    ALL = "all"


class DataType(StrEnum):
    STOCK_DAILY = "stock_daily"
    FUND_DAILY = "fund_daily"
    INDEX_DAILY = "index_daily"
    ADJ_FACTOR = "adj_factor"
    INDEX_VALUATION = "index_valuation"
    FUND_FLOW = "fund_flow"
    NORTHBOUND = "northbound"
    ETF_SUBSCRIPTION = "etf_subscription"
    MARGIN = "margin"


class UnsupportedReason(StrEnum):
    NOT_ETF = "not_etf"
    INDEX_NOT_SUPPORTED = "index_not_supported"
    ETF_NO_MAIN_FORCE_DATA = "etf_no_main_force_data"
    SYMBOL_NOT_FOUND = "symbol_not_found"
    INDEX_NOT_IN_REGISTRY = "index_not_in_registry"
    INDEX_DATA_UNAVAILABLE = "index_data_unavailable"
    DATA_UNAVAILABLE = "data_unavailable"  # 该标的适用此口径，但请求区间内没有数据
    ASSET_TYPE_NOT_SUPPORTED = "asset_type_not_supported"  # 该资产类型不支持此口径（如港股无日线）


class FinanceReportType(StrEnum):
    INCOME = "income"
    BALANCE_SHEET = "balance_sheet"
    CASHFLOW = "cashflow"
    FORECAST = "forecast"
    EXPRESS = "express"
    DIVIDEND = "dividend"
    INDICATOR = "indicator"
    AUDIT = "audit"
    MAIN_BUSINESS = "main_business"
    DISCLOSURE = "disclosure"

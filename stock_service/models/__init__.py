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

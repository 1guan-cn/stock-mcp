"""Data 层 — 估值 + 资金流 + 北向资金。"""

import logging

from stock_service.data import quote as quote_data
from stock_service.data._cache import fetch_with_cache, n_days_ago_str, today_str
from stock_service.data._registry import resolve_etf_to_index
from stock_service.data.adapters import akshare as ak
from stock_service.data.adapters import tushare as ts
from stock_service.data.store import factor as factor_store
from stock_service.data.store import quote as quote_store
from stock_service.models import AssetType, DataType, UnsupportedReason
from stock_service.models.factor import ValuationData

logger = logging.getLogger(__name__)

# 个股估值取的日线窗口：够长以覆盖停牌/长期无成交的标的（取窗口内最后一根 K 线）
_STOCK_VALUATION_WINDOW_DAYS = 90


# ── 估值 ──

def get_stock_valuation(symbol: str) -> ValuationData:
    """取个股最近一根 K 线的 pe_ttm / pb / 股息率。

    走 data.quote 的带缓存取数（会按需补拉），不能只读本地缓存 —— 否则冷启动或
    行情窗口外会静默返回「没有估值」，且下游无法区分是「标的没有数据」还是
    「这次没给」。
    """
    bars = quote_data.get_daily(
        symbol, AssetType.STOCK,
        n_days_ago_str(_STOCK_VALUATION_WINDOW_DAYS), today_str(),
    )
    if not bars:
        return ValuationData(unsupported_reason=UnsupportedReason.DATA_UNAVAILABLE)

    latest = bars[-1]
    return ValuationData(
        pe_ttm=round(latest.pe_ttm, 2) if latest.pe_ttm else None,
        pb=round(latest.pb, 2) if latest.pb else None,
        dividend_yield=round(latest.dv_ttm, 2) if latest.dv_ttm else None,
        data_as_of=latest.date,
    )


def get_index_valuation_history(index_symbol: str) -> list[dict]:
    """获取指数3年估值历史（用于计算PE/PB百分位）。"""
    today = today_str()
    start_date = n_days_ago_str(1095)

    return fetch_with_cache(
        index_symbol, DataType.INDEX_VALUATION, start_date, today,
        fetch_fn=lambda s, a, b: ts.get_index_valuation(s, a, b),
        save_fn=factor_store.save_index_valuation,
        query_fn=factor_store.get_cached_index_valuation,
    )


def get_valuation(symbol: str, asset_type: str) -> ValuationData:
    """统一入口：根据 asset_type 获取估值。**始终返回对象**，取不到时带 unsupported_reason。"""
    try:
        if asset_type == AssetType.STOCK:
            return get_stock_valuation(symbol)

        index_symbol = symbol
        if asset_type == AssetType.FUND:
            index_symbol = resolve_etf_to_index(symbol)
            if not index_symbol:
                return ValuationData(unsupported_reason=UnsupportedReason.INDEX_NOT_IN_REGISTRY)

        rows = get_index_valuation_history(index_symbol)
        if not rows:
            return ValuationData(unsupported_reason=UnsupportedReason.INDEX_DATA_UNAVAILABLE)

        latest = rows[-1]
        pe_ttm = latest.get("pe_ttm")
        pb = latest.get("pb")
        data_date = latest.get("date")
        return ValuationData(
            pe_ttm=round(pe_ttm, 2) if pe_ttm else None,
            pb=round(pb, 2) if pb else None,
            data_as_of=data_date,
        )
    except Exception as e:
        logger.warning("get_valuation(%s) failed: %s", symbol, e)
        return ValuationData(unsupported_reason=UnsupportedReason.DATA_UNAVAILABLE)


# ── 资金流 ──

def get_fund_flow(symbol: str) -> list[dict]:
    """获取个股主力资金流（Tushare moneyflow，仅 A 股个股）。

    ETF / 指数的"主力资金"口径在 A 股市场并无干净数据源（做市商撮合
    不适用大单分类），因此不在本层取数；调用方应在 service 层守门。
    """
    if not symbol.endswith((".SH", ".SZ")):
        return []
    return fetch_with_cache(
        symbol, DataType.FUND_FLOW, n_days_ago_str(30), today_str(),
        fetch_fn=ts.get_fund_flow,
        save_fn=factor_store.save_fund_flow,
        query_fn=factor_store.get_cached_fund_flow,
    )


# ── ETF 申购赎回 ──

def get_etf_fund_flow(symbol: str) -> list[dict]:
    """获取ETF申购赎回份额变动数据（增量缓存）。仅 ETF/基金有效。

    多取 15 天用于差分计算首日份额变动（adapter 靠相邻行差分得到份额变动）。
    """
    return fetch_with_cache(
        symbol, DataType.ETF_SUBSCRIPTION, n_days_ago_str(45), today_str(),
        fetch_fn=ts.get_etf_fund_flow,
        save_fn=factor_store.save_etf_subscription,
        query_fn=lambda s, a, b: factor_store.get_cached_etf_subscription(
            s, n_days_ago_str(30), today_str()
        ),
    )


# ── 商品价格 ──

def get_commodity_price(commodity_code: str) -> list[dict]:
    """获取商品历史价格（无 DB 缓存，直接透传 adapter）。"""
    try:
        return ak.get_commodity_price(commodity_code)
    except Exception as e:
        logger.warning("get_commodity_price(%s) failed: %s", commodity_code, e)
        return []


# ── ETF→指数映射 ──

def get_index_for_etf(etf_symbol: str) -> str | None:
    """将 ETF symbol 映射到其跟踪的指数 symbol。"""
    return resolve_etf_to_index(etf_symbol)


# ── 北向资金 ──

def get_northbound() -> list[dict]:
    """获取北向资金数据（增量缓存，市场级别）。"""
    return fetch_with_cache(
        "northbound", DataType.NORTHBOUND, n_days_ago_str(30), today_str(),
        fetch_fn=lambda s, a, b: ak.get_northbound_daily(),
        save_fn=lambda s, data: factor_store.save_northbound(data),
        query_fn=lambda s, a, b: factor_store.get_cached_northbound(
            n_days_ago_str(10), today_str()
        ),
    )

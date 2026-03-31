"""Data 层 — 估值 + 资金流 + 北向资金。"""

import logging

from stock_service.data._cache import (
    calc_missing_ranges,
    fetch_with_cache,
    is_today,
    n_days_ago_str,
    today_str,
)
from stock_service.data._registry import resolve_etf_to_index
from stock_service.data.adapters import akshare as ak
from stock_service.data.adapters import tushare as ts
from stock_service.data.store import factor as factor_store
from stock_service.data.store import quote as quote_store
from stock_service.models import AssetType, DataType
from stock_service.models.factor import ValuationData

logger = logging.getLogger(__name__)


# ── 估值 ──

def get_stock_valuation(symbol: str) -> ValuationData | None:
    """从 stock_daily_bar 取最近记录的 pe_ttm/pb/股息率。"""
    bars = quote_store.get_cached_stock_bars(symbol, n_days_ago_str(30), today_str())
    if not bars:
        return None
    latest = bars[-1]
    return ValuationData(
        pe_ttm=round(latest.pe_ttm, 2) if latest.pe_ttm else None,
        pb=round(latest.pb, 2) if latest.pb else None,
        dividend_yield=round(latest.dv_ttm, 2) if latest.dv_ttm else None,
    )


def get_index_valuation(index_symbol: str) -> ValuationData | None:
    """获取指数估值（增量缓存）。"""
    today = today_str()
    start_date = n_days_ago_str(1095)

    rows = fetch_with_cache(
        index_symbol, DataType.INDEX_VALUATION, start_date, today,
        fetch_fn=lambda s, a, b: ts.get_index_valuation(s, a, b),
        save_fn=factor_store.save_index_valuation,
        query_fn=factor_store.get_cached_index_valuation,
    )
    if not rows:
        return None

    latest = rows[-1]
    pe_ttm = latest.get("pe_ttm")
    pb = latest.get("pb")
    return ValuationData(
        pe_ttm=round(pe_ttm, 2) if pe_ttm else None,
        pb=round(pb, 2) if pb else None,
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


def get_valuation(symbol: str, asset_type: str) -> ValuationData | None:
    """统一入口：根据 asset_type 获取估值。"""
    try:
        if asset_type == AssetType.STOCK:
            return get_stock_valuation(symbol)

        index_symbol = symbol
        if asset_type == AssetType.FUND:
            index_symbol = resolve_etf_to_index(symbol)
            if not index_symbol:
                return None

        return get_index_valuation(index_symbol)
    except Exception as e:
        logger.warning("get_valuation(%s) failed: %s", symbol, e)
        return None


# ── 资金流 ──

def _symbol_to_akshare(symbol: str) -> tuple[str, str] | None:
    """将 symbol（如 510300.SH）转为 AKShare 需要的 (code, market)。"""
    parts = symbol.split(".")
    if len(parts) != 2:
        return None
    code, exchange = parts
    if exchange in ("SH", "SZ"):
        return code, exchange.lower()
    return None


def get_fund_flow(symbol: str) -> list[dict]:
    """获取资金流数据（增量缓存）。仅股票和 ETF 有资金流。"""
    try:
        ak_params = _symbol_to_akshare(symbol)
        if not ak_params:
            return []
        code, market = ak_params

        today = today_str()
        start_date = n_days_ago_str(30)

        coverage = quote_store.get_coverage(symbol, DataType.FUND_FLOW)
        missing = calc_missing_ranges(start_date, today, coverage)

        if missing:
            raw = ak.get_fund_flow(code, market)
            if raw:
                factor_store.save_fund_flow(symbol, raw)
                dates = [r["date"] for r in raw]
                if dates:
                    effective_max = max(dates)
                    if is_today(effective_max):
                        effective_max = n_days_ago_str(1)
                    effective_min = min(dates)
                    if effective_max >= effective_min:
                        quote_store.update_coverage(symbol, effective_min, effective_max, DataType.FUND_FLOW)

        return factor_store.get_cached_fund_flow(symbol, n_days_ago_str(30), today)
    except Exception as e:
        logger.warning("get_fund_flow(%s) failed: %s", symbol, e)
        return []


# ── ETF 申购赎回 ──

def get_etf_fund_flow(symbol: str) -> list[dict]:
    """获取ETF申购赎回份额变动数据（增量缓存）。仅 ETF/基金有效。"""
    try:
        today = today_str()
        start_date = n_days_ago_str(30)
        # 多取 15 天用于差分计算首日份额变动
        fetch_start = n_days_ago_str(45)

        coverage = quote_store.get_coverage(symbol, DataType.ETF_SUBSCRIPTION)
        missing = calc_missing_ranges(start_date, today, coverage)

        if missing:
            raw = ts.get_etf_fund_flow(symbol, fetch_start, today)
            if raw:
                factor_store.save_etf_subscription(symbol, raw)
                dates = [r["date"] for r in raw]
                if dates:
                    effective_max = max(dates)
                    if is_today(effective_max):
                        effective_max = n_days_ago_str(1)
                    effective_min = min(dates)
                    if effective_max >= effective_min:
                        quote_store.update_coverage(symbol, effective_min, effective_max, DataType.ETF_SUBSCRIPTION)

        return factor_store.get_cached_etf_subscription(symbol, n_days_ago_str(30), today)
    except Exception as e:
        logger.warning("get_etf_fund_flow(%s) failed: %s", symbol, e)
        return []


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
    try:
        today = today_str()
        start_date = n_days_ago_str(30)

        coverage = quote_store.get_coverage("northbound", DataType.NORTHBOUND)
        missing = calc_missing_ranges(start_date, today, coverage)

        if missing:
            raw = ak.get_northbound_daily()
            if raw:
                factor_store.save_northbound(raw)
                dates = [r["date"] for r in raw]
                if dates:
                    effective_max = max(dates)
                    if is_today(effective_max):
                        effective_max = n_days_ago_str(1)
                    effective_min = min(dates)
                    if effective_max >= effective_min:
                        quote_store.update_coverage("northbound", effective_min, effective_max, DataType.NORTHBOUND)

        return factor_store.get_cached_northbound(n_days_ago_str(10), today)
    except Exception as e:
        logger.warning("get_northbound failed: %s", e)
        return []

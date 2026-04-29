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

def get_fund_flow(symbol: str) -> list[dict]:
    """获取个股主力资金流（Tushare moneyflow，仅 A 股个股）。

    ETF / 指数的"主力资金"口径在 A 股市场并无干净数据源（做市商撮合
    不适用大单分类），因此不在本层取数；调用方应在 service 层守门。
    """
    if not symbol.endswith((".SH", ".SZ")):
        return []
    try:
        today = today_str()
        start_date = n_days_ago_str(30)

        coverage = quote_store.get_coverage(symbol, DataType.FUND_FLOW)
        missing = calc_missing_ranges(start_date, today, coverage)

        if missing:
            raw = ts.get_fund_flow(symbol, start_date, today)
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
    """获取ETF申购赎回份额变动数据（增量缓存）。仅 ETF/基金有效。

    数据源策略：
    - 主源 tushare etf_share_size（T+1 晚发布，盘中查询常态滞后到 T-2）
    - 兜底 akshare fund_etf_fund_flow_hist（eastmoney 源，盘后较快）
    - 触发条件：tushare 最新非空 net_inflow 滞后 ≥ 2 天 → 调 akshare 补 cutoff 之后日期
    """
    from datetime import datetime

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

            # 兜底：tushare 最新非空 net_inflow 滞后 ≥ 2 天 → 调 akshare 补
            ts_latest = max(
                (r["date"] for r in (raw or []) if r.get("net_inflow") is not None),
                default=None,
            )
            need_fallback = (ts_latest is None) or (
                (datetime.strptime(today, "%Y%m%d") - datetime.strptime(ts_latest, "%Y%m%d")).days >= 2
            )
            if need_fallback:
                code = symbol.split(".")[0]
                try:
                    ak_raw = ak.get_etf_fund_flow(code)
                except Exception as e:
                    logger.warning("akshare fallback get_etf_fund_flow(%s) failed: %s", code, e)
                    ak_raw = []
                cutoff = ts_latest or "00000000"
                ak_fill = [
                    r for r in ak_raw
                    if r.get("date") and r["date"] > cutoff and r.get("net_inflow") is not None
                ]
                if ak_fill:
                    factor_store.save_etf_subscription(symbol, ak_fill)
                    ak_dates = [r["date"] for r in ak_fill]
                    ak_max = max(ak_dates)
                    if is_today(ak_max):
                        ak_max = n_days_ago_str(1)
                    ak_min = min(ak_dates)
                    if ak_max >= ak_min:
                        quote_store.update_coverage(symbol, ak_min, ak_max, DataType.ETF_SUBSCRIPTION)
                    logger.info(
                        "etf_fund_flow(%s) akshare fallback filled %d days (cutoff=%s, latest=%s)",
                        code, len(ak_fill), cutoff, max(ak_dates),
                    )

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

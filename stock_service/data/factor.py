"""Data 层 — 估值 + 资金流 + 北向资金。"""

import logging
from datetime import datetime, timedelta

from stock_service.data import finance as finance_data
from stock_service.data import quote as quote_data
from stock_service.data._cache import fetch_with_cache, n_days_ago_str, today_str
from stock_service.data._registry import resolve_etf_to_index
from stock_service.data.adapters import akshare as ak
from stock_service.data.adapters import tushare as ts
from stock_service.data.store import factor as factor_store
from stock_service.models import AssetType, DataType, FinanceReportType, UnsupportedReason
from stock_service.models.factor import ValuationData

logger = logging.getLogger(__name__)

# 个股估值取的日线窗口：够长以覆盖停牌/长期无成交的标的（取窗口内最后一根 K 线）
_STOCK_VALUATION_WINDOW_DAYS = 90

# 股息 TTM：取最近 N 次已实施派息，N = 该标的年度派息次数（由近两年派息次数推断）
_DIVIDEND_CADENCE_DAYS = 730
_DIVIDEND_MAX_STALE_DAYS = 550  # 最近一次派息超过 18 个月视为当前无派息
_DIVIDEND_LOOKBACK_DAYS = 1095
_MAX_PAYOUTS_PER_YEAR = 4


# ── 估值 ──

def _payments_per_year(ex_dates: list[str], ref_date: str) -> int:
    """由近两年的派息次数推断年度派息次数（年付 1 / 半年付 2 / 季付 ≤4）。"""
    cutoff = (
        datetime.strptime(ref_date, "%Y%m%d") - timedelta(days=_DIVIDEND_CADENCE_DAYS)
    ).strftime("%Y%m%d")
    recent = sum(1 for ex in ex_dates if ex >= cutoff)
    return min(max(round(recent / 2), 1), _MAX_PAYOUTS_PER_YEAR)


def _dividend_ttm_per_share(symbol: str, ref_date: str) -> float | None:
    """近 12 个月的每股派息合计（税前，元）：最近 N 次已实施派息之和。

    N = 年度派息次数。两条都不行的口径：
    - `daily_basic.dv_ttm`：按除息事件阶梯式累加，实测中国移动 600941 在
      2026-09-29 给 2.2609%（只含当年已除息的一次），次日除息后才跳到 4.9177%。
    - 按报告期或按「除权日往前 365 天」归集：都对不齐派息周期。招行/长电是半年派息
      但节奏是「年中报告期次年 1-2 月除权、年末报告期当年 7 月除权」，固定 365 天窗口
      会漏掉其中一期或重复计入上一年度（实测招行 2.42% / 9.7% 两个错值）。
    实测对照（消费侧独立核算）：招行 2.016 元 / 长电 1.0 元 / 中国移动 4.71 元。
    """
    rows = finance_data.get_reports(
        symbol, FinanceReportType.DIVIDEND.value, n_days_ago_str(_DIVIDEND_LOOKBACK_DAYS), ref_date,
    )

    # 已实施、有除权日与税前每股金额；同一除权日只取一次
    payouts: dict[str, float] = {}
    for row in rows:
        record = row["data"]
        ex_date = record.get("ex_date")
        cash = record.get("cash_div_tax")
        if record.get("div_proc") != "实施" or ex_date is None or cash is None:
            continue
        ex = str(ex_date)
        if ex <= ref_date:
            payouts[ex] = max(payouts.get(ex, 0.0), float(cash))

    if not payouts:
        return None

    ex_dates = sorted(payouts, reverse=True)
    if (datetime.strptime(ref_date, "%Y%m%d") - datetime.strptime(ex_dates[0], "%Y%m%d")).days > _DIVIDEND_MAX_STALE_DAYS:
        return None

    n = _payments_per_year(ex_dates, ref_date)
    return round(sum(payouts[ex] for ex in ex_dates[:n]), 4)


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
    per_share_ttm = _dividend_ttm_per_share(symbol, latest.date)
    return ValuationData(
        pe_ttm=round(latest.pe_ttm, 2) if latest.pe_ttm else None,
        pb=round(latest.pb, 2) if latest.pb else None,
        dividend_yield=(
            round(per_share_ttm / latest.close * 100, 2)
            if per_share_ttm and latest.close else None
        ),
        dividend_per_share_ttm=per_share_ttm,
        data_as_of=latest.date,
    )


def get_index_valuation_history(index_symbol: str, days: int = 1095) -> list[dict]:
    """获取指数估值历史（PE/PB），默认回溯 3 年。

    调用方若需要更早的分位采样点（如 3y 窗口 + 若干周），传更大的 `days`；
    缓存按实际覆盖区间增量补拉，不会重复拉已覆盖部分。
    """
    today = today_str()
    start_date = n_days_ago_str(days)

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
    """刷新最近 30 天的 ETF 份额变动，另取 15 天作为差分基准。

    不能按派生结果的日期跨度只补尾部：adapter 会将每批首行用作基准，且
    NAV 可能迟到。重算有限窗口才能同时保留新增首日、回补旧缺行和更新 null。
    """
    today = today_str()
    rows = ts.get_etf_fund_flow(symbol, n_days_ago_str(45), today)
    factor_store.save_etf_subscription(symbol, rows)
    return factor_store.get_cached_etf_subscription(
        symbol, n_days_ago_str(30), today,
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

"""Service 层 — 多因子组合（估值 + 资金流 + 北向资金）。纯数据组合。"""

import logging
from concurrent.futures import ThreadPoolExecutor

from stock_service.data import factor as factor_data
from stock_service.data import listing as listing_data
from stock_service.services._utils import stale_days, today_str
from stock_service.models import AssetType, UnsupportedReason
from stock_service.models.factor import (
    CommodityPercentileData,
    EtfFundFlowData,
    EtfFundFlowItem,
    FactorItem,
    FactorResponse,
    FundFlowData,
    MainForceFlowData,
    MainForceFlowItem,
    NorthboundData,
    ValuationPercentileItem,
)
from stock_service.services._utils import calc_percentile

logger = logging.getLogger(__name__)

_MAX_WORKERS = 8


def _build_fund_flow(symbol: str, asset_type: str) -> FundFlowData | None:
    """从 data 层获取资金流并组装。仅 A 股个股有主力资金数据。"""
    if asset_type != AssetType.STOCK:
        return None

    try:
        rows = factor_data.get_fund_flow(symbol)
        if not rows:
            return None

        latest = rows[-1]
        recent_5d = rows[-5:]
        recent_5d_main = sum(r.get("main_force_net", 0) or 0 for r in recent_5d)
        data_date = latest.get("date")

        return FundFlowData(
            main_force_net=latest.get("main_force_net"),
            main_force_ratio=latest.get("main_force_ratio"),
            super_large_net=latest.get("super_large_net"),
            large_net=latest.get("large_net"),
            recent_5d_main_force=round(recent_5d_main, 2),
            data_as_of=data_date,
            stale_days=stale_days(data_date),
        )
    except Exception as e:
        logger.warning("_build_fund_flow(%s) failed: %s", symbol, e)
        return None


def _build_northbound() -> NorthboundData | None:
    """从 data 层获取北向资金并组装。"""
    try:
        rows = factor_data.get_northbound()
        if not rows:
            return None

        latest = rows[-1]
        recent_5d = rows[-5:]
        net_5d = sum(r.get("north_net_buy", 0) or 0 for r in recent_5d)
        data_date = latest.get("date")

        return NorthboundData(
            north_net_buy=latest.get("north_net_buy"),
            north_net_buy_5d=round(net_5d, 2),
            data_as_of=data_date,
            stale_days=stale_days(data_date),
        )
    except Exception as e:
        logger.warning("_build_northbound failed: %s", e)
        return None


def get_factor(
    code: str,
    date: str | None = None,
) -> FactorItem:
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code)
    if not symbols:
        return FactorItem(
            code=code, name="", asset_type=AssetType.STOCK, as_of=date or today_str()
        )
    symbol, name, asset_type = symbols[0]
    asset = AssetType(asset_type)

    with ThreadPoolExecutor(max_workers=3) as executor:
        f_val = executor.submit(factor_data.get_valuation, symbol, asset)
        f_flow = executor.submit(_build_fund_flow, symbol, asset)
        f_north = executor.submit(_build_northbound)

        valuation = f_val.result()
        fund_flow = f_flow.result()
        northbound = f_north.result()

    return FactorItem(
        code=symbol,
        name=name,
        asset_type=asset,
        as_of=date or today_str(),
        valuation=valuation,
        fund_flow=fund_flow,
        northbound=northbound,
    )


# ── P0: ETF 申购赎回 ──

def get_etf_fund_flow(code: str, date: str | None = None) -> EtfFundFlowItem:
    """获取ETF申购赎回份额变动数据。"""
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code)
    if not symbols:
        return EtfFundFlowItem(symbol=code, name="", as_of=date or today_str())
    symbol, name, asset_type = symbols[0]
    asset = AssetType(asset_type)

    # 仅对基金（ETF）执行申购赎回逻辑；股票/指数直接返回空结果
    if asset != AssetType.FUND:
        return EtfFundFlowItem(
            symbol=symbol, name=name, as_of=date or today_str(),
            unsupported_reason=UnsupportedReason.NOT_ETF,
        )

    try:
        rows = factor_data.get_etf_fund_flow(symbol)
    except Exception as e:
        logger.warning("get_etf_fund_flow(%s) failed: %s", code, e)
        rows = []

    if not rows:
        return EtfFundFlowItem(symbol=symbol, name=name, as_of=date or today_str())

    if date:
        # 显式 date 语义：调用方在问"X 日当天怎么样"。当日无数据 → 返回 net_inflow=null + 溯源标注，
        # **不做日期替换**（silent rollback 是契约说谎，与 feedback_data_layer_no_proxy_fallback 冲突）。
        matched = [r for r in rows if r.get("date") == date]
        if not matched or matched[0].get("net_inflow") is None:
            last_avail = next(
                (r for r in reversed(rows) if r.get("net_inflow") is not None),
                None,
            )
            flow = EtfFundFlowData(
                net_inflow=None,
                share_change=None,
                scale_change=None,
                recent_5d_inflow=None,
                recent_5d_inflow_series=None,
                source=last_avail.get("source") if last_avail else None,
                data_as_of=last_avail["date"] if last_avail else None,
                stale_days=stale_days(last_avail["date"], date) if last_avail else None,
            )
            return EtfFundFlowItem(symbol=symbol, name=name, as_of=date, fund_flow=flow)
        latest = matched[0]
        idx = rows.index(latest)
        start_idx = max(0, idx - 4)
        recent_5d = rows[start_idx : idx + 1]
    else:
        # date=None 默认语义：调用方要"最新的"，取最近非 null 行是合法语义，不是 rollback。
        eligible = [r for r in rows if r.get("net_inflow") is not None]
        if not eligible:
            return EtfFundFlowItem(symbol=symbol, name=name, as_of=today_str())
        latest = eligible[-1]
        idx = rows.index(latest)
        start_idx = max(0, idx - 4)
        recent_5d = rows[start_idx : idx + 1]

    # 老字段 recent_5d_inflow（deprecated）：去掉 `or 0` silent fallback；任一日 null → 整体 null
    _inflow_vals = [r.get("net_inflow") for r in recent_5d]
    recent_5d_inflow = (
        round(sum(_inflow_vals), 2)
        if all(v is not None for v in _inflow_vals)
        else None
    )
    # 新字段 recent_5d_inflow_series：暴露每日 (date, net_inflow)，由消费者自决聚合
    recent_5d_inflow_series = [
        {"date": r["date"], "net_inflow": r.get("net_inflow")}
        for r in recent_5d
    ]

    data_date = latest["date"]
    flow = EtfFundFlowData(
        net_inflow=latest.get("net_inflow"),
        share_change=latest.get("share_change"),
        scale_change=latest.get("scale_change"),
        recent_5d_inflow=recent_5d_inflow,
        recent_5d_inflow_series=recent_5d_inflow_series,
        source=latest.get("source"),
        data_as_of=data_date,
        stale_days=stale_days(data_date, date),
    )
    return EtfFundFlowItem(symbol=symbol, name=name, as_of=data_date, fund_flow=flow)


# ── P1: 主力资金结构 ──

def get_etf_main_force_flow(code: str, date: str | None = None) -> MainForceFlowItem:
    """获取主力资金结构（大单/超大单净流入）。"""
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code)
    if not symbols:
        return MainForceFlowItem(symbol=code, name="", as_of=date or today_str())
    symbol, name, asset_type = symbols[0]
    asset = AssetType(asset_type)

    # 指数无主力资金数据
    if asset == AssetType.INDEX:
        return MainForceFlowItem(
            symbol=symbol, name=name, as_of=date or today_str(),
            unsupported_reason=UnsupportedReason.INDEX_NOT_SUPPORTED,
        )

    # ETF 的"主力资金"在 A 股市场无干净数据源（做市商撮合不适用大单分类）
    if asset == AssetType.FUND:
        return MainForceFlowItem(
            symbol=symbol, name=name, as_of=date or today_str(),
            unsupported_reason=UnsupportedReason.ETF_NO_MAIN_FORCE_DATA,
        )

    try:
        rows = factor_data.get_fund_flow(symbol)
    except Exception as e:
        logger.warning("get_etf_main_force_flow(%s) failed: %s", code, e)
        rows = []

    if not rows:
        return MainForceFlowItem(symbol=symbol, name=name, as_of=date or today_str())

    if date:
        # 取 <= date 的最近一条记录；若不存在则返回 main_force=None 且 as_of=date
        eligible = [r for r in rows if r.get("date") and r["date"] <= date]
        if not eligible:
            return MainForceFlowItem(symbol=symbol, name=name, as_of=date, main_force=None)
        latest = max(eligible, key=lambda r: r["date"])
    else:
        latest = rows[-1]

    data_date = latest["date"]
    flow = MainForceFlowData(
        large_order_net=latest.get("large_net"),
        super_large_net=latest.get("super_large_net"),
        main_force_net=latest.get("main_force_net"),
        main_force_ratio=latest.get("main_force_ratio"),
        data_as_of=data_date,
        stale_days=stale_days(data_date, date),
    )
    return MainForceFlowItem(symbol=symbol, name=name, as_of=data_date, main_force=flow)


# ── P2: 指数估值分位 ──

def get_index_valuation_percentile(code: str) -> ValuationPercentileItem:
    """获取指数/ETF的PE/PB在3年历史中的百分位。"""
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code)
    if not symbols:
        return ValuationPercentileItem(symbol=code, name="", asset_type=AssetType.INDEX)
    symbol, name, asset_type_str = symbols[0]
    asset = AssetType(asset_type_str)

    index_symbol = symbol
    if asset == AssetType.FUND:
        index_symbol = factor_data.get_index_for_etf(symbol)
        if not index_symbol:
            return ValuationPercentileItem(symbol=symbol, name=name, asset_type=asset)

    try:
        rows = factor_data.get_index_valuation_history(index_symbol)
    except Exception as e:
        logger.warning("get_index_valuation_percentile(%s) failed: %s", code, e)
        rows = []

    if not rows:
        return ValuationPercentileItem(symbol=symbol, name=name, asset_type=asset)

    latest = rows[-1]
    pe_ttm = latest.get("pe_ttm")
    pb = latest.get("pb")
    as_of = latest.get("date")

    pe_ttms = [r["pe_ttm"] for r in rows if r.get("pe_ttm") and r["pe_ttm"] > 0]
    pe_pct = calc_percentile(pe_ttms, pe_ttm) if len(pe_ttms) >= 2 and pe_ttm and pe_ttm > 0 else None

    pbs = [r["pb"] for r in rows if r.get("pb") and r["pb"] > 0]
    pb_pct = calc_percentile(pbs, pb) if len(pbs) >= 2 and pb and pb > 0 else None

    return ValuationPercentileItem(
        symbol=symbol,
        name=name,
        asset_type=asset,
        pe_ttm=round(pe_ttm, 2) if pe_ttm else None,
        pe_percentile_3y=pe_pct,
        pb=round(pb, 2) if pb else None,
        pb_percentile_3y=pb_pct,
        as_of=as_of,
    )


# ── P3: 商品价格分位 ──

def get_commodity_price_percentile(
    commodity_code: str, date: str | None = None
) -> CommodityPercentileData:
    """获取商品（黄金/铜/原油等）价格在历史区间中的百分位。"""
    try:
        rows = factor_data.get_commodity_price(commodity_code)
    except Exception as e:
        logger.warning("get_commodity_price_percentile(%s) failed: %s", commodity_code, e)
        rows = []

    if not rows:
        return CommodityPercentileData(commodity_code=commodity_code.upper())

    if date:
        rows = [r for r in rows if r["date"] <= date]
    if not rows:
        return CommodityPercentileData(commodity_code=commodity_code.upper())

    latest = rows[-1]
    current_price = latest["price"]
    as_of = latest["date"]

    def _pct(n_rows: int) -> float | None:
        period = rows[-n_rows:] if len(rows) >= n_rows else rows
        prices = [r["price"] for r in period]
        if len(prices) < 2:
            return None
        return calc_percentile(prices, current_price)

    return CommodityPercentileData(
        commodity_code=commodity_code.upper(),
        current_price=round(current_price, 4),
        percentile_52w=_pct(260),   # 52周 ≈ 260 交易日
        percentile_1y=_pct(252),    # 1年 ≈ 252 交易日
        percentile_3y=_pct(756),    # 3年 ≈ 756 交易日
        as_of=as_of,
    )


def get_factor_batch(
    codes: list[str],
    date: str | None = None,
) -> FactorResponse:
    listing_data.ensure_data()

    all_symbols: list[tuple[str, str, str]] = []
    for code in codes:
        all_symbols.extend(listing_data.resolve_symbols(code=code))

    as_of = date or today_str()
    northbound = _build_northbound()

    def _build_item(symbol: str, name: str, asset_type_str: str) -> FactorItem:
        asset = AssetType(asset_type_str)
        valuation = factor_data.get_valuation(symbol, asset)
        fund_flow = _build_fund_flow(symbol, asset)
        return FactorItem(
            code=symbol,
            name=name,
            asset_type=asset,
            as_of=as_of,
            valuation=valuation,
            fund_flow=fund_flow,
            northbound=northbound,
        )

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [
            executor.submit(_build_item, symbol, name, asset_type)
            for symbol, name, asset_type in all_symbols
        ]
        items = [f.result() for f in futures]

    return FactorResponse(total=len(items), items=items)

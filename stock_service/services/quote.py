"""Service 层 — 日线行情、复权、技术指标、百分位。纯计算 + 数据组合。"""

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from stock_service.data import factor as factor_data
from stock_service.data import listing as listing_data
from stock_service.data import quote as quote_data
from stock_service.services._utils import CST, calc_percentile, stale_days
from stock_service.models import AdjustType, AssetType, DetailLevel, UnsupportedReason
from stock_service.models.quote import (
    SIMPLE_FIELDS,
    BidAsk,
    DailyBar,
    DailyItem,
    DailyResponse,
    HighLowPoint,
    PercentileData,
    PercentileHistoryItem,
    PercentileHistoryPoint,
    PercentileItem,
    PercentileResponse,
    RealtimeQuote,
    ReverseRepoQuote,
    ReverseRepoResponse,
    TechnicalData,
    TechnicalItem,
    TechnicalResponse,
)
from stock_service.data.adapters.akshare import REVERSE_REPO_CATALOG

_MAX_WORKERS = 8

logger = logging.getLogger(__name__)


# ── 复权 ──

def _apply_adj(
    bars: list[DailyBar], adj_factors: dict[str, float], adjust: AdjustType
) -> list[DailyBar]:
    if not adj_factors or not bars:
        return bars

    if adjust == AdjustType.QFQ:
        latest_factor = adj_factors.get(max(adj_factors.keys()), 1.0)
        get_ratio = lambda date: adj_factors.get(date, 1.0) / latest_factor
    elif adjust == AdjustType.HFQ:
        get_ratio = lambda date: adj_factors.get(date, 1.0)
    else:
        return bars

    return [
        bar.model_copy(
            update={
                "open": round(bar.open * get_ratio(bar.date), 4),
                "high": round(bar.high * get_ratio(bar.date), 4),
                "low": round(bar.low * get_ratio(bar.date), 4),
                "close": round(bar.close * get_ratio(bar.date), 4),
            }
        )
        for bar in bars
    ]


def _to_simple(bars: list[DailyBar]) -> list[DailyBar]:
    return [DailyBar(**bar.model_dump(include=SIMPLE_FIELDS)) for bar in bars]


# ── 日线 ──

def _get_single_daily(
    symbol: str,
    name: str,
    asset_type: AssetType,
    start_date: str,
    end_date: str,
    adjust: AdjustType,
    detail: DetailLevel,
) -> DailyItem:
    # 从 Data 层获取日线（缓存透明）
    all_bars = quote_data.get_daily(symbol, asset_type, start_date, end_date)

    # 复权处理（指数不需要复权）
    if adjust != AdjustType.NONE and asset_type != AssetType.INDEX:
        is_fund = asset_type == AssetType.FUND
        adj_factors = quote_data.get_adj_factors(symbol, start_date, end_date, is_fund=is_fund)
        all_bars = _apply_adj(all_bars, adj_factors, adjust)

    # detail 档位过滤
    if detail == DetailLevel.SIMPLE:
        all_bars = _to_simple(all_bars)

    return DailyItem(
        symbol=symbol,
        name=name,
        asset_type=asset_type,
        adjust=adjust,
        bars=all_bars,
        unsupported_reason=None if all_bars else UnsupportedReason.DATA_UNAVAILABLE,
    )


def _unsupported_item(code: str, name: str = "") -> tuple[str, str, str | None, UnsupportedReason]:
    """解析不到标的时，给出可读的失败原因。

    日线/技术指标只覆盖 A 股/ETF/指数；港股/美股/全球指数不是「代码写错」，而是
    「该资产类型不支持这条口径」，两者必须区分开。
    """
    overseas = _resolve_overseas(code)
    if overseas:
        return code, name, overseas[2], UnsupportedReason.ASSET_TYPE_NOT_SUPPORTED
    return code, name, None, UnsupportedReason.SYMBOL_NOT_FOUND


def _resolve_or_placeholder(codes: list[str]) -> list[tuple[str, str, str | None, UnsupportedReason | None]]:
    """把请求的 codes 展开成 (symbol, name, asset_type, unsupported_reason)。

    每个请求的 code **至少产出一条**：解析不到时产占位项（带原因），不静默丢弃 ——
    否则消费者不主动数 total 就发现不了少拿了标的。
    """
    listing_data.ensure_data()
    tasks: list[tuple[str, str, str | None, UnsupportedReason | None]] = []
    for code in codes:
        resolved = listing_data.resolve_symbols(code=code)
        if not resolved:
            tasks.append(_unsupported_item(code))
        else:
            tasks.extend((symbol, name, asset_type, None) for symbol, name, asset_type in resolved)
    return tasks


def get_daily(
    start_date: str,
    end_date: str,
    adjust: AdjustType = AdjustType.QFQ,
    detail: DetailLevel = DetailLevel.SIMPLE,
    *,
    code: str | None = None,
    market: str | None = None,
    industry: str | None = None,
) -> DailyResponse:
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code, market=market, industry=industry)

    if not symbols and code:
        # 指定 code 却解析不到 → 显式失败，不能让 items 变成空数组被当成「查到了但没数据」
        symbol, name, asset_type, reason = _unsupported_item(code)
        return DailyResponse(
            total=1,
            items=[
                DailyItem(
                    symbol=symbol, name=name,
                    asset_type=AssetType(asset_type) if asset_type else None,
                    adjust=adjust, bars=[], unsupported_reason=reason,
                )
            ],
        )

    if len(symbols) <= 1:
        items = [
            _get_single_daily(
                symbol, name, AssetType(asset_type),
                start_date, end_date, adjust, detail,
            )
            for symbol, name, asset_type in symbols
        ]
    else:
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    _get_single_daily,
                    symbol, name, AssetType(asset_type),
                    start_date, end_date, adjust, detail,
                )
                for symbol, name, asset_type in symbols
            ]
            items = [f.result() for f in futures]

    return DailyResponse(total=len(items), items=items)


# ── 百分位 ──

_PERCENTILE_PERIODS = [
    ("6m", 180),
    ("1y", 365),
    ("2y", 730),
    ("3y", 1095),
]

_PERCENTILE_WINDOW_DAYS = dict(_PERCENTILE_PERIODS)
_PERCENTILE_FREQ_STEPS = {"daily": 1, "weekly": 5}
_MAX_HISTORY_PERIODS = 260


def _calc_window_percentiles(
    period_bars: list[DailyBar],
    current_price: float,
    current_pe_ttm: float | None,
    current_pb: float | None,
) -> tuple[float | None, float | None, float | None]:
    """按窗口内的 bars 计算价格 / PE-TTM / PB 分位（0-100）。

    价格分位需要 >=2 个收盘价；PE/PB 分位额外要求当前值 >0（负值无意义）。
    窗口不足或当前值缺失时对应分位为 None。get_percentile 与
    get_percentile_history 共用此函数，保证两者口径一致。
    """
    closes = [b.close for b in period_bars]
    if len(closes) < 2:
        return None, None, None

    price_pct = calc_percentile(closes, current_price)

    pe_ttm_pct = None
    if current_pe_ttm is not None and current_pe_ttm > 0:
        pe_ttms = [b.pe_ttm for b in period_bars if b.pe_ttm is not None and b.pe_ttm > 0]
        if len(pe_ttms) >= 2:
            pe_ttm_pct = calc_percentile(pe_ttms, current_pe_ttm)

    pb_pct = None
    if current_pb is not None and current_pb > 0:
        pbs = [b.pb for b in period_bars if b.pb is not None and b.pb > 0]
        if len(pbs) >= 2:
            pb_pct = calc_percentile(pbs, current_pb)

    return price_pct, pe_ttm_pct, pb_pct


def _get_single_percentile(
    symbol: str,
    name: str,
    asset_type: AssetType,
    unsupported_reason: UnsupportedReason | None = None,
) -> PercentileItem:
    if asset_type is None or unsupported_reason is not None:
        return PercentileItem(
            symbol=symbol, name=name,
            asset_type=asset_type, as_of=None, percentiles=[],
            unsupported_reason=unsupported_reason,
        )

    today = datetime.now(CST).strftime("%Y%m%d")
    start_date = (datetime.now(CST) - timedelta(days=_PERCENTILE_PERIODS[-1][1])).strftime("%Y%m%d")

    detail = DetailLevel.ALL if asset_type == AssetType.STOCK else DetailLevel.SIMPLE

    daily_item = _get_single_daily(
        symbol, name, asset_type,
        start_date, today, AdjustType.QFQ, detail,
    )
    bars = daily_item.bars
    if not bars:
        return PercentileItem(
            symbol=symbol, name=name, asset_type=asset_type,
            as_of=None,
            percentiles=[
                PercentileData(period=p, price_percentile=None, current_price=None, days=0)
                for p, _ in _PERCENTILE_PERIODS
            ],
            unsupported_reason=UnsupportedReason.DATA_UNAVAILABLE,
        )

    current_price = bars[-1].close
    is_stock = asset_type == AssetType.STOCK
    current_pe_ttm = bars[-1].pe_ttm if is_stock else None
    current_pb = bars[-1].pb if is_stock else None

    percentiles: list[PercentileData] = []

    for period_name, days in _PERCENTILE_PERIODS:
        cutoff = (datetime.now(CST) - timedelta(days=days)).strftime("%Y%m%d")
        period_bars = [b for b in bars if b.date >= cutoff]
        closes = [b.close for b in period_bars]

        if len(closes) < 2:
            percentiles.append(
                PercentileData(
                    period=period_name, percentile=None,
                    current_price=current_price, days=len(closes),
                )
            )
            continue

        price_pct, pe_ttm_pct, pb_pct = _calc_window_percentiles(
            period_bars, current_price, current_pe_ttm, current_pb,
        )

        percentiles.append(
            PercentileData(
                period=period_name,
                price_percentile=price_pct,
                current_price=current_price,
                min_price=round(min(closes), 4),
                max_price=round(max(closes), 4),
                days=len(closes),
                pe_ttm_percentile=pe_ttm_pct,
                current_pe_ttm=round(current_pe_ttm, 2) if current_pe_ttm is not None else None,
                pb_percentile=pb_pct,
                current_pb=round(current_pb, 2) if current_pb is not None else None,
            )
        )

    return PercentileItem(
        symbol=symbol, name=name, asset_type=asset_type,
        as_of=bars[-1].date,
        stale_days=stale_days(bars[-1].date),
        percentiles=percentiles,
    )


def get_percentile(
    *,
    code: str | None = None,
    market: str | None = None,
    industry: str | None = None,
) -> PercentileResponse:
    tasks = (
        _resolve_or_placeholder([code]) if code
        else [(s, n, a, None) for s, n, a in _resolve_filtered(market=market, industry=industry)]
    )

    if len(tasks) <= 1:
        items = [_get_single_percentile(*_as_kwargs(t)) for t in tasks]
    else:
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = [executor.submit(_get_single_percentile, *_as_kwargs(t)) for t in tasks]
            items = [f.result() for f in futures]

    return PercentileResponse(total=len(items), items=items)


def _resolve_filtered(*, market: str | None = None, industry: str | None = None):
    """按 market/industry 筛选标的（无 code 时的批量语义，筛不到就是 0 条）。"""
    listing_data.ensure_data()
    return listing_data.resolve_symbols(market=market, industry=industry)


def _as_kwargs(task: tuple[str, str, str | None, UnsupportedReason | None]):
    symbol, name, asset_type, reason = task
    return (symbol, name, AssetType(asset_type) if asset_type else None, reason)


def get_percentile_batch(
    *,
    codes: list[str],
) -> PercentileResponse:
    tasks = _resolve_or_placeholder(codes)

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [executor.submit(_get_single_percentile, *_as_kwargs(t)) for t in tasks]
        items = [f.result() for f in futures]

    return PercentileResponse(total=len(items), items=items)


# ── 历史分位 ──

def _calc_index_valuation_percentiles(
    rows: list[dict], start_date: str, end_date: str
) -> tuple[float | None, float | None]:
    """从指数估值序列里算 PE-TTM / PB 分位。

    当前值取 `<= end_date` 的最近一条（采样日可能没有估值行），样本为
    `[start_date, end_date]` 内的所有行，要求当前值 >0 且样本 >=2。
    """
    window_rows = [r for r in rows if start_date <= r["date"] <= end_date]
    if not window_rows:
        return None, None

    current = window_rows[-1]
    pe_ttm = current.get("pe_ttm")
    pb = current.get("pb")

    pe_ttms = [r["pe_ttm"] for r in window_rows if r.get("pe_ttm") and r["pe_ttm"] > 0]
    pe_pct = (
        calc_percentile(pe_ttms, pe_ttm)
        if len(pe_ttms) >= 2 and pe_ttm and pe_ttm > 0
        else None
    )

    pbs = [r["pb"] for r in window_rows if r.get("pb") and r["pb"] > 0]
    pb_pct = (
        calc_percentile(pbs, pb)
        if len(pbs) >= 2 and pb and pb > 0
        else None
    )

    return pe_pct, pb_pct


def _validate_history_args(window: str, freq: str, periods: int) -> None:
    if window not in _PERCENTILE_WINDOW_DAYS:
        raise ValueError(
            f"window 只支持 {list(_PERCENTILE_WINDOW_DAYS)}，收到 {window!r}"
        )
    if freq not in _PERCENTILE_FREQ_STEPS:
        raise ValueError(f"freq 只支持 {list(_PERCENTILE_FREQ_STEPS)}，收到 {freq!r}")
    if not 1 <= periods <= _MAX_HISTORY_PERIODS:
        raise ValueError(f"periods 需在 1..{_MAX_HISTORY_PERIODS} 之间，收到 {periods}")


def _get_single_percentile_history(
    symbol: str,
    name: str,
    asset_type: AssetType,
    unsupported_reason: UnsupportedReason | None,
    window: str,
    freq: str,
    periods: int,
) -> PercentileHistoryItem:
    if asset_type is None or unsupported_reason is not None:
        return PercentileHistoryItem(
            symbol=symbol, name=name, asset_type=asset_type,
            window=window, freq=freq, series=[],
            unsupported_reason=unsupported_reason,
        )

    window_days = _PERCENTILE_WINDOW_DAYS[window]
    step = _PERCENTILE_FREQ_STEPS[freq]
    # 采样跨度：weekly 每点约 7 个自然日；daily 每点 1 个交易日，含周末按 2 天估。
    # 再 +30 天缓冲覆盖春节等长假，保证最早采样点的窗口也完整。
    span_days = periods * (7 if freq == "weekly" else 2)
    lookback_days = window_days + span_days + 30
    today = datetime.now(CST).strftime("%Y%m%d")
    start_date = (datetime.now(CST) - timedelta(days=lookback_days)).strftime("%Y%m%d")

    is_stock = asset_type == AssetType.STOCK
    detail = DetailLevel.ALL if is_stock else DetailLevel.SIMPLE

    daily_item = _get_single_daily(
        symbol, name, asset_type, start_date, today, AdjustType.QFQ, detail,
    )
    bars = daily_item.bars
    if not bars:
        return PercentileHistoryItem(
            symbol=symbol, name=name, asset_type=asset_type,
            window=window, freq=freq, series=[],
            unsupported_reason=UnsupportedReason.DATA_UNAVAILABLE,
        )

    # ETF/指数的 PE/PB 走 index_dailybasic（个股走日线自带的 daily_basic 字段）。
    # 映射不到跟踪指数或上游套餐不覆盖时保持空列表，PE/PB 分位为 None，不报错。
    index_rows: list[dict] = []
    if not is_stock:
        index_symbol = (
            symbol if asset_type == AssetType.INDEX
            else factor_data.get_index_for_etf(symbol)
        )
        if index_symbol:
            try:
                index_rows = factor_data.get_index_valuation_history(
                    index_symbol, days=lookback_days,
                )
            except Exception as e:
                logger.warning(
                    "get_percentile_history(%s) index valuation failed: %s", symbol, e
                )
                index_rows = []

    last_idx = len(bars) - 1
    sample_idxs = list(range(last_idx, -1, -step))[:periods]
    sample_idxs.reverse()

    points: list[PercentileHistoryPoint] = []
    for idx in sample_idxs:
        bar = bars[idx]
        # 最新采样点用 today 作窗口基准日，与 get_percentile 的 cutoff 完全一致；
        # 历史采样点用采样日，即「该日往前 window」的滚动分位。
        anchor = today if idx == last_idx else bar.date
        cutoff = (
            datetime.strptime(anchor, "%Y%m%d") - timedelta(days=window_days)
        ).strftime("%Y%m%d")
        period_bars = [b for b in bars if cutoff <= b.date <= bar.date]

        if is_stock:
            price_pct, pe_pct, pb_pct = _calc_window_percentiles(
                period_bars, bar.close, bar.pe_ttm, bar.pb,
            )
        else:
            price_pct = (
                calc_percentile([b.close for b in period_bars], bar.close)
                if len(period_bars) >= 2
                else None
            )
            pe_pct, pb_pct = _calc_index_valuation_percentiles(
                index_rows, cutoff, bar.date,
            )

        points.append(
            PercentileHistoryPoint(
                date=bar.date,
                price_percentile=price_pct,
                pe_ttm_percentile=pe_pct,
                pb_percentile=pb_pct,
            )
        )

    as_of = bars[-1].date
    return PercentileHistoryItem(
        symbol=symbol, name=name, asset_type=asset_type,
        window=window, freq=freq,
        as_of=as_of, stale_days=stale_days(as_of),
        series=points,
    )


def get_percentile_history(
    code: str,
    window: str = "1y",
    freq: str = "weekly",
    periods: int = 8,
) -> PercentileHistoryItem:
    """单标的的历史分位序列（滚动窗口）。"""
    _validate_history_args(window, freq, periods)
    symbol, name, asset_type, reason = _resolve_or_placeholder([code])[0]
    return _get_single_percentile_history(
        symbol, name,
        AssetType(asset_type) if asset_type else None,
        reason, window, freq, periods,
    )


# ── 技术指标 ──


def _calc_signal(
    *,
    ma_status: str | None,
    macd_cross: str | None,
    macd_hist: float | None,
    rsi12: float | None,
    ma20_deviation: float | None,
    volume_ratio_today: float | None,
) -> str | None:
    if ma_status is None:
        return None

    score = 0

    ma_scores = {
        "strong_bullish": 3,
        "bullish": 2,
        "weak_bullish": 1,
        "tangled": 0,
        "weak_bearish": -1,
        "bearish": -2,
        "strong_bearish": -3,
    }
    score += ma_scores.get(ma_status, 0)

    if macd_cross == "golden":
        score += 2
    elif macd_cross == "dead":
        score -= 2
    elif macd_hist is not None:
        score += 1 if macd_hist > 0 else -1

    if rsi12 is not None:
        if rsi12 < 20:
            score += 2
        elif rsi12 < 30:
            score += 1
        elif rsi12 > 80:
            score -= 2
        elif rsi12 > 70:
            score -= 1

    if ma20_deviation is not None:
        if ma20_deviation > 8:
            score -= 1
        elif ma20_deviation < -8:
            score += 1

    if volume_ratio_today is not None:
        if volume_ratio_today > 2.0 and score > 0:
            score += 1
        elif volume_ratio_today > 2.0 and score < 0:
            score -= 1

    if score >= 5:
        return "strong_buy"
    elif score >= 3:
        return "buy"
    elif score >= 1:
        return "hold"
    elif score >= -2:
        return "watch"
    elif score >= -4:
        return "sell"
    else:
        return "strong_sell"


def _calc_ema(values: list[float], n: int) -> list[float]:
    if not values:
        return []
    k = 2 / (n + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _calc_rsi(closes: list[float], n: int) -> float | None:
    if len(closes) < n + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-n:]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / n
    avg_loss = sum(losses) / n
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


def _calc_technical(bars: list[DailyBar], period: int) -> TechnicalData:
    if not bars:
        return TechnicalData()

    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]
    current = closes[-1]

    def _ma(n: int) -> float | None:
        if len(closes) < n:
            return None
        return round(sum(closes[-n:]) / n, 4)

    ma5 = _ma(5)
    ma10 = _ma(10)
    ma20 = _ma(20)
    ma60 = _ma(60)

    def _deviation(ma_val: float | None) -> float | None:
        if ma_val:
            return round((current - ma_val) / ma_val * 100, 2)
        return None

    ma5_deviation = _deviation(ma5)
    ma10_deviation = _deviation(ma10)
    ma20_deviation = _deviation(ma20)
    ma60_deviation = _deviation(ma60)

    ma_status = None
    if all(v is not None for v in (ma5, ma10, ma20, ma60)):
        if ma5 > ma10 > ma20 > ma60:
            gap_5_10 = (ma5 - ma10) / ma10 * 100
            gap_10_20 = (ma10 - ma20) / ma20 * 100
            ma_status = "strong_bullish" if gap_5_10 > 1 and gap_10_20 > 1 else "bullish"
        elif ma5 < ma10 < ma20 < ma60:
            gap_10_5 = (ma10 - ma5) / ma10 * 100
            gap_20_10 = (ma20 - ma10) / ma20 * 100
            ma_status = "strong_bearish" if gap_10_5 > 1 and gap_20_10 > 1 else "bearish"
        elif ma5 > ma10 > ma20:
            ma_status = "weak_bullish"
        elif ma5 < ma10 < ma20:
            ma_status = "weak_bearish"
        else:
            ma_status = "tangled"

    dif_val = dea_val = macd_hist = None
    macd_cross = None
    if len(closes) >= 35:
        ema12 = _calc_ema(closes, 12)
        ema26 = _calc_ema(closes, 26)
        dif_series = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
        dea_series = _calc_ema(dif_series, 9)
        dif_val = round(dif_series[-1], 4)
        dea_val = round(dea_series[-1], 4)
        macd_hist = round((dif_val - dea_val) * 2, 4)
        if len(dif_series) >= 2 and len(dea_series) >= 2:
            prev_diff = dif_series[-2] - dea_series[-2]
            curr_diff = dif_series[-1] - dea_series[-1]
            if prev_diff <= 0 < curr_diff:
                macd_cross = "golden"
            elif prev_diff >= 0 > curr_diff:
                macd_cross = "dead"
            else:
                macd_cross = "none"

    rsi6 = _calc_rsi(closes, 6)
    rsi12 = _calc_rsi(closes, 12)
    rsi24 = _calc_rsi(closes, 24)

    volume_ratio = None
    volume_ratio_today = None
    if len(volumes) >= 20:
        avg5 = sum(volumes[-5:]) / 5
        avg20 = sum(volumes[-20:]) / 20
        volume_ratio = round(avg5 / avg20, 2) if avg20 > 0 else None
    if len(volumes) >= 6:
        avg5_prev = sum(volumes[-6:-1]) / 5
        volume_ratio_today = round(volumes[-1] / avg5_prev, 2) if avg5_prev > 0 else None

    pct_5d = round((closes[-1] - closes[-6]) / closes[-6] * 100, 2) if len(closes) >= 6 else None
    pct_20d = round((closes[-1] - closes[-21]) / closes[-21] * 100, 2) if len(closes) >= 21 else None

    period_bars = bars[-period:] if len(bars) >= period else bars
    period_closes = [(b.close, b.date) for b in period_bars]
    high_close, high_date = max(period_closes, key=lambda x: x[0])
    low_close, low_date = min(period_closes, key=lambda x: x[0])

    range_pct = None
    if high_close != low_close:
        range_pct = round((current - low_close) / (high_close - low_close) * 100, 2)

    drawdown = round((high_close - current) / high_close * 100, 2)

    consecutive = 0
    for b in reversed(bars):
        if b.pct_chg is not None and b.pct_chg < 0:
            consecutive += 1
        else:
            break

    signal = _calc_signal(
        ma_status=ma_status,
        macd_cross=macd_cross,
        macd_hist=macd_hist,
        rsi12=rsi12,
        ma20_deviation=ma20_deviation,
        volume_ratio_today=volume_ratio_today,
    )

    return TechnicalData(
        ma5=ma5,
        ma10=ma10,
        ma20=ma20,
        ma60=ma60,
        ma5_deviation=ma5_deviation,
        ma10_deviation=ma10_deviation,
        ma20_deviation=ma20_deviation,
        ma60_deviation=ma60_deviation,
        ma_status=ma_status,
        dif=dif_val,
        dea=dea_val,
        macd_hist=macd_hist,
        macd_cross=macd_cross,
        rsi6=rsi6,
        rsi12=rsi12,
        rsi24=rsi24,
        volume_ratio_5_20=volume_ratio,
        volume_ratio_today=volume_ratio_today,
        pct_chg_5d=pct_5d,
        pct_chg_20d=pct_20d,
        high_point=HighLowPoint(price=round(high_close, 4), date=high_date),
        low_point=HighLowPoint(price=round(low_close, 4), date=low_date),
        range_percentile=range_pct,
        drawdown_from_high=drawdown,
        consecutive_down_days=consecutive,
        current_price=round(current, 4),
        signal=signal,
    )


def _get_single_technical(
    symbol: str,
    name: str,
    asset_type: AssetType,
    unsupported_reason: UnsupportedReason | None = None,
    period: int = 60,
) -> TechnicalItem:
    if asset_type is None or unsupported_reason is not None:
        return TechnicalItem(
            symbol=symbol, name=name, asset_type=asset_type,
            unsupported_reason=unsupported_reason,
        )

    today = datetime.now(CST).strftime("%Y%m%d")
    total_days = period + 80
    start_date = (datetime.now(CST) - timedelta(days=total_days)).strftime("%Y%m%d")

    daily_item = _get_single_daily(
        symbol, name, asset_type,
        start_date, today, AdjustType.QFQ, DetailLevel.SIMPLE,
    )

    if not daily_item.bars:
        return TechnicalItem(
            symbol=symbol, name=name, asset_type=asset_type,
            unsupported_reason=UnsupportedReason.DATA_UNAVAILABLE,
        )

    technical = _calc_technical(daily_item.bars, period)
    as_of = daily_item.bars[-1].date

    return TechnicalItem(
        symbol=symbol,
        name=name,
        asset_type=asset_type,
        as_of=as_of,
        stale_days=stale_days(as_of),
        technical=technical,
    )


def get_technical(
    *,
    code: str,
    period: int = 60,
) -> TechnicalResponse:
    tasks = _resolve_or_placeholder([code])

    items = [
        _get_single_technical(*_as_kwargs(t), period)
        for t in tasks
    ]
    return TechnicalResponse(total=len(items), items=items)


def _resolve_overseas(code: str) -> tuple[str, str, str] | None:
    """解析港股/美股/全球指数代码，返回 (pure_code, symbol, asset_type) 或 None。"""
    upper = code.upper()
    if upper.endswith(".HK"):
        pure = upper.removesuffix(".HK")
        return pure, upper, "hk"
    if upper.endswith(".US"):
        pure = upper.removesuffix(".US")
        return pure, upper, "us"
    if upper.endswith(".GI"):
        pure = upper.removesuffix(".GI")
        return pure, upper, "global_index"
    return None


def get_realtime(
    *,
    code: str,
) -> RealtimeQuote:
    """获取单只标的实时行情，支持 A 股/ETF/指数/港股/美股。"""
    overseas = _resolve_overseas(code)
    if overseas:
        pure_code, symbol, asset_type = overseas
    else:
        listing_data.ensure_data()
        symbols = listing_data.resolve_symbols(code=code)
        if not symbols:
            raise ValueError(f"未找到证券: {code}")
        symbol, name, asset_type = symbols[0]
        pure_code = symbol.split(".")[0]

    data = quote_data.get_realtime(pure_code, asset_type)
    if data is None:
        raise RuntimeError(f"获取实时行情失败: {code}")

    # 港美股/全球指数名称从 API 返回中获取
    if overseas:
        name = data.get("name") or symbol

    asks = [BidAsk(**a) for a in data["asks"]] if data.get("asks") else None
    bids = [BidAsk(**b) for b in data["bids"]] if data.get("bids") else None

    return RealtimeQuote(
        symbol=symbol,
        name=name,
        asset_type=AssetType(asset_type),
        current_price=data["current_price"],
        pct_chg=data["pct_chg"],
        change=data["change"],
        open=data["open"],
        high=data["high"],
        low=data["low"],
        pre_close=data["pre_close"],
        volume=data["volume"],
        amount=data["amount"],
        turnover_rate=data.get("turnover_rate"),
        volume_ratio=data.get("volume_ratio"),
        asks=asks,
        bids=bids,
    )


# ── 国债逆回购 ──

def _normalize_repo_key(raw: str) -> str:
    """把用户输入的代码 / 名称统一为可匹配键。

    接受 "GC001" / "gc001" / "204001" / "204001.SH" / "Ｒ-001" 等形态，
    返回大写、去后缀、半角破折号的字符串用于在白名单里查找。
    """
    s = raw.strip().upper()
    if s.endswith(".SH") or s.endswith(".SZ"):
        s = s[:-3]
    # 全角转半角的破折号
    s = s.replace("－", "-").replace("Ｒ", "R").replace("Ｇ", "G")
    return s


def get_reverse_repo_quotes(
    *,
    codes: list[str] | None = None,
) -> ReverseRepoResponse:
    """获取国债逆回购实时年化收益率。

    codes 不传则返回全部常用品种；传入则按白名单过滤（按纯代码或标准名匹配）。
    """
    if codes:
        keys = {_normalize_repo_key(c) for c in codes}
        catalog = [
            (code, name, ex)
            for code, name, ex in REVERSE_REPO_CATALOG
            if code in keys or name.upper() in keys
        ]
    else:
        catalog = list(REVERSE_REPO_CATALOG)

    raw = quote_data.get_reverse_repo_quotes(catalog)
    items = [ReverseRepoQuote(**r) for r in raw]
    return ReverseRepoResponse(total=len(items), items=items)


def get_technical_batch(
    *,
    codes: list[str],
    period: int = 60,
) -> TechnicalResponse:
    tasks = _resolve_or_placeholder(codes)

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [
            executor.submit(_get_single_technical, *_as_kwargs(t), period)
            for t in tasks
        ]
        items = [f.result() for f in futures]

    return TechnicalResponse(total=len(items), items=items)

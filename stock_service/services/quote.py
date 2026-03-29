"""Service 层 — 日线行情、复权、技术指标、百分位。纯计算 + 数据组合。"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from stock_service.data import listing as listing_data
from stock_service.data import quote as quote_data
from stock_service.services._utils import CST
from stock_service.models import AdjustType, AssetType, DetailLevel
from stock_service.models.quote import (
    SIMPLE_FIELDS,
    BidAsk,
    DailyBar,
    DailyItem,
    DailyResponse,
    HighLowPoint,
    PercentileData,
    PercentileItem,
    PercentileResponse,
    RealtimeQuote,
    TechnicalData,
    TechnicalItem,
    TechnicalResponse,
)
from stock_service.services._utils import calc_percentile

_MAX_WORKERS = 8


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
    )


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


def _get_single_percentile(
    symbol: str,
    name: str,
    asset_type: AssetType,
) -> PercentileItem:
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
            percentiles=[
                PercentileData(period=p, percentile=None, current_price=None, days=0)
                for p, _ in _PERCENTILE_PERIODS
            ],
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

        price_pct = calc_percentile(closes, current_price)

        pe_ttm_pct = None
        pb_pct = None
        if is_stock:
            pe_ttms = [b.pe_ttm for b in period_bars if b.pe_ttm is not None and b.pe_ttm > 0]
            if len(pe_ttms) >= 2 and current_pe_ttm is not None and current_pe_ttm > 0:
                pe_ttm_pct = calc_percentile(pe_ttms, current_pe_ttm)

            pbs = [b.pb for b in period_bars if b.pb is not None and b.pb > 0]
            if len(pbs) >= 2 and current_pb is not None and current_pb > 0:
                pb_pct = calc_percentile(pbs, current_pb)

        percentiles.append(
            PercentileData(
                period=period_name,
                percentile=price_pct,
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
        percentiles=percentiles,
    )


def get_percentile(
    *,
    code: str | None = None,
    market: str | None = None,
    industry: str | None = None,
) -> PercentileResponse:
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code, market=market, industry=industry)

    if len(symbols) <= 1:
        items = [
            _get_single_percentile(symbol, name, AssetType(asset_type))
            for symbol, name, asset_type in symbols
        ]
    else:
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    _get_single_percentile,
                    symbol, name, AssetType(asset_type),
                )
                for symbol, name, asset_type in symbols
            ]
            items = [f.result() for f in futures]

    return PercentileResponse(total=len(items), items=items)


def get_percentile_batch(
    *,
    codes: list[str],
) -> PercentileResponse:
    listing_data.ensure_data()

    all_symbols: list[tuple[str, str, str]] = []
    for code in codes:
        all_symbols.extend(listing_data.resolve_symbols(code=code))

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                _get_single_percentile,
                symbol, name, AssetType(asset_type),
            )
            for symbol, name, asset_type in all_symbols
        ]
        items = [f.result() for f in futures]

    return PercentileResponse(total=len(items), items=items)


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
    period: int = 60,
) -> TechnicalItem:
    today = datetime.now(CST).strftime("%Y%m%d")
    total_days = period + 80
    start_date = (datetime.now(CST) - timedelta(days=total_days)).strftime("%Y%m%d")

    daily_item = _get_single_daily(
        symbol, name, asset_type,
        start_date, today, AdjustType.QFQ, DetailLevel.SIMPLE,
    )

    technical = _calc_technical(daily_item.bars, period)

    return TechnicalItem(
        symbol=symbol,
        name=name,
        asset_type=asset_type,
        technical=technical,
    )


def get_technical(
    *,
    code: str,
    period: int = 60,
) -> TechnicalResponse:
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code)

    items = [
        _get_single_technical(symbol, name, AssetType(asset_type), period)
        for symbol, name, asset_type in symbols
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


def get_technical_batch(
    *,
    codes: list[str],
    period: int = 60,
) -> TechnicalResponse:
    listing_data.ensure_data()

    all_symbols: list[tuple[str, str, str]] = []
    for code in codes:
        all_symbols.extend(listing_data.resolve_symbols(code=code))

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                _get_single_technical,
                symbol, name, AssetType(asset_type), period,
            )
            for symbol, name, asset_type in all_symbols
        ]
        items = [f.result() for f in futures]

    return TechnicalResponse(total=len(items), items=items)

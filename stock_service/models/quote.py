from pydantic import BaseModel

from stock_service.models import AdjustType, AssetType


class DailyBar(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    pre_close: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    volume: float
    amount: float | None = None
    # 以下为股票特有字段（来自 daily_basic），基金不返回
    turnover_rate: float | None = None
    turnover_rate_f: float | None = None
    volume_ratio: float | None = None
    pe: float | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    ps: float | None = None
    ps_ttm: float | None = None
    dv_ratio: float | None = None
    dv_ttm: float | None = None
    total_share: float | None = None
    float_share: float | None = None
    free_share: float | None = None
    total_mv: float | None = None
    circ_mv: float | None = None


# detail=simple 时只保留的字段
SIMPLE_FIELDS = {
    "date", "open", "high", "low", "close",
    "pre_close", "change", "pct_chg", "volume", "amount",
}


class DailyItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    adjust: AdjustType
    bars: list[DailyBar]


class DailyResponse(BaseModel):
    total: int
    items: list[DailyItem]


class PercentileData(BaseModel):
    period: str  # 6m / 1y / 2y / 3y
    percentile: float | None  # 价格百分位（0-100），数据不足时为 None
    current_price: float | None  # 当前价格
    min_price: float | None = None  # 区间最低价
    max_price: float | None = None  # 区间最高价
    days: int  # 该区间内的交易日数量
    # 以下为股票特有的估值百分位（基金/指数不返回）
    pe_ttm_percentile: float | None = None  # PE-TTM 百分位（0-100）
    current_pe_ttm: float | None = None  # 当前 PE-TTM
    pb_percentile: float | None = None  # PB 百分位（0-100）
    current_pb: float | None = None  # 当前 PB


class PercentileItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    as_of: str | None = None  # 序列最末日 YYYYMMDD（与 index_valuation_percentile.as_of 对齐）；bars 为空时 None
    percentiles: list[PercentileData]


class PercentileResponse(BaseModel):
    total: int
    items: list[PercentileItem]


class HighLowPoint(BaseModel):
    price: float
    date: str


class TechnicalData(BaseModel):
    ma5: float | None = None
    ma10: float | None = None
    ma20: float | None = None
    ma60: float | None = None
    ma5_deviation: float | None = None        # (close - ma5) / ma5 * 100
    ma10_deviation: float | None = None       # (close - ma10) / ma10 * 100
    ma20_deviation: float | None = None       # (close - ma20) / ma20 * 100
    ma60_deviation: float | None = None       # (close - ma60) / ma60 * 100
    # strong_bullish / bullish / weak_bullish / tangled / weak_bearish / bearish / strong_bearish
    ma_status: str | None = None
    # MACD
    dif: float | None = None                  # EMA12 - EMA26
    dea: float | None = None                  # EMA9(DIF)
    macd_hist: float | None = None            # (DIF - DEA) * 2
    macd_cross: str | None = None             # golden / dead / none
    # RSI
    rsi6: float | None = None
    rsi12: float | None = None
    rsi24: float | None = None
    # 量能
    volume_ratio_5_20: float | None = None    # 5日均量 / 20日均量
    volume_ratio_today: float | None = None   # 当日成交量 / 5日均量
    pct_chg_5d: float | None = None
    pct_chg_20d: float | None = None
    high_point: HighLowPoint | None = None
    low_point: HighLowPoint | None = None
    range_percentile: float | None = None     # 区间百分位 0-100
    drawdown_from_high: float | None = None   # 从高点回撤 %
    consecutive_down_days: int | None = None
    current_price: float | None = None
    # 综合信号: strong_buy / buy / hold / watch / sell / strong_sell
    signal: str | None = None


class TechnicalItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    as_of: str | None = None  # 序列最末日 YYYYMMDD（与 index_valuation_percentile.as_of 对齐）；bars 为空时 None
    technical: TechnicalData


class TechnicalResponse(BaseModel):
    total: int
    items: list[TechnicalItem]


class BidAsk(BaseModel):
    price: float
    volume: float


class RealtimeQuote(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    current_price: float
    pct_chg: float          # 涨跌幅 %
    change: float            # 涨跌额
    open: float
    high: float
    low: float
    pre_close: float
    volume: float            # 成交量（手）
    amount: float            # 成交额（元）
    turnover_rate: float | None = None   # 换手率 %
    volume_ratio: float | None = None    # 量比
    asks: list[BidAsk] | None = None     # 卖1-5
    bids: list[BidAsk] | None = None     # 买1-5

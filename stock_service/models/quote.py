from pydantic import BaseModel

from stock_service.models import AdjustType, AssetType, UnsupportedReason


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
    asset_type: AssetType | None = None  # 标的解析不到时为 None
    adjust: AdjustType
    bars: list[DailyBar] = []
    # 请求的标的取不到日线时显式给原因（不再靠「返回条数变少」表达）：
    #   asset_type_not_supported — 该资产类型不支持日线（港股/美股/全球指数）
    #   symbol_not_found        — 代码解析不到
    #   data_unavailable        — 标的适用，但请求区间内没有数据（无交易日或上游无数据）
    unsupported_reason: UnsupportedReason | None = None


class DailyResponse(BaseModel):
    total: int  # 返回的 item 数：每个请求的 code 至少一条 item
    items: list[DailyItem]


class PercentileData(BaseModel):
    period: str  # 6m / 1y / 2y / 3y
    price_percentile: float | None  # **价格**百分位（0-100），数据不足时为 None
    current_price: float | None  # 当前价格
    min_price: float | None = None  # 区间最低价
    max_price: float | None = None  # 区间最高价
    days: int  # 该区间内的交易日数量
    # 以下为股票特有的**估值**百分位（基金/指数不返回）
    # 注意：估值当前值取自序列末日 daily_basic，不随窗口重算，只有百分位随窗口变
    pe_ttm_percentile: float | None = None  # PE-TTM 百分位（0-100）
    current_pe_ttm: float | None = None  # 当前 PE-TTM
    pb_percentile: float | None = None  # PB 百分位（0-100）
    current_pb: float | None = None  # 当前 PB


class PercentileItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType | None = None  # 标的解析不到时为 None
    as_of: str | None = None  # 序列最末日 YYYYMMDD（与 index_valuation_percentile.as_of 对齐）；bars 为空时 None
    stale_days: int | None = None  # as_of 与调用日的自然日差；>3 通常意味着该标的数据陈旧/缺失
    percentiles: list[PercentileData] = []
    # 挤不到数据时显式给原因（asset_type_not_supported / symbol_not_found / data_unavailable）
    unsupported_reason: UnsupportedReason | None = None


class PercentileResponse(BaseModel):
    total: int  # 返回的 item 数：每个请求的 code 至少一条 item
    items: list[PercentileItem]


class PercentileHistoryPoint(BaseModel):
    date: str  # 采样日 YYYYMMDD
    price_percentile: float | None = None  # 价格分位（0-100）
    pe_ttm_percentile: float | None = None  # PE-TTM 分位；ETF/指数取不到时为 None
    pb_percentile: float | None = None  # PB 分位；ETF/指数取不到时为 None


class PercentileHistoryItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType | None = None  # 标的解析不到时为 None
    window: str  # 6m / 1y / 2y / 3y
    freq: str  # daily / weekly
    as_of: str | None = None  # series 最末日（最新交易日）
    stale_days: int | None = None  # as_of 与调用日的自然日差
    series: list[PercentileHistoryPoint] = []
    # 取不到数据时显式给原因（asset_type_not_supported / symbol_not_found / data_unavailable）
    unsupported_reason: UnsupportedReason | None = None


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
    asset_type: AssetType | None = None  # 标的解析不到时为 None
    as_of: str | None = None  # 序列最末日 YYYYMMDD（与 index_valuation_percentile.as_of 对齐）；bars 为空时 None
    stale_days: int | None = None  # as_of 与调用日的自然日差；>3 通常意味着该标的数据陈旧/缺失
    technical: TechnicalData | None = None  # 取不到数据时为 None，原因见 unsupported_reason
    # 挤不到数据时显式给原因（asset_type_not_supported / symbol_not_found / data_unavailable）
    unsupported_reason: UnsupportedReason | None = None


class TechnicalResponse(BaseModel):
    total: int  # 返回的 item 数：每个请求的 code 至少一条 item
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


class ReverseRepoQuote(BaseModel):
    code: str                          # 纯代码，如 "204001"
    symbol: str                        # 带交易所后缀，如 "204001.SH"
    name: str                          # GC001 / R-001
    exchange: str                      # SSE / SZSE
    rate: float                        # 当前年化收益率 %
    pre_close: float | None = None     # 昨日加权 %
    change: float | None = None        # 收益率绝对变化（百分点）
    pct_chg: float | None = None       # 相对昨收的百分比变化 %
    open: float | None = None
    high: float | None = None
    low: float | None = None
    amount: float | None = None        # 成交额（元）


class ReverseRepoResponse(BaseModel):
    total: int
    items: list[ReverseRepoQuote]

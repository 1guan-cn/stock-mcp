from pydantic import BaseModel

from stock_service.models import AssetType, UnsupportedReason


class ValuationData(BaseModel):
    pe_ttm: float | None = None
    pb: float | None = None
    dividend_yield: float | None = None


class FundFlowData(BaseModel):
    main_force_net: float | None = None
    main_force_ratio: float | None = None
    super_large_net: float | None = None
    large_net: float | None = None
    recent_5d_main_force: float | None = None


class NorthboundData(BaseModel):
    north_net_buy: float | None = None
    north_net_buy_5d: float | None = None


class FactorItem(BaseModel):
    code: str
    name: str
    asset_type: AssetType
    as_of: str
    valuation: ValuationData | None = None
    fund_flow: FundFlowData | None = None
    northbound: NorthboundData | None = None


class FactorResponse(BaseModel):
    total: int
    items: list[FactorItem]


# ── F 因子：ETF 申购赎回 ──

class EtfFundFlowData(BaseModel):
    net_subscription: float | None = None   # 净申购额（万元），数据源不支持时为 None
    net_redemption: float | None = None     # 净赎回额（万元），数据源不支持时为 None
    net_inflow: float | None = None         # 净流入估算（万元）≈ share_change × nav
    share_change: float | None = None       # 份额变动（万份）
    scale_change: float | None = None       # 规模变动（亿元）
    recent_5d_inflow: float | None = None   # 近5日累计净流入（万元）
    source: str | None = None              # 数据口径来源


class EtfFundFlowItem(BaseModel):
    symbol: str
    name: str
    as_of: str
    fund_flow: EtfFundFlowData | None = None
    unsupported_reason: UnsupportedReason | None = None  # 不支持时说明原因


# ── I 因子：主力资金结构 ──

class MainForceFlowData(BaseModel):
    large_order_net: float | None = None    # 大单净流入（万元）
    super_large_net: float | None = None    # 超大单净流入（万元）
    main_force_net: float | None = None     # 主力净流入（大单+超大单）（万元）
    main_force_ratio: float | None = None   # 主力净流入占成交额比例


class MainForceFlowItem(BaseModel):
    symbol: str
    name: str
    as_of: str
    main_force: MainForceFlowData | None = None
    unsupported_reason: UnsupportedReason | None = None  # 不支持时说明原因


# ── V 因子：指数估值分位 ──

class ValuationPercentileItem(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    pe_ttm: float | None = None
    pe_percentile_3y: float | None = None
    pb: float | None = None
    pb_percentile_3y: float | None = None
    dividend_yield: float | None = None
    as_of: str | None = None


# ── V 因子：商品价格分位 ──

class CommodityPercentileData(BaseModel):
    commodity_code: str
    current_price: float | None = None
    percentile_52w: float | None = None     # 近52周价格分位
    percentile_1y: float | None = None      # 近1年价格分位
    percentile_3y: float | None = None      # 近3年价格分位
    as_of: str | None = None

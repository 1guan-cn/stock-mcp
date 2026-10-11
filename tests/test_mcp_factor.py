"""get_factor / get_factor_batch / etf_fund_flow / etf_main_force_flow /
index_valuation_percentile / commodity_price_percentile 契约测试。"""

import json

from stock_service.database import get_pool
from stock_service.mcp_server import (
    commodity_price_percentile,
    etf_fund_flow,
    etf_main_force_flow,
    get_factor,
    get_factor_batch,
    index_valuation_percentile,
)


# ── get_factor ──

def test_factor_stock():
    """股票多因子：因子块与 as_of 语义契约。"""
    data = json.loads(get_factor("000001.SZ"))
    for key in (
        "code", "name", "asset_type", "as_of", "requested_date",
        "valuation", "fund_flow",
    ):
        assert key in data, f"缺少字段: {key}"
    assert data["asset_type"] == "stock"
    assert data["valuation"].get("pe_ttm") is not None
    assert data["valuation"].get("unsupported_reason") is None
    # as_of = 实际数据日期（某个因子块的 data_as_of），不是调用意图日期
    data_dates = {
        block.get("data_as_of")
        for block in (data.get("valuation"), data.get("fund_flow"), data.get("northbound"))
        if block and block.get("data_as_of")
    }
    assert data["as_of"] in data_dates, f"as_of={data['as_of']} 不在各因子块数据日期 {data_dates} 内"


def test_factor_valuation_does_not_depend_on_warm_cache():
    """估值必须按需拉取，不能只读本地缓存 —— 否则冷启动/行情窗口外会静默返回
    「没有估值」，且下游无法区分是标的没数据还是这次没给。"""
    with get_pool().connection() as conn:
        conn.execute("DELETE FROM stock_daily_bar WHERE symbol = '000001.SZ'")
        conn.commit()

    data = json.loads(get_factor("000001.SZ"))
    assert "valuation" in data, "冷缓存下 valuation 块整个消失"
    assert data["valuation"].get("pe_ttm") is not None, f"冷缓存下取不到估值: {data['valuation']}"
    assert data["valuation"].get("data_as_of") is not None


def test_dividend_yield_is_ttm_not_single_payment():
    """半年派息标的的股息率必须反映连续两期派息，不能只算最近一次。

    消费侧 ISSUE-3：中国移动 600941 曾返回 2.26%（= 单次末期股息 2.2012 元），
    真实 TTM 约 4.8%（2.2012 + 2.51 = 4.71 元）。根因是直接用了 tushare
    daily_basic 的 dv_ttm，它按除息事件阶梯式累加。
    """
    data = json.loads(get_factor("600941.SH"))
    valuation = data["valuation"]
    per_share = valuation.get("dividend_per_share_ttm")
    assert per_share is not None, f"缺 dividend_per_share_ttm: {valuation}"
    assert per_share > 4.0, f"只取了一次派息，疑似退回单次口径: {per_share}"
    assert valuation.get("dividend_yield") is not None

    # 同一参考日应稳定：2026-09-25 时点消费侧独立核算为 4.70 元
    from stock_service.data.factor import _dividend_ttm_per_share

    per_share_0925 = _dividend_ttm_per_share("600941.SH", "20260925")
    assert per_share_0925 is not None and 4.0 < per_share_0925 < 5.0, (
        f"2026-09-25 时点的 TTM 应为约 4.70 元，实际 {per_share_0925}"
    )


def test_dividend_yield_annual_payer_not_double_counted():
    """年度/半年派息标的不能把上一期重复计入（固定 365 天窗口的错法）。"""
    from stock_service.data.factor import _dividend_ttm_per_share

    # 招商银行半年派息，近 12 个月两期合计 2.016 元；重复计入会接近 4.0
    per_share = _dividend_ttm_per_share("600036.SH", "20261009")
    assert per_share is not None and 1.5 < per_share < 2.5, f"疑似重复计入上一期: {per_share}"


def test_factor_blocks_always_present_with_reason():
    """取不到时不删块，块内给 unsupported_reason。"""
    # ETF：无大单分类口径
    data = json.loads(get_factor("510300.SH"))
    assert data["fund_flow"]["unsupported_reason"] == "etf_no_main_force_data"

    # 指数：无主力资金口径
    data = json.loads(get_factor("000001.SH"))
    assert data["fund_flow"]["unsupported_reason"] == "index_not_supported"

    # 不在 _registry 内的 ETF：估值块必须存在并说明原因
    data = json.loads(get_factor("560800.SH"))
    assert "valuation" in data, "valuation 块整个消失"
    assert data["valuation"]["unsupported_reason"] == "index_not_in_registry"


def test_factor_etf():
    """ETF 多因子。"""
    data = json.loads(get_factor("510300.SH"))
    assert data["asset_type"] == "fund"


def test_factor_index():
    """指数多因子（无资金流）。"""
    data = json.loads(get_factor("000001.SH"))
    assert data["asset_type"] == "index"


# ── get_factor_batch ──

def test_factor_batch():
    """批量多因子。"""
    data = json.loads(get_factor_batch(["510300.SH", "000001.SZ"]))
    assert "items" in data
    assert data["total"] == 2


def test_factor_batch_does_not_silently_drop_unknown_code():
    """不存在的代码必须占位返回并说明原因，total 反映请求数。"""
    codes = ["510300.SH", "512400.SZ"]
    data = json.loads(get_factor_batch(codes))
    assert data["total"] == len(codes)
    bad = next(i for i in data["items"] if i["code"] == "512400.SZ")
    assert bad["unsupported_reason"] == "symbol_not_found"
    assert bad.get("asset_type") is None


# ── etf_fund_flow ──

def test_etf_fund_flow_ok():
    """ETF 申购赎回数据 — schema 稳定契约：所有子字段始终存在（值可为 null）。"""
    data = json.loads(etf_fund_flow("510300.SH"))
    for key in ("symbol", "name", "as_of"):
        assert key in data
    if data.get("fund_flow"):
        # 契约：8 个子字段必须全部存在（含 deprecated 老字段 + 新 series 字段）
        for key in (
            "net_inflow", "share_change", "scale_change",
            "recent_5d_inflow", "recent_5d_inflow_series",
            "source", "data_as_of", "stale_days",
        ):
            assert key in data["fund_flow"], f"missing key: {key}"
        series = data["fund_flow"]["recent_5d_inflow_series"]
        if series is not None:
            assert isinstance(series, list)
            for item in series:
                assert "date" in item and "net_inflow" in item


def test_etf_fund_flow_non_etf():
    """非 ETF 标的应返回 unsupported_reason。"""
    data = json.loads(etf_fund_flow("000001.SZ"))
    assert data.get("unsupported_reason") is not None


def test_etf_fund_flow_explicit_date_no_data_returns_null():
    """显式 date 当日无数据时返回 net_inflow=null，as_of=date 不替换；data_as_of 标注最近可得日。

    新契约（替代旧 silent rollback）：调用方传 `date=20991231` 是问"那一天怎么样"，
    应直接返回"那一天没数据"+ 溯源标注，不应替换成另一天的值。
    """
    future_date = "20991231"
    data = json.loads(etf_fund_flow("510300.SH", future_date))
    # as_of 必须等于请求日期，不替换
    assert data["as_of"] == future_date, (
        f"as_of 应保持请求日期 {future_date}，实际 {data['as_of']}（疑似 silent rollback）"
    )
    assert data.get("fund_flow") is not None, "fund_flow 字段应始终存在"
    flow = data["fund_flow"]
    # 当日无数据 → net_inflow=null
    assert flow["net_inflow"] is None, (
        "未来日期当日不应有数据；net_inflow 必须为 null（不允许 rollback 到他日值）"
    )
    # data_as_of 标注最近可得日，stale_days 暴露差距
    assert flow["data_as_of"] is not None, "应有最近可得日的溯源标注"
    assert flow["data_as_of"] != future_date, "data_as_of 应是真实交易日，非未来日"
    assert flow["stale_days"] is not None and flow["stale_days"] > 0


def test_etf_fund_flow_explicit_date_match_returns_value():
    """显式 date 当日有数据 → 返回该日值，stale_days=0。"""
    # 用一个最近可能有数据的日期；如果 DB 里恰好没这日，跳过断言以容错
    data_default = json.loads(etf_fund_flow("510300.SH"))
    if not data_default.get("fund_flow") or not data_default["fund_flow"].get("data_as_of"):
        return  # DB 无数据时跳过
    real_date = data_default["fund_flow"]["data_as_of"]
    data = json.loads(etf_fund_flow("510300.SH", real_date))
    assert data["as_of"] == real_date
    assert data["fund_flow"]["net_inflow"] is not None
    assert data["fund_flow"]["data_as_of"] == real_date
    assert data["fund_flow"]["stale_days"] == 0


# ── etf_main_force_flow ──

def test_main_force_flow_stock():
    """股票主力资金。"""
    data = json.loads(etf_main_force_flow("000001.SZ"))
    for key in ("symbol", "name", "as_of"):
        assert key in data
    if data.get("main_force"):
        assert "main_force_net" in data["main_force"]


def test_main_force_flow_index():
    """指数不支持主力资金。"""
    data = json.loads(etf_main_force_flow("000001.SH"))
    assert data.get("unsupported_reason") is not None


def test_main_force_flow_etf():
    """ETF 在 A 股市场无主力资金口径，应显式返回 unsupported。"""
    data = json.loads(etf_main_force_flow("510300.SH"))
    assert data.get("unsupported_reason") == "etf_no_main_force_data"
    assert data.get("main_force") is None


# ── index_valuation_percentile ──

def test_index_valuation():
    """指数估值分位。"""
    data = json.loads(index_valuation_percentile("000300.SH"))
    for key in ("symbol", "name", "asset_type"):
        assert key in data
    # 沪深300 应有估值数据
    assert data.get("pe_ttm") is not None or data.get("pb") is not None


def test_etf_valuation():
    """ETF 通过跟踪指数映射获取估值。"""
    data = json.loads(index_valuation_percentile("510300.SH"))
    assert data["asset_type"] == "fund"


def test_etf_valuation_not_in_registry():
    """ETF 未在 _registry 时显式返回 unsupported_reason=index_not_in_registry。"""
    # 行业主题 ETF（非 _registry 内）
    data = json.loads(index_valuation_percentile("560800.SH"))
    assert data["asset_type"] == "fund"
    assert data.get("pe_ttm") is None
    assert data.get("unsupported_reason") == "index_not_in_registry"


def test_index_valuation_data_unavailable():
    """中证主题指数当前 tushare 套餐不返 PE/PB 时显式返回 unsupported_reason=index_data_unavailable。"""
    # _registry 内但 tushare 不返该指数 PE/PB 数据
    data = json.loads(index_valuation_percentile("512010.SH"))
    assert data["asset_type"] == "fund"
    if data.get("pe_ttm") is None:
        assert data.get("unsupported_reason") == "index_data_unavailable"


def test_index_valuation_symbol_not_found():
    """无效代码显式返回 unsupported_reason=symbol_not_found。"""
    data = json.loads(index_valuation_percentile("AAAA.XX"))
    assert data.get("pe_ttm") is None
    assert data.get("unsupported_reason") == "symbol_not_found"


# ── commodity_price_percentile ──

def test_gold_percentile():
    """黄金价格分位（数据源可能无数据，验证结构正确）。"""
    data = json.loads(commodity_price_percentile("AU"))
    assert data["commodity_code"] == "AU"
    # AU 数据源可能无数据，此时 exclude_none 后只剩 commodity_code
    # 有数据时应包含 current_price 等字段
    if data.get("current_price") is not None:
        for key in ("percentile_52w", "percentile_1y", "percentile_3y"):
            assert key in data


def test_copper_percentile():
    """铜价格分位（验证有数据返回）。"""
    data = json.loads(commodity_price_percentile("CU"))
    assert data["commodity_code"] == "CU"
    assert data.get("current_price") is not None

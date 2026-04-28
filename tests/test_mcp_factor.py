"""get_factor / get_factor_batch / etf_fund_flow / etf_main_force_flow /
index_valuation_percentile / commodity_price_percentile 契约测试。"""

import json

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
    """股票多因子。"""
    data = json.loads(get_factor("000001.SZ"))
    for key in ("code", "name", "asset_type", "as_of"):
        assert key in data
    assert data["asset_type"] == "stock"
    assert "valuation" in data
    assert "fund_flow" in data


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


def test_etf_fund_flow_rollback_to_valid_net_inflow():
    """rollback 终点必须是 net_inflow 非 null 的日；查询未来日期，as_of 落到最近有效日 + stale_days > 0。"""
    # 查询一个远未来日期；DB 不会有该日数据，MCP 应回溯到最近有 net_inflow 的真实交易日
    future_date = "20991231"
    data = json.loads(etf_fund_flow("510300.SH", future_date))
    assert data["as_of"] != future_date  # as_of 必须 rollback 到真实日，不能停留在未来
    if data.get("fund_flow"):
        # rollback 终点的 net_inflow 必须有值（否则 rollback 没起作用）
        assert data["fund_flow"]["net_inflow"] is not None, (
            "rollback 终点 net_inflow 不应为 null"
        )
        assert data["fund_flow"]["stale_days"] is not None
        assert data["fund_flow"]["stale_days"] > 0  # 未来 date → stale_days 必然 > 0


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

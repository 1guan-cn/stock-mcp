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
    """ETF 申购赎回数据。"""
    data = json.loads(etf_fund_flow("510300.SH"))
    for key in ("symbol", "name", "as_of"):
        assert key in data
    # fund_flow 可能有值也可能为 None（数据源未更新）
    if data.get("fund_flow"):
        assert "net_inflow" in data["fund_flow"] or "share_change" in data["fund_flow"]


def test_etf_fund_flow_non_etf():
    """非 ETF 标的应返回 unsupported_reason。"""
    data = json.loads(etf_fund_flow("000001.SZ"))
    assert data.get("unsupported_reason") is not None


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

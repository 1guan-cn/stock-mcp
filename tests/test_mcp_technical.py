"""get_technical / get_technical_batch 契约测试。"""

import json

from stock_service.mcp_server import get_technical, get_technical_batch


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    assert "items" in data
    assert "total" in data
    return data


def test_stock_technical():
    """股票技术指标。"""
    data = _parse(get_technical("000001.SZ"))
    assert data["total"] == 1
    item = data["items"][0]
    assert item["symbol"] == "000001.SZ"
    assert item["asset_type"] == "stock"
    tech = item["technical"]
    for key in ("ma5", "ma10", "ma20", "ma_status", "current_price"):
        assert key in tech, f"缺少字段: {key}"


def test_etf_technical():
    """ETF 技术指标。"""
    data = _parse(get_technical("510300.SH"))
    assert data["total"] == 1
    assert data["items"][0]["asset_type"] == "fund"


def test_index_technical():
    """指数技术指标。"""
    data = _parse(get_technical("000001.SH"))
    assert data["total"] == 1
    assert data["items"][0]["asset_type"] == "index"


def test_custom_period():
    """自定义计算区间。"""
    data = _parse(get_technical("000001.SZ", period=120))
    assert data["total"] == 1


def test_batch():
    """批量查询技术指标。"""
    codes = ["000001.SH", "399001.SZ", "399006.SZ"]
    data = _parse(get_technical_batch(codes))
    assert data["total"] == len(codes)
    symbols = [item["symbol"] for item in data["items"]]
    for code in codes:
        assert code in symbols, f"缺少标的: {code}"

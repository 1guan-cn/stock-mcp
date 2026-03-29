"""get_percentile / get_percentile_batch 契约测试。"""

import json

from stock_service.mcp_server import get_percentile, get_percentile_batch


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    assert "items" in data
    assert "total" in data
    return data


def test_stock_percentile():
    """股票百分位，应包含 PE/PB 百分位。"""
    data = _parse(get_percentile("000001.SZ"))
    assert data["total"] == 1
    item = data["items"][0]
    assert item["asset_type"] == "stock"
    assert len(item["percentiles"]) == 4  # 6m, 1y, 2y, 3y
    p = item["percentiles"][0]
    for key in ("period", "percentile", "current_price", "days"):
        assert key in p, f"缺少字段: {key}"


def test_etf_percentile():
    """ETF 百分位。"""
    data = _parse(get_percentile("510300.SH"))
    assert data["total"] == 1
    assert data["items"][0]["asset_type"] == "fund"
    assert len(data["items"][0]["percentiles"]) == 4


def test_index_percentile():
    """指数百分位。"""
    data = _parse(get_percentile("000001.SH"))
    assert data["total"] == 1
    assert data["items"][0]["asset_type"] == "index"


def test_batch():
    """批量查询百分位。"""
    codes = ["510300.SH", "510500.SH"]
    data = _parse(get_percentile_batch(codes))
    assert data["total"] == len(codes)
    symbols = [item["symbol"] for item in data["items"]]
    for code in codes:
        assert code in symbols

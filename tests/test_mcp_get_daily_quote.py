"""get_daily_quote 契约测试 — 验证返回 JSON 结构符合预期。"""

import json

from stock_service.mcp_server import get_daily_quote


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    assert "items" in data
    assert "total" in data
    assert isinstance(data["items"], list)
    return data


def test_stock_simple():
    """股票日线 simple 模式，返回基础字段。"""
    data = _parse(get_daily_quote("000001.SZ", "20250101", "20250110"))
    assert data["total"] == 1
    item = data["items"][0]
    assert item["name"] == "平安银行"
    assert item["asset_type"] == "stock"
    assert len(item["bars"]) > 0
    bar = item["bars"][0]
    for key in ("date", "open", "high", "low", "close", "volume"):
        assert key in bar, f"缺少字段: {key}"


def test_stock_all_detail():
    """股票日线 all 模式，返回额外字段如 amount。"""
    data = _parse(get_daily_quote("000001.SZ", "20250101", "20250110", detail="all"))
    bar = data["items"][0]["bars"][0]
    for key in ("date", "open", "high", "low", "close", "volume", "amount"):
        assert key in bar, f"缺少字段: {key}"


def test_etf():
    """ETF 日线查询。"""
    data = _parse(get_daily_quote("510300.SH", "20250101", "20250110"))
    item = data["items"][0]
    assert item["asset_type"] == "fund"
    assert len(item["bars"]) > 0


def test_index():
    """指数日线查询。"""
    data = _parse(get_daily_quote("000001.SH", "20250101", "20250110"))
    item = data["items"][0]
    assert item["asset_type"] == "index"
    assert len(item["bars"]) > 0


def test_adjust_hfq():
    """后复权模式，价格应显著大于前复权。"""
    data_hfq = _parse(get_daily_quote("000001.SZ", "20250101", "20250110", adjust="hfq"))
    data_qfq = _parse(get_daily_quote("000001.SZ", "20250101", "20250110", adjust="qfq"))
    hfq_close = data_hfq["items"][0]["bars"][0]["close"]
    qfq_close = data_qfq["items"][0]["bars"][0]["close"]
    assert hfq_close > qfq_close, "后复权价格应大于前复权"


def test_adjust_none():
    """不复权模式。"""
    data = _parse(get_daily_quote("000001.SZ", "20250101", "20250110", adjust="none"))
    assert data["items"][0]["adjust"] == "none"
    assert len(data["items"][0]["bars"]) > 0


def test_empty_range():
    """查询周末范围，bars 应为空列表。"""
    data = _parse(get_daily_quote("000001.SZ", "20250104", "20250105"))
    assert data["items"][0]["bars"] == []

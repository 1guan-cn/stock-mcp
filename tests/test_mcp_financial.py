"""get_financial 契约测试。"""

import json

from stock_service.mcp_server import get_financial


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    for key in ("symbol", "report_type", "total", "records"):
        assert key in data, f"缺少字段: {key}"
    return data


def test_income():
    """利润表查询。"""
    data = _parse(get_financial("000001.SZ", "income", "20240101", "20241231"))
    assert data["report_type"] == "income"
    assert data["total"] > 0
    record = data["records"][0]
    assert "period" in record
    assert "data" in record


def test_balance_sheet():
    """资产负债表查询。"""
    data = _parse(get_financial("000001.SZ", "balance_sheet", "20240101", "20241231"))
    assert data["report_type"] == "balance_sheet"
    assert data["total"] > 0


def test_cashflow():
    """现金流量表查询。"""
    data = _parse(get_financial("000001.SZ", "cashflow", "20240101", "20241231"))
    assert data["report_type"] == "cashflow"
    assert data["total"] > 0


def test_indicator():
    """财务指标查询。"""
    data = _parse(get_financial("000001.SZ", "indicator", "20240101", "20241231"))
    assert data["report_type"] == "indicator"
    assert data["total"] > 0


def test_dividend():
    """分红送股查询。"""
    data = _parse(get_financial("000001.SZ", "dividend", "20230101", "20241231"))
    assert data["report_type"] == "dividend"
    assert data["total"] >= 0

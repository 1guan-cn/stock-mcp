"""list_stocks / list_funds / list_indexes 契约测试。"""

import json

from stock_service.mcp_server import list_funds, list_indexes, list_stocks


# ── list_stocks ──

def test_list_stocks_all():
    """无筛选条件，返回全部股票。"""
    result = json.loads(list_stocks())
    assert isinstance(result, list)
    assert len(result) > 100
    item = result[0]
    for key in ("symbol", "name"):
        assert key in item


def test_list_stocks_by_industry():
    """按行业筛选。"""
    result = json.loads(list_stocks(industry="银行"))
    assert isinstance(result, list)
    assert len(result) > 0
    assert all("银行" in item.get("industry", "") for item in result)


def test_list_stocks_by_market():
    """按市场筛选。"""
    result = json.loads(list_stocks(market="科创板"))
    assert isinstance(result, list)
    assert len(result) > 0


def test_list_stocks_by_keyword():
    """按关键词筛选。"""
    result = json.loads(list_stocks(keyword="平安"))
    assert isinstance(result, list)
    assert len(result) > 0


# ── list_funds ──

def test_list_funds_all():
    """无筛选条件。"""
    result = json.loads(list_funds())
    assert isinstance(result, list)
    assert len(result) > 0
    item = result[0]
    for key in ("symbol", "name"):
        assert key in item


def test_list_funds_by_keyword():
    """按关键词筛选 ETF。"""
    result = json.loads(list_funds(keyword="沪深300"))
    assert isinstance(result, list)
    assert len(result) > 0


# ── list_indexes ──

def test_list_indexes_all():
    """无筛选条件。"""
    result = json.loads(list_indexes())
    assert isinstance(result, list)
    assert len(result) > 0
    item = result[0]
    for key in ("symbol", "name"):
        assert key in item


def test_list_indexes_by_keyword():
    """按关键词筛选。"""
    result = json.loads(list_indexes(keyword="沪深300"))
    assert isinstance(result, list)
    assert len(result) > 0

"""search_stock 契约测试。"""

import json

from stock_service.mcp_server import search_stock


def test_search_by_name():
    """按名称搜索。"""
    result = json.loads(search_stock("平安"))
    assert isinstance(result, list)
    assert len(result) > 0
    item = result[0]
    for key in ("symbol", "name", "asset_type"):
        assert key in item, f"缺少字段: {key}"


def test_search_by_code():
    """按代码搜索。"""
    result = json.loads(search_stock("000001"))
    assert isinstance(result, list)
    assert len(result) > 0


def test_search_no_match():
    """搜索不存在的关键词，应返回空列表。"""
    result = json.loads(search_stock("ZZZZZZZZZ不存在"))
    assert isinstance(result, list)
    assert len(result) == 0

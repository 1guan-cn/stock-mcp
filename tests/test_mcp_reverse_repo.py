"""get_reverse_repo_rate 契约测试 — 真实拉取腾讯财经实时数据。"""

import json

from stock_service.mcp_server import get_reverse_repo_rate


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    assert "items" in data
    assert "total" in data
    assert data["total"] == len(data["items"])
    return data


def test_default_returns_all():
    """默认返回全部 18 个常用品种。"""
    data = _parse(get_reverse_repo_rate())
    assert data["total"] >= 15  # 容忍少量临时取不到，但应覆盖大多数
    names = {item["name"] for item in data["items"]}
    assert {"GC001", "GC007", "R-001", "R-007"}.issubset(names)


def test_item_fields():
    """单个 item 应至少包含 rate / pre_close 等核心字段。"""
    data = _parse(get_reverse_repo_rate(codes=["GC001"]))
    assert data["total"] == 1
    item = data["items"][0]
    assert item["code"] == "204001"
    assert item["symbol"] == "204001.SH"
    assert item["name"] == "GC001"
    assert item["exchange"] == "SSE"
    assert isinstance(item["rate"], (int, float))
    assert item["rate"] > 0


def test_filter_by_pure_code():
    """按纯数字代码过滤。"""
    data = _parse(get_reverse_repo_rate(codes=["204007", "131810"]))
    names = {item["name"] for item in data["items"]}
    assert names == {"GC007", "R-001"}


def test_filter_by_full_code():
    """带交易所后缀的完整代码也能匹配。"""
    data = _parse(get_reverse_repo_rate(codes=["204001.SH", "131810.SZ"]))
    names = {item["name"] for item in data["items"]}
    assert names == {"GC001", "R-001"}


def test_filter_case_insensitive():
    """名称匹配大小写无关。"""
    data = _parse(get_reverse_repo_rate(codes=["gc001", "r-001"]))
    names = {item["name"] for item in data["items"]}
    assert names == {"GC001", "R-001"}


def test_unknown_code_filtered():
    """未知代码静默过滤，不报错。"""
    data = _parse(get_reverse_repo_rate(codes=["NOTEXIST", "GC001"]))
    names = {item["name"] for item in data["items"]}
    assert names == {"GC001"}

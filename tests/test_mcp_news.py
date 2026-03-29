"""search_news 契约测试 — 分步验证搜索与总结。"""

import json

import pytest

from stock_service.data.adapters.bailian import _mcp_web_search
from stock_service.mcp_server import search_news


# ── Step 1: WebSearch MCP 原始搜索 ──


@pytest.mark.asyncio
async def test_web_search_returns_real_results():
    """WebSearch MCP 应返回真实搜索结果（非空、有一定长度）。"""
    raw = await _mcp_web_search("半导体 最近一周 重要新闻")
    assert raw is not None, "搜索返回 None"
    assert isinstance(raw, str), f"搜索返回类型异常: {type(raw)}"
    assert len(raw.strip()) > 50, f"搜索结果过短，疑似无真实数据: {raw[:200]}"


# ── Step 2: 端到端 pipeline ──


@pytest.mark.asyncio
async def test_search_news_single_keyword():
    """单关键词端到端：验证返回结构。"""
    data = json.loads(await search_news(["半导体"]))
    assert isinstance(data, list)
    assert len(data) == 1
    item = data[0]
    assert item["keyword"] == "半导体"
    assert isinstance(item["summary"], str)
    assert len(item["summary"]) > 0, "AI 总结为空"
    assert "searched_at" in item


@pytest.mark.asyncio
async def test_search_news_multiple_keywords():
    """多关键词：返回数量应等于输入数量。"""
    keywords = ["新能源车", "医药"]
    data = json.loads(await search_news(keywords))
    assert isinstance(data, list)
    assert len(data) == len(keywords)
    returned_keywords = {item["keyword"] for item in data}
    for kw in keywords:
        assert kw in returned_keywords, f"缺少关键词: {kw}"

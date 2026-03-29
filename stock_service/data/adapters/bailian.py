import asyncio
import logging

import httpx
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from openai import OpenAI

from stock_service.config import settings

logger = logging.getLogger(__name__)

_WEBSEARCH_URL = "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp"

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(
            base_url=settings.ai_base_url,
            api_key=settings.ai_api_key,
        )
    return _client


async def _mcp_web_search(query: str) -> str:
    """通过 WebSearch MCP 服务搜索，返回原始搜索结果文本。"""
    http_client = httpx.AsyncClient(
        headers={"Authorization": f"Bearer {settings.dashscope_api_key}"},
        timeout=httpx.Timeout(30, read=120),
    )

    async with http_client:
        async with streamable_http_client(
            url=_WEBSEARCH_URL,
            http_client=http_client,
        ) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool(
                    name="bailian_web_search",
                    arguments={"query": query},
                )
                parts = []
                for content in result.content:
                    if hasattr(content, "text"):
                        parts.append(content.text)
                return "\n".join(parts)


async def search_news(keyword: str) -> str:
    """搜索行业新闻：WebSearch MCP 获取原文 → Coding Plan 模型总结。"""
    try:
        raw = await _mcp_web_search(f"{keyword} 最近一周 重要新闻")
    except Exception:
        logger.exception("WebSearch MCP 调用失败: keyword=%s", keyword)
        return ""

    logger.info("WebSearch 原始结果: keyword=%s, raw=%s", keyword, raw[:500] if raw else "(empty)")
    if not raw.strip():
        logger.warning("WebSearch 无结果: keyword=%s", keyword)
        return ""

    prompt = (
        f"以下是关于「{keyword}」的网络搜索结果：\n\n{raw}\n\n"
        "请严格基于上述搜索结果进行总结，不要使用你自身的知识补充。"
        "如果搜索结果中没有相关新闻，请直接回复「无相关新闻」。\n"
        "要求：用简洁的中文总结，包含关键事件、政策变化和市场影响。"
        "每条新闻一行，按重要性排序，最多10条。"
    )

    try:
        response = await asyncio.to_thread(
            _get_client().chat.completions.create,
            model=settings.ai_model,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()
    except Exception:
        logger.exception("百炼总结失败: keyword=%s", keyword)
        return ""

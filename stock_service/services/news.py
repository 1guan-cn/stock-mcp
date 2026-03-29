import asyncio

from stock_service.data import news as news_data
from stock_service.models.news import NewsItem


async def search_news(keywords: list[str]) -> list[NewsItem]:
    """并发搜索多个行业新闻，返回总结列表。"""
    unique_keywords = list(dict.fromkeys(keywords))

    results = await asyncio.gather(*[news_data.search_news(kw) for kw in unique_keywords])

    return [r for r in results if r is not None]

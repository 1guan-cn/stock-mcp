from datetime import datetime, timedelta

from stock_service.data._cache import CST
from stock_service.data.adapters import bailian
from stock_service.data.store import news as news_store
from stock_service.models.news import NewsItem

_CACHE_TTL = timedelta(minutes=10)


async def search_news(keyword: str) -> NewsItem | None:
    """搜索行业新闻并存储到 DB，返回 NewsItem。

    同一关键词在 1 小时内命中 DB 缓存，不重复调用 AI API。
    """
    recent = news_store.get_recent_news(keyword, limit=1)
    if recent:
        age = datetime.now(CST) - recent[0].searched_at
        if age < _CACHE_TTL:
            return recent[0]

    summary = await bailian.search_news(keyword)
    if not summary:
        return None

    searched_at = datetime.now(CST)
    news_store.save_news(keyword, summary, searched_at)

    return NewsItem(keyword=keyword, summary=summary, searched_at=searched_at)


def get_recent_news(keyword: str, limit: int = 10) -> list[NewsItem]:
    """获取历史搜索结果。"""
    return news_store.get_recent_news(keyword, limit)

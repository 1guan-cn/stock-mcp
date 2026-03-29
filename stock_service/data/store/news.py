from datetime import datetime

from psycopg.rows import dict_row

from stock_service.database import get_pool
from stock_service.models.news import NewsItem


def save_news(keyword: str, summary: str, searched_at: datetime) -> None:
    sql = """
        INSERT INTO news_search (keyword, summary, searched_at)
        VALUES (%s, %s, %s)
    """
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (keyword, summary, searched_at))
        conn.commit()


def get_recent_news(keyword: str, limit: int = 10) -> list[NewsItem]:
    sql = """
        SELECT keyword, summary, searched_at
        FROM news_search
        WHERE keyword = %s
        ORDER BY searched_at DESC
        LIMIT %s
    """
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(sql, (keyword, limit)).fetchall()
    return [NewsItem(**row) for row in rows]

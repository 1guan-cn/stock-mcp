from datetime import datetime

from pydantic import BaseModel


class NewsItem(BaseModel):
    keyword: str
    summary: str
    searched_at: datetime

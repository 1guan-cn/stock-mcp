"""Service 层 — 融资融券。"""

import logging
from datetime import datetime, timedelta, timezone

from stock_service.data import listing as listing_data
from stock_service.data import margin as margin_data
from stock_service.models.margin import MarginBar, MarginItem
from stock_service.services._utils import today_str

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))


def _n_days_ago(n: int) -> str:
    return (datetime.now(_CST) - timedelta(days=n)).strftime("%Y%m%d")


def get_margin(
    code: str,
    date: str | None = None,
    n_days: int = 1,
) -> MarginItem:
    """获取融资融券数据。

    Args:
        code: 证券代码（带交易所后缀）
        date: 截止日期，不传则使用最新数据
        n_days: 返回近N个交易日数据，默认1（仅最新）
    """
    listing_data.ensure_data()
    symbols = listing_data.resolve_symbols(code=code)
    if not symbols:
        return MarginItem(code=code, name="", as_of=date or today_str(), data=[])

    symbol, name, _ = symbols[0]
    end_date = date or today_str()
    # 多取一些日历日以覆盖足够的交易日（交易日约为自然日的72%）
    start_date = _n_days_ago(max(n_days * 2 + 30, 60))

    try:
        rows = margin_data.get_margin(symbol, start_date, end_date)
    except Exception as e:
        logger.warning("get_margin(%s) failed: %s", code, e)
        rows = []

    rows = [r for r in rows if r.get("date") and r["date"] <= end_date]
    rows = rows[-n_days:] if len(rows) > n_days else rows

    as_of = rows[-1]["date"] if rows else end_date
    bars = [MarginBar(**r) for r in rows]

    return MarginItem(code=symbol, name=name, as_of=as_of, data=bars)

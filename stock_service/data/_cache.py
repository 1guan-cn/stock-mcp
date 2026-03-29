"""通用增量缓存管理。

所有 data 层模块共享此缓存机制：检查 coverage → 算缺失 → 拉取 → 存储 → 更新 coverage → 返回 DB 数据。
"""

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from stock_service.data.store import quote as coverage_store

CST = timezone(timedelta(hours=8))

T = TypeVar("T")


def today_str() -> str:
    return datetime.now(CST).strftime("%Y%m%d")


def n_days_ago_str(n: int) -> str:
    return (datetime.now(CST) - timedelta(days=n)).strftime("%Y%m%d")


def is_today(date_str: str) -> bool:
    return date_str == today_str()


def calc_missing_ranges(
    start_date: str,
    end_date: str,
    coverage: tuple[str, str] | None,
) -> list[tuple[str, str]]:
    """根据已缓存的日期范围，计算需要补拉的缺失区间。"""
    if coverage is None:
        return [(start_date, end_date)]

    cached_min, cached_max = coverage
    ranges: list[tuple[str, str]] = []

    if start_date < cached_min:
        day_before = (
            datetime.strptime(cached_min, "%Y%m%d") - timedelta(days=1)
        ).strftime("%Y%m%d")
        ranges.append((start_date, day_before))

    if end_date > cached_max:
        day_after = (
            datetime.strptime(cached_max, "%Y%m%d") + timedelta(days=1)
        ).strftime("%Y%m%d")
        ranges.append((day_after, end_date))

    return ranges


def fetch_with_cache(
    symbol: str,
    data_type: str,
    start_date: str,
    end_date: str,
    *,
    fetch_fn: Callable[[str, str, str], list[T]],
    save_fn: Callable[[str, list[T]], None],
    query_fn: Callable[[str, str, str], list[T]],
    exclude_today: bool = True,
) -> list[T]:
    """通用增量缓存。

    参数:
        fetch_fn(symbol, start, end) — adapter 拉取
        save_fn(symbol, data) — store 存储
        query_fn(symbol, start, end) — store 查询
    """
    coverage = coverage_store.get_coverage(symbol, data_type)
    missing = calc_missing_ranges(start_date, end_date, coverage)

    for m_start, m_end in missing:
        data = fetch_fn(symbol, m_start, m_end)
        save_fn(symbol, data)

    # 更新 coverage（当天数据不纳入，保证下次能获取最新）
    effective_end = end_date
    if exclude_today and is_today(end_date):
        effective_end = (datetime.now(CST) - timedelta(days=1)).strftime("%Y%m%d")

    if effective_end >= start_date:
        coverage_store.update_coverage(symbol, start_date, effective_end, data_type)

    return query_fn(symbol, start_date, end_date)

"""通用增量缓存管理。

所有 data 层模块共享此缓存机制：读 DB 实际跨度 → 算缺失区间 → 拉取 → 存储 → 返回 DB 数据。

缓存的「已覆盖范围」永远从 DB 实际数据推导（`store.get_data_span`），不单独维护
水位线 —— 否则水位线一旦与真实数据脱节，缺口会被永久判成「已覆盖」。
"""

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from stock_service.data.store import quote as span_store

CST = timezone(timedelta(hours=8))

T = TypeVar("T")


def today_str() -> str:
    return datetime.now(CST).strftime("%Y%m%d")


def n_days_ago_str(n: int) -> str:
    return (datetime.now(CST) - timedelta(days=n)).strftime("%Y%m%d")


def is_today(date_str: str) -> bool:
    return date_str == today_str()


def yesterday_str() -> str:
    return (datetime.now(CST) - timedelta(days=1)).strftime("%Y%m%d")


def _norm_date(value: str) -> str:
    """容错处理历史数据中可能存在的 'YYYY-MM-DD' 格式。"""
    return value.replace("-", "") if value else value


def calc_missing_ranges(
    start_date: str,
    end_date: str,
    covered: tuple[str, str] | None,
) -> list[tuple[str, str]]:
    """根据 DB 里已有的日期跨度，计算需要补拉的缺失区间。"""
    if covered is None:
        return [(start_date, end_date)]

    cached_min = _norm_date(covered[0])
    cached_max = _norm_date(covered[1])
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
) -> list[T]:
    """通用增量缓存。

    参数:
        fetch_fn(symbol, start, end) — adapter 拉取
        save_fn(symbol, data) — store 存储
        query_fn(symbol, start, end) — store 查询

    「已覆盖范围」= DB 里实际存在的日期跨度（最晚日期视为昨天，保证当天数据
    每次都能重新拉到，避免把尚未定型的当日行永久缓存）。
    """
    covered = span_store.get_data_span(symbol, data_type)
    if covered and is_today(covered[1]):
        covered = (covered[0], yesterday_str())

    for m_start, m_end in calc_missing_ranges(start_date, end_date, covered):
        save_fn(symbol, fetch_fn(symbol, m_start, m_end))

    return query_fn(symbol, start_date, end_date)

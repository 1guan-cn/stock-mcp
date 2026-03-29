"""Service 层共享工具函数。"""

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))


def today_str() -> str:
    """返回当天日期字符串（CST，YYYYMMDD）。"""
    return datetime.now(CST).strftime("%Y%m%d")


def calc_percentile(values: list[float], current: float) -> float:
    """计算 current 在 values 中的百分位（0-100）。"""
    below = sum(1 for v in values if v < current)
    return round(below / len(values) * 100, 2)

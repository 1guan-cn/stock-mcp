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


def stale_days(data_date: str | None, ref_date: str | None = None) -> int | None:
    """计算 ref_date（默认今天）与 data_date 之间的自然日差。

    用于资金流类数据的陈旧度标记：None 表示无法判断（data_date 缺失），
    0 表示当日，>1 通常可视为陈旧（跨周末时可能为 2-3）。
    """
    if not data_date:
        return None
    try:
        d = datetime.strptime(data_date, "%Y%m%d")
        r = datetime.strptime(ref_date or today_str(), "%Y%m%d")
        return max(0, (r - d).days)
    except ValueError:
        return None

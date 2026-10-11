"""增量缓存缺口自愈的回归测试。

历史 bug：缓存水位线按「请求区间」推进（而不是「实际拿到的数据」），一旦水位线
被推到数据并不存在的日期，`calc_missing_ranges` 就永远判定「已覆盖」，缺口不再
回补 —— 表现为 `as_of` 长期停在几个月前、`current_price` 与实时价严重背离，
且没有任何 stale 标记。本测试锁定「DB 缺多少就补多少」。
"""

import json

from stock_service.database import get_pool
from stock_service.mcp_server import get_percentile

SYMBOL = "000768.SZ"


def _bar_count() -> int:
    with get_pool().connection() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM stock_daily_bar WHERE symbol = %s", (SYMBOL,)
        ).fetchone()[0]


def test_tail_gap_is_refilled():
    """尾部缺口（K 线停在数月前）应在下次调用被补拉，而不是静默返回停更序列。"""
    first = json.loads(get_percentile(SYMBOL))["items"][0]
    assert first["as_of"] is not None, "预热失败：该标的应有日线数据"

    baseline = _bar_count()
    assert baseline > 100, f"预热数据异常：仅 {baseline} 根 K 线"

    # 造缺口：删掉最近一根之前的全部 K 线，DB 的 max(date) 不变，
    # 因此「按水位线要不要补拉」的老实现会认为无事可做
    with get_pool().connection() as conn:
        conn.execute(
            "DELETE FROM stock_daily_bar WHERE symbol = %s AND date < %s",
            (SYMBOL, first["as_of"]),
        )
        conn.commit()
    assert _bar_count() == 1, "缺口构造失败"

    healed = json.loads(get_percentile(SYMBOL))["items"][0]

    assert _bar_count() >= baseline, "缺口未被回补，缓存仍在返回残缺序列"
    assert healed["as_of"] == first["as_of"]
    assert healed["percentiles"][3]["price_percentile"] == first["percentiles"][3]["price_percentile"]

"""services._utils.stale_days 单元测试（纯函数，无需 DB）。"""

from stock_service.services._utils import stale_days


def test_stale_days_same_day():
    assert stale_days("20260408", "20260408") == 0


def test_stale_days_one_day():
    assert stale_days("20260407", "20260408") == 1


def test_stale_days_fifteen_days():
    assert stale_days("20260324", "20260408") == 15


def test_stale_days_future_data_clamped_to_zero():
    """data_date 在 ref_date 之后（异常情况）返回 0 而非负数。"""
    assert stale_days("20260410", "20260408") == 0


def test_stale_days_none_input():
    assert stale_days(None) is None
    assert stale_days("") is None


def test_stale_days_invalid_format():
    assert stale_days("2026-04-08") is None
    assert stale_days("abc") is None

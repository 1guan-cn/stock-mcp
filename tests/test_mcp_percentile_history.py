"""get_percentile_history 契约测试。

验收对齐 invest-tao `MCP-需求清单.md` 第 1 条：
1. series[-1] 与 get_percentile 同窗口值一致（差 < 0.1）
2. series[-2] 恰为 5 个交易日前，且与客户端用日线重算的分位一致（差 < 0.1）
3. 510300.SH 3y PB 分位有值（指数/ETF 路径打通）
4. 港股/美股 → asset_type_not_supported，不静默空数组
"""

import json
from datetime import datetime, timedelta

import pytest

from stock_service.mcp_server import get_daily_quote, get_percentile, get_percentile_history
from stock_service.services._utils import CST, calc_percentile


def _parse(raw: str) -> dict:
    return json.loads(raw)


def _period(data: dict, period: str) -> dict:
    return next(p for p in data["items"][0]["percentiles"] if p["period"] == period)


def _daily_bars(code: str) -> list[dict]:
    today = datetime.now(CST)
    start = (today - timedelta(days=730)).strftime("%Y%m%d")
    raw = get_daily_quote(
        code,
        start_date=start,
        end_date=today.strftime("%Y%m%d"),
        adjust="qfq",
        detail="all",
    )
    return _parse(raw)["items"][0]["bars"]


def test_stock_series_contract():
    """股票序列字段齐全、按日期升序、as_of 是最新采样日。"""
    data = _parse(
        get_percentile_history("000001.SZ", window="1y", freq="weekly", periods=8)
    )
    assert data["symbol"] == "000001.SZ"
    assert data["asset_type"] == "stock"
    assert data["window"] == "1y"
    assert data["freq"] == "weekly"
    assert len(data["series"]) == 8

    dates = [p["date"] for p in data["series"]]
    assert dates == sorted(dates)
    assert data["as_of"] == dates[-1]
    assert data["stale_days"] <= 5

    for point in data["series"]:
        for key in ("date", "price_percentile", "pe_ttm_percentile", "pb_percentile"):
            assert key in point, f"缺少字段: {key}"


@pytest.mark.parametrize(
    "field", ["price_percentile", "pe_ttm_percentile", "pb_percentile"]
)
def test_last_point_matches_get_percentile(field: str):
    """验收 1：series[-1] 与 get_percentile 的 1y 分位差 < 0.1。"""
    hist = _parse(
        get_percentile_history("000001.SZ", window="1y", freq="weekly", periods=2)
    )
    current = _period(_parse(get_percentile("000001.SZ")), "1y")
    assert abs(hist["series"][-1][field] - current[field]) < 0.1


def test_previous_weekly_point_is_five_trading_days_back():
    """验收 2（前半）：weekly 的 series[-2] 恰为 5 个交易日前。"""
    hist = _parse(
        get_percentile_history("000001.SZ", window="1y", freq="weekly", periods=2)
    )
    bars = _daily_bars("000001.SZ")
    dates = [b["date"] for b in bars]

    # 日线最新交易日必须与序列最后一点一致
    assert dates[-1] == hist["series"][-1]["date"]
    assert dates.index(dates[-1]) - dates.index(hist["series"][-2]["date"]) == 5


def test_previous_point_recomputes_from_daily_bars():
    """验收 2（后半）：series[-2] 与客户端本地重算的 5 日前 PB 分位一致。"""
    hist = _parse(
        get_percentile_history("000001.SZ", window="1y", freq="weekly", periods=2)
    )
    prev = hist["series"][-2]
    bars = _daily_bars("000001.SZ")

    cutoff = (
        datetime.strptime(prev["date"], "%Y%m%d") - timedelta(days=365)
    ).strftime("%Y%m%d")
    window = [b for b in bars if cutoff <= b["date"] <= prev["date"]]
    pbs = [b["pb"] for b in window if b.get("pb") and b["pb"] > 0]
    current_pb = next(b["pb"] for b in bars if b["date"] == prev["date"])

    assert abs(calc_percentile(pbs, current_pb) - prev["pb_percentile"]) < 0.1


def test_etf_three_year_pb_history():
    """验收 3：510300.SH 3y weekly 的 PB 分位有值（指数路径打通）。"""
    data = _parse(
        get_percentile_history("510300.SH", window="3y", freq="weekly", periods=8)
    )
    assert data["asset_type"] == "fund"
    assert len(data["series"]) == 8
    assert all(p["pb_percentile"] is not None for p in data["series"])
    assert all(p["pe_ttm_percentile"] is not None for p in data["series"])


def test_index_price_and_valuation_history():
    """指数：价格分位与 PE/PB 分位同时给出。"""
    data = _parse(
        get_percentile_history("000001.SH", window="1y", freq="weekly", periods=2)
    )
    last = data["series"][-1]
    assert last["price_percentile"] is not None
    assert last["pb_percentile"] is not None


def test_daily_freq_returns_last_n_trading_days():
    """daily：periods=3 即最近 3 个交易日，日期与日线一致。"""
    hist = _parse(
        get_percentile_history("000001.SZ", window="6m", freq="daily", periods=3)
    )
    assert [p["date"] for p in hist["series"]] == [
        b["date"] for b in _daily_bars("000001.SZ")[-3:]
    ]


def test_overseas_asset_type_not_supported():
    """验收 4：港股不静默空数组。"""
    data = _parse(get_percentile_history("00700.HK"))
    assert data["series"] == []
    assert data["unsupported_reason"] == "asset_type_not_supported"


def test_unknown_symbol_not_found():
    """未知代码显式给 symbol_not_found。"""
    data = _parse(get_percentile_history("999999.SH"))
    assert data["series"] == []
    assert data["unsupported_reason"] == "symbol_not_found"


@pytest.mark.parametrize(
    "kwargs",
    [{"window": "5y"}, {"freq": "monthly"}, {"periods": 0}, {"periods": 261}],
)
def test_invalid_args(kwargs: dict):
    """非法参数直接报错，不静默退化成默认值。"""
    with pytest.raises(ValueError):
        get_percentile_history("000001.SZ", **kwargs)

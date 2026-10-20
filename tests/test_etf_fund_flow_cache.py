"""ETF flow refreshes must retain the share-difference baseline and repair gaps."""

from datetime import date, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from stock_service.data import factor
from stock_service.data import _cache
from stock_service.data.adapters import tushare
from stock_service.services import factor as factor_service


@pytest.fixture(scope="session", autouse=True)
def _db_pool():
    """These boundary tests use an in-memory store, not the integration database."""
    yield


@pytest.fixture
def flow_source(monkeypatch):
    rows = [
        {"trade_date": "20261014", "total_share": 1000.0, "nav": 1.0},
        {"trade_date": "20261015", "total_share": 1100.0, "nav": 1.1},
        {"trade_date": "20261016", "total_share": 1300.0, "nav": 1.2},
    ]
    cached = {
        "20260901": {"date": "20260901", "net_inflow": 0.0},
        "20261014": {"date": "20261014", "net_inflow": 25.0},
    }
    calls = []

    def fetch(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame([
            row.copy() for row in rows
            if kwargs["start_date"] <= row["trade_date"] <= kwargs["end_date"]
        ])

    def save(symbol, records):
        assert symbol == "159869.SZ"
        cached.update({row["date"]: row for row in records})

    def query(symbol, start, end):
        assert symbol == "159869.SZ"
        return [cached[day] for day in sorted(cached) if start <= day <= end]

    monkeypatch.setattr(tushare, "_api", lambda: SimpleNamespace(etf_share_size=fetch))
    monkeypatch.setattr(factor, "today_str", lambda: "20261016")
    monkeypatch.setattr(
        factor, "n_days_ago_str",
        lambda days: (date(2026, 10, 16) - timedelta(days=days)).strftime("%Y%m%d"),
    )
    monkeypatch.setattr(_cache, "is_today", lambda day: day == "20261016")
    monkeypatch.setattr(_cache, "yesterday_str", lambda: "20261015")
    monkeypatch.setattr(
        _cache.span_store, "get_data_span", lambda *_: (min(cached), max(cached)),
    )
    monkeypatch.setattr(factor.factor_store, "save_etf_subscription", save)
    monkeypatch.setattr(factor.factor_store, "get_cached_etf_subscription", query)
    return SimpleNamespace(rows=rows, cached=cached, calls=calls)


def test_first_incremental_day_keeps_previous_share_baseline(flow_source):
    rows = factor.get_etf_fund_flow("159869.SZ")

    by_date = {row["date"]: row for row in rows}
    assert by_date["20261015"]["net_inflow"] == 110.0
    assert by_date["20261016"]["net_inflow"] == 240.0
    assert len(flow_source.calls) == 1
    assert flow_source.calls[0]["start_date"] == "20260901"
    assert all(row["date"] >= "20260916" for row in rows)


def test_warm_cache_repairs_internal_missing_day(flow_source):
    flow_source.cached["20261016"] = {"date": "20261016", "net_inflow": 240.0}

    rows = factor.get_etf_fund_flow("159869.SZ")

    assert {row["date"]: row["net_inflow"] for row in rows}["20261015"] == 110.0


def test_delayed_nav_is_refreshed_inside_existing_span(flow_source):
    flow_source.cached.update({
        "20261015": {"date": "20261015", "net_inflow": None},
        "20261016": {"date": "20261016", "net_inflow": 240.0},
    })

    factor.get_etf_fund_flow("159869.SZ")

    assert flow_source.cached["20261015"]["net_inflow"] == 110.0


def test_unpublished_nav_stays_null_and_is_filled_when_published(flow_source):
    flow_source.rows[1]["nav"] = None

    first = {row["date"]: row for row in factor.get_etf_fund_flow("159869.SZ")}

    assert first["20261015"]["share_change"] == 100.0
    assert first["20261015"]["net_inflow"] is None
    flow_source.rows[1]["nav"] = 1.1

    second = {row["date"]: row for row in factor.get_etf_fund_flow("159869.SZ")}
    assert second["20261015"]["net_inflow"] == 110.0


def test_weekend_baseline_and_zero_share_change(flow_source):
    flow_source.rows[:] = [
        {"trade_date": "20261009", "total_share": 1000.0, "nav": 1.0},
        {"trade_date": "20261012", "total_share": 1000.0, "nav": 1.1},
        {"trade_date": "20261013", "total_share": 1100.0, "nav": 1.2},
    ]
    flow_source.cached.pop("20261014")
    flow_source.cached["20261009"] = {"date": "20261009", "net_inflow": 25.0}

    rows = {row["date"]: row for row in factor.get_etf_fund_flow("159869.SZ")}

    assert rows["20261012"]["net_inflow"] == 0.0
    assert rows["20261013"]["net_inflow"] == 120.0


def test_source_failure_is_not_hidden_by_warm_cache(flow_source, monkeypatch):
    def fail(*_):
        raise RuntimeError("upstream unavailable")

    monkeypatch.setattr(factor.ts, "get_etf_fund_flow", fail)

    with pytest.raises(RuntimeError, match="upstream unavailable"):
        factor.get_etf_fund_flow("159869.SZ")


@pytest.mark.parametrize("requested_date", ["20261015", "20261019"])
def test_explicit_missing_date_does_not_use_another_day(
    flow_source, monkeypatch, requested_date,
):
    flow_source.rows[1]["nav"] = None
    monkeypatch.setattr(factor_service.listing_data, "ensure_data", lambda: None)
    monkeypatch.setattr(
        factor_service.listing_data, "resolve_symbols",
        lambda **_: [("159869.SZ", "动漫游戏ETF", "fund")],
    )

    result = factor_service.get_etf_fund_flow("159869.SZ", requested_date)

    assert result.as_of == requested_date
    assert result.fund_flow.net_inflow is None
    assert result.fund_flow.recent_5d_inflow_series is None

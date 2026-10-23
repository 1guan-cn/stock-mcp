"""ETF valuation must come from its actual tracked index, even if a wrong index has data."""

from unittest.mock import Mock

import pytest

from stock_service.data import factor as factor_data
from stock_service.models import AssetType, UnsupportedReason
from stock_service.services import factor as factor_service


@pytest.fixture(scope="session", autouse=True)
def _db_pool():
    """Routing tests replace the history boundary and do not use the integration DB."""
    yield


# Manager/exchange references, independent of the production map:
# https://www.nffund.com/main/nffund/personal-financing/detail.shtml?fundCode=512100
# https://accountquery.chinaamc.com/product/getPortfolioCompositionFile.do?fundcode=159845
# https://www.chinaamc.com/product/getPortfolioCompositionFile.do?fundcode=159869
# https://m.chinaamc.com/activity/588000/index.html
# https://www.nffund.com/new/personal-financing/detail.html?fundCode=512200
# https://www.ciis.com.hk/hongkong/tc/news/etfs/index.shtml
# https://pujiang.sse.com.cn/ListedCompany/learning/learning2/c/5728926.pdf
TRACKING_CASES = [
    ("512100.SH", "000852.SH"),
    ("159845.SZ", "000852.SH"),
    ("588000.SH", "000688.SH"),
    ("159869.SZ", "930901.CSI"),
    ("512200.SH", "931775.CSI"),
    ("512880.SH", "399975.SZ"),
    ("515790.SH", "931151.CSI"),
    ("512660.SH", "399967.SZ"),
    ("510300.SH", "000300.SH"),
]


@pytest.fixture
def resolve_listing(monkeypatch):
    monkeypatch.setattr(factor_service.listing_data, "ensure_data", lambda: None)
    monkeypatch.setattr(
        factor_service.listing_data, "resolve_symbols",
        lambda *, code: [(code, "requested ETF", AssetType.FUND)],
    )


@pytest.mark.parametrize("etf,index", TRACKING_CASES)
def test_percentile_uses_exact_tracking_index(monkeypatch, resolve_listing, etf, index):
    correct_history = [
        {"date": "20261021", "pe_ttm": 20.0, "pb": 2.0},
        {"date": "20261022", "pe_ttm": 30.0, "pb": 3.0},
    ]
    # A valid-looking wrong-index response must not pass merely because it has PE/PB.
    wrong_history = [{"date": "20261022", "pe_ttm": 999.0, "pb": 99.0}]
    history = Mock(side_effect=lambda symbol: correct_history if symbol == index else wrong_history)
    monkeypatch.setattr(factor_data, "get_index_valuation_history", history)

    result = factor_service.get_index_valuation_percentile(etf)

    history.assert_called_once_with(index)
    assert result.symbol == etf
    assert result.asset_type == AssetType.FUND
    assert result.pe_ttm == 30.0
    assert result.pb == 3.0
    assert result.pe_percentile_3y == 50.0
    assert result.pb_percentile_3y == 50.0
    assert result.as_of == "20261022"
    assert result.unsupported_reason is None


@pytest.mark.parametrize("etf,index", TRACKING_CASES)
def test_general_valuation_uses_same_tracking_index(monkeypatch, etf, index):
    history = Mock(return_value=[{"date": "20261022", "pe_ttm": 30.0, "pb": 3.0}])
    monkeypatch.setattr(factor_data, "get_index_valuation_history", history)

    result = factor_data.get_valuation(etf, AssetType.FUND)

    history.assert_called_once_with(index)
    assert result.pe_ttm == 30.0
    assert result.pb == 3.0
    assert result.data_as_of == "20261022"


@pytest.mark.parametrize("etf", ["560800.SH", "513100.SH"])
def test_unmapped_or_unsupported_etf_does_not_query_another_index(
    monkeypatch, resolve_listing, etf,
):
    history = Mock()
    monkeypatch.setattr(factor_data, "get_index_valuation_history", history)

    result = factor_service.get_index_valuation_percentile(etf)

    history.assert_not_called()
    assert result.unsupported_reason == UnsupportedReason.INDEX_NOT_IN_REGISTRY
    assert result.pe_ttm is None
    assert result.pb is None


def test_correct_theme_mapping_does_not_fabricate_valuation(monkeypatch, resolve_listing):
    history = Mock(return_value=[])
    monkeypatch.setattr(factor_data, "get_index_valuation_history", history)

    result = factor_service.get_index_valuation_percentile("159869.SZ")

    history.assert_called_once_with("930901.CSI")
    assert result.unsupported_reason == UnsupportedReason.INDEX_DATA_UNAVAILABLE
    assert result.pe_ttm is None
    assert result.pb is None

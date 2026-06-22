"""MCP tool source-label contract tests."""

import inspect

import stock_service.mcp_server as mcp_server


TUSHARE_TOOLS = (
    "get_daily_quote",
    "search_stock",
    "list_stocks",
    "list_funds",
    "list_indexes",
    "get_financial",
    "get_technical",
    "get_technical_batch",
    "get_percentile_batch",
    "get_percentile",
    "etf_fund_flow",
    "etf_main_force_flow",
    "index_valuation_percentile",
    "get_margin_detail",
    "check_trade_date",
)

NON_TUSHARE_TOOLS = (
    "get_factor",
    "get_factor_batch",
    "commodity_price_percentile",
    "get_realtime_quote",
    "get_reverse_repo_rate",
    "search_news",
)


def _doc(name: str) -> str:
    return inspect.getdoc(getattr(mcp_server, name)) or ""


def test_tushare_tools_have_source_label() -> None:
    for name in TUSHARE_TOOLS:
        assert "[tushare]" in _doc(name), f"{name} missing [tushare] source label"


def test_non_tushare_tools_do_not_have_tushare_label() -> None:
    for name in NON_TUSHARE_TOOLS:
        assert "[tushare]" not in _doc(name), f"{name} must not carry [tushare] source label"


def test_realtime_quote_documents_non_tushare_sources() -> None:
    doc = _doc("get_realtime_quote")
    assert "腾讯财经" in doc
    assert "东方财富" in doc

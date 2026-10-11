"""get_financial 契约测试。"""

import json

from stock_service.mcp_server import get_financial


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    for key in ("symbol", "report_type", "total", "records"):
        assert key in data, f"缺少字段: {key}"
    return data


def test_income():
    """利润表查询。"""
    data = _parse(get_financial("000001.SZ", "income", "20240101", "20241231"))
    assert data["report_type"] == "income"
    assert data["total"] > 0
    record = data["records"][0]
    assert "period" in record
    assert "data" in record


def test_balance_sheet():
    """资产负债表查询。"""
    data = _parse(get_financial("000001.SZ", "balance_sheet", "20240101", "20241231"))
    assert data["report_type"] == "balance_sheet"
    assert data["total"] > 0


def test_cashflow():
    """现金流量表查询。"""
    data = _parse(get_financial("000001.SZ", "cashflow", "20240101", "20241231"))
    assert data["report_type"] == "cashflow"
    assert data["total"] > 0


def test_indicator():
    """财务指标查询。"""
    data = _parse(get_financial("000001.SZ", "indicator", "20240101", "20241231"))
    assert data["report_type"] == "indicator"
    assert data["total"] > 0


def test_dividend():
    """分红送股查询。"""
    data = _parse(get_financial("000001.SZ", "dividend", "20230101", "20241231"))
    assert data["report_type"] == "dividend"
    assert data["total"] >= 0


def test_main_business_keeps_multi_row_period():
    """main_business 同一报告期有多行（按 bz_item 区分），不能被主键压成一行。"""
    data = _parse(get_financial("002311.SZ", "main_business", "20260101", "20260630"))
    assert data["total"] > 1, "同一报告期只返回一行 = 主键把分产品/分行业数据吃掉了"
    items = [r["data"].get("bz_item") for r in data["records"]]
    assert len(items) == len(set(items)), f"bz_item 出现重复: {items}"
    # 分产品 + 分行业 + 分地区三类都应取得到
    assert any("饲料" in str(i) for i in items), f"取不到分产品行: {items}"
    assert any("地区" in str(i) for i in items), f"取不到分地区行: {items}"


def test_dividend_keeps_all_procedures():
    """dividend 同一报告期的不同 div_proc（预案/实施…）都要保留。"""
    data = _parse(get_financial("600941.SH", "dividend", "20250101", "20260630"))
    procs = {r["period"]: set() for r in data["records"]}
    for r in data["records"]:
        procs[r["period"]].add(r["data"].get("div_proc"))
    assert any(len(v) > 1 for v in procs.values()), f"同一报告期只剩一个 div_proc: {procs}"

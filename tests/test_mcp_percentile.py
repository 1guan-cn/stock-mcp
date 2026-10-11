"""get_percentile / get_percentile_batch 契约测试。"""

import json

from stock_service.mcp_server import get_percentile, get_percentile_batch


def _parse(raw: str) -> dict:
    data = json.loads(raw)
    assert "items" in data
    assert "total" in data
    return data


def test_stock_percentile():
    """股票百分位，应包含 PE/PB 百分位。"""
    data = _parse(get_percentile("000001.SZ"))
    assert data["total"] == 1
    item = data["items"][0]
    assert item["asset_type"] == "stock"
    assert len(item["percentiles"]) == 4  # 6m, 1y, 2y, 3y
    p = item["percentiles"][0]
    for key in ("period", "price_percentile", "current_price", "days"):
        assert key in p, f"缺少字段: {key}"
    # 估值分位与价格分位必须字段名可区分，且都在同一层
    for key in ("pe_ttm_percentile", "current_pe_ttm", "pb_percentile", "current_pb"):
        assert key in p, f"缺少字段: {key}"
    assert "percentile" not in p, "旧字段 percentile 应已改名为 price_percentile"


def test_etf_percentile():
    """ETF 百分位。"""
    data = _parse(get_percentile("510300.SH"))
    assert data["total"] == 1
    assert data["items"][0]["asset_type"] == "fund"
    assert len(data["items"][0]["percentiles"]) == 4


def test_index_percentile():
    """指数百分位。"""
    data = _parse(get_percentile("000001.SH"))
    assert data["total"] == 1
    assert data["items"][0]["asset_type"] == "index"


def test_batch():
    """批量查询百分位。"""
    codes = ["510300.SH", "510500.SH"]
    data = _parse(get_percentile_batch(codes))
    assert data["total"] == len(codes)
    symbols = [item["symbol"] for item in data["items"]]
    for code in codes:
        assert code in symbols


def test_batch_does_not_silently_drop_unknown_code():
    """不存在的代码必须占位返回并说明原因，total 反映请求数。"""
    codes = ["510300.SH", "512400.SZ"]
    data = _parse(get_percentile_batch(codes))
    assert data["total"] == len(codes)
    bad = next(i for i in data["items"] if i["symbol"] == "512400.SZ")
    assert bad["unsupported_reason"] == "symbol_not_found"


def test_percentile_as_of_present():
    """as_of 字段在序列非空时暴露序列最末日（YYYYMMDD），并带 stale_days。"""
    data = _parse(get_percentile("000001.SH"))
    item = data["items"][0]
    # 主流指数必有数据 → as_of 应有值且为 8 位日期
    assert "as_of" in item
    assert item["as_of"] is not None
    assert len(item["as_of"]) == 8
    assert item["as_of"].isdigit()
    # 最新交易日的数据不应被判为陈旧（允许跨周末，取 5 天容差）
    assert item["stale_days"] <= 5, f"主流指数 stale_days 异常: {item['stale_days']}"

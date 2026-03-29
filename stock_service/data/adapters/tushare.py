"""Tushare 数据源适配器 — 原始数据翻译为标准模型。"""

import math
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import tushare as ts

from stock_service.config import settings
from stock_service.models.listing import FundInfo, IndexInfo, StockInfo
from stock_service.models.quote import DailyBar


def _clean_nan(records: list[dict]) -> list[dict]:
    for row in records:
        for k, v in row.items():
            if isinstance(v, float) and math.isnan(v):
                row[k] = None
    return records


@lru_cache(maxsize=1)
def _api():
    return ts.pro_api(settings.tushare_token)


# ── 股票日线 ──

def get_stock_daily(symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_daily = executor.submit(
            _api().daily, ts_code=symbol, start_date=start_date, end_date=end_date
        )
        f_basic = executor.submit(
            _api().daily_basic, ts_code=symbol, start_date=start_date, end_date=end_date
        )
        df_daily = f_daily.result()
        df_basic = f_basic.result()

    if df_daily is None or df_daily.empty:
        return []

    basic_map: dict[str, dict] = {}
    if df_basic is not None and not df_basic.empty:
        basic_map = {
            row["trade_date"]: row for row in df_basic.to_dict("records")
        }

    bars: list[DailyBar] = []
    for row in df_daily.to_dict("records"):
        trade_date = row["trade_date"]
        basic = basic_map.get(trade_date, {})
        bars.append(
            DailyBar(
                date=trade_date,
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                pre_close=row.get("pre_close"),
                change=row.get("change"),
                pct_chg=row.get("pct_chg"),
                volume=row["vol"],
                amount=row.get("amount"),
                turnover_rate=basic.get("turnover_rate"),
                turnover_rate_f=basic.get("turnover_rate_f"),
                volume_ratio=basic.get("volume_ratio"),
                pe=basic.get("pe"),
                pe_ttm=basic.get("pe_ttm"),
                pb=basic.get("pb"),
                ps=basic.get("ps"),
                ps_ttm=basic.get("ps_ttm"),
                dv_ratio=basic.get("dv_ratio"),
                dv_ttm=basic.get("dv_ttm"),
                total_share=basic.get("total_share"),
                float_share=basic.get("float_share"),
                free_share=basic.get("free_share"),
                total_mv=basic.get("total_mv"),
                circ_mv=basic.get("circ_mv"),
            )
        )
    bars.sort(key=lambda b: b.date)
    return bars


# ── 基金日线 ──

def get_fund_daily(symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
    df = _api().fund_daily(ts_code=symbol, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    bars = [
        DailyBar(
            date=row["trade_date"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            pre_close=row.get("pre_close"),
            change=row.get("change"),
            pct_chg=row.get("pct_chg"),
            volume=row["vol"],
            amount=row.get("amount"),
        )
        for row in df.to_dict("records")
    ]
    bars.sort(key=lambda b: b.date)
    return bars


# ── 指数日线 ──

def get_index_daily(symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
    df = _api().index_daily(ts_code=symbol, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    bars = [
        DailyBar(
            date=row["trade_date"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            pre_close=row.get("pre_close"),
            change=row.get("change"),
            pct_chg=row.get("pct_chg"),
            volume=row["vol"],
            amount=row.get("amount"),
        )
        for row in df.to_dict("records")
    ]
    bars.sort(key=lambda b: b.date)
    return bars


# ── 复权因子 ──

def get_adj_factor(
    symbol: str, start_date: str, end_date: str, *, is_fund: bool = False
) -> dict[str, float]:
    if is_fund:
        df = _api().fund_adj(ts_code=symbol, start_date=start_date, end_date=end_date)
    else:
        df = _api().adj_factor(ts_code=symbol, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return {}
    return {row["trade_date"]: row["adj_factor"] for row in df.to_dict("records")}


# ── 指数估值 ──

def get_index_valuation(symbol: str, start_date: str, end_date: str) -> list[dict]:
    df = _api().index_dailybasic(
        ts_code=symbol,
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,pe,pe_ttm,pb,turnover_rate,total_mv,float_mv",
    )
    if df is None or df.empty:
        return []
    return _clean_nan(df.to_dict("records"))


# ── 列表 ──

def get_stock_list() -> list[StockInfo]:
    df = _api().stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,name,area,industry,market,list_date",
    )
    if df is None or df.empty:
        return []
    return [
        StockInfo(
            symbol=row["ts_code"],
            name=row["name"],
            area=row.get("area"),
            industry=row.get("industry"),
            market=row.get("market"),
            list_date=row.get("list_date"),
        )
        for row in _clean_nan(df.to_dict("records"))
    ]


def get_fund_list() -> list[FundInfo]:
    df = _api().fund_basic(
        market="E",
        status="L",
        fields="ts_code,name,fund_type,management,list_date",
    )
    if df is None or df.empty:
        return []
    return [
        FundInfo(
            symbol=row["ts_code"],
            name=row["name"],
            fund_type=row.get("fund_type"),
            management=row.get("management"),
            list_date=row.get("list_date"),
        )
        for row in _clean_nan(df.to_dict("records"))
    ]


def get_index_list() -> list[IndexInfo]:
    frames = []
    for market in ("SSE", "SZSE"):
        df = _api().index_basic(market=market)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return []
    import pandas as pd
    combined = pd.concat(frames, ignore_index=True)
    return [
        IndexInfo(
            symbol=row["ts_code"],
            name=row["name"],
            fullname=row.get("fullname"),
            market=row.get("market"),
            publisher=row.get("publisher"),
            category=row.get("category"),
            list_date=row.get("list_date"),
        )
        for row in _clean_nan(combined.to_dict("records"))
    ]


# ── 财务数据 ──

_FINANCE_API_MAP = {
    "income": "income",
    "balance_sheet": "balancesheet",
    "cashflow": "cashflow",
    "forecast": "forecast",
    "express": "express",
    "dividend": "dividend",
    "indicator": "fina_indicator",
    "audit": "fina_audit",
    "main_business": "fina_mainbz",
    "disclosure": "disclosure_date",
}


# ── 融资融券 ──

def get_margin(symbol: str, start_date: str, end_date: str) -> list[dict]:
    df = _api().margin_detail(ts_code=symbol, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    records = [
        {
            "date": row["trade_date"],
            "rzye": row.get("rzye"),
            "rzmre": row.get("rzmre"),
            "rzche": row.get("rzche"),
            "rqye": row.get("rqye"),
            "rqmcl": row.get("rqmcl"),
            "rqchl": row.get("rqchl"),
            "rqyl": row.get("rqyl"),
        }
        for row in _clean_nan(df.to_dict("records"))
    ]
    records.sort(key=lambda r: r["date"])
    return records


# ── 财务数据 ──

def get_financial(symbol: str, report_type: str, start_date: str, end_date: str) -> list[dict]:
    api_name = _FINANCE_API_MAP[report_type]
    api_func = getattr(_api(), api_name)

    if report_type == "dividend":
        df = api_func(ts_code=symbol)
    elif report_type == "disclosure":
        df = api_func(ts_code=symbol, end_date=end_date)
    else:
        df = api_func(ts_code=symbol, start_date=start_date, end_date=end_date)

    if df is None or df.empty:
        return []

    if "ts_code" in df.columns:
        df = df.drop(columns=["ts_code"])
    return _clean_nan(df.to_dict("records"))

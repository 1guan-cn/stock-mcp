"""AKShare 数据源适配器 — 原始中文字段翻译为标准格式。"""

import logging
import math

import requests

logger = logging.getLogger(__name__)


def _nan_to_none(val: object) -> object:
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def _eastmoney_market_code(code: str, asset_type: str = "stock") -> int | str:
    """东方财富 market code: 沪市=1, 深市/北交=0, 港股=116, 美股=105, 全球指数=100。"""
    if asset_type == "hk":
        return 116
    if asset_type == "us":
        return 105
    if asset_type == "global_index":
        return 100
    # 沪市: 6xx(股票), 5xx(ETF)
    if code.startswith(("6", "5")):
        return 1
    # 指数: 000xxx → 上证(1), 399xxx → 深证(0)
    if asset_type == "index":
        return 0 if code.startswith("399") else 1
    # 深市股票: 0xx/3xx
    return 0


def _safe_num(val: object) -> float | None:
    """将东方财富 API 返回值转为 float，无效值（'-'、None）返回 None。"""
    if val is None or val == "-":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _tencent_market_prefix(code: str, asset_type: str = "stock") -> str:
    """腾讯财经 market prefix: 沪市=sh, 深市=sz, 港股=hk, 美股=us。"""
    if asset_type == "hk":
        return "hk"
    if asset_type == "us":
        return "us"
    if asset_type == "global_index":
        # 腾讯不支持全球指数，返回空让调用方 fallback
        return ""
    if code.startswith(("6", "5")):
        return "sh"
    if asset_type == "index":
        return "sh" if code.startswith(("000", "9")) else "sz"
    return "sz"


def _fetch_bid_ask_tencent(code: str, asset_type: str = "stock") -> dict | None:
    """调用腾讯财经行情 API，返回与东方财富相同格式的标准化 dict。

    腾讯接口返回固定位置的逗号分隔字符串，字段含义参考:
    https://blog.csdn.net/luanpeng825485697/article/details/78442062
    主要字段位置 (0-based):
      1:名称 3:现价 4:昨收 5:今开 6:成交量(手) 7:外盘 8:内盘
      9-28:五档买卖盘  30:涨跌 31:涨幅% 33:最高 34:最低
      36:成交额(万) 37:成交量(手) 38:换手率%
    """
    prefix = _tencent_market_prefix(code, asset_type)
    if not prefix:
        return None
    symbol = f"{prefix}{code}"
    url = f"http://qt.gtimg.cn/q={symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.qq.com/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        text = r.text
        # 解析 v_xxNNNNNN="..."
        start = text.find('"')
        end = text.rfind('"')
        if start == -1 or end <= start:
            return None
        fields = text[start + 1 : end].split("~")
        if len(fields) < 50 or not fields[3]:
            return None

        def _p(idx: int) -> float | None:
            try:
                v = float(fields[idx])
                return v if v != 0 else None
            except (ValueError, IndexError):
                return None

        return {
            # 腾讯: [9-18] 卖五→卖一(价格递减), [19-28] 买一→买五
            "sell_5": _p(9), "sell_5_vol": _p(10),
            "sell_4": _p(11), "sell_4_vol": _p(12),
            "sell_3": _p(13), "sell_3_vol": _p(14),
            "sell_2": _p(15), "sell_2_vol": _p(16),
            "sell_1": _p(17), "sell_1_vol": _p(18),
            "buy_1": _p(19), "buy_1_vol": _p(20),
            "buy_2": _p(21), "buy_2_vol": _p(22),
            "buy_3": _p(23), "buy_3_vol": _p(24),
            "buy_4": _p(25), "buy_4_vol": _p(26),
            "buy_5": _p(27), "buy_5_vol": _p(28),
            "最新": _p(3),
            "最高": _p(33),
            "最低": _p(34),
            "今开": _p(5),
            "总手": _p(36),
            "金额": _p(37) * 10000 if _p(37) is not None else None,
            "量比": _p(49),
            "昨收": _p(4),
            "换手": _p(38),
            "涨跌": _p(31),
            "涨幅": _p(32),
            "name": fields[1] if len(fields) > 1 else None,
        }
    except Exception as e:
        logger.warning("_fetch_bid_ask_tencent(%s) failed: %s", code, e)
        return None


def _fetch_bid_ask_em(code: str, asset_type: str = "stock") -> dict | None:
    """直接调用东方财富行情 API，返回标准化 dict。"""
    market_code = _eastmoney_market_code(code, asset_type)
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    fields = (
        "f43,f44,f45,f46,f47,f48,f50,f57,f58,f60,f168,f169,f170,"
        "f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,"
        "f31,f32,f33,f34,f35,f36,f37,f38,f39,f40"
    )
    params = {"fltt": "2", "invt": "2", "fields": fields, "secid": f"{market_code}.{code}"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        raw = r.json().get("data")
        if not raw:
            return None

        def _vol(key: str) -> float | None:
            v = _safe_num(raw.get(key))
            return v * 100 if v is not None else None

        return {
            "sell_5": _safe_num(raw.get("f31")), "sell_5_vol": _vol("f32"),
            "sell_4": _safe_num(raw.get("f33")), "sell_4_vol": _vol("f34"),
            "sell_3": _safe_num(raw.get("f35")), "sell_3_vol": _vol("f36"),
            "sell_2": _safe_num(raw.get("f37")), "sell_2_vol": _vol("f38"),
            "sell_1": _safe_num(raw.get("f39")), "sell_1_vol": _vol("f40"),
            "buy_1": _safe_num(raw.get("f19")), "buy_1_vol": _vol("f20"),
            "buy_2": _safe_num(raw.get("f17")), "buy_2_vol": _vol("f18"),
            "buy_3": _safe_num(raw.get("f15")), "buy_3_vol": _vol("f16"),
            "buy_4": _safe_num(raw.get("f13")), "buy_4_vol": _vol("f14"),
            "buy_5": _safe_num(raw.get("f11")), "buy_5_vol": _vol("f12"),
            "最新": _safe_num(raw.get("f43")),
            "最高": _safe_num(raw.get("f44")),
            "最低": _safe_num(raw.get("f45")),
            "今开": _safe_num(raw.get("f46")),
            "总手": _safe_num(raw.get("f47")),
            "金额": _safe_num(raw.get("f48")),
            "量比": _safe_num(raw.get("f50")),
            "昨收": _safe_num(raw.get("f60")),
            "换手": _safe_num(raw.get("f168")),
            "涨跌": _safe_num(raw.get("f169")),
            "涨幅": _safe_num(raw.get("f170")),
            "name": raw.get("f58"),
        }
    except Exception as e:
        logger.warning("_fetch_bid_ask_em(%s) failed: %s", code, e)
        return None


def _normalize_date(date_val: object) -> str | None:
    if date_val is None:
        return None
    if hasattr(date_val, "strftime"):
        return date_val.strftime("%Y%m%d")
    return str(date_val).replace("-", "")


def get_etf_fund_flow(code: str) -> list[dict]:
    """获取ETF份额变动历史（申购赎回代理数据）。

    使用 fund_etf_fund_flow_hist，返回每日份额变动和规模变动。
    净流入 = 份额变动（万份）× 当日净值（元），为近似值。

    Args:
        code: 纯数字代码，如 "510300"
    """
    try:
        import akshare as ak

        df = ak.fund_etf_fund_flow_hist(symbol=code)
        if df is None or df.empty:
            return []

        normalized = []
        for r in df.to_dict("records"):
            date_str = _normalize_date(r.get("日期"))
            if date_str is None:
                continue

            # 份额变动：亿份 → 万份
            share_chg_yi = _nan_to_none(r.get("基金份额增减"))
            share_change = round(float(share_chg_yi) * 10000, 2) if share_chg_yi is not None else None

            # 规模变动：亿元
            scale_chg = _nan_to_none(r.get("基金规模增减"))
            scale_change = round(float(scale_chg), 4) if scale_chg is not None else None

            # 当日净值
            nav = _nan_to_none(r.get("净值"))

            # 净流入估算：份额变动（万份）× 净值（元）= 万元
            net_inflow = None
            if share_change is not None and nav is not None:
                net_inflow = round(share_change * float(nav), 2)

            if share_change is None and net_inflow is None:
                continue

            normalized.append({
                "date": date_str,
                "share_change": share_change,
                "scale_change": scale_change,
                "net_inflow": net_inflow,
                "source": "akshare:fund_etf_fund_flow_hist",
            })
        return normalized
    except Exception as e:
        logger.warning("AKShare get_etf_fund_flow(%s) failed: %s", code, e)
        return []


# 商品代码 → (akshare参数, 数据源类型)
_COMMODITY_MAP: dict[str, tuple[str, str]] = {
    "AU": ("Au99.99", "sge"),    # 黄金（上海黄金交易所现货）
    "AG": ("Ag99.99", "sge"),    # 白银（上海黄金交易所现货）
    "CU": ("CU0", "futures"),    # 铜（上期所主力合约）
    "AL": ("AL0", "futures"),    # 铝（上期所主力合约）
    "SC": ("SC0", "futures"),    # 原油（上海国际能源中心主力合约）
}


def get_commodity_price(commodity_code: str) -> list[dict]:
    """获取商品历史价格，返回 [{date, price}, ...] 列表。

    Args:
        commodity_code: 商品代码，如 "AU"（黄金）、"CU"（铜）、"SC"（原油）
    """
    try:
        import akshare as ak

        info = _COMMODITY_MAP.get(commodity_code.upper())
        if not info:
            logger.warning("get_commodity_price: unsupported code %s", commodity_code)
            return []
        symbol, source_type = info

        if source_type == "sge":
            df = ak.spot_hist_sge(symbol=symbol)
            price_col = "close"
            date_col = "date"
        else:
            df = ak.futures_main_sina(symbol=symbol)
            price_col = "收盘价"
            date_col = "日期"

        if df is None or df.empty:
            return []

        normalized = []
        for r in df.to_dict("records"):
            date_str = _normalize_date(r.get(date_col))
            if not date_str:
                continue
            price = _nan_to_none(r.get(price_col))
            if price is None:
                continue
            try:
                normalized.append({"date": date_str, "price": round(float(price), 4)})
            except (TypeError, ValueError):
                continue

        return sorted(normalized, key=lambda x: x["date"])
    except Exception as e:
        logger.warning("AKShare get_commodity_price(%s) failed: %s", commodity_code, e)
        return []


def get_realtime_quote(code: str, asset_type: str) -> dict | None:
    """获取单只股票/ETF/指数的实时行情 + 五档盘口。

    数据源优先级: 腾讯财经 → 东方财富（fallback）。
    腾讯不支持全球指数，此时直接走东方财富。

    Args:
        code: 纯数字代码，如 "000001"；美股为 ticker 如 "AAPL"
        asset_type: "stock" / "fund" / "index" / "hk" / "us" / "global_index"
    """
    data = _fetch_bid_ask_tencent(code, asset_type)
    if data is None:
        logger.debug("腾讯源失败，fallback 到东方财富: %s", code)
        data = _fetch_bid_ask_em(code, asset_type)
    if data is None:
        return None

    asks = []
    bids = []
    for i in range(1, 6):
        sell_price = data.get(f"sell_{i}")
        sell_vol = data.get(f"sell_{i}_vol")
        if sell_price is not None and sell_vol is not None:
            asks.append({"price": sell_price, "volume": sell_vol})
        buy_price = data.get(f"buy_{i}")
        buy_vol = data.get(f"buy_{i}_vol")
        if buy_price is not None and buy_vol is not None:
            bids.append({"price": buy_price, "volume": buy_vol})

    return {
        "current_price": data.get("最新"),
        "pct_chg": data.get("涨幅"),
        "change": data.get("涨跌"),
        "open": data.get("今开"),
        "high": data.get("最高"),
        "low": data.get("最低"),
        "pre_close": data.get("昨收"),
        "volume": data.get("总手"),
        "amount": data.get("金额"),
        "turnover_rate": data.get("换手"),
        "volume_ratio": data.get("量比"),
        "asks": asks if asks else None,
        "bids": bids if bids else None,
    }


def get_northbound_daily() -> list[dict]:
    """获取北向资金历史，返回标准化 dict 列表。"""
    try:
        import akshare as ak

        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        if df is None or df.empty:
            return []

        normalized = []
        for r in df.to_dict("records"):
            date_str = _normalize_date(r.get("日期"))
            if date_str is None:
                continue
            net_buy = _nan_to_none(r.get("当日成交净买额"))
            if net_buy is None:
                continue
            try:
                net_buy = round(float(net_buy), 2)
            except (TypeError, ValueError):
                continue
            normalized.append({
                "date": date_str,
                "north_net_buy": net_buy,
            })
        return normalized
    except Exception as e:
        logger.warning("AKShare get_northbound_daily failed: %s", e)
        return []

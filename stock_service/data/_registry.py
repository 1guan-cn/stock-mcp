"""配置数据注册表。

ETF → 跟踪指数映射。

注意 tushare `index_dailybasic` 当前账户套餐覆盖范围有限：
- ✅ 主流宽基指数（000300 沪深300 / 000905 中证500 / 000016 上证50 / 399006 创业板指）返 PE/PB
- ❌ 中证主题/行业指数（93xxxx / H3xxxx / 部分 000xxx 主题）`index_dailybasic` 返空

→ 即使本表为某 ETF 配了"看似正确"的中证主题指数代码，下游 `get_index_valuation_percentile`
仍会因 tushare 不返数据而 unsupported_reason=index_data_unavailable。仅宽基 ETF 的映射真正生效。
"""

# ETF → 跟踪指数映射（None 表示不支持）
ETF_INDEX_MAP: dict[str, str | None] = {
    # ✅ 实测可用（tushare index_dailybasic 返 PE/PB 数据）
    "510300.SH": "000300.SH",   # 沪深300ETF → 沪深300
    "510500.SH": "000905.SH",   # 中证500ETF → 中证500
    "510050.SH": "000016.SH",   # 上证50ETF → 上证50
    "159915.SZ": "399006.SZ",   # 创业板ETF → 创业板指
    "159919.SZ": "000300.SH",   # 嘉实沪深300ETF → 沪深300
    "512100.SH": "399673.SZ",   # 1000ETF → 中证1000（待实测确认 tushare 是否返 399673.SZ PE/PB）

    # ⚠ 中证主题/行业指数：tushare 当前账户实测返空（2026-05-04 验证）
    # 保留映射但下游会报 index_data_unavailable，等 stock-mcp 接第二数据源后即可启用
    # 错配核对方法：基金招募说明书 / 基金公司官网 / fund-log shared-fund-etf-index-mapping.md SSOT
    "512010.SH": "000913.SH",   # 医药ETF易方达 → 沪深300医药（原配 000819.SH 实为有色金属，错配修正 2026-05-04）
    "512690.SH": "399987.SZ",   # 酒ETF鹏华 → 中证酒（原配 000932.SH 实为 800消费，错配修正 2026-05-04）
    "512200.SH": "399987.SZ",   # 白酒ETF → 中证酒（同 512690 修正路径，原配 000932.SH 错配）
    "512880.SH": "399986.SZ",   # 证券ETF — 实测 name="中证银行" 与注释"中证全指证券"不符，待招募说明书核实后修正
    "515790.SH": "931009.CSI",  # 光伏ETF → 中证光伏
    "512660.SH": "930997.CSI",  # 军工ETF → 中证军工
    "159845.SZ": "930997.CSI",  # 国防军工ETF → 中证军工
    "515030.SH": "399808.SZ",   # 新能源ETF → 中证新能源
    "159869.SZ": "930997.CSI",  # 军工龙头ETF → 中证军工

    # ❌ 海外指数 / 不支持
    "513100.SH": None,          # 纳指ETF → 不支持
    "159941.SZ": None,          # 纳指ETF → 不支持
}


def resolve_etf_to_index(symbol: str) -> str | None:
    """ETF symbol → 跟踪指数 symbol，不在映射表中返回 None。"""
    return ETF_INDEX_MAP.get(symbol)

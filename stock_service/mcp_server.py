import json

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from stock_service.database import init_pool
from stock_service.models import AdjustType, DetailLevel, FinanceReportType
from stock_service.services import factor as factor_service
from stock_service.services import finance as finance_service
from stock_service.services import listing as listing_service
from stock_service.services import margin as margin_service
from stock_service.services import news as news_service
from stock_service.services import quote as quote_service

mcp = FastMCP(
    "stock-service",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*", "1guan.cn", "www.1guan.cn"],
        allowed_origins=["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*", "https://1guan.cn", "https://www.1guan.cn"],
    ),
)


def _to_json(data, *, exclude_none: bool = True) -> str:
    if isinstance(data, list):
        return json.dumps([d.model_dump(mode="json", exclude_none=exclude_none) for d in data], ensure_ascii=False)
    return json.dumps(data.model_dump(mode="json", exclude_none=exclude_none), ensure_ascii=False)


@mcp.tool()
def get_daily_quote(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
    detail: str = "simple",
) -> str:
    """查询股票/基金/指数日线行情数据。

    Args:
        code: 证券代码（带交易所后缀），如 "000001.SZ"（平安银行）、"510300.SH"（沪深300ETF）、"000001.SH"（上证指数）、"399001.SZ"（深证成指）、"399006.SZ"（创业板指）
        start_date: 开始日期，YYYYMMDD 格式，如 "20240101"
        end_date: 结束日期，YYYYMMDD 格式，如 "20240131"
        adjust: 复权类型 - "qfq"(前复权，默认)、"hfq"(后复权)、"none"(不复权)。指数数据不受复权影响。
        detail: 详情级别 - "simple"(基础字段，默认)、"all"(全部字段)
    """
    result = quote_service.get_daily(
        start_date,
        end_date,
        AdjustType(adjust),
        DetailLevel(detail),
        code=code,
    )
    return _to_json(result)


@mcp.tool()
def search_stock(keyword: str) -> str:
    """搜索股票、基金或指数，按名称或代码模糊匹配。

    Args:
        keyword: 搜索关键词，如 "平安"、"银行"、"000001"、"沪深300"、"上证"、"创业板"
    """
    items = listing_service.search(keyword)
    return _to_json(items)


@mcp.tool()
def list_stocks(
    market: str | None = None,
    industry: str | None = None,
    area: str | None = None,
    keyword: str | None = None,
) -> str:
    """按条件筛选股票列表。所有参数均可选，不传则返回全部。

    Args:
        market: 市场类别，可选值："主板"、"创业板"、"科创板"、"CDR"、"北交所"
        industry: 所属行业，如 "银行"、"医药生物"、"电子"
        area: 地域，如 "深圳"、"北京"、"上海"
        keyword: 关键词搜索（名称或代码）
    """
    items = listing_service.get_stock_list(
        market=market, industry=industry, area=area, keyword=keyword
    )
    return _to_json(items)


@mcp.tool()
def list_funds(
    fund_type: str | None = None,
    management: str | None = None,
    keyword: str | None = None,
) -> str:
    """按条件筛选基金/ETF列表。所有参数均可选，不传则返回全部。

    Args:
        fund_type: 基金类型，如 "ETF"、"LOF"
        management: 管理人/基金公司名称
        keyword: 关键词搜索（名称或代码）
    """
    items = listing_service.get_fund_list(
        fund_type=fund_type, management=management, keyword=keyword
    )
    return _to_json(items)


@mcp.tool()
def list_indexes(
    keyword: str | None = None,
) -> str:
    """按条件筛选指数列表。参数可选，不传则返回全部。

    Args:
        keyword: 关键词搜索（名称或代码），如 "上证"、"沪深300"、"000001.SH"
    """
    items = listing_service.get_index_list(keyword=keyword)
    return _to_json(items)


@mcp.tool()
def get_financial(
    code: str,
    report_type: str,
    start_period: str,
    end_period: str,
) -> str:
    """查询上市公司财务报表数据。

    Args:
        code: 股票代码（带交易所后缀），如 "000001.SZ"（平安银行）
        report_type: 报表类型 - "income"(利润表)、"balance_sheet"(资产负债表)、"cashflow"(现金流量表)、"indicator"(财务指标)、"forecast"(业绩预告)、"express"(业绩快报)、"dividend"(分红送股)、"audit"(审计意见)、"main_business"(主营业务构成)、"disclosure"(财报披露计划)
        start_period: 起始报告期，YYYYMMDD 格式，如 "20230101"
        end_period: 截止报告期，YYYYMMDD 格式，如 "20231231"
    """
    result = finance_service.get_financial(
        code,
        FinanceReportType(report_type),
        start_period,
        end_period,
    )
    return _to_json(result, exclude_none=False)


@mcp.tool()
def get_technical(
    code: str,
    period: int = 60,
) -> str:
    """查询股票/基金/指数的技术指标（均线、量能、涨跌幅、高低点等）。

    返回 MA5/MA10/MA20/MA60、MA20偏离度、均线状态(bullish/bearish/tangled)、
    5日/20日量比、5日/20日涨跌幅、区间高低点及百分位、从高点回撤幅度、连续下跌天数。

    Args:
        code: 证券代码（带交易所后缀），如 "000001.SZ"（平安银行）、"510300.SH"（沪深300ETF）、"000001.SH"（上证指数）
        period: 计算区间天数，默认 60
    """
    result = quote_service.get_technical(code=code, period=period)
    return _to_json(result)


@mcp.tool()
def get_technical_batch(
    codes: list[str],
    period: int = 60,
) -> str:
    """批量查询多个股票/基金/指数的技术指标。

    一次性获取多个标的的 MA、量能、涨跌幅、高低点等技术指标，减少多次调用开销。

    Args:
        codes: 证券代码列表，如 ["000001.SH", "399001.SZ", "399006.SZ"]
        period: 计算区间天数，默认 60
    """
    result = quote_service.get_technical_batch(codes=codes, period=period)
    return _to_json(result)


@mcp.tool()
def get_percentile_batch(
    codes: list[str],
) -> str:
    """批量查询多个股票/基金/指数当前价格在历史区间中的百分位。

    一次性获取多个标的在 6个月/1年/2年/3年四个时间窗口的价格百分位（0-100），减少多次调用开销。
    股票还会额外返回 PE-TTM 百分位和 PB 百分位（过滤掉负值后计算），基金/指数不返回。

    Args:
        codes: 证券代码列表，如 ["510300.SH", "510500.SH", "512010.SH"]
    """
    result = quote_service.get_percentile_batch(codes=codes)
    return _to_json(result)


@mcp.tool()
def get_percentile(
    code: str,
) -> str:
    """查询股票/基金/指数当前价格在历史区间中的百分位。

    返回最近 6 个月、1 年、2 年、3 年四个时间窗口的价格百分位（0-100），
    百分位越低表示当前价格越接近历史低位。使用前复权价格计算。
    股票还会额外返回 PE-TTM 百分位和 PB 百分位（过滤掉负值后计算），基金/指数不返回。

    Args:
        code: 证券代码（带交易所后缀），如 "000001.SZ"（平安银行）、"510300.SH"（沪深300ETF）、"000001.SH"（上证指数）
    """
    result = quote_service.get_percentile(code=code)
    return _to_json(result)


@mcp.tool()
def get_factor(
    code: str,
    date: str | None = None,
) -> str:
    """查询股票/基金/指数的多因子数据（估值V + 资金流F + 北向资金I）。

    返回三类因子：
    - valuation: PE-TTM、PB、股息率（当前值，百分位请用 get_percentile）
    - fund_flow: 主力净流入（净额/占比）、超大单/大单净流入、近5日累计
    - northbound: 北向资金当日净买入、近5日累计

    ETF 估值通过跟踪指数映射获取。指数无资金流数据。

    Args:
        code: 证券代码（带交易所后缀），如 "510300.SH"（沪深300ETF）、"000001.SZ"（平安银行）、"000300.SH"（沪深300指数）
        date: 可选，指定日期 YYYYMMDD 格式。不传则使用最新数据
    """
    result = factor_service.get_factor(code, date)
    return _to_json(result)


@mcp.tool()
def get_factor_batch(
    codes: list[str],
    date: str | None = None,
) -> str:
    """批量查询多个股票/基金/指数的多因子数据（估值V + 资金流F + 北向资金I）。

    一次性获取多个标的的估值（当前值）、资金流、北向资金因子，减少多次调用开销。
    估值百分位请用 get_percentile_batch。

    Args:
        codes: 证券代码列表，如 ["510300.SH", "510500.SH", "000001.SZ"]
        date: 可选，指定日期 YYYYMMDD 格式。不传则使用最新数据
    """
    result = factor_service.get_factor_batch(codes, date)
    return _to_json(result)


@mcp.tool()
def etf_fund_flow(
    code: str,
    date: str | None = None,
) -> str:
    """查询ETF申购赎回资金流数据（F因子）。

    返回ETF净申购/赎回情况：
    - net_inflow: 净流入估算（万元）= 份额变动 × 当日净值
    - share_change: 份额变动（万份），正数为净申购，负数为净赎回
    - scale_change: 规模变动（亿元）
    - recent_5d_inflow: 近5日累计净流入（万元）
    - source: 数据口径来源说明

    注意：净申购/净赎回分开数据暂不支持，net_inflow 为份额变动代理估算值。
    数据来源：AKShare fund_etf_fund_flow_hist。

    Args:
        code: ETF代码（带交易所后缀），如 "510300.SH"（沪深300ETF）、"159915.SZ"（创业板ETF）
        date: 可选，指定日期 YYYYMMDD 格式。不传则返回最新一日数据
    """
    result = factor_service.get_etf_fund_flow(code, date)
    return _to_json(result)


@mcp.tool()
def etf_main_force_flow(
    code: str,
    date: str | None = None,
) -> str:
    """查询ETF/股票主力资金结构数据（I因子）。

    返回主力资金构成：
    - large_order_net: 大单净流入（万元）
    - super_large_net: 超大单净流入（万元）
    - main_force_net: 主力净流入 = 大单 + 超大单（万元）
    - main_force_ratio: 主力净流入占当日成交额比例

    数据来源：AKShare stock_individual_fund_flow（东方财富）。
    指数无此数据。

    Args:
        code: 证券代码（带交易所后缀），如 "510300.SH"（沪深300ETF）、"000001.SZ"（平安银行）
        date: 可选，指定日期 YYYYMMDD 格式。不传则返回最新一日数据
    """
    result = factor_service.get_etf_main_force_flow(code, date)
    return _to_json(result)


@mcp.tool()
def index_valuation_percentile(
    code: str,
) -> str:
    """查询指数/ETF的PE-TTM和PB在3年历史中的分位（V因子）。

    返回真实估值分位（相对于3年历史），精度优于价格代理分位：
    - pe_ttm: 当前PE-TTM
    - pe_percentile_3y: PE在3年历史中的百分位（0-100，越低越便宜）
    - pb: 当前PB
    - pb_percentile_3y: PB在3年历史中的百分位
    - as_of: 数据日期

    覆盖范围：A股宽基/行业指数、ETF（通过跟踪指数映射）。
    ETF估值通过 _registry 映射到跟踪指数获取。

    Args:
        code: 证券代码（带交易所后缀），如 "000300.SH"（沪深300）、"510300.SH"（沪深300ETF）、"399006.SZ"（创业板指）
    """
    result = factor_service.get_index_valuation_percentile(code)
    return _to_json(result)


@mcp.tool()
def commodity_price_percentile(
    commodity_code: str,
    date: str | None = None,
) -> str:
    """查询商品（黄金/铜/原油等）价格在历史区间中的百分位（V因子补充）。

    返回商品价格百分位：
    - current_price: 当前价格
    - percentile_52w: 近52周价格分位（0-100）
    - percentile_1y: 近1年价格分位
    - percentile_3y: 近3年价格分位

    支持的商品代码：
    - "AU": 黄金（上海黄金交易所 Au99.99 现货）
    - "AG": 白银（上海黄金交易所 Ag99.99 现货）
    - "CU": 铜（上期所主力合约）
    - "AL": 铝（上期所主力合约）
    - "SC": 原油（上海国际能源中心主力合约）

    Args:
        commodity_code: 商品代码，如 "AU"（黄金）、"CU"（铜）、"SC"（原油）
        date: 可选，截止日期 YYYYMMDD 格式。不传则使用最新数据
    """
    result = factor_service.get_commodity_price_percentile(commodity_code, date)
    return _to_json(result)


@mcp.tool()
def get_realtime_quote(
    code: str,
) -> str:
    """获取股票/ETF/指数/港股/美股的实时行情（含五档盘口）。

    返回当前价格、涨跌幅、涨跌额、今开、最高、最低、昨收、成交量、成交额，
    股票和ETF还返回五档买卖盘口（asks/bids）、换手率、量比。
    指数无盘口数据。

    注意：此接口返回的是盘中实时数据，非交易时段返回的是最近一个交易日的收盘数据。

    Args:
        code: 证券代码，支持 A 股/港股/美股/全球指数：
            - A 股: "000001.SZ"（平安银行）、"510300.SH"（沪深300ETF）、"000001.SH"（上证指数）
            - 港股: "00700.HK"（腾讯）、"09988.HK"（阿里巴巴）
            - 美股: "AAPL.US"（苹果）、"TSLA.US"（特斯拉）
            - 全球指数: "SPX.GI"（标普500）、"NDX.GI"（纳斯达克100）、"DJIA.GI"（道琼斯）、"HSI.GI"（恒生指数）、"FTSE.GI"（富时100）、"N225.GI"（日经225）
    """
    result = quote_service.get_realtime(code=code)
    return _to_json(result)


@mcp.tool()
async def search_news(
    keywords: list[str],
) -> str:
    """搜索行业实时新闻并返回 AI 总结。

    输入行业关键词列表，通过大模型联网搜索最近一周的重要新闻，
    返回每个行业的新闻摘要（关键事件、政策变化、市场影响）。

    Args:
        keywords: 行业关键词列表，如 ["半导体", "新能源车", "医药"]
    """
    items = await news_service.search_news(keywords)
    return _to_json(items)


@mcp.tool()
def get_margin_detail(
    code: str,
    date: str | None = None,
    n_days: int = 1,
) -> str:
    """查询股票/ETF的融资融券数据。

    返回字段：
    - rzye:  融资余额（元）
    - rzmre: 融资买入额（元）
    - rzche: 融资偿还额（元）
    - rqye:  融券余额（元）
    - rqmcl: 融券卖出量（股）
    - rqchl: 融券偿还量（股）
    - rqyl:  融券余量（股）

    融资余额趋势可通过 n_days 参数获取近N日历史数据（用于判断融资盘是否持续加仓或撤退）。
    数据来源：Tushare margin_detail（上交所/深交所两融明细）。

    Args:
        code: 证券代码（带交易所后缀），如 "000001.SZ"（平安银行）、"510300.SH"（沪深300ETF）
        date: 可选，截止日期 YYYYMMDD 格式。不传则返回最新数据
        n_days: 返回近N个交易日数据，默认1（仅最新一日）。可传 10/20/60 等查看趋势
    """
    result = margin_service.get_margin(code, date, n_days)
    return _to_json(result)


if __name__ == "__main__":
    init_pool()
    mcp.run(transport="sse")

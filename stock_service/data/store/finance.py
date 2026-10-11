from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from stock_service.database import get_pool

# period 字段候选名（按优先级）
_PERIOD_FIELDS = ("end_date", "period", "ann_date", "report_date", "ex_date")

# 存在「同一报告期多行」的报表类型 → 区分该行的业务字段（row_key 取值来源）。
# 实测（002311.SZ 全历史，按 period_of 归并）：
#   main_business 10 期多行、最坏 17 行 → 按 bz_item（饲料销售/养殖行业/华东地区…）
#   dividend      17 期多行、最坏  4 行 → 按 div_proc（预案/股东提议/股东大会通过/实施）
#   income / balance_sheet / cashflow / forecast / express / indicator / audit /
#   disclosure 每期一行（indicator 的重复行是字段完全相同的上游重复，已在 adapter 去重）
_ROW_KEY_FIELDS = {
    "main_business": "bz_item",
    "dividend": "div_proc",
}


def period_of(record: dict) -> str:
    """从记录中提取报告期（YYYYMMDD）。

    各报表接口的报告期字段名不一致，按优先级取第一个非空值。
    """
    for field in _PERIOD_FIELDS:
        val = record.get(field)
        if val is not None:
            return str(val)
    raise ValueError(f"无法从记录中提取 period: {list(record.keys())}")


def row_key_of(report_type: str, record: dict) -> str:
    """同一报告期内区分多行的业务键，无多行的类型返回空串。

    主键含 row_key，否则一期多行的报表（main_business / dividend）会互相覆盖，
    只剩最后写入的那一行。
    """
    field = _ROW_KEY_FIELDS.get(report_type)
    if not field:
        return ""
    return str(record.get(field) or "")


def save_reports(symbol: str, report_type: str, records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO financial_report (symbol, report_type, period, row_key, ann_date, data)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, report_type, period, row_key) DO UPDATE SET
            ann_date = EXCLUDED.ann_date,
            data = EXCLUDED.data
    """
    params = [
        (
            symbol,
            report_type,
            period_of(rec),
            row_key_of(report_type, rec),
            rec.get("ann_date"),
            Jsonb(rec),
        )
        for rec in records
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


def get_cached_reports(
    symbol: str, report_type: str, start_period: str, end_period: str
) -> list[dict]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT period, ann_date, data FROM financial_report "
                "WHERE symbol = %s AND report_type = %s "
                "AND period >= %s AND period <= %s "
                "ORDER BY period, row_key",
                (symbol, report_type, start_period, end_period),
            ).fetchall()
    return list(rows)

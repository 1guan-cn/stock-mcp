from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from stock_service.database import get_pool

# period 字段候选名（按优先级）
_PERIOD_FIELDS = ("end_date", "period", "ann_date", "report_date", "ex_date")


def _extract_period(record: dict) -> str:
    """从记录中提取 period 值。"""
    for field in _PERIOD_FIELDS:
        val = record.get(field)
        if val is not None:
            return str(val)
    raise ValueError(f"无法从记录中提取 period: {list(record.keys())}")


def save_reports(symbol: str, report_type: str, records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO financial_report (symbol, report_type, period, ann_date, data)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol, report_type, period) DO UPDATE SET
            ann_date = EXCLUDED.ann_date,
            data = EXCLUDED.data
    """
    params = [
        (
            symbol,
            report_type,
            _extract_period(rec),
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
                "ORDER BY period",
                (symbol, report_type, start_period, end_period),
            ).fetchall()
    return list(rows)

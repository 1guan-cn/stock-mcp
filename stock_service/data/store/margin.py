from psycopg.rows import dict_row

from stock_service.database import get_pool


def get_cached_margin(symbol: str, start_date: str, end_date: str) -> list[dict]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT date, rzye, rzmre, rzche, rqye, rqmcl, rqchl, rqyl "
                "FROM margin_detail "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return list(rows)


def save_margin(symbol: str, records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO margin_detail (symbol, date, rzye, rzmre, rzche, rqye, rqmcl, rqchl, rqyl)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            rzye  = EXCLUDED.rzye,
            rzmre = EXCLUDED.rzmre,
            rzche = EXCLUDED.rzche,
            rqye  = EXCLUDED.rqye,
            rqmcl = EXCLUDED.rqmcl,
            rqchl = EXCLUDED.rqchl,
            rqyl  = EXCLUDED.rqyl
    """
    params = [
        (
            symbol,
            r["date"],
            r.get("rzye"),
            r.get("rzmre"),
            r.get("rzche"),
            r.get("rqye"),
            r.get("rqmcl"),
            r.get("rqchl"),
            r.get("rqyl"),
        )
        for r in records
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()

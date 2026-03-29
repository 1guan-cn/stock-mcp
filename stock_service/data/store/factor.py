from psycopg.rows import dict_row

from stock_service.database import get_pool

# ── 指数估值 ──


def get_cached_index_valuation(
    symbol: str, start_date: str, end_date: str
) -> list[dict]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT date, pe, pe_ttm, pb, turnover_rate, total_mv, float_mv "
                "FROM index_valuation "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return list(rows)


def save_index_valuation(symbol: str, records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO index_valuation (symbol, date, pe, pe_ttm, pb, turnover_rate, total_mv, float_mv)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            pe = EXCLUDED.pe, pe_ttm = EXCLUDED.pe_ttm, pb = EXCLUDED.pb,
            turnover_rate = EXCLUDED.turnover_rate,
            total_mv = EXCLUDED.total_mv, float_mv = EXCLUDED.float_mv
    """
    params = [
        (
            symbol,
            r["trade_date"],
            r.get("pe"),
            r.get("pe_ttm"),
            r.get("pb"),
            r.get("turnover_rate"),
            r.get("total_mv"),
            r.get("float_mv"),
        )
        for r in records
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── 资金流 ──


def get_cached_fund_flow(
    symbol: str, start_date: str, end_date: str
) -> list[dict]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT date, main_force_net, main_force_ratio, super_large_net, large_net "
                "FROM fund_flow "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return list(rows)


def save_fund_flow(symbol: str, records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO fund_flow (symbol, date, main_force_net, main_force_ratio, super_large_net, large_net)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            main_force_net = EXCLUDED.main_force_net,
            main_force_ratio = EXCLUDED.main_force_ratio,
            super_large_net = EXCLUDED.super_large_net,
            large_net = EXCLUDED.large_net
    """
    params = [
        (symbol, r["date"], r.get("main_force_net"), r.get("main_force_ratio"),
         r.get("super_large_net"), r.get("large_net"))
        for r in records
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── ETF 申购赎回 ──


def get_cached_etf_subscription(
    symbol: str, start_date: str, end_date: str
) -> list[dict]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT date, share_change, scale_change, net_inflow, source "
                "FROM etf_subscription "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return list(rows)


def save_etf_subscription(symbol: str, records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO etf_subscription (symbol, date, share_change, scale_change, net_inflow, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            share_change = EXCLUDED.share_change,
            scale_change = EXCLUDED.scale_change,
            net_inflow = EXCLUDED.net_inflow,
            source = EXCLUDED.source
    """
    params = [
        (symbol, r["date"], r.get("share_change"), r.get("scale_change"),
         r.get("net_inflow"), r.get("source"))
        for r in records
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── 北向资金 ──


def get_cached_northbound(start_date: str, end_date: str) -> list[dict]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT date, north_net_buy FROM northbound_flow "
                "WHERE date >= %s AND date <= %s ORDER BY date",
                (start_date, end_date),
            ).fetchall()
    return list(rows)


def save_northbound(records: list[dict]) -> None:
    if not records:
        return
    sql = """
        INSERT INTO northbound_flow (date, north_net_buy)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE SET north_net_buy = EXCLUDED.north_net_buy
    """
    params = [(r["date"], r["north_net_buy"]) for r in records]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()

from psycopg.rows import dict_row

from stock_service.database import get_pool
from stock_service.models.quote import DailyBar

# ── 股票日线 ──

_STOCK_COLUMNS = (
    "date, open, high, low, close, pre_close, change, pct_chg, volume, amount, "
    "turnover_rate, turnover_rate_f, volume_ratio, "
    "pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, "
    "total_share, float_share, free_share, total_mv, circ_mv"
)


def get_cached_stock_bars(
    symbol: str, start_date: str, end_date: str
) -> list[DailyBar]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                f"SELECT {_STOCK_COLUMNS} FROM stock_daily_bar "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return [DailyBar(**row) for row in rows]


def save_stock_bars(symbol: str, bars: list[DailyBar]) -> None:
    if not bars:
        return
    sql = """
        INSERT INTO stock_daily_bar (
            symbol, date, open, high, low, close, pre_close, change,
            pct_chg, volume, amount, turnover_rate, turnover_rate_f,
            volume_ratio, pe, pe_ttm, pb, ps, ps_ttm,
            dv_ratio, dv_ttm, total_share, float_share, free_share,
            total_mv, circ_mv
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high,
            low = EXCLUDED.low, close = EXCLUDED.close,
            pre_close = EXCLUDED.pre_close, change = EXCLUDED.change,
            pct_chg = EXCLUDED.pct_chg, volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            turnover_rate = EXCLUDED.turnover_rate,
            turnover_rate_f = EXCLUDED.turnover_rate_f,
            volume_ratio = EXCLUDED.volume_ratio,
            pe = EXCLUDED.pe, pe_ttm = EXCLUDED.pe_ttm,
            pb = EXCLUDED.pb, ps = EXCLUDED.ps, ps_ttm = EXCLUDED.ps_ttm,
            dv_ratio = EXCLUDED.dv_ratio, dv_ttm = EXCLUDED.dv_ttm,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            free_share = EXCLUDED.free_share,
            total_mv = EXCLUDED.total_mv, circ_mv = EXCLUDED.circ_mv
    """
    params = [
        (
            symbol, bar.date, bar.open, bar.high, bar.low, bar.close,
            bar.pre_close, bar.change, bar.pct_chg, bar.volume,
            bar.amount, bar.turnover_rate, bar.turnover_rate_f,
            bar.volume_ratio, bar.pe, bar.pe_ttm, bar.pb, bar.ps,
            bar.ps_ttm, bar.dv_ratio, bar.dv_ttm, bar.total_share,
            bar.float_share, bar.free_share, bar.total_mv, bar.circ_mv,
        )
        for bar in bars
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── 基金日线 ──

_FUND_COLUMNS = "date, open, high, low, close, pre_close, change, pct_chg, volume, amount"


def get_cached_fund_bars(
    symbol: str, start_date: str, end_date: str
) -> list[DailyBar]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                f"SELECT {_FUND_COLUMNS} FROM fund_daily_bar "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return [DailyBar(**row) for row in rows]


def save_fund_bars(symbol: str, bars: list[DailyBar]) -> None:
    if not bars:
        return
    sql = """
        INSERT INTO fund_daily_bar (
            symbol, date, open, high, low, close, pre_close,
            change, pct_chg, volume, amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high,
            low = EXCLUDED.low, close = EXCLUDED.close,
            pre_close = EXCLUDED.pre_close, change = EXCLUDED.change,
            pct_chg = EXCLUDED.pct_chg, volume = EXCLUDED.volume,
            amount = EXCLUDED.amount
    """
    params = [
        (
            symbol, bar.date, bar.open, bar.high, bar.low, bar.close,
            bar.pre_close, bar.change, bar.pct_chg, bar.volume,
            bar.amount,
        )
        for bar in bars
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── 指数日线 ──

_INDEX_COLUMNS = "date, open, high, low, close, pre_close, change, pct_chg, volume, amount"


def get_cached_index_bars(
    symbol: str, start_date: str, end_date: str
) -> list[DailyBar]:
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                f"SELECT {_INDEX_COLUMNS} FROM index_daily_bar "
                "WHERE symbol = %s AND date >= %s AND date <= %s ORDER BY date",
                (symbol, start_date, end_date),
            ).fetchall()
    return [DailyBar(**row) for row in rows]


def save_index_bars(symbol: str, bars: list[DailyBar]) -> None:
    if not bars:
        return
    sql = """
        INSERT INTO index_daily_bar (
            symbol, date, open, high, low, close, pre_close,
            change, pct_chg, volume, amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high,
            low = EXCLUDED.low, close = EXCLUDED.close,
            pre_close = EXCLUDED.pre_close, change = EXCLUDED.change,
            pct_chg = EXCLUDED.pct_chg, volume = EXCLUDED.volume,
            amount = EXCLUDED.amount
    """
    params = [
        (
            symbol, bar.date, bar.open, bar.high, bar.low, bar.close,
            bar.pre_close, bar.change, bar.pct_chg, bar.volume,
            bar.amount,
        )
        for bar in bars
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── 复权因子 ──

def get_cached_adj_factors(
    symbol: str, start_date: str, end_date: str
) -> dict[str, float]:
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT date, adj_factor FROM adj_factor "
            "WHERE symbol = %s AND date >= %s AND date <= %s",
            (symbol, start_date, end_date),
        ).fetchall()
    return {row[0]: row[1] for row in rows}


def save_adj_factors(symbol: str, factors: dict[str, float]) -> None:
    if not factors:
        return
    sql = """
        INSERT INTO adj_factor (symbol, date, adj_factor)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET adj_factor = EXCLUDED.adj_factor
    """
    params = [(symbol, date, factor) for date, factor in factors.items()]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()


# ── Coverage ──

def get_coverage(symbol: str, data_type: str = "daily") -> tuple[str, str] | None:
    with get_pool().connection() as conn:
        row = conn.execute(
            "SELECT min_date, max_date FROM data_coverage "
            "WHERE symbol = %s AND data_type = %s",
            (symbol, data_type),
        ).fetchone()
    if row is None:
        return None
    return (row[0], row[1])


def update_coverage(
    symbol: str, min_date: str, max_date: str, data_type: str = "daily"
) -> None:
    with get_pool().connection() as conn:
        conn.execute(
            """
            INSERT INTO data_coverage (symbol, data_type, min_date, max_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, data_type) DO UPDATE SET
                min_date = LEAST(data_coverage.min_date, EXCLUDED.min_date),
                max_date = GREATEST(data_coverage.max_date, EXCLUDED.max_date),
                updated_at = NOW()
            """,
            (symbol, data_type, min_date, max_date),
        )
        conn.commit()

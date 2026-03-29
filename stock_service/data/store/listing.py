from datetime import datetime, timezone

from psycopg.rows import dict_row

from stock_service.database import get_pool
from stock_service.models import AssetType
from stock_service.models.listing import FundInfo, IndexInfo, SearchItem, StockInfo


def get_stocks(
    *,
    market: str | None = None,
    industry: str | None = None,
    area: str | None = None,
    keyword: str | None = None,
) -> list[StockInfo]:
    conditions = []
    params: list[str] = []

    if market:
        conditions.append("market = %s")
        params.append(market)
    if industry:
        conditions.append("industry = %s")
        params.append(industry)
    if area:
        conditions.append("area = %s")
        params.append(area)
    if keyword:
        conditions.append("(name ILIKE %s OR symbol ILIKE %s)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                f"SELECT symbol, name, area, industry, market, list_date "
                f"FROM stock_list {where} ORDER BY symbol",
                params,
            ).fetchall()
    return [StockInfo(**row) for row in rows]


def save_stocks(stocks: list[StockInfo]) -> None:
    if not stocks:
        return
    now = datetime.now(timezone.utc)
    sql = """
        INSERT INTO stock_list (symbol, name, area, industry, market, list_date, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (s.symbol, s.name, s.area, s.industry, s.market, s.list_date, now)
        for s in stocks
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stock_list")
            cur.executemany(sql, params)
        conn.commit()


def get_funds(
    *,
    fund_type: str | None = None,
    management: str | None = None,
    keyword: str | None = None,
) -> list[FundInfo]:
    conditions = []
    params: list[str] = []

    if fund_type:
        conditions.append("fund_type = %s")
        params.append(fund_type)
    if management:
        conditions.append("management = %s")
        params.append(management)
    if keyword:
        conditions.append("(name ILIKE %s OR symbol ILIKE %s)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                f"SELECT symbol, name, fund_type, management, list_date "
                f"FROM fund_list {where} ORDER BY symbol",
                params,
            ).fetchall()
    return [FundInfo(**row) for row in rows]


def save_funds(funds: list[FundInfo]) -> None:
    if not funds:
        return
    now = datetime.now(timezone.utc)
    sql = """
        INSERT INTO fund_list (symbol, name, fund_type, management, list_date, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    params = [
        (f.symbol, f.name, f.fund_type, f.management, f.list_date, now)
        for f in funds
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fund_list")
            cur.executemany(sql, params)
        conn.commit()


def get_indexes(
    *,
    keyword: str | None = None,
) -> list[IndexInfo]:
    conditions = []
    params: list[str] = []

    if keyword:
        conditions.append("(name ILIKE %s OR symbol ILIKE %s)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                f"SELECT symbol, name, fullname, market, publisher, category, list_date "
                f"FROM index_list {where} ORDER BY symbol",
                params,
            ).fetchall()
    return [IndexInfo(**row) for row in rows]


def save_indexes(indexes: list[IndexInfo]) -> None:
    if not indexes:
        return
    now = datetime.now(timezone.utc)
    sql = """
        INSERT INTO index_list (symbol, name, fullname, market, publisher, category, list_date, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (i.symbol, i.name, i.fullname, i.market, i.publisher, i.category, i.list_date, now)
        for i in indexes
    ]
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM index_list")
            cur.executemany(sql, params)
        conn.commit()


def search(keyword: str) -> list[SearchItem]:
    pattern = f"%{keyword}%"
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                """
                SELECT symbol, name, 'stock' AS asset_type FROM stock_list
                WHERE name ILIKE %s OR symbol ILIKE %s
                UNION ALL
                SELECT symbol, name, 'fund' AS asset_type FROM fund_list
                WHERE name ILIKE %s OR symbol ILIKE %s
                UNION ALL
                SELECT symbol, name, 'index' AS asset_type FROM index_list
                WHERE name ILIKE %s OR symbol ILIKE %s
                ORDER BY symbol
                """,
                (pattern, pattern, pattern, pattern, pattern, pattern),
            ).fetchall()
    return [SearchItem(**row) for row in rows]


def resolve_symbols(
    *,
    code: str | None = None,
    market: str | None = None,
    industry: str | None = None,
) -> list[tuple[str, str, str]]:
    """根据筛选条件查找匹配的 symbol，返回 [(symbol, name, asset_type), ...]"""
    results: list[tuple[str, str, str]] = []

    stock_conditions = []
    stock_params: list[str] = []
    if code:
        stock_conditions.append("symbol = %s")
        stock_params.append(code)
    if market:
        stock_conditions.append("market = %s")
        stock_params.append(market)
    if industry:
        stock_conditions.append("industry = %s")
        stock_params.append(industry)

    stock_where = f"WHERE {' AND '.join(stock_conditions)}" if stock_conditions else ""

    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            rows = cur.execute(
                f"SELECT symbol, name FROM stock_list {stock_where} ORDER BY symbol",
                stock_params,
            ).fetchall()
            results.extend((r[0], r[1], AssetType.STOCK) for r in rows)

            if not industry:
                fund_conditions = []
                fund_params: list[str] = []
                if code:
                    fund_conditions.append("symbol = %s")
                    fund_params.append(code)

                fund_where = f"WHERE {' AND '.join(fund_conditions)}" if fund_conditions else ""

                rows = cur.execute(
                    f"SELECT symbol, name FROM fund_list {fund_where} ORDER BY symbol",
                    fund_params,
                ).fetchall()
                results.extend((r[0], r[1], AssetType.FUND) for r in rows)

            if not industry and not market:
                index_conditions = []
                index_params: list[str] = []
                if code:
                    index_conditions.append("symbol = %s")
                    index_params.append(code)

                index_where = f"WHERE {' AND '.join(index_conditions)}" if index_conditions else ""

                rows = cur.execute(
                    f"SELECT symbol, name FROM index_list {index_where} ORDER BY symbol",
                    index_params,
                ).fetchall()
                results.extend((r[0], r[1], AssetType.INDEX) for r in rows)

    return results


_ALLOWED_TABLES = frozenset({"stock_list", "fund_list", "index_list"})


def get_last_updated(table: str) -> datetime | None:
    """获取表中最新的 updated_at 时间。"""
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table: {table!r}")
    with get_pool().connection() as conn:
        row = conn.execute(
            f"SELECT MAX(updated_at) FROM {table}"
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]

from psycopg_pool import ConnectionPool

from stock_service.config import settings

pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    return pool


def init_pool() -> ConnectionPool:
    global pool
    pool = ConnectionPool(settings.database_url, min_size=2, max_size=10)
    pool.open()
    return pool


def close_pool() -> None:
    global pool
    if pool is not None:
        pool.close()
        pool = None

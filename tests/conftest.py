import pytest

from stock_service.database import init_pool, close_pool


@pytest.fixture(scope="session", autouse=True)
def _db_pool():
    """整个测试会话共享一个数据库连接池。"""
    init_pool()
    yield
    close_pool()

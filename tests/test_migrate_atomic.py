"""迁移引擎的原子性契约：多条待执行迁移必须「整体提交」或「整体回滚」。

2026-10-11 事故的根因之一：迁移引擎逐条提交，让 v10/v11 落库、v12 失败，
留下「半迁移」schema —— 而旧进程依赖的 data_coverage 已被 v11 删掉，
服务在人工干预前一直处于故障态。本测试锁定「失败 = schema 未发生任何变更」。
"""

import importlib.util
import pathlib
import sys

import psycopg
import pytest

from stock_service.config import settings

_MIGRATE_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "migrate.py"

PROBE_OK = {
    "version": 9900,
    "name": "probe_should_be_rolled_back",
    "up": "CREATE TABLE IF NOT EXISTS _migrate_atomicity_probe (id INT)",
    "down": "DROP TABLE IF EXISTS _migrate_atomicity_probe",
}

PROBE_FAIL = {
    "version": 9901,
    "name": "probe_failing",
    "up": "SELECT 1 / 0",
    "down": "",
}


def _load_migrate():
    """scripts/ 不是包，按路径加载迁移模块。"""
    spec = importlib.util.spec_from_file_location("_migrate_under_test", _MIGRATE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_failed_migration_rolls_back_earlier_ones():
    migrate = _load_migrate()
    migrate.MIGRATIONS = [*migrate.MIGRATIONS, PROBE_OK, PROBE_FAIL]

    with psycopg.connect(settings.database_url) as conn:
        migrate._ensure_migration_table(conn)
        conn.execute("DROP TABLE IF EXISTS _migrate_atomicity_probe")
        conn.execute("DELETE FROM schema_migrations WHERE version >= 9900")
        conn.commit()

        with pytest.raises(SystemExit):
            migrate.cmd_migrate(conn)

    with psycopg.connect(settings.database_url) as conn:
        assert conn.execute(
            "SELECT to_regclass('_migrate_atomicity_probe')"
        ).fetchone()[0] is None, "失败的迁移留下了半成品 schema"
        assert conn.execute(
            "SELECT version FROM schema_migrations WHERE version >= 9900"
        ).fetchall() == [], "失败的迁移仍被登记进 schema_migrations"

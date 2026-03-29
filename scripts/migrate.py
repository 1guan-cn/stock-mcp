"""
数据库迁移脚本。

用法：
    uv run python scripts/migrate.py          # 执行所有未执行的迁移
    uv run python scripts/migrate.py status    # 查看迁移状态
    uv run python scripts/migrate.py rollback  # 回滚最近一次迁移

迁移记录保存在 schema_migrations 表中。
每个迁移函数定义 up() 和 down()，按版本号顺序执行。
"""

import sys
from datetime import datetime, timezone

import psycopg

from stock_service.config import settings

# ─── 迁移定义 ───
# 每个迁移是一个 dict: { "version": int, "name": str, "up": str, "down": str }
# up/down 为 SQL 语句，多条用分号分隔

MIGRATIONS: list[dict] = [
    {
        "version": 1,
        "name": "init_tables",
        "up": """
            CREATE TABLE IF NOT EXISTS stock_daily_bar (
                symbol          TEXT NOT NULL,
                date            TEXT NOT NULL,
                open            DOUBLE PRECISION NOT NULL,
                high            DOUBLE PRECISION NOT NULL,
                low             DOUBLE PRECISION NOT NULL,
                close           DOUBLE PRECISION NOT NULL,
                pre_close       DOUBLE PRECISION,
                change          DOUBLE PRECISION,
                pct_chg         DOUBLE PRECISION,
                volume          DOUBLE PRECISION NOT NULL,
                amount          DOUBLE PRECISION,
                turnover_rate   DOUBLE PRECISION,
                turnover_rate_f DOUBLE PRECISION,
                volume_ratio    DOUBLE PRECISION,
                pe              DOUBLE PRECISION,
                pe_ttm          DOUBLE PRECISION,
                pb              DOUBLE PRECISION,
                ps              DOUBLE PRECISION,
                ps_ttm          DOUBLE PRECISION,
                dv_ratio        DOUBLE PRECISION,
                dv_ttm          DOUBLE PRECISION,
                total_share     DOUBLE PRECISION,
                float_share     DOUBLE PRECISION,
                free_share      DOUBLE PRECISION,
                total_mv        DOUBLE PRECISION,
                circ_mv         DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS fund_daily_bar (
                symbol    TEXT NOT NULL,
                date      TEXT NOT NULL,
                open      DOUBLE PRECISION NOT NULL,
                high      DOUBLE PRECISION NOT NULL,
                low       DOUBLE PRECISION NOT NULL,
                close     DOUBLE PRECISION NOT NULL,
                pre_close DOUBLE PRECISION,
                change    DOUBLE PRECISION,
                pct_chg   DOUBLE PRECISION,
                volume    DOUBLE PRECISION NOT NULL,
                amount    DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS adj_factor (
                symbol      TEXT NOT NULL,
                date        TEXT NOT NULL,
                adj_factor  DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS data_coverage (
                symbol     TEXT NOT NULL,
                data_type  TEXT NOT NULL,
                min_date   TEXT NOT NULL,
                max_date   TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (symbol, data_type)
            );

            CREATE TABLE IF NOT EXISTS stock_list (
                symbol     TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                area       TEXT,
                industry   TEXT,
                market     TEXT,
                list_date  TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS fund_list (
                symbol     TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                fund_type  TEXT,
                management TEXT,
                list_date  TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """,
        "down": """
            DROP TABLE IF EXISTS fund_list;
            DROP TABLE IF EXISTS stock_list;
            DROP TABLE IF EXISTS data_coverage;
            DROP TABLE IF EXISTS adj_factor;
            DROP TABLE IF EXISTS fund_daily_bar;
            DROP TABLE IF EXISTS stock_daily_bar;
        """,
    },
    {
        "version": 2,
        "name": "migrate_legacy_tables",
        "up": """
            -- 迁移旧 daily_bar 数据到 stock_daily_bar（如果旧表存在）
            INSERT INTO stock_daily_bar (symbol, date, open, high, low, close, pct_chg, volume, amount)
            SELECT symbol, date, open, high, low, close, pct_chg, volume, amount
            FROM daily_bar
            ON CONFLICT (symbol, date) DO NOTHING;

            -- 迁移旧 etf_adj_factor 数据到 adj_factor
            INSERT INTO adj_factor (symbol, date, adj_factor)
            SELECT symbol, date, adj_factor
            FROM etf_adj_factor
            ON CONFLICT (symbol, date) DO NOTHING;

            -- 删除旧表
            DROP TABLE IF EXISTS daily_bar;
            DROP TABLE IF EXISTS etf_adj_factor;
        """,
        "down": """
            -- 无法完美回滚（旧表已删除），仅重建空表结构
            CREATE TABLE IF NOT EXISTS daily_bar (
                symbol    TEXT NOT NULL,
                date      TEXT NOT NULL,
                open      DOUBLE PRECISION NOT NULL,
                high      DOUBLE PRECISION NOT NULL,
                low       DOUBLE PRECISION NOT NULL,
                close     DOUBLE PRECISION NOT NULL,
                volume    DOUBLE PRECISION NOT NULL,
                amount    DOUBLE PRECISION,
                pct_chg   DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );
            CREATE TABLE IF NOT EXISTS etf_adj_factor (
                symbol      TEXT NOT NULL,
                date        TEXT NOT NULL,
                adj_factor  DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (symbol, date)
            );
        """,
    },
    {
        "version": 3,
        "name": "create_financial_report",
        "up": """
            CREATE TABLE IF NOT EXISTS financial_report (
                symbol      TEXT NOT NULL,
                report_type TEXT NOT NULL,
                period      TEXT NOT NULL,
                ann_date    TEXT,
                data        JSONB NOT NULL,
                PRIMARY KEY (symbol, report_type, period)
            );
            CREATE INDEX IF NOT EXISTS idx_financial_report_lookup
                ON financial_report (symbol, report_type);
        """,
        "down": """
            DROP INDEX IF EXISTS idx_financial_report_lookup;
            DROP TABLE IF EXISTS financial_report;
        """,
    },
    {
        "version": 4,
        "name": "create_index_tables",
        "up": """
            CREATE TABLE IF NOT EXISTS index_daily_bar (
                symbol    TEXT NOT NULL,
                date      TEXT NOT NULL,
                open      DOUBLE PRECISION NOT NULL,
                high      DOUBLE PRECISION NOT NULL,
                low       DOUBLE PRECISION NOT NULL,
                close     DOUBLE PRECISION NOT NULL,
                pre_close DOUBLE PRECISION,
                change    DOUBLE PRECISION,
                pct_chg   DOUBLE PRECISION,
                volume    DOUBLE PRECISION NOT NULL,
                amount    DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS index_list (
                symbol     TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                fullname   TEXT,
                market     TEXT,
                publisher  TEXT,
                category   TEXT,
                list_date  TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """,
        "down": """
            DROP TABLE IF EXISTS index_list;
            DROP TABLE IF EXISTS index_daily_bar;
        """,
    },
    {
        "version": 5,
        "name": "unify_symbol_with_exchange_suffix",
        "up": """
            -- stock_list: 6开头→.SH, 8/4开头→.BJ, 其余→.SZ
            UPDATE stock_list SET symbol = symbol || CASE
                WHEN symbol LIKE '6%' THEN '.SH'
                WHEN symbol LIKE '8%' OR symbol LIKE '4%' THEN '.BJ'
                ELSE '.SZ'
            END WHERE symbol NOT LIKE '%.%';

            -- fund_list: 51/58/56开头→.SH, 其余→.SZ
            UPDATE fund_list SET symbol = symbol || CASE
                WHEN symbol LIKE '51%' OR symbol LIKE '58%' OR symbol LIKE '56%' THEN '.SH'
                ELSE '.SZ'
            END WHERE symbol NOT LIKE '%.%';

            -- index_list: 39开头→.SZ, 其余→.SH
            UPDATE index_list SET symbol = symbol || CASE
                WHEN symbol LIKE '39%' THEN '.SZ'
                ELSE '.SH'
            END WHERE symbol NOT LIKE '%.%';

            -- stock_daily_bar
            UPDATE stock_daily_bar SET symbol = symbol || CASE
                WHEN symbol LIKE '6%' THEN '.SH'
                WHEN symbol LIKE '8%' OR symbol LIKE '4%' THEN '.BJ'
                ELSE '.SZ'
            END WHERE symbol NOT LIKE '%.%';

            -- fund_daily_bar
            UPDATE fund_daily_bar SET symbol = symbol || CASE
                WHEN symbol LIKE '51%' OR symbol LIKE '58%' OR symbol LIKE '56%' THEN '.SH'
                ELSE '.SZ'
            END WHERE symbol NOT LIKE '%.%';

            -- index_daily_bar
            UPDATE index_daily_bar SET symbol = symbol || CASE
                WHEN symbol LIKE '39%' THEN '.SZ'
                ELSE '.SH'
            END WHERE symbol NOT LIKE '%.%';

            -- adj_factor
            UPDATE adj_factor SET symbol = symbol || CASE
                WHEN symbol LIKE '6%' OR symbol LIKE '51%' OR symbol LIKE '58%' OR symbol LIKE '56%' THEN '.SH'
                WHEN symbol LIKE '8%' OR symbol LIKE '4%' THEN '.BJ'
                ELSE '.SZ'
            END WHERE symbol NOT LIKE '%.%';

            -- data_coverage: 根据 data_type 区分规则
            UPDATE data_coverage SET symbol = symbol || CASE
                WHEN data_type = 'index_daily' THEN
                    CASE WHEN symbol LIKE '39%' THEN '.SZ' ELSE '.SH' END
                WHEN data_type = 'fund_daily' THEN
                    CASE WHEN symbol LIKE '51%' OR symbol LIKE '58%' OR symbol LIKE '56%' THEN '.SH' ELSE '.SZ' END
                ELSE
                    CASE
                        WHEN symbol LIKE '6%' OR symbol LIKE '51%' OR symbol LIKE '58%' OR symbol LIKE '56%' THEN '.SH'
                        WHEN symbol LIKE '8%' OR symbol LIKE '4%' THEN '.BJ'
                        ELSE '.SZ'
                    END
            END WHERE symbol NOT LIKE '%.%';

            -- financial_report
            UPDATE financial_report SET symbol = symbol || CASE
                WHEN symbol LIKE '6%' THEN '.SH'
                WHEN symbol LIKE '8%' OR symbol LIKE '4%' THEN '.BJ'
                ELSE '.SZ'
            END WHERE symbol NOT LIKE '%.%';
        """,
        "down": """
            UPDATE stock_list SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE fund_list SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE index_list SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE stock_daily_bar SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE fund_daily_bar SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE index_daily_bar SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE adj_factor SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE data_coverage SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
            UPDATE financial_report SET symbol = SPLIT_PART(symbol, '.', 1) WHERE symbol LIKE '%.%';
        """,
    },
    {
        "version": 6,
        "name": "create_factor_tables",
        "up": """
            CREATE TABLE IF NOT EXISTS index_valuation (
                symbol          TEXT NOT NULL,
                date            TEXT NOT NULL,
                pe              DOUBLE PRECISION,
                pe_ttm          DOUBLE PRECISION,
                pb              DOUBLE PRECISION,
                turnover_rate   DOUBLE PRECISION,
                total_mv        DOUBLE PRECISION,
                float_mv        DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS fund_flow (
                symbol          TEXT NOT NULL,
                date            TEXT NOT NULL,
                main_force_net  DOUBLE PRECISION,
                main_force_ratio DOUBLE PRECISION,
                super_large_net DOUBLE PRECISION,
                large_net       DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS northbound_flow (
                date            TEXT NOT NULL PRIMARY KEY,
                north_net_buy   DOUBLE PRECISION
            );
        """,
        "down": """
            DROP TABLE IF EXISTS northbound_flow;
            DROP TABLE IF EXISTS fund_flow;
            DROP TABLE IF EXISTS index_valuation;
        """,
    },
    {
        "version": 7,
        "name": "create_etf_subscription_table",
        "up": """
            CREATE TABLE IF NOT EXISTS etf_subscription (
                symbol       TEXT NOT NULL,
                date         TEXT NOT NULL,
                share_change DOUBLE PRECISION,
                scale_change DOUBLE PRECISION,
                net_inflow   DOUBLE PRECISION,
                source       TEXT,
                PRIMARY KEY (symbol, date)
            );
        """,
        "down": """
            DROP TABLE IF EXISTS etf_subscription;
        """,
    },
    {
        "version": 8,
        "name": "create_news_search_table",
        "up": """
            CREATE TABLE IF NOT EXISTS news_search (
                id          SERIAL PRIMARY KEY,
                keyword     TEXT NOT NULL,
                summary     TEXT NOT NULL,
                searched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_news_search_keyword
                ON news_search (keyword, searched_at DESC);
        """,
        "down": """
            DROP INDEX IF EXISTS idx_news_search_keyword;
            DROP TABLE IF EXISTS news_search;
        """,
    },
    {
        "version": 9,
        "name": "create_margin_detail_table",
        "up": """
            CREATE TABLE IF NOT EXISTS margin_detail (
                symbol TEXT NOT NULL,
                date   TEXT NOT NULL,
                rzye   DOUBLE PRECISION,
                rzmre  DOUBLE PRECISION,
                rzche  DOUBLE PRECISION,
                rqye   DOUBLE PRECISION,
                rqmcl  DOUBLE PRECISION,
                rqchl  DOUBLE PRECISION,
                rqyl   DOUBLE PRECISION,
                PRIMARY KEY (symbol, date)
            );
        """,
        "down": """
            DROP TABLE IF EXISTS margin_detail;
        """,
    },
]


# ─── 迁移引擎 ───


def _ensure_migration_table(conn: psycopg.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    conn.commit()


def _get_applied_versions(conn: psycopg.Connection) -> set[int]:
    rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    return {row[0] for row in rows}


def _table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
        (table_name,),
    ).fetchone()
    return row[0] if row else False


def cmd_migrate(conn: psycopg.Connection) -> None:
    applied = _get_applied_versions(conn)
    pending = [m for m in MIGRATIONS if m["version"] not in applied]

    if not pending:
        print("所有迁移已执行，无需操作。")
        return

    for m in sorted(pending, key=lambda x: x["version"]):
        version, name = m["version"], m["name"]
        print(f"执行迁移 v{version}: {name} ...", end=" ")

        # 迁移 v2 需要旧表存在才执行数据迁移部分
        if version == 2:
            if not _table_exists(conn, "daily_bar") and not _table_exists(conn, "etf_adj_factor"):
                print("旧表不存在，跳过数据迁移。")
                conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at) VALUES (%s, %s, %s)",
                    (version, name, datetime.now(timezone.utc)),
                )
                conn.commit()
                continue

        try:
            conn.execute(m["up"])
            conn.execute(
                "INSERT INTO schema_migrations (version, name, applied_at) VALUES (%s, %s, %s)",
                (version, name, datetime.now(timezone.utc)),
            )
            conn.commit()
            print("OK")
        except Exception as e:
            conn.rollback()
            print(f"失败: {e}")
            sys.exit(1)


def cmd_status(conn: psycopg.Connection) -> None:
    applied = _get_applied_versions(conn)
    print(f"{'版本':<6} {'名称':<30} {'状态'}")
    print("-" * 50)
    for m in MIGRATIONS:
        status = "已执行" if m["version"] in applied else "未执行"
        print(f"v{m['version']:<5} {m['name']:<30} {status}")


def cmd_rollback(conn: psycopg.Connection) -> None:
    applied = _get_applied_versions(conn)
    if not applied:
        print("没有已执行的迁移可回滚。")
        return

    latest = max(applied)
    m = next((m for m in MIGRATIONS if m["version"] == latest), None)
    if m is None:
        print(f"找不到版本 v{latest} 的迁移定义。")
        sys.exit(1)

    print(f"回滚 v{m['version']}: {m['name']} ...", end=" ")
    try:
        conn.execute(m["down"])
        conn.execute("DELETE FROM schema_migrations WHERE version = %s", (latest,))
        conn.commit()
        print("OK")
    except Exception as e:
        conn.rollback()
        print(f"失败: {e}")
        sys.exit(1)


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "migrate"

    with psycopg.connect(settings.database_url) as conn:
        _ensure_migration_table(conn)

        if command == "migrate":
            cmd_migrate(conn)
        elif command == "status":
            cmd_status(conn)
        elif command == "rollback":
            cmd_rollback(conn)
        else:
            print(f"未知命令: {command}")
            print("可用命令: migrate, status, rollback")
            sys.exit(1)


if __name__ == "__main__":
    main()

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

A 股行情查询 + MCP (Model Context Protocol) 服务。基于 FastAPI 提供 HTTP 服务，通过 SSE 挂载 MCP 工具供 AI 客户端调用。数据来源为 Tushare、AKShare 和百炼（阿里云 DashScope）。

## 常用命令

```bash
uv sync                              # 安装依赖
uv run python scripts/migrate.py     # 数据库迁移
uv run python -m stock_service.main  # 启动服务
uv run pytest                        # 运行全部测试
uv run pytest tests/test_mcp_factor.py          # 运行单个测试文件
uv run pytest tests/test_mcp_factor.py -k "test_name"  # 运行单个测试
```

## 架构

三层结构，每层按业务领域拆分为同名模块（quote / listing / finance / factor / margin / news）：

```
stock_service/
├── mcp_server.py          # MCP 工具定义层（FastMCP），所有 @mcp.tool() 在此注册
├── services/              # 业务逻辑层，被 mcp_server.py 调用
├── models/                # Pydantic 模型 & StrEnum（AssetType, AdjustType 等）
├── data/
│   ├── store/             # 数据访问层（PostgreSQL 查询，psycopg）
│   ├── adapters/          # 外部数据源适配器（tushare.py, akshare.py, bailian.py）
│   ├── _registry.py       # ETF → 跟踪指数映射表
│   └── _cache.py          # 数据缓存
├── main.py                # FastAPI 入口，lifespan 管理数据库连接池
├── auth.py                # API Key 中间件（X-API-Key header，查 api_key 表）
├── config.py              # pydantic-settings 配置（读 .env）
└── database.py            # psycopg ConnectionPool 管理
```

**关键路径**: MCP 客户端请求 → `/mcp` (SSE) → `mcp_server.py` → `services/` → `data/store/` (DB) + `data/adapters/` (外部 API)

## 技术要点

- **Python 3.12+**，使用 `uv` 管理依赖
- MCP 服务挂载在 `/mcp` 路径，健康检查: `/health` 和 `/mcp/health`（公开，无需认证）
- 其他接口需要 `X-API-Key` header，key 存在 PostgreSQL `api_key` 表中
- 测试需要真实数据库连接（`conftest.py` 中 session 级别连接池），需配置 `.env`
- 部署通过 GitHub Actions SSH + rsync 到服务器，pm2 管理进程（`ecosystem.config.cjs`）
- 所有 MCP 工具返回值统一经 `_to_json()` 序列化为 JSON 字符串

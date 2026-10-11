# stock-mcp

独立的 A 股行情与 MCP 服务项目。

## 包含内容

- `stock_service/`: FastAPI + MCP 服务实现
- `scripts/migrate.py`: 数据库迁移脚本
- `tests/`: MCP 接口测试
- `.github/workflows/deploy.yml`: 部署工作流

## 本地启动

1. 复制环境变量模板：

```bash
cp .env.example .env
```

2. 安装依赖：

```bash
uv sync
```

3. 执行数据库迁移：

```bash
uv run python scripts/migrate.py
```

4. 启动服务：

```bash
uv run python -m stock_service.main
```

默认会启动 FastAPI 应用，并在 `/mcp` 提供 Streamable HTTP MCP 服务。
MCP 客户端直接连接 `/mcp`，不再使用旧的 `/mcp/sse` 和 `/mcp/messages` 两段式端点。
健康检查同时提供 `/mcp/health`（推荐，便于网关按 `/mcp` 前缀转发）和 `/health`（兼容旧配置）。

## MCP 工具数据源标记

MCP tool 描述中带 `[tushare]` 的工具表示该工具的正式数据路径由 Tushare 支撑。
混合源或非 Tushare 工具不会带这个标记，并会在描述中说明实际来源（例如腾讯财经、东方财富或 AKShare）。

## 测试

```bash
uv run pytest
```

## 环境变量

- `TUSHARE_TOKEN`
- `DATABASE_URL`
- `HOST`
- `PORT`
- `TUSHARE_BASE_URL`（可选，自建 tushare 代理）

## License

Released under the MIT License. See [LICENSE](./LICENSE).

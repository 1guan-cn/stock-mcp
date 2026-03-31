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

## 测试

```bash
uv run pytest
```

## 环境变量

- `TUSHARE_TOKEN`
- `DATABASE_URL`
- `HOST`
- `PORT`
- `AI_BASE_URL`
- `AI_API_KEY`
- `AI_MODEL`
- `DASHSCOPE_API_KEY`

## License

Released under the MIT License. See [LICENSE](./LICENSE).

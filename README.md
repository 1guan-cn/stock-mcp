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

默认会启动 FastAPI 应用，并在 `/mcp` 挂载 SSE MCP 服务。

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

from contextlib import asynccontextmanager

from fastapi import FastAPI

from stock_service.auth import ApiKeyMiddleware
from stock_service.database import close_pool, init_pool
from stock_service.mcp_server import mcp


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    yield
    close_pool()


app = FastAPI(title="Stock Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(ApiKeyMiddleware)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/mcp/health")
def mcp_health():
    return {"status": "ok"}


app.mount("/mcp", mcp.sse_app())


if __name__ == "__main__":
    import uvicorn

    from stock_service.config import settings

    uvicorn.run("stock_service.main:app", host=settings.host, port=settings.port)

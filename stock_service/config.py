from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # 部署环境的 .env 由运维手工维护，不会随代码同步。忽略多余变量，
        # 避免删除配置字段后残留变量导致服务启动即 ValidationError。
        extra="ignore",
    )

    tushare_token: str
    tushare_base_url: str = ""
    database_url: str
    host: str = "0.0.0.0"
    port: int = 8000


settings = Settings()

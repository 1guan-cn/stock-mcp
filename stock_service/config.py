from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    tushare_token: str
    database_url: str
    host: str = "0.0.0.0"
    port: int = 8000
    ai_base_url: str = "https://coding.dashscope.aliyuncs.com/v1"
    ai_api_key: str = ""
    ai_model: str = "qwen3.5-plus"
    dashscope_api_key: str = ""


settings = Settings()

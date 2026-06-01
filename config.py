from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_base_url: str = "http://host.docker.internal:8000"
    database_url: str = "sqlite+aiosqlite:////data/proxy.db"
    image_storage_path: str = "/data/images"
    image_download_concurrency: int = 5

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

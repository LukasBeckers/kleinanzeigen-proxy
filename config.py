from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    api_base_url: str = "http://host.docker.internal:8000"
    # Comma-separated list of upstreams (e.g. "http://a:8000,http://b:8001").
    # If set via API_BASE_URLS, takes precedence.
    api_base_urls_raw: str = Field("", alias="API_BASE_URLS")

    database_url: str = "sqlite+aiosqlite:////data/proxy.db"
    image_storage_path: str = "/data/images"
    image_download_concurrency: int = 5

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def upstream_urls(self) -> list[str]:
        """Return the list of upstreams to load-balance across."""
        if self.api_base_urls_raw:
            return [x.strip() for x in self.api_base_urls_raw.split(",") if x.strip()]
        return [self.api_base_url]


settings = Settings()

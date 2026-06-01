from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class Settings(BaseSettings):
    api_base_url: str = "http://host.docker.internal:8000"
    # Comma-separated list of upstreams. If set, takes precedence over api_base_url.
    api_base_urls: list[str] = []

    database_url: str = "sqlite+aiosqlite:////data/proxy.db"
    image_storage_path: str = "/data/images"
    image_download_concurrency: int = 5

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator("api_base_urls", mode="before")
    @classmethod
    def parse_api_base_urls(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        if v is None:
            return []
        return v

    @property
    def upstream_urls(self) -> list[str]:
        """Return the list of upstreams to use (from API_BASE_URLS or fallback to single api_base_url)."""
        if self.api_base_urls:
            return self.api_base_urls
        return [self.api_base_url]


settings = Settings()

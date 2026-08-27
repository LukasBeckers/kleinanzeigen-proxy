from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    api_base_url: str = "http://host.docker.internal:8000"
    # Comma-separated list of upstreams (e.g. "http://a:8000,http://b:8001").
    # If set via API_BASE_URLS, takes precedence.
    api_base_urls_raw: str = Field("", alias="API_BASE_URLS")
    # Optional comma-separated multiplicative pick weights aligned with
    # API_BASE_URLS (e.g. "1.5,1"). Defaults to 1.0 per upstream.
    api_base_weights_raw: str = Field("", alias="API_BASE_WEIGHTS")

    database_url: str = "sqlite+aiosqlite:////data/proxy.db"
    image_storage_path: str = "/data/images"
    image_download_concurrency: int = 5
    max_fail_rate: float = Field(0.25, alias="UPSTREAM_MAX_FAIL_RATE")
    cooldown_s: float = Field(3600.0, alias="UPSTREAM_COOLDOWN_S")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def upstream_urls(self) -> list[str]:
        """Return the list of upstreams to load-balance across."""
        if self.api_base_urls_raw:
            return [x.strip() for x in self.api_base_urls_raw.split(",") if x.strip()]
        return [self.api_base_url]

    @property
    def upstream_weights(self) -> list[float]:
        """Multiplicative pick weights, one per ``upstream_urls`` entry."""
        urls = self.upstream_urls
        if not urls:
            return []
        if not self.api_base_weights_raw.strip():
            return [1.0] * len(urls)
        parts = [p.strip() for p in self.api_base_weights_raw.split(",") if p.strip()]
        weights: list[float] = []
        for p in parts:
            w = float(p)
            if w < 0:
                raise ValueError(f"upstream weight must be >= 0 (got {w})")
            weights.append(w)
        if len(weights) != len(urls):
            raise ValueError(
                f"API_BASE_WEIGHTS has {len(weights)} entries but "
                f"API_BASE_URLS has {len(urls)}; they must match"
            )
        return weights


settings = Settings()

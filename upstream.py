import logging
import random

import httpx

logger = logging.getLogger(__name__)


class LoadBalancedClient:
    _LOG_INTERVAL = 50  # Log distribution every N requests

    def __init__(self, urls: list[str], timeout: float = 300.0):
        self._urls = list(urls)
        self._clients: dict[str, httpx.AsyncClient] = {}
        for url in self._urls:
            self._clients[url] = httpx.AsyncClient(
                base_url=url,
                timeout=timeout,
            )
        self._timeout = timeout
        self._request_counts: dict[str, int] = {url: 0 for url in self._urls}
        self._total_requests = 0

    @property
    def urls(self) -> list[str]:
        return list(self._urls)

    def _log_distribution(self):
        if self._total_requests == 0:
            return
        parts = []
        for url in self._urls:
            count = self._request_counts.get(url, 0)
            pct = (count / self._total_requests) * 100
            parts.append(f"{url}: {count} ({pct:.1f}%)")
        logger.info(
            "Upstream usage distribution after %d requests: %s",
            self._total_requests, " | ".join(parts)
        )

    async def get(self, path: str, *, params=None) -> tuple[httpx.Response, str]:
        """Perform a GET against a randomly chosen upstream.

        Returns (response, chosen_upstream_url) on success.
        The returned upstream URL can be used for visibility (e.g. X-Upstream-Used header).

        Selection is deliberately pure random on every call (random.choice).
        There is no stickiness or prioritization between servers — every request
        (search or detail) is routed independently to one server.

        If the chosen server fails for this request, the error is raised immediately.
        There is no retry or failover to other servers for the same request.
        """
        url = random.choice(self._urls)
        client = self._clients[url]

        # Track usage for distribution logging
        self._request_counts[url] += 1
        self._total_requests += 1
        if self._total_requests % self._LOG_INTERVAL == 0:
            self._log_distribution()

        try:
            response = await client.get(path, params=params)
            response.raise_for_status()
            logger.info("Proxy using upstream %s for %s", url, path)
            return response, url
        except Exception as exc:
            logger.warning("Upstream %s failed for %s: %s", url, path, exc)
            raise

    async def aclose(self):
        for client in self._clients.values():
            await client.aclose()

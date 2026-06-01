import logging
import random

import httpx

logger = logging.getLogger(__name__)


def is_degraded_inserate_search(data: dict) -> bool:
    """True when upstream claims success but produced no usable search cards."""
    if not data.get("success"):
        return False
    results = data.get("results")
    if results and len(results) > 0:
        return False
    pm = data.get("performance_metrics") or {}
    pages_ok = pm.get("pages_successful")
    if pages_ok is not None and int(pages_ok) == 0:
        return True
    return len(results or []) == 0


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

    def _track_request(self, url: str):
        self._request_counts[url] += 1
        self._total_requests += 1
        if self._total_requests % self._LOG_INTERVAL == 0:
            self._log_distribution()

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
        self._track_request(url)

        try:
            response = await client.get(path, params=params)
            response.raise_for_status()
            logger.info("Proxy using upstream %s for %s", url, path)
            return response, url
        except Exception as exc:
            logger.warning("Upstream %s failed for %s: %s", url, path, exc)
            raise

    async def get_inserate_with_failover(
        self, path: str = "/inserate", *, params=None
    ) -> tuple[httpx.Response, str]:
        """GET ``/inserate`` with failover when an upstream returns a degraded empty result.

        Tries each configured upstream (shuffled order) until one returns listings or
        all have been attempted.  HTTP errors also advance to the next upstream.
        Detail routes should keep using ``get()`` (single random pick per listing).
        """
        if path != "/inserate":
            return await self.get(path, params=params)

        if len(self._urls) == 1:
            return await self.get(path, params=params)

        urls = list(self._urls)
        random.shuffle(urls)
        last_response: httpx.Response | None = None
        last_url: str | None = None
        errors: list[str] = []

        for url in urls:
            client = self._clients[url]
            self._track_request(url)
            try:
                response = await client.get(path, params=params)
                response.raise_for_status()
                data = response.json()
                last_response, last_url = response, url
                if not is_degraded_inserate_search(data):
                    logger.info(
                        "Proxy using upstream %s for %s (%d results)",
                        url, path, len(data.get("results") or []),
                    )
                    return response, url
                logger.warning(
                    "Upstream %s returned degraded empty search for %s "
                    "(success=%s, results=%d, pages_successful=%s), trying next upstream",
                    url,
                    path,
                    data.get("success"),
                    len(data.get("results") or []),
                    (data.get("performance_metrics") or {}).get("pages_successful"),
                )
            except Exception as exc:
                errors.append(f"{url}: {exc}")
                logger.warning("Upstream %s failed for %s: %s", url, path, exc)

        if last_response is not None and last_url is not None:
            logger.warning(
                "All upstreams returned degraded empty search for %s; using last response from %s",
                path, last_url,
            )
            return last_response, last_url

        detail = "; ".join(errors) if errors else "no upstreams configured"
        raise httpx.HTTPError(f"All upstreams failed for {path}: {detail}")

    async def aclose(self):
        for client in self._clients.values():
            await client.aclose()
import logging
import random
from collections import deque

import httpx

logger = logging.getLogger(__name__)


class LoadBalancedClient:
    _LOG_INTERVAL = 50  # Log distribution every N requests
    _HISTORY_SIZE = 100  # Rolling window of attempts per upstream

    def __init__(
        self,
        urls: list[str],
        timeout: float = 300.0,
        *,
        history_size: int = _HISTORY_SIZE,
    ):
        self._urls = list(urls)
        self._clients: dict[str, httpx.AsyncClient] = {}
        for url in self._urls:
            self._clients[url] = httpx.AsyncClient(
                base_url=url,
                timeout=timeout,
            )
        self._timeout = timeout
        self._history_size = history_size
        # Optimistic prior: each upstream starts with a full window of
        # successes so selection is 50/50 (or 1/N) until real outcomes
        # displace them.  Avoids one lucky first pick monopolizing traffic.
        self._outcomes: dict[str, deque[bool]] = {
            url: deque([True] * history_size, maxlen=history_size)
            for url in self._urls
        }
        self._request_counts: dict[str, int] = {url: 0 for url in self._urls}
        self._total_requests = 0

    @property
    def urls(self) -> list[str]:
        return list(self._urls)

    def _success_count(self, url: str) -> int:
        return sum(1 for ok in self._outcomes[url] if ok)

    def _pick_upstream(self) -> str:
        """Weighted pick: P(i) = successes_i / sum(successes_j).

        Each upstream keeps the last ``history_size`` attempt outcomes
        (success or failure). Only successes contribute weight. Histories
        are initialised to all-success so new proxies start with equal
        weights per upstream.
        """
        if len(self._urls) == 1:
            return self._urls[0]

        success_counts = {url: self._success_count(url) for url in self._urls}
        weights = [success_counts[url] for url in self._urls]
        return random.choices(self._urls, weights=weights, k=1)[0]

    def _record_outcome(self, url: str, success: bool) -> None:
        self._outcomes[url].append(success)

    def _log_distribution(self):
        if self._total_requests == 0:
            return
        parts = []
        for url in self._urls:
            count = self._request_counts.get(url, 0)
            pct = (count / self._total_requests) * 100
            successes = self._success_count(url)
            attempts = len(self._outcomes[url])
            parts.append(
                f"{url}: {count} picks ({pct:.1f}%), "
                f"window {successes}/{attempts} ok"
            )
        logger.info(
            "Upstream usage distribution after %d requests: %s",
            self._total_requests,
            " | ".join(parts),
        )

    async def get(self, path: str, *, params=None) -> tuple[httpx.Response, str]:
        """Perform a GET against a success-weighted upstream.

        Returns (response, chosen_upstream_url) on success.
        The returned upstream URL can be used for visibility (e.g. X-Upstream-Used header).

        Selection uses the last ``history_size`` attempt outcomes per upstream.
        Probability of picking upstream *i* is::

            successes_i / sum(successes_j for all j)

        where *successes* counts HTTP 2xx completions in that upstream's window.
        Failed attempts are recorded but add no weight. Each upstream's window
        is pre-filled with successes so traffic starts evenly split.

        If the chosen server fails for this request, the error is raised immediately.
        There is no retry or failover to other servers for the same request.
        """
        url = self._pick_upstream()
        client = self._clients[url]

        self._request_counts[url] += 1
        self._total_requests += 1
        if self._total_requests % self._LOG_INTERVAL == 0:
            self._log_distribution()

        try:
            response = await client.get(path, params=params)
            response.raise_for_status()
            self._record_outcome(url, True)
            logger.info("Proxy using upstream %s for %s", url, path)
            return response, url
        except Exception as exc:
            self._record_outcome(url, False)
            logger.warning("Upstream %s failed for %s: %s", url, path, exc)
            raise

    async def aclose(self):
        for client in self._clients.values():
            await client.aclose()
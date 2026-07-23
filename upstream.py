import logging
import random
from collections import deque

import httpx

logger = logging.getLogger(__name__)


class LoadBalancedClient:
    _LOG_INTERVAL = 50  # Log distribution every N requests
    _HISTORY_SIZE = 100  # Rolling window of attempts per upstream
    # Floor so a fully-failed worker is still probed and can recover.
    # Clamped to 1/N when there are more than 1/min_p upstreams.
    _MIN_PICK_PROBABILITY = 0.05
    # Fail fast on dead hosts; keep a generous read budget for scrapes.
    _CONNECT_TIMEOUT = 10.0
    _READ_TIMEOUT = 300.0

    def __init__(
        self,
        urls: list[str],
        timeout: float = 300.0,
        *,
        history_size: int = _HISTORY_SIZE,
        min_pick_probability: float = _MIN_PICK_PROBABILITY,
        connect_timeout: float = _CONNECT_TIMEOUT,
    ):
        self._urls = list(urls)
        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._history_size = history_size
        self._min_pick_probability = min_pick_probability
        # httpx: short connect timeout so offline workers don't block for
        # the full scrape budget; read/write use the caller's timeout.
        client_timeout = httpx.Timeout(
            connect=connect_timeout,
            read=timeout,
            write=timeout,
            pool=connect_timeout,
        )
        self._clients: dict[str, httpx.AsyncClient] = {}
        for url in self._urls:
            self._clients[url] = httpx.AsyncClient(
                base_url=url,
                timeout=client_timeout,
            )
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

    def _failure_count(self, url: str) -> int:
        return sum(1 for ok in self._outcomes[url] if not ok)

    def _effective_min_pick_probability(self) -> float:
        """Per-upstream floor, clamped so N * min_p never exceeds 1."""
        n = len(self._urls)
        if n == 0:
            return 0.0
        return min(self._min_pick_probability, 1.0 / n)

    def _probabilities(self) -> dict[str, float]:
        """Selection probabilities matching ``_pick_upstream`` weights.

        Success-weighted share of the residual mass after reserving a
        minimum pick probability for every upstream::

            P(i) = min_p + (1 - N*min_p) * successes_i / sum(successes)

        When every upstream has zero successes (or only one upstream),
        traffic is split evenly (1/N). The floor keeps a long-failing
        worker from sticking at 0% and never being retried.
        """
        n = len(self._urls)
        if n == 0:
            return {}
        if n == 1:
            return {self._urls[0]: 1.0}

        min_p = self._effective_min_pick_probability()
        success_counts = {url: self._success_count(url) for url in self._urls}
        total = sum(success_counts.values())
        if total == 0:
            equal = 1.0 / n
            return {url: equal for url in self._urls}

        residual = 1.0 - n * min_p
        return {
            url: min_p + residual * (success_counts[url] / total)
            for url in self._urls
        }

    def stats(self) -> dict:
        """Snapshot of sliding-window outcomes and current pick probabilities.

        Intended for admin dashboards (hunter admin panel) and ops probes.
        """
        probs = self._probabilities()
        min_p = self._effective_min_pick_probability()
        upstreams = []
        for url in self._urls:
            successes = self._success_count(url)
            failures = self._failure_count(url)
            window = len(self._outcomes[url])
            upstreams.append(
                {
                    "url": url,
                    "successes": successes,
                    "failures": failures,
                    "window_size": window,
                    "history_size": self._history_size,
                    "probability": probs[url],
                    "pick_count": self._request_counts.get(url, 0),
                }
            )
        return {
            "history_size": self._history_size,
            "min_pick_probability": min_p,
            "total_requests": self._total_requests,
            "upstreams": upstreams,
        }

    def _pick_upstream(self) -> str:
        """Weighted pick with a per-upstream minimum probability floor.

        Each upstream keeps the last ``history_size`` attempt outcomes
        (success or failure). Success counts set relative weight; a
        configurable floor (default 5%) ensures every upstream keeps
        receiving some traffic so it can recover after a long failure run.
        Histories are initialised to all-success so new proxies start even.
        """
        if len(self._urls) == 1:
            return self._urls[0]

        probs = self._probabilities()
        weights = [probs[url] for url in self._urls]
        return random.choices(self._urls, weights=weights, k=1)[0]

    def _record_outcome(self, url: str, success: bool) -> None:
        self._outcomes[url].append(success)

    def _log_distribution(self):
        if self._total_requests == 0:
            return
        parts = []
        probs = self._probabilities()
        for url in self._urls:
            count = self._request_counts.get(url, 0)
            pct = (count / self._total_requests) * 100
            successes = self._success_count(url)
            attempts = len(self._outcomes[url])
            parts.append(
                f"{url}: {count} picks ({pct:.1f}%), "
                f"window {successes}/{attempts} ok, "
                f"P={probs[url]*100:.1f}%"
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

            min_p + (1 - N*min_p) * successes_i / sum(successes_j)

        where *successes* counts HTTP 2xx completions in that upstream's window
        and *min_p* defaults to 5% (clamped to 1/N). Failed attempts are
        recorded but add no success weight. Each upstream's window is
        pre-filled with successes so traffic starts evenly split.

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

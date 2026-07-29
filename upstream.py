import logging
import random
from collections import deque

import httpx

logger = logging.getLogger(__name__)

# Admin seed UI offers this step size (0%, 5%, …, 100%).
PROBABILITY_STEP = 0.05


class LoadBalancedClient:
    _LOG_INTERVAL = 50  # Log distribution every N requests
    _HISTORY_SIZE = 100  # Rolling window of attempts per upstream
    # Fail fast on dead hosts; keep a generous read budget for scrapes.
    _CONNECT_TIMEOUT = 10.0

    def __init__(
        self,
        urls: list[str],
        timeout: float = 300.0,
        *,
        history_size: int = _HISTORY_SIZE,
        connect_timeout: float = _CONNECT_TIMEOUT,
    ):
        self._urls = list(urls)
        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._history_size = history_size
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

    def _probabilities(self) -> dict[str, float]:
        """Selection probabilities matching ``_pick_upstream`` weights.

        P(i) = successes_i / sum(successes_j). When every upstream has
        zero successes in its window the picker uses equal shares (1/N)
        so dashboards stay well-defined and random.choices stays valid.
        """
        if not self._urls:
            return {}
        success_counts = {url: self._success_count(url) for url in self._urls}
        total = sum(success_counts.values())
        if total == 0:
            equal = 1.0 / len(self._urls)
            return {url: equal for url in self._urls}
        return {url: success_counts[url] / total for url in self._urls}

    def stats(self) -> dict:
        """Snapshot of sliding-window outcomes and current pick probabilities.

        Each upstream includes ``outcomes``: ordered booleans (oldest → newest)
        so the admin UI can render a per-attempt timeline (green=ok, red=fail).
        """
        probs = self._probabilities()
        upstreams = []
        for url in self._urls:
            successes = self._success_count(url)
            failures = self._failure_count(url)
            window = len(self._outcomes[url])
            # Oldest first so left side of the bar is the past.
            outcomes = list(self._outcomes[url])
            upstreams.append(
                {
                    "url": url,
                    "successes": successes,
                    "failures": failures,
                    "window_size": window,
                    "history_size": self._history_size,
                    "probability": probs[url],
                    "pick_count": self._request_counts.get(url, 0),
                    "outcomes": outcomes,
                }
            )
        return {
            "history_size": self._history_size,
            "total_requests": self._total_requests,
            "upstreams": upstreams,
        }

    def _fill_window(self, url: str, successes: int) -> None:
        """Replace the sliding window with ``successes`` ok + rest fail.

        Ordering: failures first (older), successes last (newer) so a
        recovered worker's timeline shows green at the right edge.
        """
        h = self._history_size
        successes = max(0, min(h, int(successes)))
        failures = h - successes
        self._outcomes[url] = deque(
            [False] * failures + [True] * successes,
            maxlen=h,
        )

    @staticmethod
    def _normalize_rate(rate: float, *, name: str = "rate") -> float:
        """Validate and snap to the 5% admin step grid."""
        p = float(rate)
        if p < 0.0 or p > 1.0:
            raise ValueError(f"{name} must be between 0 and 1 inclusive")
        steps = round(p / PROBABILITY_STEP)
        snapped = steps * PROBABILITY_STEP
        if abs(p - snapped) > 1e-9:
            raise ValueError(
                f"{name} must be a multiple of {PROBABILITY_STEP:g} (got {rate})"
            )
        return round(snapped, 10)

    def seed_window_fail_rate(self, url: str, fail_rate: float) -> dict:
        """Rewrite one upstream's window to a given failure rate.

        ``fail_rate`` is the fraction of the sliding window that should be
        failures (0 = all success, 1 = all fail), in 5% steps. Only this
        worker's window is touched — other workers are unchanged. Pick
        probability then follows from relative success counts as usual.

        Returns the usual ``stats()`` payload plus a ``seeded`` diagnostic.
        """
        if url not in self._outcomes:
            raise KeyError(f"unknown upstream: {url}")

        rate = self._normalize_rate(fail_rate, name="fail_rate")
        h = self._history_size
        failures = int(round(rate * h))
        successes = h - failures
        self._fill_window(url, successes)

        window_fail = failures / h if h else 0.0
        actual_pick = self._probabilities()[url]
        logger.info(
            "Seeded upstream %s to fail_rate=%.0f%% "
            "(window %d ok / %d fail); pick P=%.1f%%",
            url,
            rate * 100,
            successes,
            failures,
            actual_pick * 100,
        )
        result = self.stats()
        result["seeded"] = {
            "url": url,
            "requested_fail_rate": rate,
            "actual_fail_rate": window_fail,
            "actual_probability": actual_pick,
            "successes": successes,
            "failures": failures,
        }
        return result

    def _pick_upstream(self) -> str:
        """Weighted pick: P(i) = successes_i / sum(successes_j).

        Each upstream keeps the last ``history_size`` attempt outcomes
        (success or failure). Only successes contribute weight. Histories
        are initialised to all-success so new proxies start with equal
        weights per upstream. When every window is all-fail, pick uniformly.
        """
        if len(self._urls) == 1:
            return self._urls[0]

        success_counts = {url: self._success_count(url) for url in self._urls}
        total = sum(success_counts.values())
        if total == 0:
            return random.choice(self._urls)
        weights = [success_counts[url] for url in self._urls]
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

            successes_i / sum(successes_j for all j)

        where *successes* counts HTTP 2xx completions in that upstream's window.
        Failed attempts are recorded but add no weight. Each upstream's window
        is pre-filled with successes so traffic starts evenly split.

        Admins can reseed a window via ``seed_window_fail_rate`` (proxy
        ``POST /upstream-seed``) so a recovered worker's history is not
        stuck at all-failures after an outage.

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

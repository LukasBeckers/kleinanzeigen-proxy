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

        Intended for admin dashboards (hunter admin panel) and ops probes.
        """
        probs = self._probabilities()
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
            "total_requests": self._total_requests,
            "upstreams": upstreams,
        }

    def _fill_window(self, url: str, successes: int) -> None:
        """Replace the sliding window with ``successes`` ok + rest fail."""
        h = self._history_size
        successes = max(0, min(h, int(successes)))
        failures = h - successes
        self._outcomes[url] = deque(
            [True] * successes + [False] * failures,
            maxlen=h,
        )

    @staticmethod
    def _normalize_probability(probability: float) -> float:
        """Validate and snap to the 5% admin step grid."""
        p = float(probability)
        if p < 0.0 or p > 1.0:
            raise ValueError("probability must be between 0 and 1 inclusive")
        steps = round(p / PROBABILITY_STEP)
        snapped = steps * PROBABILITY_STEP
        # Guard float noise (e.g. 0.1+0.2) while rejecting off-grid values.
        if abs(p - snapped) > 1e-9:
            raise ValueError(
                f"probability must be a multiple of {PROBABILITY_STEP:g} "
                f"(got {probability})"
            )
        # Avoid 0.30000000000000004 style drift in responses.
        return round(snapped, 10)

    def seed_pick_probability(self, url: str, probability: float) -> dict:
        """Rewrite windows so ``url`` has approximately ``probability`` pick weight.

        Fills the target upstream's sliding window with a success/failure mix
        that yields the requested share under success-weighted selection
        (P = successes_i / sum successes). Other upstreams are left alone
        when they already contribute success mass. Special cases:

        * ``probability == 0`` → all failures for this worker.
        * ``probability == 1`` → all successes here, all failures on others
          (exclusive traffic).
        * Other workers at 0 successes and ``0 < p < 1`` → residual success
          mass is seeded evenly across the others so the ratio is realisable.

        Returns the usual ``stats()`` payload plus a ``seeded`` diagnostic.
        """
        if url not in self._outcomes:
            raise KeyError(f"unknown upstream: {url}")

        p = self._normalize_probability(probability)
        h = self._history_size
        others = [u for u in self._urls if u != url]

        if p <= 0.0:
            self._fill_window(url, 0)
        elif p >= 1.0 or not others:
            self._fill_window(url, h)
            for o in others:
                self._fill_window(o, 0)
        else:
            s_other = sum(self._success_count(o) for o in others)
            if s_other == 0:
                # No weight elsewhere — seed residual successes on others so
                # P(target) ≈ p rather than collapsing to 100%.
                target_s = max(1, min(h, round(p * h)))
                residual = max(1, round((1.0 - p) * h))
                self._fill_window(url, target_s)
                base, rem = divmod(residual, len(others))
                for i, o in enumerate(others):
                    self._fill_window(o, min(h, base + (1 if i < rem else 0)))
            else:
                # s / (s + s_other) = p  =>  s = p/(1-p) * s_other
                raw = p * s_other / (1.0 - p)
                target_s = max(0, min(h, int(round(raw))))
                self._fill_window(url, target_s)

        actual = self._probabilities()[url]
        logger.info(
            "Seeded upstream %s to requested pick probability %.0f%% "
            "(window %d/%d ok); actual P=%.1f%%",
            url,
            p * 100,
            self._success_count(url),
            h,
            actual * 100,
        )
        result = self.stats()
        result["seeded"] = {
            "url": url,
            "requested_probability": p,
            "actual_probability": actual,
            "successes": self._success_count(url),
            "failures": self._failure_count(url),
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

        Admins can reseed a window via ``seed_pick_probability`` (proxy
        ``POST /upstream-seed``) so a recovered worker can re-enter at a
        chosen pick share without waiting for the failure history to age out.

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

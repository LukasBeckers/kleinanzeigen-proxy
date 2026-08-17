import logging
import random
import time
from collections import deque
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

# Admin seed UI offers this step size (0%, 5%, …, 100%).
PROBABILITY_STEP = 0.05

# Headers the scraper may attach so the admin UI can show recycle state
# without an extra round-trip to a possibly hung worker.
HEADER_RECYCLE_EVERY = "x-recycle-every"
HEADER_REQUESTS_SINCE_RECYCLE = "x-requests-since-recycle"
HEADER_RECYCLE_COUNT = "x-recycle-count"


@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    """One proxy→worker attempt in the sliding window."""

    ok: bool
    duration_s: float | None = None
    error: str | None = None

    def to_json(self) -> dict:
        return {
            "ok": self.ok,
            "duration_s": self.duration_s,
            "error": self.error,
        }


def _synthetic_outcome(ok: bool, *, error: str | None = None) -> AttemptOutcome:
    """Window filler (optimistic prior or admin seed) — no real timing."""
    return AttemptOutcome(ok=ok, duration_s=None, error=error)


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
        weights: list[float] | None = None,
    ):
        self._urls = list(urls)
        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._history_size = history_size
        if weights is None:
            weight_list = [1.0] * len(self._urls)
        else:
            if len(weights) != len(self._urls):
                raise ValueError(
                    f"weights length {len(weights)} != urls length {len(self._urls)}"
                )
            weight_list = [float(w) for w in weights]
            if any(w < 0 for w in weight_list):
                raise ValueError("weights must be >= 0")
        self._weights: dict[str, float] = {
            url: weight_list[i] for i, url in enumerate(self._urls)
        }
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
        self._outcomes: dict[str, deque[AttemptOutcome]] = {
            url: deque(
                [_synthetic_outcome(True) for _ in range(history_size)],
                maxlen=history_size,
            )
            for url in self._urls
        }
        self._request_counts: dict[str, int] = {url: 0 for url in self._urls}
        self._total_requests = 0
        # Last recycle stats seen on a worker response (headers or setter).
        self._browser_info: dict[str, dict] = {url: {} for url in self._urls}

    @property
    def urls(self) -> list[str]:
        return list(self._urls)

    def _success_count(self, url: str) -> int:
        return sum(1 for o in self._outcomes[url] if o.ok)

    def _failure_count(self, url: str) -> int:
        return sum(1 for o in self._outcomes[url] if not o.ok)

    def _weighted_score(self, url: str) -> float:
        """successes × configured multiplicative weight."""
        return self._success_count(url) * self._weights[url]

    def _probabilities(self) -> dict[str, float]:
        """Selection probabilities matching ``_pick_upstream`` weights.

        P(i) = (successes_i · weight_i) / sum_j(successes_j · weight_j).

        When every upstream has zero successes, fall back to pure configured
        weights (or equal 1/N if all weights are zero) so dashboards and
        random.choices stay well-defined.
        """
        if not self._urls:
            return {}
        scores = {url: self._weighted_score(url) for url in self._urls}
        total = sum(scores.values())
        if total > 0:
            return {url: scores[url] / total for url in self._urls}
        # No successes in any window — use configured weights alone.
        wsum = sum(self._weights[url] for url in self._urls)
        if wsum > 0:
            return {url: self._weights[url] / wsum for url in self._urls}
        equal = 1.0 / len(self._urls)
        return {url: equal for url in self._urls}

    def set_weight(self, url: str, weight: float) -> dict:
        """Update the multiplicative pick weight for one upstream."""
        if url not in self._weights:
            raise KeyError(f"unknown upstream: {url}")
        w = float(weight)
        if w < 0:
            raise ValueError("weight must be >= 0")
        self._weights[url] = w
        logger.info("Set upstream %s weight=%.4g; pick P=%.1f%%",
                    url, w, self._probabilities()[url] * 100)
        result = self.stats()
        result["weight_updated"] = {"url": url, "weight": w}
        return result

    def stats(self) -> dict:
        """Snapshot of sliding-window outcomes and current pick probabilities.

        Each upstream includes ``outcomes``: ordered attempt records
        (oldest → newest) so the admin UI can render a per-attempt timeline
        (green=ok, red=fail, bar height ∝ log duration), plus the configured
        multiplicative ``weight`` and last-seen recycle stats.
        """
        probs = self._probabilities()
        upstreams = []
        for url in self._urls:
            successes = self._success_count(url)
            failures = self._failure_count(url)
            window = len(self._outcomes[url])
            # Oldest first so left side of the bar is the past.
            outcomes = [o.to_json() for o in self._outcomes[url]]
            info = self._browser_info.get(url) or {}
            upstreams.append(
                {
                    "url": url,
                    "successes": successes,
                    "failures": failures,
                    "window_size": window,
                    "history_size": self._history_size,
                    "weight": self._weights[url],
                    "probability": probs[url],
                    "pick_count": self._request_counts.get(url, 0),
                    "outcomes": outcomes,
                    "recycle_every": info.get("recycle_every"),
                    "requests_since_recycle": info.get("requests_since_recycle"),
                    "recycle_count": info.get("recycle_count"),
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
            [_synthetic_outcome(False, error="seeded") for _ in range(failures)]
            + [_synthetic_outcome(True, error="seeded") for _ in range(successes)],
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

    def seed_window_success_rate(self, url: str, success_rate: float) -> dict:
        """Rewrite one upstream's window to a given success rate.

        ``success_rate`` is the fraction of the sliding window that should be
        successes (0 = all fail, 1 = all success), in 5% steps. Only this
        worker's window is touched — other workers are unchanged. Pick
        probability then follows from relative success counts as usual.

        Returns the usual ``stats()`` payload plus a ``seeded`` diagnostic.
        """
        if url not in self._outcomes:
            raise KeyError(f"unknown upstream: {url}")

        rate = self._normalize_rate(success_rate, name="success_rate")
        h = self._history_size
        successes = int(round(rate * h))
        failures = h - successes
        self._fill_window(url, successes)

        window_success = successes / h if h else 0.0
        actual_pick = self._probabilities()[url]
        logger.info(
            "Seeded upstream %s to success_rate=%.0f%% "
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
            "requested_success_rate": rate,
            "actual_success_rate": window_success,
            "actual_probability": actual_pick,
            "successes": successes,
            "failures": failures,
        }
        return result

    def _pick_upstream(self) -> str:
        """Weighted pick: P(i) = (successes_i · weight_i) / Σ (successes_j · weight_j).

        Each upstream keeps the last ``history_size`` attempt outcomes
        (success or failure). Successes contribute weight, scaled by the
        configured multiplicative ``weight`` (default 1). Histories are
        initialised to all-success so new proxies start even (modulo weights).
        When every window is all-fail, pick by configured weights alone.
        """
        if len(self._urls) == 1:
            return self._urls[0]

        probs = self._probabilities()
        weights = [probs[url] for url in self._urls]
        if sum(weights) <= 0:
            return random.choice(self._urls)
        return random.choices(self._urls, weights=weights, k=1)[0]

    def _record_outcome(
        self,
        url: str,
        *,
        ok: bool,
        duration_s: float | None,
        error: str | None = None,
    ) -> None:
        self._outcomes[url].append(
            AttemptOutcome(ok=ok, duration_s=duration_s, error=error)
        )

    def _remember_browser_info(self, url: str, response: httpx.Response) -> None:
        headers = response.headers
        info = self._browser_info.setdefault(url, {})
        every = headers.get(HEADER_RECYCLE_EVERY)
        since = headers.get(HEADER_REQUESTS_SINCE_RECYCLE)
        count = headers.get(HEADER_RECYCLE_COUNT)
        if every is not None:
            try:
                info["recycle_every"] = int(every)
            except ValueError:
                pass
        if since is not None:
            try:
                info["requests_since_recycle"] = int(since)
            except ValueError:
                pass
        if count is not None:
            try:
                info["recycle_count"] = int(count)
            except ValueError:
                pass

    def _apply_browser_metrics(self, url: str, metrics: dict) -> None:
        info = self._browser_info.setdefault(url, {})
        for key in ("recycle_every", "requests_since_recycle", "recycle_count"):
            if key in metrics and metrics[key] is not None:
                info[key] = int(metrics[key])

    async def set_recycle_every(self, url: str, recycle_every: int) -> dict:
        """Tell one worker to recycle Chromium every ``recycle_every`` scrapes."""
        if url not in self._clients:
            raise KeyError(f"unknown upstream: {url}")
        n = int(recycle_every)
        if n < 1:
            raise ValueError("recycle_every must be >= 1")
        client = self._clients[url]
        response = await client.post(
            "/browser/recycle-every",
            json={"recycle_every": n},
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except Exception:
            payload = {}
        browser = payload.get("browser") if isinstance(payload, dict) else None
        if isinstance(browser, dict):
            self._apply_browser_metrics(url, browser)
        else:
            self._remember_browser_info(url, response)
        self._browser_info.setdefault(url, {})["recycle_every"] = n
        logger.info("Set upstream %s recycle_every=%d", url, n)
        result = self.stats()
        result["recycle_updated"] = {"url": url, "recycle_every": n}
        return result

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

            (successes_i · weight_i) / sum_j(successes_j · weight_j)

        where *successes* counts HTTP 2xx completions in that upstream's window
        and *weight* is a configurable multiplicative bias (default 1).
        Failed attempts are recorded but add no success mass. Each upstream's
        window is pre-filled with successes so traffic starts evenly split
        (modulo weights).

        Admins can reseed a window via ``seed_window_success_rate`` (proxy
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

        started = time.perf_counter()
        try:
            response = await client.get(path, params=params)
            duration_s = time.perf_counter() - started
            response.raise_for_status()
            self._record_outcome(url, ok=True, duration_s=duration_s)
            self._remember_browser_info(url, response)
            logger.info("Proxy using upstream %s for %s", url, path)
            return response, url
        except Exception as exc:
            duration_s = time.perf_counter() - started
            self._record_outcome(
                url,
                ok=False,
                duration_s=duration_s,
                error=str(exc) or type(exc).__name__,
            )
            logger.warning("Upstream %s failed for %s: %s", url, path, exc)
            raise

    async def aclose(self):
        for client in self._clients.values():
            await client.aclose()

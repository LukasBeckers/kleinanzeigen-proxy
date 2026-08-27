import random

import httpx
import pytest

from collections import deque

from upstream import AttemptOutcome, LoadBalancedClient, NoEligibleUpstream


def _ok(**kwargs) -> AttemptOutcome:
    return AttemptOutcome(ok=True, **kwargs)


def _fail(**kwargs) -> AttemptOutcome:
    return AttemptOutcome(ok=False, **kwargs)


def _flags(*oks: bool) -> list[AttemptOutcome]:
    return [AttemptOutcome(ok=ok) for ok in oks]


class TestLoadBalancedClient:
    @pytest.mark.asyncio
    async def test_returns_response_on_first_success(self):
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True})

        client = LoadBalancedClient(["http://a:8000"], timeout=1.0)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler),
        )

        resp, upstream = await client.get("/inserate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert upstream in client.urls

        await client.aclose()

    @pytest.mark.asyncio
    async def test_raises_immediately_on_failure_no_cross_server_retry(self):
        """A failure on the chosen server must raise immediately."""
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("server down")

        urls = ["http://a:8000", "http://b:8000"]
        client = LoadBalancedClient(urls, timeout=1.0)
        for url in urls:
            client._clients[url] = httpx.AsyncClient(
                base_url=url,
                transport=httpx.MockTransport(handler),
            )

        with pytest.raises(httpx.ConnectError):
            await client.get("/inserate")

        await client.aclose()

    @pytest.mark.asyncio
    async def test_forwards_params_to_upstream(self):
        received_params = []

        def handler(req: httpx.Request) -> httpx.Response:
            received_params.append(dict(req.url.params))
            return httpx.Response(200, json={"success": True})

        client = LoadBalancedClient(["http://a:8000"], timeout=1.0)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler),
        )

        resp, upstream = await client.get("/inserate", params={"query": "bmw", "radius": 50})

        assert len(received_params) == 1
        assert received_params[0]["query"] == "bmw"
        assert received_params[0]["radius"] == "50"
        assert upstream in client.urls

        await client.aclose()

    @pytest.mark.asyncio
    async def test_optimistic_prior_splits_evenly_before_real_outcomes(self):
        """Pre-filled success histories give ~50/50 before any live traffic."""
        client = LoadBalancedClient(["http://a:8000", "http://b:8000"], timeout=1.0)
        assert client._success_count("http://a:8000") == 100
        assert client._success_count("http://b:8000") == 100
        random.seed(1)
        picks = [client._pick_upstream() for _ in range(1000)]
        a = picks.count("http://a:8000")
        b = picks.count("http://b:8000")
        assert 400 < a < 600
        assert 400 < b < 600
        await client.aclose()

    @pytest.mark.asyncio
    async def test_equal_success_counts_split_evenly(self):
        """When both upstreams have the same success count, picks are ~50/50."""
        call_counts = {"http://a:8000": 0, "http://b:8000": 0}

        def make_handler(url: str):
            def h(req: httpx.Request) -> httpx.Response:
                call_counts[url] += 1
                return httpx.Response(200, json={"success": True})
            return h

        urls = ["http://a:8000", "http://b:8000"]
        client = LoadBalancedClient(urls, timeout=1.0)
        for url in urls:
            client._clients[url] = httpx.AsyncClient(
                base_url=url,
                transport=httpx.MockTransport(make_handler(url)),
            )

        client._outcomes["http://a:8000"].extend([_ok() for _ in range(50)])
        client._outcomes["http://b:8000"].extend([_ok() for _ in range(50)])

        random.seed(12345)
        n_calls = 200
        for _ in range(n_calls):
            await client.get("/inserate")

        assert call_counts["http://a:8000"] > 10
        assert call_counts["http://b:8000"] > 10
        assert call_counts["http://a:8000"] + call_counts["http://b:8000"] == n_calls

        await client.aclose()

    @pytest.mark.asyncio
    async def test_success_weighted_prefers_reliable_upstream(self):
        """After history is seeded, picks should follow success weights."""
        call_counts = {"http://a:8000": 0, "http://b:8000": 0}

        def handler_a(req: httpx.Request) -> httpx.Response:
            call_counts["http://a:8000"] += 1
            return httpx.Response(200, json={"success": True})

        def handler_b(req: httpx.Request) -> httpx.Response:
            call_counts["http://b:8000"] += 1
            raise httpx.ConnectError("unreliable")

        urls = ["http://a:8000", "http://b:8000"]
        client = LoadBalancedClient(
            urls, timeout=1.0, history_size=100, max_fail_rate=1.0
        )
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler_a),
        )
        client._clients["http://b:8000"] = httpx.AsyncClient(
            base_url="http://b:8000",
            transport=httpx.MockTransport(handler_b),
        )

        # Seed: A 80 ok, B 20 ok → ~80% traffic to A under pure success weights.
        client._outcomes["http://a:8000"].clear()
        client._outcomes["http://a:8000"].extend(_flags(*([True] * 80 + [False] * 20)))
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend(_flags(*([True] * 20 + [False] * 80)))

        random.seed(99)
        n_calls = 500
        for _ in range(n_calls):
            try:
                await client.get("/inserate")
            except httpx.ConnectError:
                pass

        a_share = call_counts["http://a:8000"] / n_calls
        assert a_share > 0.70
        assert a_share < 0.98
        assert call_counts["http://a:8000"] + call_counts["http://b:8000"] == n_calls

        await client.aclose()

    @pytest.mark.asyncio
    async def test_failure_recorded_in_history(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("down")

        client = LoadBalancedClient(["http://a:8000"], timeout=1.0)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler),
        )

        with pytest.raises(httpx.ConnectError):
            await client.get("/inserate")

        outcomes = list(client._outcomes["http://a:8000"])
        assert outcomes[-1].ok is False
        assert outcomes[-1].error
        assert sum(1 for o in outcomes if not o.ok) == 1
        assert client._success_count("http://a:8000") == 99

        await client.aclose()

    @pytest.mark.asyncio
    async def test_single_failure_does_not_starve_upstream(self):
        """One failure must not drop an upstream from ~equal share immediately."""
        call_counts = {"http://a:8000": 0, "http://b:8000": 0}

        def handler_a(req: httpx.Request) -> httpx.Response:
            call_counts["http://a:8000"] += 1
            return httpx.Response(200, json={"success": True})

        def handler_b(req: httpx.Request) -> httpx.Response:
            call_counts["http://b:8000"] += 1
            if call_counts["http://b:8000"] == 1:
                raise httpx.ConnectError("transient")
            return httpx.Response(200, json={"success": True})

        urls = ["http://a:8000", "http://b:8000"]
        client = LoadBalancedClient(urls, timeout=1.0, history_size=100)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler_a),
        )
        client._clients["http://b:8000"] = httpx.AsyncClient(
            base_url="http://b:8000",
            transport=httpx.MockTransport(handler_b),
        )

        random.seed(42)
        for _ in range(200):
            try:
                await client.get("/inserate")
            except httpx.ConnectError:
                pass

        b_share = call_counts["http://b:8000"] / 200
        assert b_share > 0.25
        assert call_counts["http://a:8000"] + call_counts["http://b:8000"] <= 200

        await client.aclose()

    def test_stats_optimistic_prior_equal_probabilities(self):
        client = LoadBalancedClient(["http://a:8000", "http://b:8000"], timeout=1.0)
        snap = client.stats()
        assert snap["history_size"] == 100
        assert snap["total_requests"] == 0
        assert len(snap["upstreams"]) == 2
        by_url = {u["url"]: u for u in snap["upstreams"]}
        for url in ("http://a:8000", "http://b:8000"):
            row = by_url[url]
            assert row["successes"] == 100
            assert row["failures"] == 0
            assert row["window_size"] == 100
            assert row["probability"] == pytest.approx(0.5)
            assert row["pick_count"] == 0

    def test_stats_probabilities_follow_success_weights(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=100
        )
        client._outcomes["http://a:8000"].clear()
        client._outcomes["http://a:8000"].extend(_flags(*([True] * 80 + [False] * 20)))
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend(_flags(*([True] * 20 + [False] * 80)))

        snap = client.stats()
        by_url = {u["url"]: u for u in snap["upstreams"]}
        assert by_url["http://a:8000"]["successes"] == 80
        assert by_url["http://a:8000"]["failures"] == 20
        assert by_url["http://b:8000"]["successes"] == 20
        assert by_url["http://b:8000"]["failures"] == 80
        assert by_url["http://a:8000"]["probability"] == pytest.approx(0.8)
        assert by_url["http://b:8000"]["probability"] == pytest.approx(0.2)

    def test_stats_all_failures_uses_equal_probability(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=10
        )
        client._outcomes["http://a:8000"].clear()
        client._outcomes["http://a:8000"].extend(_flags(*([False] * 10)))
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend(_flags(*([False] * 10)))

        snap = client.stats()
        for row in snap["upstreams"]:
            assert row["successes"] == 0
            assert row["failures"] == 10
            assert row["probability"] == pytest.approx(0.5)

    def test_zero_success_worker_has_zero_pick_probability(self):
        """Without admin reseed, a fully-failed upstream has P=0."""
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"],
            timeout=1.0,
            history_size=100,
        )
        client._outcomes["http://a:8000"].clear()
        client._outcomes["http://a:8000"].extend(_flags(*([True] * 100)))
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend(_flags(*([False] * 100)))

        probs = client._probabilities()
        assert probs["http://b:8000"] == pytest.approx(0.0)
        assert probs["http://a:8000"] == pytest.approx(1.0)

    def test_seed_window_success_rate_sets_success_fail_counts(self):
        """Admin reseed sets this worker's window success fraction only."""
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"],
            timeout=1.0,
            history_size=100,
        )
        client._outcomes["http://a:8000"].clear()
        client._outcomes["http://a:8000"].extend(_flags(*([True] * 100)))
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend(_flags(*([False] * 100)))

        # 80% success → 80 ok / 20 fail; pick weight = 80/(100+80) ≈ 44.4%
        result = client.seed_window_success_rate("http://b:8000", 0.80)
        by_url = {u["url"]: u for u in result["upstreams"]}
        assert by_url["http://b:8000"]["successes"] == 80
        assert by_url["http://b:8000"]["failures"] == 20
        assert by_url["http://a:8000"]["successes"] == 100  # untouched
        assert result["seeded"]["requested_success_rate"] == 0.80
        assert result["seeded"]["actual_success_rate"] == pytest.approx(0.80)
        assert by_url["http://b:8000"]["probability"] == pytest.approx(80 / 180)

    def test_seed_window_success_rate_zero_and_full(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=100
        )
        client.seed_window_success_rate("http://b:8000", 1.0)
        assert client._success_count("http://b:8000") == 100
        assert client._failure_count("http://b:8000") == 0

        client.seed_window_success_rate("http://b:8000", 0.0)
        assert client._success_count("http://b:8000") == 0
        assert client._failure_count("http://b:8000") == 100

    def test_seed_rejects_unknown_url_and_off_grid_success_rate(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=100
        )
        with pytest.raises(KeyError):
            client.seed_window_success_rate("http://missing:8000", 0.5)
        with pytest.raises(ValueError, match="multiple of"):
            client.seed_window_success_rate("http://a:8000", 0.33)
        with pytest.raises(ValueError, match="between 0 and 1"):
            client.seed_window_success_rate("http://a:8000", 1.5)

    def test_stats_outcomes_are_oldest_to_newest(self):
        client = LoadBalancedClient(
            ["http://a:8000"], timeout=1.0, history_size=5
        )
        client._outcomes["http://a:8000"].clear()
        # Append in time order: oldest first
        for ok in (True, True, False, True, False):
            client._outcomes["http://a:8000"].append(AttemptOutcome(ok=ok))
        snap = client.stats()
        assert [o["ok"] for o in snap["upstreams"][0]["outcomes"]] == [
            True, True, False, True, False
        ]

    def test_seed_window_orders_failures_older_than_successes(self):
        client = LoadBalancedClient(
            ["http://a:8000"], timeout=1.0, history_size=10
        )
        # 70% success → 7 ok / 3 fail (fails older, successes newer)
        client.seed_window_success_rate("http://a:8000", 0.70)
        outcomes = list(client._outcomes["http://a:8000"])
        assert [o.ok for o in outcomes] == [False] * 3 + [True] * 7
        assert all(o.error == "seeded" for o in outcomes)
        assert all(o.duration_s is None for o in outcomes)

    def test_configured_weight_biases_pick_when_both_healthy(self):
        """Equal success windows + weights 1.5 vs 1 → P = 1.5/2.5 = 0.6."""
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"],
            timeout=1.0,
            history_size=100,
            weights=[1.5, 1.0],
        )
        # Both still at optimistic all-success prior.
        probs = client._probabilities()
        assert probs["http://a:8000"] == pytest.approx(1.5 / 2.5)
        assert probs["http://b:8000"] == pytest.approx(1.0 / 2.5)
        snap = client.stats()
        by_url = {u["url"]: u for u in snap["upstreams"]}
        assert by_url["http://a:8000"]["weight"] == 1.5
        assert by_url["http://b:8000"]["weight"] == 1.0

    def test_set_weight_updates_probabilities(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, weights=[1.0, 1.0]
        )
        result = client.set_weight("http://a:8000", 3.0)
        assert result["weight_updated"]["weight"] == 3.0
        probs = client._probabilities()
        assert probs["http://a:8000"] == pytest.approx(3.0 / 4.0)
        assert probs["http://b:8000"] == pytest.approx(1.0 / 4.0)

    def test_weight_zero_excludes_upstream_from_picks(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, weights=[1.0, 0.0]
        )
        probs = client._probabilities()
        assert probs["http://a:8000"] == pytest.approx(1.0)
        assert probs["http://b:8000"] == pytest.approx(0.0)
        random.seed(0)
        picks = [client._pick_upstream() for _ in range(50)]
        assert all(p == "http://a:8000" for p in picks)

    def test_weights_length_must_match_urls(self):
        with pytest.raises(ValueError, match="weights length"):
            LoadBalancedClient(
                ["http://a:8000", "http://b:8000"], timeout=1.0, weights=[1.0]
            )

    @pytest.mark.asyncio
    async def test_records_duration_and_error_on_failure(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("down")

        client = LoadBalancedClient(["http://a:8000"], timeout=1.0, history_size=5)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler),
        )
        with pytest.raises(httpx.ConnectError):
            await client.get("/inserate")
        last = client._outcomes["http://a:8000"][-1]
        assert last.ok is False
        assert last.duration_s is not None and last.duration_s >= 0
        assert "down" in (last.error or "")
        snap = client.stats()["upstreams"][0]["outcomes"][-1]
        assert snap["ok"] is False
        assert snap["duration_s"] == last.duration_s
        assert "down" in (snap["error"] or "")
        await client.aclose()

    @pytest.mark.asyncio
    async def test_records_duration_on_success_and_browser_headers(self):
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"success": True},
                headers={
                    "X-Recycle-Every": "250",
                    "X-Requests-Since-Recycle": "12",
                    "X-Recycle-Count": "3",
                },
            )

        client = LoadBalancedClient(["http://a:8000"], timeout=1.0, history_size=3)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler),
        )
        await client.get("/inserate")
        last = client._outcomes["http://a:8000"][-1]
        assert last.ok is True
        assert last.duration_s is not None and last.duration_s >= 0
        assert last.error is None
        row = client.stats()["upstreams"][0]
        assert row["recycle_every"] == 250
        assert row["requests_since_recycle"] == 12
        assert row["recycle_count"] == 3
        await client.aclose()

    def test_circuit_trips_above_fail_rate_and_skips_worker(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"],
            timeout=1.0,
            history_size=10,
            max_fail_rate=0.25,
            cooldown_s=60,
        )
        client._outcomes["http://a:8000"] = deque(
            [_fail()] * 3 + [_ok()] * 7, maxlen=10
        )
        client._maybe_trip_circuit("http://a:8000")
        assert client._is_disabled("http://a:8000")
        picks = [client._pick_upstream() for _ in range(30)]
        assert all(p == "http://b:8000" for p in picks)
        assert client.stats()["upstreams"][0]["disabled"] is True

    def test_circuit_does_not_trip_at_threshold(self):
        client = LoadBalancedClient(
            ["http://a:8000"],
            timeout=1.0,
            history_size=4,
            max_fail_rate=0.25,
            cooldown_s=60,
        )
        # 1/4 = 0.25 is allowed; trip only when strictly greater.
        client._outcomes["http://a:8000"] = deque([_fail()] + [_ok()] * 3, maxlen=4)
        client._maybe_trip_circuit("http://a:8000")
        assert not client._is_disabled("http://a:8000")

    def test_all_workers_in_cooldown_raises(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"],
            timeout=1.0,
            history_size=4,
            max_fail_rate=0.0,
            cooldown_s=60,
        )
        import time as time_mod
        now = time_mod.time()
        client._disabled_until["http://a:8000"] = now + 60
        client._disabled_until["http://b:8000"] = now + 60
        with pytest.raises(NoEligibleUpstream):
            client._pick_upstream()

    def test_cooldown_expiry_reseeds_window(self):
        client = LoadBalancedClient(
            ["http://a:8000"],
            timeout=1.0,
            history_size=8,
            max_fail_rate=0.25,
            cooldown_s=60,
        )
        client._outcomes["http://a:8000"] = deque([_fail()] * 8, maxlen=8)
        client._disabled_until["http://a:8000"] = 1.0  # already expired
        client._expire_cooldowns()
        assert not client._is_disabled("http://a:8000")
        assert client._success_count("http://a:8000") == 8

    def test_set_circuit_validates_and_updates(self):
        client = LoadBalancedClient(["http://a:8000"], timeout=1.0)
        snap = client.set_circuit(max_fail_rate=0.4, cooldown_s=120)
        assert snap["max_fail_rate"] == 0.4
        assert snap["cooldown_s"] == 120
        with pytest.raises(ValueError):
            client.set_circuit(max_fail_rate=1.5)
        with pytest.raises(ValueError):
            client.set_circuit(cooldown_s=0)

    @pytest.mark.asyncio
    async def test_set_recycle_every_posts_to_worker(self):
        seen = {}

        def handler(req: httpx.Request) -> httpx.Response:
            seen["method"] = req.method
            seen["path"] = req.url.path
            seen["body"] = req.content
            return httpx.Response(
                200,
                json={
                    "browser": {
                        "recycle_every": 100,
                        "requests_since_recycle": 4,
                        "recycle_count": 1,
                    }
                },
            )

        client = LoadBalancedClient(["http://a:8000"], timeout=1.0)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler),
        )
        result = await client.set_recycle_every("http://a:8000", 100)
        assert seen["method"] == "POST"
        assert seen["path"] == "/browser/recycle-every"
        assert result["recycle_updated"] == {
            "url": "http://a:8000",
            "recycle_every": 100,
        }
        row = {u["url"]: u for u in result["upstreams"]}["http://a:8000"]
        assert row["recycle_every"] == 100
        assert row["requests_since_recycle"] == 4
        await client.aclose()

    @pytest.mark.asyncio
    async def test_set_recycle_every_rejects_unknown_url_and_non_positive(self):
        client = LoadBalancedClient(["http://a:8000"], timeout=1.0)
        with pytest.raises(KeyError):
            await client.set_recycle_every("http://missing:8000", 10)
        with pytest.raises(ValueError, match=">= 1"):
            await client.set_recycle_every("http://a:8000", 0)
        await client.aclose()

import random

import httpx
import pytest

from upstream import LoadBalancedClient


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

        client._outcomes["http://a:8000"].extend([True] * 50)
        client._outcomes["http://b:8000"].extend([True] * 50)

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
        client = LoadBalancedClient(urls, timeout=1.0, history_size=100)
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
        client._outcomes["http://a:8000"].extend([True] * 80 + [False] * 20)
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend([True] * 20 + [False] * 80)

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
        assert outcomes[-1] is False
        assert outcomes.count(False) == 1
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
        client._outcomes["http://a:8000"].extend([True] * 80 + [False] * 20)
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend([True] * 20 + [False] * 80)

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
        client._outcomes["http://a:8000"].extend([False] * 10)
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend([False] * 10)

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
        client._outcomes["http://a:8000"].extend([True] * 100)
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend([False] * 100)

        probs = client._probabilities()
        assert probs["http://b:8000"] == pytest.approx(0.0)
        assert probs["http://a:8000"] == pytest.approx(1.0)

    def test_seed_pick_probability_restores_failed_worker(self):
        """Admin reseed fills the window so a dead worker re-enters at ~35%."""
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"],
            timeout=1.0,
            history_size=100,
        )
        client._outcomes["http://a:8000"].clear()
        client._outcomes["http://a:8000"].extend([True] * 100)
        client._outcomes["http://b:8000"].clear()
        client._outcomes["http://b:8000"].extend([False] * 100)

        result = client.seed_pick_probability("http://b:8000", 0.35)
        by_url = {u["url"]: u for u in result["upstreams"]}
        # s = 0.35/0.65 * 100 ≈ 54 → P ≈ 54/154 ≈ 0.3506
        assert by_url["http://b:8000"]["successes"] == 54
        assert by_url["http://b:8000"]["failures"] == 46
        assert by_url["http://b:8000"]["probability"] == pytest.approx(0.35, abs=0.02)
        assert result["seeded"]["requested_probability"] == 0.35
        assert result["seeded"]["actual_probability"] == pytest.approx(0.35, abs=0.02)

    def test_seed_pick_probability_zero_and_full(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=100
        )
        client.seed_pick_probability("http://b:8000", 0.0)
        assert client._success_count("http://b:8000") == 0
        assert client._probabilities()["http://b:8000"] == pytest.approx(0.0)

        client.seed_pick_probability("http://b:8000", 1.0)
        assert client._success_count("http://b:8000") == 100
        assert client._success_count("http://a:8000") == 0
        assert client._probabilities()["http://b:8000"] == pytest.approx(1.0)

    def test_seed_rejects_unknown_url_and_off_grid_probability(self):
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=100
        )
        with pytest.raises(KeyError):
            client.seed_pick_probability("http://missing:8000", 0.5)
        with pytest.raises(ValueError, match="multiple of"):
            client.seed_pick_probability("http://a:8000", 0.33)
        with pytest.raises(ValueError, match="between 0 and 1"):
            client.seed_pick_probability("http://a:8000", 1.5)

    def test_seed_when_others_have_zero_successes(self):
        """If everyone is at 0, residual mass is seeded on others so P≈target."""
        client = LoadBalancedClient(
            ["http://a:8000", "http://b:8000"], timeout=1.0, history_size=100
        )
        client._fill_window("http://a:8000", 0)
        client._fill_window("http://b:8000", 0)

        result = client.seed_pick_probability("http://b:8000", 0.35)
        assert result["seeded"]["actual_probability"] == pytest.approx(0.35, abs=0.05)
        assert client._success_count("http://b:8000") > 0
        assert client._success_count("http://a:8000") > 0

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
    async def test_cold_start_first_pick_is_uniform(self):
        """Before any attempts, selection is uniform random."""
        client = LoadBalancedClient(["http://a:8000", "http://b:8000"], timeout=1.0)
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

        def make_handler(url: str):
            def h(req: httpx.Request) -> httpx.Response:
                call_counts[url] += 1
                return httpx.Response(200, json={"success": True})
            return h

        urls = ["http://a:8000", "http://b:8000"]
        client = LoadBalancedClient(urls, timeout=1.0, history_size=100)
        for url in urls:
            client._clients[url] = httpx.AsyncClient(
                base_url=url,
                transport=httpx.MockTransport(make_handler(url)),
            )

        # Seed: A has 80 successes, B has 20 successes in the last 100 attempts each.
        client._outcomes["http://a:8000"].extend([True] * 80 + [False] * 20)
        client._outcomes["http://b:8000"].extend([True] * 20 + [False] * 80)

        random.seed(99)
        n_calls = 500
        for _ in range(n_calls):
            await client.get("/inserate")

        # Expected weight ratio 80:20 => ~80% to A.
        a_share = call_counts["http://a:8000"] / n_calls
        assert a_share > 0.65
        assert a_share < 0.92
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

        assert list(client._outcomes["http://a:8000"]) == [False]
        assert client._success_count("http://a:8000") == 0

        await client.aclose()
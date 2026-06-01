import random
from unittest.mock import patch

import httpx
import pytest

from upstream import LoadBalancedClient, is_degraded_inserate_search


class TestDegradedSearchDetection:
    def test_nonempty_results_not_degraded(self):
        assert is_degraded_inserate_search({"success": True, "results": [{"adid": "1"}]}) is False

    def test_success_empty_results_is_degraded(self):
        assert is_degraded_inserate_search({"success": True, "results": []}) is True

    def test_pages_successful_zero_is_degraded(self):
        assert is_degraded_inserate_search({
            "success": True,
            "results": [],
            "performance_metrics": {"pages_successful": 0},
        }) is True

    def test_failed_search_not_degraded_marker(self):
        assert is_degraded_inserate_search({"success": False, "results": []}) is False


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
        """With the new contract, a failure on the chosen server must raise immediately.
        There is no fallback to other servers for the same request.
        """
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
    async def test_pure_random_distribution_over_many_calls(self):
        """Over many calls, both servers should be chosen (pure random selection)."""
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

        random.seed(12345)  # for reproducibility

        n_calls = 200
        for _ in range(n_calls):
            await client.get("/inserate")

        # Both servers must have been chosen at least a few times.
        assert call_counts["http://a:8000"] > 10
        assert call_counts["http://b:8000"] > 10
        assert call_counts["http://a:8000"] + call_counts["http://b:8000"] == n_calls

        await client.aclose()

    @pytest.mark.asyncio
    async def test_inserate_failover_skips_degraded_empty_upstream(self):
        """First upstream returns success+empty; second returns cards."""
        call_order: list[str] = []

        def make_handler(url: str, payload: dict):
            def h(req: httpx.Request) -> httpx.Response:
                call_order.append(url)
                return httpx.Response(200, json=payload)
            return h

        empty = {
            "success": True,
            "results": [],
            "performance_metrics": {"pages_successful": 0},
        }
        good = {
            "success": True,
            "results": [{"adid": "99", "title": "Bike"}],
            "performance_metrics": {"pages_successful": 1},
        }

        urls = ["http://a:8000", "http://b:8000"]
        client = LoadBalancedClient(urls, timeout=1.0)
        payloads = {"http://a:8000": empty, "http://b:8000": good}
        for url in urls:
            client._clients[url] = httpx.AsyncClient(
                base_url=url,
                transport=httpx.MockTransport(make_handler(url, payloads[url])),
            )

        # Deterministic order: try a (empty) then b (good).
        with patch("upstream.random.shuffle", lambda urls: urls.sort()):
            resp, upstream = await client.get_inserate_with_failover("/inserate")
        data = resp.json()
        assert len(data["results"]) == 1
        assert upstream == "http://b:8000"
        assert call_order == ["http://a:8000", "http://b:8000"]

        await client.aclose()

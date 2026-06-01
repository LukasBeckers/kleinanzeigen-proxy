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
    async def test_retry_on_second_server_after_first_fails(self):
        call_order = []

        def handler_a(req: httpx.Request) -> httpx.Response:
            call_order.append("a")
            raise httpx.ConnectError("server a down")

        def handler_b(req: httpx.Request) -> httpx.Response:
            call_order.append("b")
            return httpx.Response(200, json={"success": True})

        urls = ["http://a:8000", "http://b:8000"]

        client = LoadBalancedClient(urls, timeout=1.0)
        client._clients["http://a:8000"] = httpx.AsyncClient(
            base_url="http://a:8000",
            transport=httpx.MockTransport(handler_a),
        )
        client._clients["http://b:8000"] = httpx.AsyncClient(
            base_url="http://b:8000",
            transport=httpx.MockTransport(handler_b),
        )

        import random
        random.seed(0)

        resp, upstream = await client.get("/inserate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert upstream in client.urls
        assert "b" in call_order

        await client.aclose()

    @pytest.mark.asyncio
    async def test_raises_after_all_upstreams_fail(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("all down")

        urls = ["http://a:8000", "http://b:8000", "http://c:8000"]
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
    async def test_all_urls_tried_after_partial_failures(self):
        attempts: set[str] = set()

        def make_handler(url: str):
            async def h(req: httpx.Request) -> httpx.Response:
                attempts.add(url)
                if url == "http://c:8000":
                    return httpx.Response(200, json={"success": True})
                raise httpx.ConnectError(f"{url} down")
            return h

        urls = ["http://a:8000", "http://b:8000", "http://c:8000"]
        client = LoadBalancedClient(urls, timeout=1.0)
        for url in urls:
            client._clients[url] = httpx.AsyncClient(
                base_url=url,
                transport=httpx.MockTransport(make_handler(url)),
            )

        import random
        random.seed(42)

        resp, upstream = await client.get("/inserate")
        assert resp.status_code == 200
        assert upstream in client.urls
        assert "http://c:8000" in attempts

        await client.aclose()

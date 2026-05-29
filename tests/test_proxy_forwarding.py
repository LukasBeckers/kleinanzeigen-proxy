"""Verify that proxy endpoints forward every query param to the upstream
API.  The upstream is a MockTransport that captures the requests."""
import httpx
import pytest


class _Capture:
    def __init__(self):
        self.requests: list[httpx.Request] = []


@pytest.fixture
def capture(upstream_handler):
    cap = _Capture()

    def handler(req):
        cap.requests.append(req)
        if "/inserate" in str(req.url):
            return httpx.Response(200, json={
                "success": True, "results": [], "unique_results": 0,
                "time_taken": 0.0,
                "performance_metrics": {}, "browser_metrics": {},
            })
        if "/inserat/" in str(req.url):
            return httpx.Response(200, json={"success": True, "data": {
                "id": "x", "title": "t", "description": "d", "url": "u",
                "price": {"amount": "1", "currency": "€", "negotiable": False},
                "images": [], "details": {}, "features": [], "seller": {},
                "extra_info": {}, "location": {"zip": "", "city": "", "state": ""},
                "status": "active", "delivery": None, "views": "0",
                "categories": [],
            }, "time_taken": 0.0, "performance_metrics": {}})
        return httpx.Response(404)
    upstream_handler.handler = handler
    return cap


class TestInserateForwarding:
    async def test_forwards_all_params(self, client, capture):
        await client.get("/inserate", params={
            "query": "mofa", "location": "52538", "radius": 100,
            "min_price": 0, "max_price": 300,
            "category": "305", "page_count": 2,
        })
        assert len(capture.requests) == 1
        url = capture.requests[0].url
        q = dict(url.params)
        assert q["query"] == "mofa"
        assert q["location"] == "52538"
        assert q["radius"] == "100"
        assert q["min_price"] == "0"
        assert q["max_price"] == "300"
        assert q["category"] == "305"
        assert q["page_count"] == "2"

    async def test_category_param_included_in_upstream_call(self, client, capture):
        """Regression: the proxy was initially missing the ``category``
        pass-through, so even after we fixed the upstream URL builder the
        category filter wouldn't reach it."""
        await client.get("/inserate", params={"category": "305"})
        assert len(capture.requests) == 1
        q = dict(capture.requests[0].url.params)
        assert q.get("category") == "305"

    async def test_sort_param_forwarded(self, client, capture):
        await client.get("/inserate", params={"sort": "price_asc"})
        assert len(capture.requests) == 1
        q = dict(capture.requests[0].url.params)
        assert q.get("sort") == "price_asc"

    async def test_sort_omitted_when_unset(self, client, capture):
        await client.get("/inserate", params={"query": "x"})
        q = dict(capture.requests[0].url.params)
        assert "sort" not in q, "sort must not appear in upstream params when unset"


class TestInseratDetailForwarding:
    async def test_detail_endpoint_forwards_id(self, client, capture):
        await client.get("/inserat/123456789")
        assert any("/inserat/123456789" in str(r.url) for r in capture.requests)

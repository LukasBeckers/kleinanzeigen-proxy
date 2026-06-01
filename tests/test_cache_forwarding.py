"""Contract tests for ``GET /inserate-detailed-cached``.

Spec:
- First step is always a cheap ``/inserate`` search.
- For each adid, if the proxy already has a detail-source version stored,
  use that; otherwise fetch ``/inserat/{id}``.
- Response shape matches ``/inserate-detailed`` — same top-level keys,
  same ``data[i].details`` sub-object keys — so downstream clients are
  endpoint-agnostic.
"""
from __future__ import annotations

import json

import httpx
import pytest


DETAIL_SHAPE_KEYS = {
    "id", "categories", "title", "status", "price", "delivery", "location",
    "views", "description", "images", "details", "features", "seller",
    "extra_info",
}


def _search_response(adids: list[str]) -> httpx.Response:
    results = [{
        "adid": a,
        "url": f"https://ka.de/s-anzeige/x/{a}",
        "title": f"Listing {a}",
        "price": "100",
        "description": "some description",
    } for a in adids]
    return httpx.Response(200, json={
        "success": True,
        "results": results,
        "unique_results": len(results),
        "time_taken": 0.0,
        "performance_metrics": {},
        "browser_metrics": {},
    })


def _detail_response(adid: str, images: list[str] | None = None) -> httpx.Response:
    return httpx.Response(200, json={
        "success": True,
        "data": {
            "id": adid,
            "categories": ["Motorräder"],
            "title": f"Detail for {adid}",
            "status": "active",
            "price": {"amount": "150", "currency": "€", "negotiable": True},
            "delivery": None,
            "location": {"zip": "52538", "city": "Gangelt", "state": "NRW"},
            "views": "42",
            "description": "full description",
            "images": images if images is not None else [f"https://img/{adid}.jpg"],
            "details": {"Marke": "Honda"},
            "features": [],
            "seller": {"name": "seller"},
            "extra_info": {"created_at": "2026-04-01"},
        },
        "time_taken": 0.1,
        "performance_metrics": {},
    })


class _Router:
    """Lets a test script out a sequence of upstream responses per URL."""
    def __init__(self):
        self.calls: list[httpx.Request] = []
        self.inserat_calls: int = 0
        self.inserate_calls: int = 0

    def handle(self, req: httpx.Request) -> httpx.Response:
        self.calls.append(req)
        path = req.url.path
        if path == "/inserate":
            self.inserate_calls += 1
            return _search_response(self._adids_for_search)
        if path.startswith("/inserat/"):
            self.inserat_calls += 1
            adid = path.rsplit("/", 1)[-1]
            return _detail_response(adid)
        return httpx.Response(404, json={"error": f"unhandled {path}"})

    def set_adids(self, adids: list[str]):
        self._adids_for_search = adids


@pytest.fixture
def router(upstream_handler):
    r = _Router()
    upstream_handler.handler = r.handle
    return r


class TestCachedEndpoint:
    async def test_all_cache_misses_on_cold_db(self, client, router, session_factory):
        router.set_adids(["A1", "A2", "A3"])
        r = await client.get("/inserate-detailed-cached", params={"query": "mofa"})
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert body["unique_results"] == 3
        assert len(body["data"]) == 3
        # Every one was a miss, so upstream /inserat/{id} was called once each.
        assert router.inserate_calls == 1
        assert router.inserat_calls == 3
        # Response shape guarantee.
        assert all(set(item["details"].keys()) >= DETAIL_SHAPE_KEYS for item in body["data"])
        assert body["performance_metrics"]["cache_hits"] == 0
        assert body["performance_metrics"]["cache_misses"] == 3

    async def test_all_cache_hits_on_warm_db(self, client, router, session_factory):
        # First call seeds the cache.
        router.set_adids(["B1", "B2"])
        await client.get("/inserate-detailed-cached", params={"query": "mofa"})
        router.inserat_calls = 0  # reset counter for the second call
        router.inserate_calls = 0

        # Second call — same adids, should be all cache hits.
        r = await client.get("/inserate-detailed-cached", params={"query": "mofa"})
        assert r.status_code == 200
        body = r.json()
        assert body["unique_results"] == 2
        assert router.inserate_calls == 1, "the cheap search step still happens"
        assert router.inserat_calls == 0, "cached listings must NOT re-fetch detail"
        assert body["performance_metrics"]["cache_hits"] == 2
        assert body["performance_metrics"]["cache_misses"] == 0
        # Even from cache, the detail sub-object must carry the full shape.
        for item in body["data"]:
            assert set(item["details"].keys()) >= DETAIL_SHAPE_KEYS

    async def test_mixed_hits_and_misses(self, client, router, session_factory):
        # Seed just one adid.
        router.set_adids(["X1"])
        await client.get("/inserate-detailed-cached", params={"query": "mofa"})
        router.inserat_calls = 0
        router.inserate_calls = 0

        # Now ask for that adid plus two new ones.
        router.set_adids(["X1", "X2", "X3"])
        r = await client.get("/inserate-detailed-cached", params={"query": "mofa"})
        body = r.json()
        assert body["performance_metrics"]["cache_hits"] == 1
        assert body["performance_metrics"]["cache_misses"] == 2
        assert router.inserat_calls == 2, "only the two new adids go through the detail fetch"

    async def test_search_only_version_counts_as_miss(self, client, router, session_factory, upstream_handler):
        """If the only thing we've ever stored about an adid is a search
        card (no detail), it must NOT be treated as a cache hit — otherwise
        the response would have no images, no location, no seller, etc."""
        from storage import store_listing
        # Pre-seed by calling the proxy's existing /inserate endpoint so the
        # listing is stored with source="search".  Image_urls will be NULL.
        search_only_handler = lambda req: _search_response(["Y1"])
        upstream_handler.handler = search_only_handler
        await client.get("/inserate", params={"query": "mofa"})

        # Switch handler back to the router and request the cached endpoint.
        upstream_handler.handler = router.handle
        router.set_adids(["Y1"])
        r = await client.get("/inserate-detailed-cached", params={"query": "mofa"})
        body = r.json()
        assert body["performance_metrics"]["cache_hits"] == 0, (
            "search-only version must not satisfy a cache hit; detail must "
            "be fetched"
        )
        assert body["performance_metrics"]["cache_misses"] == 1
        assert router.inserat_calls == 1


class TestCachedEndpointDiagnostics:
    """Tests for the new diagnostic fields added to /inserate-detailed-cached."""

    async def test_diagnostic_fields_present_on_success(self, client, router, upstream_handler):
        """When the search succeeds, the new diagnostic fields must be present."""
        router.set_adids(["D1", "D2"])
        r = await client.get("/inserate-detailed-cached", params={"query": "test"})
        body = r.json()

        pm = body["performance_metrics"]
        assert "raw_cards_from_search" in pm
        assert "detail_fetch_attempts" in pm
        assert "detail_fetch_successes" in pm
        assert "detail_fetch_failures" in pm
        assert "search_upstream" in pm

        assert pm["raw_cards_from_search"] == 2
        assert body["unique_results"] == 2

    async def test_cards_are_dropped_when_detail_fetch_fails(self, client, router, upstream_handler):
        """
        When a live detail fetch fails (the chosen random upstream returns
        an unsuccessful response), the card is dropped. A warning is logged
        and the failure is reflected in the metrics.
        This matches the requirement to not create partial cached records.
        """
        router.set_adids(["F1", "F2"])

        # Make detail fetches fail for these adids
        original_handle = router.handle

        def failing_detail(req: httpx.Request) -> httpx.Response:
            if req.url.path.startswith("/inserat/"):
                return httpx.Response(200, json={"success": False, "data": None})
            return original_handle(req)

        upstream_handler.handler = failing_detail

        r = await client.get("/inserate-detailed-cached", params={"query": "failtest"})
        body = r.json()

        # Cards are dropped when all upstreams fail for the detail
        assert body["unique_results"] == 0
        assert len(body["data"]) == 0

        pm = body["performance_metrics"]
        assert pm["detail_fetch_failures"] == 2
        assert pm["detail_fetch_successes"] == 0

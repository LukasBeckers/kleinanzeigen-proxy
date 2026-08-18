"""GET /seller/{user_id} is cache-first: one upstream scrape per seller."""

from __future__ import annotations

import httpx
import pytest


def _profile(user_id: str = "9") -> dict:
    return {
        "id": user_id,
        "name": "Ada",
        "type": "private",
        "since": "01.01.2020",
        "badges": ["TOP Zufriedenheit"],
        "url": f"https://www.kleinanzeigen.de/s-bestandsliste.html?userId={user_id}",
        "shop_url": None,
        "response_time": "Antwortet in der Regel innerhalb von 6 Stunden",
        "response_time_hours": 6,
        "followers": 4,
        "ads_online": None,
        "ads_total": None,
    }


class TestSellerEndpoint:
    async def test_miss_then_hit_skips_second_upstream(self, client, upstream_handler):
        calls = {"n": 0}

        def handle(req: httpx.Request) -> httpx.Response:
            if req.url.path == "/seller/9":
                calls["n"] += 1
                return httpx.Response(200, json={"success": True, "data": _profile()})
            return httpx.Response(404, json={"error": req.url.path})

        upstream_handler.handler = handle

        r1 = await client.get("/seller/9")
        assert r1.status_code == 200
        assert r1.json()["data"]["followers"] == 4
        assert r1.headers.get("x-seller-cache") == "miss"
        assert calls["n"] == 1

        r2 = await client.get("/seller/9")
        assert r2.status_code == 200
        assert r2.json()["data"]["name"] == "Ada"
        assert r2.headers.get("x-seller-cache") == "hit"
        assert calls["n"] == 1, "cached seller must not be re-scraped"

"""Cache-first seller profile endpoint."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import database as db_module
from storage import get_cached_seller, store_seller

logger = logging.getLogger(__name__)

router = APIRouter()


async def ensure_seller_profile(client, session, user_id: str) -> dict | None:
    """Return a profile snapshot, scraping upstream only on first miss."""
    cached = await get_cached_seller(session, user_id)
    if cached is not None:
        return cached

    try:
        response, _upstream = await client.get(f"/seller/{user_id}")
        data = response.json()
    except Exception as exc:
        logger.warning("seller fetch failed user_id=%s error=%s", user_id, exc)
        return None

    if not data.get("success") or not data.get("data"):
        logger.warning(
            "seller response not successful user_id=%s success=%s",
            user_id,
            data.get("success"),
        )
        return None

    profile = data["data"]
    await store_seller(session, profile, source="profile")
    return await get_cached_seller(session, user_id) or profile


@router.get("/seller/{user_id}")
async def get_seller(request: Request, user_id: str):
    client = request.app.state.upstream_client
    async with db_module.async_session() as session:
        cached = await get_cached_seller(session, user_id)
        if cached is not None:
            return JSONResponse(
                content={"success": True, "data": cached, "cache": "hit"},
                headers={"X-Seller-Cache": "hit"},
            )

        profile = await ensure_seller_profile(client, session, user_id)
        await session.commit()

    if profile is None:
        return JSONResponse(
            content={"success": False, "data": None, "error": "seller not found"},
            status_code=404,
            headers={"X-Seller-Cache": "miss"},
        )
    return JSONResponse(
        content={"success": True, "data": profile, "cache": "miss"},
        headers={"X-Seller-Cache": "miss"},
    )

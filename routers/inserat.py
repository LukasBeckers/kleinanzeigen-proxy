from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import database as db_module
from routers.seller import ensure_seller_profile
from storage import store_listing

router = APIRouter()


def _seller_id(listing_data: dict) -> str | None:
    seller = listing_data.get("seller") or {}
    if not isinstance(seller, dict):
        return None
    raw = seller.get("id") or seller.get("user_id")
    return str(raw).strip() if raw else None


@router.get("/inserat/{listing_id}")
async def get_inserat(request: Request, listing_id: str):
    client = request.app.state.upstream_client
    image_worker = request.app.state.image_worker

    upstream_response, upstream_url = await client.get(f"/inserat/{listing_id}")
    data = upstream_response.json()

    if data.get("success") and data.get("data"):
        listing_data = data["data"]
        adid = listing_data.get("id", listing_id)
        async with db_module.async_session() as session:
            lid, version_id, is_new, image_urls = await store_listing(
                session, listing_data, source="detail"
            )
            if lid and version_id and image_urls:
                await image_worker.create_pending_records(
                    session, lid, version_id, image_urls
                )
            user_id = _seller_id(listing_data)
            if user_id:
                profile = await ensure_seller_profile(client, session, user_id)
                if profile:
                    listing_data = dict(listing_data)
                    listing_data["seller"] = profile
                    data = dict(data)
                    data["data"] = listing_data
            await session.commit()

            if lid and version_id and image_urls:
                image_worker.enqueue(adid, lid, version_id, image_urls)

    return JSONResponse(
        content=data,
        headers={"X-Upstream-Used": upstream_url},
    )

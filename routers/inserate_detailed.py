from fastapi import APIRouter, Request

from database import async_session
from storage import store_listings_batch

router = APIRouter()


@router.get("/inserate-detailed")
async def get_inserate_detailed(
    request: Request,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: int = 1,
    max_concurrent_details: int = 5,
):
    client = request.app.state.upstream_client
    image_worker = request.app.state.image_worker

    params = {}
    if query is not None:
        params["query"] = query
    if location is not None:
        params["location"] = location
    if radius is not None:
        params["radius"] = radius
    if min_price is not None:
        params["min_price"] = min_price
    if max_price is not None:
        params["max_price"] = max_price
    params["page_count"] = page_count
    params["max_concurrent_details"] = max_concurrent_details

    response = await client.get("/inserate-detailed", params=params)
    data = response.json()

    if data.get("success") and data.get("data"):
        async with async_session() as session:
            results = await store_listings_batch(session, data["data"], source="combined")

            for adid, listing_id, version_id, image_urls in results:
                if version_id and image_urls:
                    await image_worker.create_pending_records(
                        session, listing_id, version_id, image_urls
                    )
            await session.commit()

            for adid, listing_id, version_id, image_urls in results:
                if version_id and image_urls:
                    image_worker.enqueue(adid, listing_id, version_id, image_urls)

    return data

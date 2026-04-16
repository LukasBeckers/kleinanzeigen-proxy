from fastapi import APIRouter, Request

from database import async_session
from storage import store_listings_batch

router = APIRouter()


@router.get("/inserate")
async def get_inserate(
    request: Request,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: int = 1,
):
    client = request.app.state.upstream_client

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

    response = await client.get("/inserate", params=params)
    data = response.json()

    if data.get("success") and data.get("results"):
        async with async_session() as session:
            await store_listings_batch(session, data["results"], source="search")

    return data

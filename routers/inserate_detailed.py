import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import database as db_module
from storage import get_cached_detail, store_listing, store_listings_batch

logger = logging.getLogger(__name__)

router = APIRouter()


def _build_search_params(
    query: str | None,
    location: str | None,
    radius: int | None,
    min_price: int | None,
    max_price: int | None,
    category: str | None,
    page_count: int,
    sort: str | None = None,
    attribute_filters: str | None = None,
) -> dict:
    params: dict = {}
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
    if category is not None:
        params["category"] = category
    if sort is not None:
        params["sort"] = sort
    if attribute_filters is not None:
        params["attribute_filters"] = attribute_filters
    params["page_count"] = page_count
    return params


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
    category: str = None,
    sort: str = None,
    attribute_filters: str = None,
):
    client = request.app.state.upstream_client
    image_worker = request.app.state.image_worker

    params = _build_search_params(
        query, location, radius, min_price, max_price, category, page_count, sort, attribute_filters
    )
    params["max_concurrent_details"] = max_concurrent_details

    upstream_response, upstream_url = await client.get("/inserate-detailed", params=params)
    data = upstream_response.json()

    if data.get("success") and data.get("data"):
        async with db_module.async_session() as session:
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

    return JSONResponse(
        content=data,
        headers={"X-Upstream-Used": upstream_url},
    )


@router.get("/inserate-detailed-cached")
async def get_inserate_detailed_cached(
    request: Request,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: int = 1,
    category: str = None,
    sort: str = None,
    attribute_filters: str = None,
):
    """Cache-first variant of ``/inserate-detailed``.

    Flow:
      1. Cheap upstream ``/inserate`` search → list of cards.
      2. For each adid, try ``storage.get_cached_detail``.  Hit → use local;
         miss → fetch upstream ``/inserat/{id}`` and store it.
      3. Return the same shape as ``/inserate-detailed`` so downstream
         clients (e.g. kleinanzeigen-hunter) are agnostic about which
         endpoint fed them.

    Tradeoff: we never refetch detail for a listing already seen, so
    updates to kleinanzeigen (price edit, description edit) are not
    picked up.  Hunter handles this gracefully via its pipeline-hash
    dedup + last_notified_at on seen_listings.
    """
    t_start = time.time()
    client = request.app.state.upstream_client
    image_worker = request.app.state.image_worker

    params = _build_search_params(
        query, location, radius, min_price, max_price, category, page_count, sort, attribute_filters
    )

    # 1. Cheap search step (failover when an upstream returns success+empty).
    search_resp, search_upstream = await client.get_inserate_with_failover(
        "/inserate", params=params
    )
    search_data = search_resp.json()

    raw_cards = len(search_data.get("results") or []) if search_data.get("success") else 0
    logger.info(
        "Cached search received %d raw cards from upstream=%s (params: %s)",
        raw_cards, search_upstream, {k: v for k, v in params.items() if k in ("query", "category", "location", "radius")}
    )

    if not search_data.get("success"):
        # Pass the upstream failure through — keep the response shape the
        # same as /inserate-detailed (``data`` key) so callers don't have
        # to special-case.
        return JSONResponse(
            content={
                "success": False,
                "data": [],
                "unique_results": 0,
                "time_taken": round(time.time() - t_start, 3),
                "performance_metrics": {"cache_hits": 0, "cache_misses": 0},
                "error": search_data.get("error") or "upstream search failed",
            },
            headers={
                "X-Upstream-Used": search_upstream,
                "X-Cache-Hits": "0",
                "X-Cache-Misses": "0",
            },
        )

    cards = search_data.get("results") or []

    combined: list[dict] = []
    cache_hits = 0
    cache_misses = 0

    # 2. Partition using a short-lived session (only for cache lookups).
    need_fetch: list[dict] = []
    async with db_module.async_session() as session:
        for card in cards:
            adid = str(card.get("adid") or "")
            if not adid:
                continue
            cached = await get_cached_detail(session, adid)
            if cached is not None:
                cache_hits += 1
                combined.append({
                    "adid": adid,
                    "url": card.get("url"),
                    "title": card.get("title"),
                    "price": card.get("price"),
                    "description": card.get("description"),
                    "posted_at": card.get("posted_at"),
                    "posted_at_raw": card.get("posted_at_raw"),
                    "location_zip": card.get("location_zip"),
                    "location_city": card.get("location_city"),
                    "distance_km": card.get("distance_km"),
                    "details": cached,
                    "detail_fetch_time": 0.0,
                })
            else:
                need_fetch.append(card)

    # 3. Fetch details for misses — NO DB session is held during network I/O.
    #    This prevents long-held connections when upstreams are slow.
    attempted_misses = len(need_fetch)
    successful_details = 0
    failed_details = 0

    to_store: list[tuple[dict, dict, str]] = []  # (card, detail, upstream_used)

    for card in need_fetch:
        adid = str(card.get("adid") or "")
        if not adid:
            continue

        t0 = time.time()

        try:
            response, upstream_used = await client.get(f"/inserat/{adid}")
            data = response.json()
        except Exception as exc:
            logger.warning(
                "Detail fetch failed for adid=%s (upstream=%s, error: %s)",
                adid, "unknown", exc
            )
            failed_details += 1
            continue

        if not data.get("success") or not data.get("data"):
            logger.warning(
                "Detail response not successful for adid=%s (success=%s, has_data=%s)",
                adid, data.get("success"), bool(data.get("data"))
            )
            failed_details += 1
            continue

        detail = data["data"]
        to_store.append((card, detail, upstream_used))
        successful_details += 1

    if failed_details > 0:
        logger.warning(
            "Cached search failed to fetch full details for %d/%d cache-miss cards "
            "(search_upstream=%s). These cards were dropped for this response. "
            "Underlying API worker health for /inserat/{id} should be investigated.",
            failed_details, attempted_misses, search_upstream,
        )

    # 4. Store successful details using a fresh short-lived session.
    #    Network work is already complete, so the session is only held for DB work.
    cache_misses = attempted_misses
    fetched: list[dict] = []

    if to_store:
        async with db_module.async_session() as session:
            for card, detail, upstream_used in to_store:
                t0 = time.time()  # re-measure only the store part if desired
                lid, version_id, is_new, image_urls = await store_listing(
                    session, detail, source="detail"
                )
                if lid and version_id and image_urls:
                    await image_worker.create_pending_records(
                        session, lid, version_id, image_urls
                    )

                row = {
                    "adid": str(card.get("adid")),
                    "url": card.get("url"),
                    "title": card.get("title"),
                    "price": card.get("price"),
                    "description": card.get("description"),
                    "posted_at": card.get("posted_at"),
                    "posted_at_raw": card.get("posted_at_raw"),
                    "location_zip": card.get("location_zip"),
                    "location_city": card.get("location_city"),
                    "distance_km": card.get("distance_km"),
                    "details": detail,
                    "detail_fetch_time": round(time.time() - t0, 3),
                    "_detail_upstream": upstream_used,
                    "_enqueue": (lid, version_id, image_urls) if (lid and version_id and image_urls) else None,
                }
                fetched.append(row)

            await session.commit()

        # Enqueue image downloads after commit.
        for row in fetched:
            enq = row.pop("_enqueue", None)
            if enq:
                lid, version_id, image_urls = enq
                image_worker.enqueue(row["adid"], lid, version_id, image_urls)
            combined.append(row)

    # Response shape mirrors /inserate-detailed.
    # For the cached variant we also expose cache effectiveness so callers
    # (especially the hunter) can see how much came from local storage vs live
    # upstream calls for this particular search.
    return JSONResponse(
        content={
            "success": True,
            "data": combined,
            "unique_results": len(combined),
            "time_taken": round(time.time() - t_start, 3),
            "performance_metrics": {
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "raw_cards_from_search": raw_cards,
                "detail_fetch_attempts": attempted_misses,
                "detail_fetch_successes": successful_details,
                "detail_fetch_failures": failed_details,
                "search_upstream": search_upstream,
                "pages_requested": page_count,
            },
        },
        headers={
            "X-Upstream-Used": search_upstream,
            "X-Cache-Hits": str(cache_hits),
            "X-Cache-Misses": str(cache_misses),
            "X-Raw-Cards-From-Search": str(raw_cards),
            "X-Detail-Failures": str(failed_details),
        },
    )

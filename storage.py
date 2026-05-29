import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Listing, ListingVersion, new_uuid, utcnow


def _extract_version_fields(data: dict, source: str) -> dict:
    """Extract version fields from either a search result or a detail result."""
    fields = {
        "title": data.get("title"),
        "description": data.get("description"),
        "url": data.get("url"),
    }

    if source == "search":
        fields["price_amount"] = data.get("price")
        fields["price_currency"] = None
        fields["price_negotiable"] = None
        fields["status"] = None
        fields["location_zip"] = None
        fields["location_city"] = None
        fields["location_state"] = None
        fields["delivery"] = None
        fields["views"] = None
        fields["categories"] = None
        fields["details"] = None
        fields["features"] = None
        fields["seller"] = None
        fields["extra_info"] = None
        fields["image_urls"] = None
    elif source == "detail":
        price = data.get("price", {})
        if isinstance(price, dict):
            fields["price_amount"] = price.get("amount")
            fields["price_currency"] = price.get("currency")
            fields["price_negotiable"] = price.get("negotiable")
        else:
            fields["price_amount"] = str(price) if price else None
            fields["price_currency"] = None
            fields["price_negotiable"] = None

        fields["status"] = data.get("status")

        location = data.get("location", {}) or {}
        fields["location_zip"] = location.get("zip")
        fields["location_city"] = location.get("city")
        fields["location_state"] = location.get("state")

        fields["delivery"] = data.get("delivery")
        fields["views"] = data.get("views")
        fields["categories"] = json.dumps(data.get("categories")) if data.get("categories") else None
        fields["details"] = json.dumps(data.get("details")) if data.get("details") else None
        fields["features"] = json.dumps(data.get("features")) if data.get("features") else None
        fields["seller"] = json.dumps(data.get("seller")) if data.get("seller") else None
        fields["extra_info"] = json.dumps(data.get("extra_info")) if data.get("extra_info") else None
        fields["image_urls"] = json.dumps(data.get("images")) if data.get("images") else None
    elif source == "combined":
        # Combined endpoint: top-level has search fields, "details" has detail fields
        detail = data.get("details", {}) or {}

        price = detail.get("price", {})
        if isinstance(price, dict):
            fields["price_amount"] = price.get("amount")
            fields["price_currency"] = price.get("currency")
            fields["price_negotiable"] = price.get("negotiable")
        else:
            fields["price_amount"] = data.get("price")
            fields["price_currency"] = None
            fields["price_negotiable"] = None

        fields["status"] = detail.get("status")

        location = detail.get("location", {}) or {}
        fields["location_zip"] = location.get("zip")
        fields["location_city"] = location.get("city")
        fields["location_state"] = location.get("state")

        fields["delivery"] = detail.get("delivery")
        fields["views"] = detail.get("views")
        fields["categories"] = json.dumps(detail.get("categories")) if detail.get("categories") else None
        fields["details"] = json.dumps(detail.get("details")) if detail.get("details") else None
        fields["features"] = json.dumps(detail.get("features")) if detail.get("features") else None
        fields["seller"] = json.dumps(detail.get("seller")) if detail.get("seller") else None
        fields["extra_info"] = json.dumps(detail.get("extra_info")) if detail.get("extra_info") else None
        fields["image_urls"] = json.dumps(detail.get("images")) if detail.get("images") else None

    return fields


def _compute_hash(fields: dict) -> str:
    """Compute SHA256 hash of content fields, excluding volatile ones like views."""
    hashable = {k: v for k, v in fields.items() if k != "views"}
    canonical = json.dumps(hashable, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def store_listing(
    session: AsyncSession, data: dict, source: str
) -> tuple[str, str | None, bool, list[str]]:
    """
    Store a listing with dedup.

    Returns: (listing_uuid, version_id_or_None, is_new_version, image_urls)
    """
    adid = data.get("adid") or data.get("id")
    if not adid:
        return None, None, False, []

    fields = _extract_version_fields(data, source)
    data_hash = _compute_hash(fields)
    now = utcnow()

    # Look up existing listing
    result = await session.execute(select(Listing).where(Listing.adid == adid))
    listing = result.scalar_one_or_none()

    image_urls = []
    if fields.get("image_urls"):
        try:
            image_urls = json.loads(fields["image_urls"])
        except (json.JSONDecodeError, TypeError):
            pass

    if listing is None:
        # New listing
        listing_id = new_uuid()
        version_id = new_uuid()

        listing = Listing(
            id=listing_id,
            adid=adid,
            first_seen_at=now,
            last_seen_at=now,
            current_version_id=version_id,
        )
        version = ListingVersion(
            id=version_id,
            listing_id=listing_id,
            fetched_at=now,
            data_hash=data_hash,
            **fields,
        )
        session.add(listing)
        session.add(version)
        await session.flush()
        return listing_id, version_id, True, image_urls

    # Existing listing - check if data changed
    listing.last_seen_at = now

    if listing.current_version_id:
        result = await session.execute(
            select(ListingVersion.data_hash).where(
                ListingVersion.id == listing.current_version_id
            )
        )
        current_hash = result.scalar_one_or_none()

        if current_hash == data_hash:
            # No change
            await session.flush()
            return listing.id, None, False, image_urls

    # Data changed - create new version
    version_id = new_uuid()
    version = ListingVersion(
        id=version_id,
        listing_id=listing.id,
        fetched_at=now,
        data_hash=data_hash,
        **fields,
    )
    listing.current_version_id = version_id
    session.add(version)
    await session.flush()
    return listing.id, version_id, True, image_urls


async def store_listings_batch(
    session: AsyncSession, listings: list[dict], source: str
) -> list[tuple[str, str, str | None, list[str]]]:
    """
    Store a batch of listings.

    Returns: list of (adid, listing_uuid, version_id_or_None, image_urls)
    """
    results = []
    for item in listings:
        adid = item.get("adid") or item.get("id", "")
        listing_id, version_id, is_new, image_urls = await store_listing(session, item, source)
        if listing_id:
            results.append((adid, listing_id, version_id, image_urls))
    await session.commit()
    return results


def _json_or(default, value):
    if value is None or value == "":
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


async def get_cached_detail(session: AsyncSession, adid: str) -> dict | None:
    """Rebuild a ``/inserat/{id}.data`` shaped dict from the archive.

    Returns ``None`` when we have no detail-source version for this adid
    (i.e. we've only seen it via ``/inserate`` search cards, or never at
    all).  The cache-hit marker is ``image_urls IS NOT NULL``, because
    ``storage.py`` only populates it on ``source in {"detail", "combined"}``.

    Caller is responsible for falling back to the live ``/inserat/{id}``
    upstream call when this returns ``None``.
    """
    result = await session.execute(select(Listing).where(Listing.adid == adid))
    listing = result.scalar_one_or_none()
    if listing is None or listing.current_version_id is None:
        return None

    result = await session.execute(
        select(ListingVersion).where(ListingVersion.id == listing.current_version_id)
    )
    v = result.scalar_one_or_none()
    if v is None or v.image_urls is None:
        # ``image_urls is None`` means the version was captured via the
        # search-cards endpoint only — not a full detail fetch.
        return None

    return {
        "id": adid,
        "categories": _json_or([], v.categories),
        "title": v.title or "",
        "status": v.status or "active",
        "price": {
            "amount": v.price_amount,
            "currency": v.price_currency or "€",
            "negotiable": bool(v.price_negotiable) if v.price_negotiable is not None else False,
        },
        "delivery": v.delivery,
        "location": {
            "zip": v.location_zip or "",
            "city": v.location_city or "",
            "state": v.location_state or "",
        },
        "views": v.views or "0",
        "description": v.description or "",
        "images": _json_or([], v.image_urls),
        "details": _json_or({}, v.details),
        "features": _json_or([], v.features),
        "seller": _json_or({}, v.seller),
        "extra_info": _json_or({}, v.extra_info),
    }

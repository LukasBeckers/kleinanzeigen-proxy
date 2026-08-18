import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from models import Listing, ListingVersion, Seller, SellerVersion, new_uuid, utcnow


def _source_is_detail(source: str) -> bool:
    """Whether *source* represents a full listing page (not a search card)."""
    return source in {"detail", "combined"}


def _detail_image_urls(images) -> str:
    """Persist gallery URLs for detail/combined fetches (``[]`` when empty)."""
    return json.dumps(images if images is not None else [])


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
        fields["image_urls"] = _detail_image_urls(data.get("images"))
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
        fields["image_urls"] = _detail_image_urls(detail.get("images"))

    return fields


def _compute_hash(fields: dict) -> str:
    """Compute SHA256 hash of content fields, excluding volatile ones like views."""
    hashable = {k: v for k, v in fields.items() if k != "views"}
    canonical = json.dumps(hashable, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _seller_user_id(data: dict | None) -> str | None:
    if not isinstance(data, dict):
        return None
    raw = data.get("id") or data.get("user_id")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _extract_seller_fields(data: dict) -> dict:
    badges = data.get("badges") or []
    extra = {
        k: v
        for k, v in data.items()
        if k
        not in {
            "id",
            "user_id",
            "name",
            "type",
            "since",
            "badges",
            "url",
            "shop_url",
            "response_time",
            "response_time_hours",
            "followers",
            "ads_online",
            "ads_total",
        }
    }
    return {
        "name": data.get("name"),
        "type": data.get("type") or "private",
        "since": data.get("since"),
        "badges": json.dumps(badges, ensure_ascii=False) if badges else None,
        "url": data.get("url"),
        "shop_url": data.get("shop_url"),
        "response_time": data.get("response_time"),
        "response_time_hours": data.get("response_time_hours"),
        "followers": data.get("followers"),
        "ads_online": data.get("ads_online"),
        "ads_total": data.get("ads_total"),
        "extra": json.dumps(extra, ensure_ascii=False) if extra else None,
    }


def seller_to_dict(seller: Seller, version: SellerVersion) -> dict:
    return {
        "id": seller.user_id,
        "user_id": seller.user_id,
        "name": version.name,
        "type": version.type or "private",
        "since": version.since,
        "badges": _json_or([], version.badges),
        "url": version.url
        or f"https://www.kleinanzeigen.de/s-bestandsliste.html?userId={seller.user_id}",
        "shop_url": version.shop_url,
        "response_time": version.response_time,
        "response_time_hours": version.response_time_hours,
        "followers": version.followers,
        "ads_online": version.ads_online,
        "ads_total": version.ads_total,
    }


async def store_seller(
    session: AsyncSession, data: dict, source: str
) -> tuple[str | None, str | None, bool]:
    """Upsert a seller. ``source`` is ``profile`` or ``listing``.

    A listing-sidebar snapshot never overwrites a stored profile scrape.
    """
    user_id = _seller_user_id(data)
    if not user_id:
        return None, None, False

    is_profile = source == "profile"
    fields = _extract_seller_fields(data)
    data_hash = _compute_hash(fields)
    now = utcnow()

    result = await session.execute(select(Seller).where(Seller.user_id == user_id))
    seller = result.scalar_one_or_none()

    if seller is None:
        seller_id = new_uuid()
        version_id = new_uuid()
        insert_stmt = (
            sqlite_insert(Seller)
            .values(
                id=seller_id,
                user_id=user_id,
                first_seen_at=now,
                last_seen_at=now,
                current_version_id=version_id,
                has_profile=is_profile,
            )
            .on_conflict_do_nothing(index_elements=["user_id"])
        )
        insert_result = await session.execute(insert_stmt)
        if insert_result.rowcount:
            session.add(
                SellerVersion(
                    id=version_id,
                    seller_id=seller_id,
                    fetched_at=now,
                    data_hash=data_hash,
                    is_profile=is_profile,
                    **fields,
                )
            )
            await session.flush()
            return seller_id, version_id, True

        result = await session.execute(select(Seller).where(Seller.user_id == user_id))
        seller = result.scalar_one_or_none()
        if seller is None:
            raise RuntimeError(f"seller insert conflict for user_id={user_id} but row missing")

    seller.last_seen_at = now

    if seller.has_profile and not is_profile:
        await session.flush()
        return seller.id, None, False

    if seller.current_version_id:
        result = await session.execute(
            select(SellerVersion.data_hash).where(
                SellerVersion.id == seller.current_version_id
            )
        )
        current_hash = result.scalar_one_or_none()
        if current_hash == data_hash:
            if is_profile:
                seller.has_profile = True
            await session.flush()
            return seller.id, None, False

    version_id = new_uuid()
    session.add(
        SellerVersion(
            id=version_id,
            seller_id=seller.id,
            fetched_at=now,
            data_hash=data_hash,
            is_profile=is_profile,
            **fields,
        )
    )
    seller.current_version_id = version_id
    if is_profile:
        seller.has_profile = True
    await session.flush()
    return seller.id, version_id, True


async def enrich_listing_seller(session: AsyncSession, listing_data: dict) -> dict:
    """Replace a listing's seller blob with the cached profile when we have one."""
    seller = listing_data.get("seller") or {}
    user_id = _seller_user_id(seller)
    if not user_id:
        return listing_data
    cached = await get_cached_seller(session, user_id)
    if cached is None:
        return listing_data
    out = dict(listing_data)
    out["seller"] = cached
    return out


async def get_cached_seller(session: AsyncSession, user_id: str) -> dict | None:
    """Return the profile snapshot if we already scraped this seller."""
    result = await session.execute(select(Seller).where(Seller.user_id == str(user_id)))
    seller = result.scalar_one_or_none()
    if seller is None or not seller.has_profile or seller.current_version_id is None:
        return None
    result = await session.execute(
        select(SellerVersion).where(SellerVersion.id == seller.current_version_id)
    )
    version = result.scalar_one_or_none()
    if version is None:
        return None
    return seller_to_dict(seller, version)


async def _link_listing_seller(
    session: AsyncSession, listing: Listing, data: dict, source: str
) -> None:
    seller_data = data.get("seller")
    if source == "combined":
        details = data.get("details") or {}
        if isinstance(details, dict) and details.get("seller"):
            seller_data = details["seller"]
    if not isinstance(seller_data, dict):
        return
    sid, _, _ = await store_seller(session, seller_data, source="listing")
    if sid:
        listing.seller_id = sid


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
    is_detail = _source_is_detail(source)
    data_hash = _compute_hash(fields)
    now = utcnow()

    # Look up existing listing
    result = await session.execute(select(Listing).where(Listing.adid == adid))
    listing = result.scalar_one_or_none()

    image_urls = _json_or([], fields.get("image_urls"))

    if listing is None:
        # INSERT OR IGNORE avoids 500s when concurrent cache misses race on the
        # same adid (SELECT-then-INSERT TOCTOU across parallel requests).
        listing_id = new_uuid()
        version_id = new_uuid()
        insert_stmt = (
            sqlite_insert(Listing)
            .values(
                id=listing_id,
                adid=adid,
                first_seen_at=now,
                last_seen_at=now,
                current_version_id=version_id,
                has_detail=is_detail,
            )
            .on_conflict_do_nothing(index_elements=["adid"])
        )
        insert_result = await session.execute(insert_stmt)
        if insert_result.rowcount:
            version = ListingVersion(
                id=version_id,
                listing_id=listing_id,
                fetched_at=now,
                data_hash=data_hash,
                is_detail=is_detail,
                **fields,
            )
            session.add(version)
            await session.flush()
            created = await session.get(Listing, listing_id)
            if created is not None:
                await _link_listing_seller(session, created, data, source)
            return listing_id, version_id, True, image_urls

        result = await session.execute(select(Listing).where(Listing.adid == adid))
        listing = result.scalar_one_or_none()
        if listing is None:
            raise RuntimeError(f"listing insert conflict for adid={adid} but row missing")

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
            await _link_listing_seller(session, listing, data, source)
            await session.flush()
            return listing.id, None, False, image_urls

    # Data changed - create new version
    version_id = new_uuid()
    version = ListingVersion(
        id=version_id,
        listing_id=listing.id,
        fetched_at=now,
        data_hash=data_hash,
        is_detail=is_detail,
        **fields,
    )
    listing.current_version_id = version_id
    listing.has_detail = is_detail
    session.add(version)
    await _link_listing_seller(session, listing, data, source)
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

    Returns ``None`` when we have no detail snapshot for this adid (search-card
    archival only, or never stored).  Uses ``listings.has_detail`` and the
    current version's ``is_detail`` flag — not field presence heuristics.

    Caller is responsible for falling back to the live ``/inserat/{id}``
    upstream call when this returns ``None``.
    """
    result = await session.execute(select(Listing).where(Listing.adid == adid))
    listing = result.scalar_one_or_none()
    if listing is None or not listing.has_detail or listing.current_version_id is None:
        return None

    result = await session.execute(
        select(ListingVersion).where(ListingVersion.id == listing.current_version_id)
    )
    v = result.scalar_one_or_none()
    if v is None or not v.is_detail:
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
    return await enrich_listing_seller(session, payload)

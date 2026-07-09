# Kleinanzeigen Proxy & Storage

A transparent proxy and archival service for the [ebay-kleinanzeigen-api](https://github.com/DanielWTE/ebay-kleinanzeigen-api). It forwards all requests to the upstream scraping API while persistently storing every listing and its images in a local database, building a historical archive with full change tracking.

## Features

- **Transparent Proxy** - Exposes the same endpoints as the upstream API. Drop-in replacement, returns identical responses.
- **Persistent Storage** - All listings are stored in SQLite with their own UUID primary keys.
- **Change Tracking** - Each listing gets a new version only when its content actually changes (hash-based deduplication). View count changes alone don't trigger new versions.
- **Image Archival** - Images are downloaded asynchronously in the background and stored on disk, referenced back to the listing and version that introduced them.
- **Crash Recovery** - Pending image downloads are re-enqueued on startup.

## Architecture

```
Client  ──▶  Proxy (:8001)  ──▶  kleinanzeigen-api (:8000)  ──▶  kleinanzeigen.de
                │
                ├── SQLite (listings, versions, images metadata)
                └── /data/images/ (downloaded image files)
```

### Database Schema

Three tables form the storage layer:

**`listings`** - Stable anchor per Kleinanzeigen ad. One row per unique `adid`, never duplicated.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT (UUID) | Proxy's own primary key |
| `adid` | TEXT | Original Kleinanzeigen ad ID |
| `first_seen_at` | DATETIME | When the listing was first encountered |
| `last_seen_at` | DATETIME | Updated on every fetch (even without changes) |
| `current_version_id` | TEXT (FK) | Points to the latest version |

**`listing_versions`** - A new row is created only when listing content changes.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT (UUID) | Version primary key |
| `listing_id` | TEXT (FK) | References `listings.id` |
| `fetched_at` | DATETIME | When this version was captured |
| `title` | TEXT | Listing title |
| `price_amount` | TEXT | Price value |
| `price_currency` | TEXT | Currency symbol |
| `price_negotiable` | BOOLEAN | Whether price is negotiable (VB) |
| `status` | TEXT | active / sold / reserved / deleted |
| `description` | TEXT | Full listing description |
| `url` | TEXT | Kleinanzeigen URL |
| `location_zip/city/state` | TEXT | Location fields |
| `delivery` | TEXT | Delivery option |
| `views` | TEXT | View count (stored but excluded from hash) |
| `categories` | TEXT (JSON) | Category breadcrumbs |
| `details` | TEXT (JSON) | Structured attributes (e.g., Marke, Hubraum) |
| `features` | TEXT (JSON) | Feature tags |
| `seller` | TEXT (JSON) | Seller info (name, type, badges) |
| `extra_info` | TEXT (JSON) | Additional metadata |
| `image_urls` | TEXT (JSON) | Original image URLs at time of fetch |
| `data_hash` | TEXT | SHA256 hash for deduplication |

**`images`** - One row per downloaded image file.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT (UUID) | Image primary key |
| `listing_id` | TEXT (FK) | References `listings.id` |
| `version_id` | TEXT (FK) | References `listing_versions.id` (which version introduced this image) |
| `original_url` | TEXT | Source URL on Kleinanzeigen CDN |
| `local_path` | TEXT | Path relative to image storage root |
| `downloaded_at` | DATETIME | When the image was downloaded |
| `file_size` | INTEGER | File size in bytes |
| `content_type` | TEXT | MIME type |
| `status` | TEXT | pending / downloaded / failed |

### Deduplication Strategy

Each listing version is hashed using SHA256 over a canonical JSON of all content fields **except `views`** (which changes constantly and isn't meaningful for tracking real updates). When a listing is fetched:

1. If the `adid` is new: create a `listings` row + first `listing_versions` row
2. If the `adid` exists and the hash matches the current version: only update `last_seen_at`
3. If the `adid` exists but the hash differs: create a new `listing_versions` row and update `current_version_id`

Concurrent cache misses for the same `adid` (e.g. several hunter jobs finishing detail fetches at once) use `INSERT OR IGNORE` on `listings.adid` so a lost race falls back to the existing row instead of returning HTTP 500.

## Prerequisites

- Docker and Docker Compose
- A running instance of [ebay-kleinanzeigen-api](https://github.com/DanielWTE/ebay-kleinanzeigen-api) (default: port 8000)

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/LukasBeckers/kleinanzeigen-proxy.git
cd kleinanzeigen-proxy
```

### 2. Configure the environment

Edit `.env` to point to your running API instance:

```env
API_BASE_URL=http://host.docker.internal:8000
DATABASE_URL=sqlite+aiosqlite:////data/proxy.db
IMAGE_STORAGE_PATH=/data/images
IMAGE_DOWNLOAD_CONCURRENCY=5
```

If both services run on the same Docker network, use the container name instead:
```env
API_BASE_URL=http://kleinanzeigen-api:8000
```

Multiple upstream API workers can be configured via `API_BASE_URLS` (comma-separated). The proxy load-balances across them using a **success-weighted** strategy:

- Each upstream keeps the last **100** attempt outcomes (HTTP 2xx = success, anything else = failure).
- Selection probability for upstream *i* is `successes_i / sum(successes_j)` over those windows.
- On startup each upstream's window is pre-filled with successes (optimistic prior) so traffic starts evenly split (~50/50 for two workers) until real failures displace them.

Example:

```env
API_BASE_URLS=http://host.docker.internal:8000,http://100.68.101.87:8001
```

Distribution stats (pick counts and per-upstream success ratios) are logged every 50 requests.

### 3. Start the proxy

```bash
docker compose up -d --build
```

Runtime data (`proxy.db`, cached images) lives in `./data` on the host and is bind-mounted to `/data` in the container. `.dockerignore` excludes `data/` from the image build so rebuilds stay fast; the volume mount is unchanged.

The proxy will be available at `http://localhost:8001`.

## API Endpoints

All endpoints mirror the upstream API and return identical responses.

### `GET /inserate` - Search listings

Search for listings with filters. Results are stored in the database.

```bash
curl "http://localhost:8001/inserate?query=mofa&location=52538&radius=100&max_price=300&page_count=5"
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | string | Search term |
| `location` | string | Location / postal code |
| `radius` | int | Search radius in km |
| `min_price` | int | Minimum price in EUR |
| `max_price` | int | Maximum price in EUR |
| `page_count` | int (1-20) | Number of pages to fetch (default: 1, 25 results/page) |

### `GET /inserat/{id}` - Get listing details

Fetch detailed information for a single listing. Triggers background image downloads.

```bash
curl "http://localhost:8001/inserat/3382586410"
```

Returns full details including title, description, price, location, seller info, images, and more.

### `GET /inserate-detailed` - Search with full details

Combined search + detail fetch in one request. Stores all data and downloads all images.

```bash
curl "http://localhost:8001/inserate-detailed?query=mofa&location=52538&radius=100&max_price=300"
```

Same parameters as `/inserate`, plus:

| Parameter | Type | Description |
|-----------|------|-------------|
| `max_concurrent_details` | int (1-10) | Concurrent detail fetches (default: 5) |

### `GET /inserate-detailed-cached` - Search with cache-first details

Same response shape as `/inserate-detailed`, but detail payloads are served from the proxy SQLite archive when available. Only cache misses call upstream `/inserat/{id}`.

```bash
curl "http://localhost:8001/inserate-detailed-cached?query=mofa&location=52538&radius=100&max_price=300"
```

The JSON body includes `performance_metrics` with `cache_hits`, `cache_misses`, `raw_cards_from_search`, and `search_upstream`. These measure **proxy-level** detail caching only.

Detail cache hits require a full `/inserat/{id}` snapshot in SQLite, not just a search card from `/inserate`. Search-only rows leave `image_urls`, `seller`, and `status` unset; detail rows always set them (including `image_urls = "[]"` when a listing has no photos) so imageless listings are cached after the first detail fetch and do not trigger repeat upstream navigations.

**Note for kleinanzeigen-hunter consumers:** `cache_hits` / `cache_misses` are unrelated to hunter's `new_count`. A run can scrape 25 listings (23 cache hits, 2 misses) yet show `new_count=0` when every adid was already seen by that job's `seen_listings` dedup. Cache hits mean "detail came from proxy storage"; `new_count` means "adid not yet processed under this job's pipeline hash".

## Data Access

### Query the database

```bash
# Enter the container
docker compose exec kleinanzeigen-proxy python3 -c "
import sqlite3
conn = sqlite3.connect('/data/proxy.db')
c = conn.cursor()

# Count stored listings
print(c.execute('SELECT count(*) FROM listings').fetchone()[0], 'listings')
print(c.execute('SELECT count(*) FROM listing_versions').fetchone()[0], 'versions')
print(c.execute('SELECT count(*) FROM images').fetchone()[0], 'images')
"
```

### Useful queries

```sql
-- All current listings with their latest data
SELECT l.id, l.adid, v.title, v.price_amount, v.location_city, v.fetched_at
FROM listings l
JOIN listing_versions v ON v.id = l.current_version_id;

-- Listings that have changed over time (multiple versions)
SELECT l.adid, count(*) as versions
FROM listings l
JOIN listing_versions v ON v.listing_id = l.id
GROUP BY l.adid
HAVING count(*) > 1;

-- All images for a specific listing
SELECT i.local_path, i.status, i.file_size, v.fetched_at
FROM images i
JOIN listing_versions v ON v.id = i.version_id
WHERE i.listing_id = 'some-uuid';

-- Images from the current version only
SELECT i.* FROM images i
JOIN listings l ON l.id = i.listing_id
WHERE l.current_version_id = i.version_id;
```

### Access downloaded images

Images are stored in the Docker volume under `/data/images/{adid}/{image-uuid}.{ext}`:

```bash
docker compose exec kleinanzeigen-proxy ls /data/images/
docker compose exec kleinanzeigen-proxy ls /data/images/3382586410/
```

## File Structure

```
kleinanzeigen-proxy/
├── main.py              # FastAPI app with lifespan (DB init, httpx client, image worker)
├── config.py            # Settings via pydantic-settings, loads .env
├── database.py          # Async SQLAlchemy engine and session
├── models.py            # ORM models: Listing, ListingVersion, Image
├── storage.py           # Dedup logic, hash computation, upsert operations
├── image_worker.py      # Background async image downloader (asyncio.Queue + workers)
├── routers/
│   ├── inserate.py      # GET /inserate proxy
│   ├── inserat.py       # GET /inserat/{id} proxy
│   └── inserate_detailed.py  # GET /inserate-detailed proxy
├── Dockerfile
├── docker-compose.yml
├── .env
└── requirements.txt
```

## Tech Stack

- **FastAPI** - Web framework
- **httpx** - Async HTTP client for upstream communication
- **SQLAlchemy** (async) + **aiosqlite** - Database ORM and async SQLite driver
- **pydantic-settings** - Configuration management
- **Docker** - Containerization with volume-based persistence

## License

MIT

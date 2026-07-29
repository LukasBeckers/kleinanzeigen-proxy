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
| `has_detail` | BOOLEAN | True when `current_version_id` is a full detail snapshot (not search-card only) |

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
| `is_detail` | BOOLEAN | True when this version came from a full `/inserat/{id}` (or equivalent) fetch |

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

Multiple upstream API workers can be configured via `API_BASE_URLS` (comma-separated). The proxy load-balances across them using a **success-weighted** strategy with optional **multiplicative weights**:

- Each upstream keeps the last **100** attempt outcomes (HTTP 2xx = success, anything else = failure).
- Selection probability for upstream *i* is:

  `P(i) = (successes_i · weight_i) / Σ_j (successes_j · weight_j)`

  Example: both workers fully healthy, weights `1.5` and `1` → P = 60% / 40%.
- On startup each upstream's window is pre-filled with successes (optimistic prior) so traffic starts evenly split (~50/50 for two workers with equal weights) until real failures displace them.
- Connect timeout is **10s** (fail fast on offline hosts); read/write still use the full scrape budget (default 300s).
- A long failure run can push a worker to **0% pick probability**. Admins can reseed its sliding window to a chosen **success rate** — see `POST /upstream-seed` and the hunter Admin page (5% steps).
- Weights default to `1` each. Set at boot via `API_BASE_WEIGHTS` (comma-separated, same order as URLs) or at runtime via `POST /upstream-weight` / the Admin UI.

Example:

```env
API_BASE_URLS=http://host.docker.internal:8000,http://100.68.101.87:8001
API_BASE_WEIGHTS=1.5,1
```

Distribution stats (pick counts and per-upstream success ratios) are logged every 50 requests.

Live stats are also exposed as JSON for dashboards:

```bash
curl http://localhost:8001/upstream-stats
```

Response shape:

```json
{
  "history_size": 100,
  "total_requests": 42,
  "upstreams": [
    {
      "url": "http://scraper-a:8000",
      "successes": 95,
      "failures": 5,
      "window_size": 100,
      "history_size": 100,
      "weight": 1.5,
      "probability": 0.66,
      "pick_count": 28,
      "outcomes": [true, true, false]
    }
  ]
}
```

### Admin: reseed window success rate

When a worker is stuck at 0% pick weight after a long outage, reseed **that worker's** sliding window so a chosen fraction of the last N attempts are successes (rest failures), in 5% steps. Other workers are left alone; pick probability then follows from relative success counts.

```bash
curl -X POST http://localhost:8001/upstream-seed \
  -H 'Content-Type: application/json' \
  -d '{"url":"http://100.68.101.87:8001","success_rate":0.80}'
```

Example: `success_rate: 0.80` on a history of 100 → 80 successes + 20 failures for that worker. Response is the usual stats payload plus a `seeded` diagnostic.

`successes` / `failures` count outcomes in the sliding window. `outcomes` is the ordered list (oldest → newest, `true`=ok) for the timeline UI. `probability` is the current selection weight (`successes_i / sum(successes_j)`). kleinanzeigen-hunter's admin panel reads this via `PROXY_BASE_URL`.

### 3. Start the proxy

```bash
docker compose up -d --build
```

Runtime data (`proxy.db`, cached images) lives in `./data` on the host and is bind-mounted to `/data` in the container. `.dockerignore` excludes `data/` from the image build so rebuilds stay fast; the volume mount is unchanged.

The proxy will be available at `http://localhost:8001`.

## API Endpoints

Most endpoints mirror the upstream API and return identical responses. Operational endpoints (`/`, `/upstream-stats`, `/upstream-seed`) are proxy-only.

### `GET /upstream-stats` - Load-balancer window + probabilities

Returns the rolling success/failure window and current pick probability for every configured upstream worker. See the load-balancing section above for field definitions.

### `POST /upstream-seed` - Reseed one worker's window success rate

Body: `{ "url": "<exact upstream base URL>", "success_rate": 0.80 }` with `success_rate` in 5% steps (0 = all fail, 1 = all success). Rewrites only that worker's sliding window. See the load-balancing section above.

### `POST /upstream-weight` - Set multiplicative pick weight

Body: `{ "url": "<exact upstream base URL>", "weight": 1.5 }` with `weight >= 0`. Applies immediately to pick probability; does not rewrite `.env` (restart reloads `API_BASE_WEIGHTS`).

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

Detail cache hits require `listings.has_detail = true` (current version has `is_detail = true`). Search-card archival from `GET /inserate` stores versions with `is_detail = false`; only full detail fetches flip the flags. Imageless listings are cached after the first detail fetch (`image_urls = "[]"`).

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

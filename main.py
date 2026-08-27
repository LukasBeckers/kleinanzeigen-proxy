import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config import settings
from database import init_db
from image_worker import ImageWorker
from routers import inserate, inserat, inserate_detailed
from upstream import LoadBalancedClient, NoEligibleUpstream

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    urls = settings.upstream_urls
    weights = settings.upstream_weights
    logger.info(
        "Connecting to upstream API(s): %s (weights=%s)",
        ", ".join(urls),
        weights,
    )

    await init_db()
    logger.info("Database initialized")

    app.state.upstream_client = LoadBalancedClient(
        urls,
        timeout=300.0,
        weights=weights,
        max_fail_rate=settings.max_fail_rate,
        cooldown_s=settings.cooldown_s,
    )

    app.state.image_worker = ImageWorker()
    await app.state.image_worker.start()

    yield

    # Shutdown
    await app.state.image_worker.stop()
    await app.state.upstream_client.aclose()
    logger.info("Shutdown complete")


app = FastAPI(title="Kleinanzeigen Proxy", version="1.0.0", lifespan=lifespan)


@app.exception_handler(NoEligibleUpstream)
async def _no_eligible_upstream(_request: Request, exc: NoEligibleUpstream):
    return JSONResponse(status_code=503, content={"detail": str(exc)})


app.include_router(inserate.router)
app.include_router(inserat.router)
app.include_router(inserate_detailed.router)


@app.get("/")
async def root():
    return {
        "service": "kleinanzeigen-proxy",
        "upstreams": settings.upstream_urls,
        "endpoints": [
            "/inserate",
            "/inserat/{id}",
            "/inserate-detailed",
            "/inserate-detailed-cached",
            "/upstream-stats",
            "/upstream-seed",
            "/upstream-weight",
            "/upstream-recycle",
            "/upstream-settings",
        ],
    }


@app.get("/upstream-stats")
async def upstream_stats(request: Request):
    """Sliding-window success/failure counts and current pick probabilities.

    Used by the kleinanzeigen-hunter admin panel to visualize load-balancer
    health per downstream scraper worker.
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    return client.stats()


class UpstreamSeedBody(BaseModel):
    url: str = Field(..., min_length=1, description="Exact upstream base URL")
    success_rate: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description=(
            "Fraction of this worker's sliding window that should be successes "
            "(0 = all fail, 1 = all success), in 5% steps"
        ),
    )


@app.post("/upstream-seed")
async def upstream_seed(body: UpstreamSeedBody, request: Request):
    """Reseed one upstream's sliding window to a given success rate.

    Sets that worker's last-N outcomes so ``success_rate`` of them are
    successes (and the rest failures). Does not change other workers. Used
    by the hunter admin panel after a recovered scraper was stuck at 0%
    pick weight.
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    try:
        return client.seed_window_success_rate(body.url, body.success_rate)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class UpstreamWeightBody(BaseModel):
    url: str = Field(..., min_length=1, description="Exact upstream base URL")
    weight: float = Field(
        ...,
        ge=0.0,
        description=(
            "Multiplicative pick weight (>= 0). With equal success windows, "
            "P(i) ∝ weight_i."
        ),
    )


@app.post("/upstream-weight")
async def upstream_weight(body: UpstreamWeightBody, request: Request):
    """Set the multiplicative pick weight for one upstream worker.

    Pick probability is ``(successes_i * weight_i) / sum_j(...)``. Runtime
    changes apply immediately; they do not rewrite ``API_BASE_WEIGHTS`` in
    the environment (restart reloads env defaults).
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    try:
        return client.set_weight(body.url, body.weight)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class UpstreamRecycleBody(BaseModel):
    url: str = Field(..., min_length=1, description="Exact upstream base URL")
    recycle_every: int = Field(
        ...,
        ge=1,
        description="Recycle Playwright Chromium after this many scrape operations",
    )


@app.post("/upstream-recycle")
async def upstream_recycle(body: UpstreamRecycleBody, request: Request):
    """Set one worker's Chromium recycle interval (scrape-count, not a timer).

    Forwards to that worker's ``POST /browser/recycle-every``. Runtime only;
    does not rewrite the worker's ``BROWSER_RECYCLE_EVERY`` env.
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    try:
        return await client.set_recycle_every(body.url, body.recycle_every)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"worker {body.url} unreachable or rejected recycle update: {exc}",
        ) from exc


class UpstreamSettingsBody(BaseModel):
    max_fail_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Trip a worker when its window fail rate exceeds this (default 0.25)",
    )
    cooldown_s: float | None = Field(
        default=None,
        ge=1,
        description="Seconds a tripped worker stays out of the pick pool (default 3600)",
    )


@app.get("/upstream-settings")
async def get_upstream_settings(request: Request):
    """Fail-rate trip + cooldown used by the load balancer."""
    client: LoadBalancedClient = request.app.state.upstream_client
    snap = client.stats()
    return {
        "max_fail_rate": snap["max_fail_rate"],
        "cooldown_s": snap["cooldown_s"],
    }


@app.patch("/upstream-settings")
async def patch_upstream_settings(body: UpstreamSettingsBody, request: Request):
    """Update fail-rate trip and/or cooldown. Runtime only; not env."""
    if body.max_fail_rate is None and body.cooldown_s is None:
        raise HTTPException(
            status_code=400, detail="provide max_fail_rate and/or cooldown_s"
        )
    client: LoadBalancedClient = request.app.state.upstream_client
    try:
        snap = client.set_circuit(
            max_fail_rate=body.max_fail_rate,
            cooldown_s=body.cooldown_s,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "max_fail_rate": snap["max_fail_rate"],
        "cooldown_s": snap["cooldown_s"],
        "upstreams": snap["upstreams"],
    }

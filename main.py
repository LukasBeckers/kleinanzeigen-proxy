import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from config import settings
from database import init_db
from image_worker import ImageWorker
from routers import inserate, inserat, inserate_detailed, seller
from upstream import LoadBalancedClient

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
        urls, timeout=300.0, weights=weights
    )

    app.state.image_worker = ImageWorker()
    await app.state.image_worker.start()

    yield

    # Shutdown
    await app.state.image_worker.stop()
    await app.state.upstream_client.aclose()
    logger.info("Shutdown complete")


app = FastAPI(title="Kleinanzeigen Proxy", version="1.0.0", lifespan=lifespan)

app.include_router(inserate.router)
app.include_router(inserat.router)
app.include_router(inserate_detailed.router)
app.include_router(seller.router)


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
            "/seller/{user_id}",
            "/upstream-stats",
            "/upstream-seed",
            "/upstream-weight",
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

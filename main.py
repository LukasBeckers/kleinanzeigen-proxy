import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from config import settings
from database import init_db
from image_worker import ImageWorker
from routers import inserate, inserat, inserate_detailed
from upstream import LoadBalancedClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    urls = settings.upstream_urls
    logger.info(f"Connecting to upstream API(s): {', '.join(urls)}")

    await init_db()
    logger.info("Database initialized")

    app.state.upstream_client = LoadBalancedClient(urls, timeout=300.0)

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
    fail_rate: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description=(
            "Fraction of this worker's sliding window that should be failures "
            "(0 = all success, 1 = all fail), in 5% steps"
        ),
    )


@app.post("/upstream-seed")
async def upstream_seed(body: UpstreamSeedBody, request: Request):
    """Reseed one upstream's sliding window to a given failure rate.

    Sets that worker's last-N outcomes so ``fail_rate`` of them are failures
    (and the rest successes). Does not change other workers. Used by the
    hunter admin panel after a recovered scraper was stuck at 0% pick weight.
    """
    client: LoadBalancedClient = request.app.state.upstream_client
    try:
        return client.seed_window_fail_rate(body.url, body.fail_rate)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

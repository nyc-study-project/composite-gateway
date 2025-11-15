from __future__ import annotations

import os
import asyncio
from datetime import datetime, timezone
from typing import Optional

import httpx
from httpx import AsyncClient, HTTPStatusError, RequestError
from fastapi import FastAPI, HTTPException, Body

COMPOSITE_SERVICE_NAME = "Composite Gateway"
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8001")
SPOT_SERVICE_URL = os.getenv(
    "SPOT_SERVICE_URL",
    "https://spot-management-642518168067.us-east1.run.app",
)
REVIEWS_SERVICE_URL = os.getenv("REVIEWS_SERVICE_URL", "http://localhost:8003")

DEFAULT_TIMEOUT = 5.0  # seconds

app = FastAPI(
    title="Composite Gateway Service",
    description="Aggregates Spot, User, and Reviews microservices into a unified API.",
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": COMPOSITE_SERVICE_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/")
async def root():
    return {
        "message": "Welcome to the Composite Gateway API. See /docs for OpenAPI UI."
    }


async def _get_json(client: AsyncClient, url: str):
    """Helper: GET a URL and return JSON or raise for HTTP error."""
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


@app.get("/composite/spots/{spot_id}/full")
async def get_spot_full(spot_id: str):
    """
    Composite endpoint.

    Fan-out in parallel to:
      - Spot service: /spots/{spot_id}
      - Reviews service: /reviews?spot_id={spot_id}
      - User service: /users (example index; adjust as needed)
    """
    async with AsyncClient() as client:
        try:
            spot_task = _get_json(client, f"{SPOT_SERVICE_URL}/studyspots/{spot_id}")

            reviews_task = _get_json(
                client, f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}"
            )
            users_task = _get_json(client, f"{USER_SERVICE_URL}/users")
            spot, reviews, users = await asyncio.gather(
                spot_task, reviews_task, users_task
            )
        except HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=e.response.text,
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Downstream request error: {e}",
            )

    return {
        "spot": spot,
        "reviews": reviews,
        "users": users,
    }


@app.get("/api/spots/{spot_id}")
async def proxy_get_spot(spot_id: str):
    """Façade that proxies GET studyspot by ID to the Spot service."""
    async with AsyncClient() as client:
        resp = await client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}")
    return resp.json(), resp.status_code, resp.headers.items()


@app.get("/api/reviews")
async def proxy_list_reviews(spot_id: Optional[str] = None):
    """
    Façade for listing reviews.

    Optional: /api/reviews?spot_id={id} to filter by spot.
    """
    query = f"?spot_id={spot_id}" if spot_id else ""
    async with AsyncClient() as client:
        resp = await client.get(f"{REVIEWS_SERVICE_URL}/reviews{query}")
    return resp.json(), resp.status_code, resp.headers.items()


@app.get("/api/users/{user_id}")
async def proxy_get_user(user_id: str):
    """Façade that proxies GET user by ID to the User service."""
    async with AsyncClient() as client:
        resp = await client.get(f"{USER_SERVICE_URL}/users/{user_id}")
    return resp.json(), resp.status_code, resp.headers.items()


@app.post("/api/reviews")
async def create_review_with_fk_check(payload: dict = Body(...)):
    """
    Create a review, but first validate that the referenced spot_id and user_id exist.
    """
    spot_id = payload.get("spot_id")
    user_id = payload.get("user_id")

    if not spot_id or not user_id:
        raise HTTPException(
            status_code=400, detail="spot_id and user_id are required"
        )

    async with AsyncClient() as client:
        spot_resp, user_resp = await asyncio.gather(
            client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}"),
            client.get(f"{USER_SERVICE_URL}/users/{user_id}"),
        )

        if spot_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Spot not found")
        if user_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found")

        if spot_resp.status_code >= 400:
            raise HTTPException(
                status_code=spot_resp.status_code,
                detail=spot_resp.text,
            )
        if user_resp.status_code >= 400:
            raise HTTPException(
                status_code=user_resp.status_code,
                detail=user_resp.text,
            )

        create_resp = await client.post(
            f"{REVIEWS_SERVICE_URL}/reviews", json=payload
        )

    return (
        create_resp.json()
        if create_resp.headers.get("content-type", "").startswith("application/json")
        else None,
        create_resp.status_code,
        create_resp.headers.items(),
    )


@app.post("/api/spots/{spot_id}/geocode")
async def start_geocode_job(spot_id: str):
    """Proxy to Spot service async geocode."""
    async with AsyncClient() as client:
        resp = await client.post(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode"
        )

    return (
        resp.json()
        if resp.headers.get("content-type", "").startswith("application/json")
        else None,
        resp.status_code,
        resp.headers.items(),
    )


@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Polling endpoint that proxies job status from the Spot service."""
    async with AsyncClient() as client:
        resp = await client.get(f"{SPOT_SERVICE_URL}/jobs/{job_id}")

    return resp.json(), resp.status_code, resp.headers.items()


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("FASTAPIPORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

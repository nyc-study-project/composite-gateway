from __future__ import annotations

import os
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException, Body, Response, Query
from httpx import AsyncClient, HTTPStatusError, RequestError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COMPOSITE_SERVICE_NAME = "Composite Gateway"

# For local dev these default to localhost ports.
# In Cloud Run you override them with env vars.
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8001")
SPOT_SERVICE_URL = os.getenv("SPOT_SERVICE_URL", "http://localhost:8002")
REVIEWS_SERVICE_URL = os.getenv("REVIEWS_SERVICE_URL", "http://localhost:8003")

DEFAULT_TIMEOUT = 5.0  # seconds

app = FastAPI(
    title="Composite Gateway Service",
    description=(
        "Aggregates Spot, User, and Reviews microservices into a unified API.\n\n"
        "Key features:\n"
        "- /composite/spots/{id}/full → spot + reviews + users (parallel fan-out)\n"
        "- FK validation in POST /api/reviews\n"
        "- Async geocode 202 flow via POST /api/spots/{id}/geocode + GET /api/tasks/{task_id}\n"
    ),
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_json(client: AsyncClient, url: str) -> Any:
    """
    Helper: GET a URL and return JSON or raise HTTPStatusError.
    Network errors are surfaced as RequestError and handled by callers.
    """
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


async def _safe_get_reviews(client: AsyncClient, url: str) -> List[Dict[str, Any]]:
    """
    GET reviews for a spot.

    - If the Reviews service returns 404, treat that as "no reviews yet" and return [].
    - For any other 4xx/5xx, raise HTTPException so the caller can decide.
    """
    try:
        resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    except RequestError as e:
        # Network-level failure reaching reviews service
        raise HTTPException(
            status_code=502,
            detail=f"Reviews service unreachable: {e}",
        ) from e

    if resp.status_code == 404:
        # Interpret "not found" as "no reviews yet" for aggregation purposes
        return []

    try:
        resp.raise_for_status()
    except HTTPStatusError as e:
        # Bubble up the underlying HTTP error from the reviews service
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.text,
        ) from e

    data = resp.json()
    # If your reviews service wraps responses like {"data": [...]}, normalize here:
    # return data.get("data", [])
    return data  # assuming it's already a list


# ---------------------------------------------------------------------------
# Basic/internal endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["internal"])
async def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": COMPOSITE_SERVICE_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "upstreams": {
            "USER_SERVICE_URL": USER_SERVICE_URL,
            "SPOT_SERVICE_URL": SPOT_SERVICE_URL,
            "REVIEWS_SERVICE_URL": REVIEWS_SERVICE_URL,
        },
    }


@app.get("/", tags=["internal"])
async def root() -> Dict[str, str]:
    return {
        "message": "Welcome to the Composite Gateway API. See /docs for OpenAPI UI."
    }


# ---------------------------------------------------------------------------
# Composite endpoint (parallel fan-out)
# ---------------------------------------------------------------------------

@app.get("/composite/spots/{spot_id}/full", tags=["composite"])
async def get_spot_full(spot_id: str) -> Dict[str, Any]:
    """
    Composite endpoint that aggregates data from all three microservices.

    - Spot service:    GET {SPOT_SERVICE_URL}/studyspots/{spot_id}
    - Reviews service: GET {REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}
                       (404 => no reviews, returns [])
    - User service:    GET {USER_SERVICE_URL}/users
                       (returns list of user profiles)

    Response shape:

    {
      "spot":    { ... },   # single spot object (normalized from Spot service)
      "reviews": [ ... ],   # list of reviews (may be empty)
      "users":   [ ... ]    # list of users (may be empty)
    }
    """
    async with AsyncClient() as client:
        try:
            spot_task = _get_json(
                client, f"{SPOT_SERVICE_URL}/studyspots/{spot_id}"
            )
            reviews_task = _safe_get_reviews(
                client, f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}"
            )
            users_task = _get_json(
                client, f"{USER_SERVICE_URL}/users"
            )

            spot_raw, reviews, users = await asyncio.gather(
                spot_task, reviews_task, users_task
            )

        except HTTPStatusError as e:
            # For non-404 errors from Spot/User (since Reviews handled above),
            # bubble up their status codes and messages.
            raise HTTPException(
                status_code=e.response.status_code,
                detail=e.response.text,
            ) from e
        except RequestError as e:
            # Network, DNS, or connection-level issues reaching downstream services
            raise HTTPException(
                status_code=502,
                detail=f"Downstream request error: {e}",
            ) from e

    # Spot service currently returns {"data": {...}, "links": [...]}
    # Normalize so composite response has just the spot object.
    spot = spot_raw.get("data", spot_raw)

    return {
        "spot": spot,
        "reviews": reviews,
        "users": users,
    }


# ---------------------------------------------------------------------------
# Facade / proxy endpoints
# ---------------------------------------------------------------------------

@app.get("/api/spots", tags=["proxy"])
async def proxy_list_spots(
    response: Response,
    limit: Optional[int] = Query(None),
    offset: Optional[int] = Query(None),
) -> Any:
    """
    Proxy for listing study spots, mirroring Spot service (if it supports paging).

    Example mapping:
      GET /api/spots?limit=10&offset=0 ->
      GET {SPOT_SERVICE_URL}/studyspots?limit=10&offset=0
    """
    params = {}
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset

    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots",
            params=params,
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag
    return resp.json()


@app.get("/api/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot(spot_id: str, response: Response) -> Any:
    """
    Facade that proxies GET studyspot by ID to the Spot service.
    Also forwards the ETag header so clients can do conditional requests.
    """
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag
    return resp.json()


@app.get("/api/reviews", tags=["proxy"])
async def proxy_list_reviews(
    response: Response,
    spot_id: Optional[str] = None,
) -> Any:
    """
    Facade for listing reviews.

    Optional query:
      - /api/reviews?spot_id={id} to filter by studyspot.
    """
    query = f"?spot_id={spot_id}" if spot_id else ""
    async with AsyncClient() as client:
        resp = await client.get(
            f"{REVIEWS_SERVICE_URL}/reviews{query}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    return resp.json()


@app.get("/api/users", tags=["proxy"])
async def proxy_list_users(response: Response) -> Any:
    """
    Facade that proxies GET /users (list) to the User service.
    """
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/users",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"User service unreachable: {e}",
            ) from e

    response.status_code = resp.status_code
    try:
        return resp.json()
    except ValueError:
        return {"detail": f"User service returned non-JSON: {resp.text}"}


@app.get("/api/users/{user_id}", tags=["proxy"])
async def proxy_get_user(user_id: str, response: Response) -> Any:
    """
    Facade that proxies GET user by ID to the User service.
    """
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/users/{user_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"User service unreachable: {e}",
            ) from e

    response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": f"User service returned non-JSON: {resp.text}"}


# ---------------------------------------------------------------------------
# FK-validated review creation
# ---------------------------------------------------------------------------
@app.post("/api/reviews", tags=["composite"])
async def create_review_with_fk_check(
    payload: Dict[str, Any] = Body(...),
    response: Response = None,
) -> Any:
    """
    Create a review, but first validate that the referenced spot_id and user_id exist.

    Steps:
      1. Check payload has spot_id and user_id.
      2. GET Spot and User from their respective services.
      3. If both exist, forward POST to reviews service.
    """
    spot_id = payload.get("spot_id")
    user_id = payload.get("user_id")

    if not spot_id or not user_id:
        raise HTTPException(
            status_code=400,
            detail="spot_id and user_id are required",
        )

    async with AsyncClient() as client:
        # Parallel FK validation calls
        try:
            spot_resp, user_resp = await asyncio.gather(
                client.get(
                    f"{SPOT_SERVICE_URL}/studyspots/{spot_id}",
                    timeout=DEFAULT_TIMEOUT,
                ),
                client.get(
                    f"{USER_SERVICE_URL}/users/{user_id}",
                    timeout=DEFAULT_TIMEOUT,
                ),
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Downstream FK validation error: {e}",
            ) from e

        # FK existence checks
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

        # Both exist → create the review in Reviews service
        create_resp = await client.post(
            f"{REVIEWS_SERVICE_URL}/review/{spot_id}/user/{user_id}",
            json=payload,
            timeout=DEFAULT_TIMEOUT,
        )

    if response is not None:
        response.status_code = create_resp.status_code

    try:
        return create_resp.json()
    except ValueError:
        return {"detail": f"Reviews service returned non-JSON: {create_resp.text}"}



# ---------------------------------------------------------------------------
# Async geocode 202 + polling (proxying Spot service jobs)
# ---------------------------------------------------------------------------

@app.post("/api/spots/{spot_id}/geocode", tags=["async"])
async def start_geocode_job(spot_id: str, response: Response) -> Any:
    """
    Start an asynchronous geocoding job for a studyspot.

    Proxies to:
      POST {SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode

    If Spot service returns:
      { "message": "...", "job_id": "..." } with 202 + Location: /jobs/{job_id}

    This endpoint:
      - returns same body
      - rewrites Location to /api/tasks/{job_id} so clients stay on Composite
    """
    async with AsyncClient() as client:
        try:
            resp = await client.post(
                f"{SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Spot service unreachable for geocode job: {e}",
            ) from e

    response.status_code = resp.status_code

    try:
        job_json = resp.json()
    except ValueError:
        return {"detail": f"Unexpected response from Spot service: {resp.text}"}

    job_id = job_json.get("job_id") or job_json.get("id")

    if job_id:
        # Rewrite Location so clients only talk to composite
        response.headers["Location"] = f"/api/tasks/{job_id}"

    return job_json


@app.get("/api/tasks/{task_id}", tags=["async"])
async def get_task_status(task_id: str, response: Response) -> Any:
    """
    Poll the status of an async geocode job.

    Proxies to:
      GET {SPOT_SERVICE_URL}/jobs/{task_id}
    """
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{SPOT_SERVICE_URL}/jobs/{task_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Spot service unreachable for task status: {e}",
            ) from e

    response.status_code = resp.status_code
    try:
        return resp.json()
    except ValueError:
        return {"detail": f"Spot service returned non-JSON: {resp.text}"}


# ---------------------------------------------------------------------------
# Local dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("FASTAPIPORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

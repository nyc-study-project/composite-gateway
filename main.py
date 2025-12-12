from __future__ import annotations

import os
import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import httpx
from httpx import AsyncClient, RequestError
from fastapi import FastAPI, HTTPException, Body, Response, Request, Header
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4
from google.cloud import pubsub_v1
from concurrent.futures import ThreadPoolExecutor
import httpx
executor = ThreadPoolExecutor(max_workers=5)


COMPOSITE_SERVICE_NAME = "Composite Gateway"

USER_SERVICE_URL = "http://34.139.134.144:8002"
SPOT_SERVICE_URL = "https://spot-management-642518168067.us-east1.run.app"
REVIEWS_SERVICE_URL = "https://reviews-api-c73xxvyjwq-ue.a.run.app"

DEFAULT_TIMEOUT = 20.0

GCP_PROJECT_ID = "study-spot-nyc"
PUBSUB_TOPIC_ID = "composite-events"

publisher = pubsub_v1.PublisherClient()
TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)


def emit_event(event_type: str, data: dict) -> None:
    envelope = {
        "event_type": event_type,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    message_bytes = json.dumps(envelope).encode("utf-8")
    try:
        publisher.publish(TOPIC_PATH, message_bytes)
    except Exception:
        pass


app = FastAPI(
    title="Composite Gateway Service",
    description=(
        "Aggregates Spot, User, and Reviews microservices into a unified API.\n\n"
        "Key features:\n"
        "- /composite/spots/{id}/full\n"
        "- FK validation in POST /api/reviews\n"
        "- Async geocode flow\n"
        "- Pub/Sub events\n"
    ),
    version="1.0.0",
)

ALLOWED_ORIGINS = [
    "http://localhost",
    "http://127.0.0.1",
    "https://nyc-study-spots-frontend.storage.googleapis.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_auth_headers(request: Request) -> dict:
    """Helper to extract token from Cookie or Header and format for User Service."""
    token = request.cookies.get("access_token")
    
    if not token:
        # Fallback: Check Authorization header (for API calls/Mobile)
        auth = request.headers.get("Authorization") or request.headers.get("auth")
        if auth:
            token = auth.split(" ")[1] if "Bearer" in auth else auth
            
    return {"auth": f"Bearer {token}"} if token else {}

async def _get_json(client: AsyncClient, url: str):
    try:
        resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    except RequestError as e:
        raise HTTPException(502, f"Downstream unreachable: {e}")
    resp.raise_for_status()
    return resp.json()


async def _safe_get_reviews(client: AsyncClient, url: str):
    try:
        resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    except RequestError as e:
        raise HTTPException(502, f"Downstream unreachable: {e}")
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json()


@app.get("/health", tags=["internal"])
async def health():
    return {
        "status": "ok",
        "service": COMPOSITE_SERVICE_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", tags=["internal"])
async def root():
    return {"message": "Welcome to the Composite Gateway API. See /docs."}


def fetch_all_users_blocking():
    with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
        r = client.get(f"{USER_SERVICE_URL}/users")
        r.raise_for_status()
        return r.json()


@app.get("/composite/spots/{spot_id}/full", tags=["composite"])
async def get_spot_full(spot_id: str):
    user_future = executor.submit(fetch_all_users_blocking)

    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            results = await asyncio.gather(
                client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{REVIEWS_SERVICE_URL}/reviews/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{REVIEWS_SERVICE_URL}/ratings/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{REVIEWS_SERVICE_URL}/ratings/{spot_id}/average", timeout=DEFAULT_TIMEOUT),
                return_exceptions=True
            )

            spot_res, review_res, rating_res, avg_res = results

            try:
                users = user_future.result()
            except Exception:
                users = []

            spot = (
                spot_res.json().get("data", spot_res.json())
                if not isinstance(spot_res, Exception) and spot_res.status_code == 200
                else None
            )

            def unwrap_list(response):
                if isinstance(response, Exception) or response.status_code != 200:
                    return []
                jd = response.json()
                if isinstance(jd, list):
                    return [item["data"] if "data" in item else item for item in jd]
                return jd

            reviews = unwrap_list(review_res)
            ratings = unwrap_list(rating_res)

            rating_summary = None
            if not isinstance(avg_res, Exception) and avg_res.status_code == 200:
                rating_summary = avg_res.json().get("data")

            for r in reviews:
                uid = r.get("user_id")
                user_rating = next((item for item in ratings if item.get("user_id") == uid), None)
                if user_rating:
                    r["rating"] = user_rating.get("rating")

        except Exception as e:
            raise HTTPException(500, str(e))

    return {
        "spot": spot,
        "reviews": reviews,
        "users": users,
        "rating_summary": rating_summary
    }


@app.get("/api/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot_api(spot_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT)
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot_simple(spot_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT)
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/api/reviews", tags=["proxy"])
async def proxy_list_reviews(spot_id: Optional[str] = None, response: Response = None):
    url = f"{REVIEWS_SERVICE_URL}/reviews/{spot_id}" if spot_id else f"{REVIEWS_SERVICE_URL}/reviews"

    async with AsyncClient() as client:
        try:
            resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    if response is not None:
        response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/api/spots", tags=["proxy"])
async def proxy_list_spots(
    response: Response,
    page: int = 1,
    page_size: int = 10,
    city: Optional[str] = None,
    open_now: Optional[bool] = None
):
    params = {"page": page, "page_size": page_size}
    if city:
        params["city"] = city
    if open_now is not None:
        params["open_now"] = str(open_now).lower()

    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{SPOT_SERVICE_URL}/studyspots",
                params=params,
                timeout=DEFAULT_TIMEOUT
            )
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code
    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/api/users/{user_id}", tags=["proxy"])
async def proxy_get_user(user_id: str, request: Request, response: Response):
    headers = _get_auth_headers(request)

    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/users/{user_id}", 
                headers=headers,
                timeout=DEFAULT_TIMEOUT
            )
        except RequestError as e:
            raise HTTPException(502, f"User service error: {e}")

    response.status_code = resp.status_code
    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}

@app.post("/api/reviews", tags=["composite"])
async def create_review_with_fk_check(payload: dict = Body(...), response: Response = None):
    spot_id = payload.get("spot_id")
    user_id = payload.get("user_id")
    rating_val = payload.get("rating")
    comment_val = payload.get("comment") or payload.get("review") or ""

    if not spot_id or not user_id:
        raise HTTPException(400, "spot_id and user_id required")

    async with AsyncClient() as client:
        try:
            spot_resp, user_resp = await asyncio.gather(
                client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{USER_SERVICE_URL}/users/{user_id}", timeout=DEFAULT_TIMEOUT),
            )
        except RequestError as e:
            raise HTTPException(502, f"Downstream service unavailable: {e}")

        if spot_resp.status_code == 404:
            raise HTTPException(404, "Spot not found")
        if user_resp.status_code == 404:
            raise HTTPException(404, "User not found")

        current_time = datetime.now(timezone.utc).isoformat()

        review_payload = {
            "id": str(uuid4()),
            "postDate": current_time,
            "review": comment_val,
        }

        rating_payload = {
            "id": str(uuid4()),
            "postDate": current_time,
            "rating": int(rating_val) if rating_val else 0
        }

        tasks = []
        tasks.append(client.post(
            f"{REVIEWS_SERVICE_URL}/review/{spot_id}/user/{user_id}",
            json=review_payload,
            timeout=DEFAULT_TIMEOUT,
        ))

        if rating_val is not None:
            tasks.append(client.post(
                f"{REVIEWS_SERVICE_URL}/rating/{spot_id}/user/{user_id}",
                json=rating_payload,
                timeout=DEFAULT_TIMEOUT,
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                raise HTTPException(500, f"Downstream error: {res}")
            if hasattr(res, "status_code") and res.status_code >= 400:
                raise HTTPException(res.status_code, f"Review service failed: {res.text}")

    emit_event(
        "review_created",
        {"spot_id": spot_id, "user_id": user_id, "rating": rating_val, "comment": comment_val},
    )

    if response:
        response.status_code = 201

    return {"message": "Review and rating created", "review_id": review_payload["id"], "rating": rating_val}


@app.post("/api/spots/{spot_id}/geocode", tags=["async"])
async def start_geocode_job(spot_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.post(
                f"{SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code
    try:
        job_json = resp.json()
    except ValueError:
        return {"detail": f"Unexpected response: {resp.text}"}

    job_id = job_json.get("job_id") or job_json.get("id")
    if job_id:
        response.headers["Location"] = f"/api/tasks/{job_id}"

    return job_json


@app.get("/api/tasks/{task_id}", tags=["async"])
async def get_task_status(task_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.get(f"{SPOT_SERVICE_URL}/jobs/{task_id}", timeout=DEFAULT_TIMEOUT)
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code
    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/auth/callback/google", tags=["proxy"])
async def google_oauth_callback(request: Request):
    FRONTEND_URL = "https://nyc-study-spots-frontend.storage.googleapis.com/index.html"
    user_management_url = "http://34.139.134.144:8002/auth/callback/google"
    
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                user_management_url,
                params=request.query_params,
                cookies=request.cookies,
                timeout=10.0
            )
        except RequestError as e:
            raise HTTPException(502, f"Could not connect to User Service: {e}")

    if resp.status_code != 200:
        return RedirectResponse(url=f"{FRONTEND_URL}#/error?msg=LoginFailed")

    try:
        data = resp.json()
        token = data.get("jwt")
    except Exception:
        token = None

    if not token:
        return RedirectResponse(url=f"{FRONTEND_URL}#/error?msg=NoToken")

    # 3. Redirect with token in URL (Keeps Frontend compatible)
    # The frontend likely looks for 'session_id', so we trick it by passing the JWT there.
    redirect_url = f"{FRONTEND_URL}#/callback?session_id={token}"
    response = RedirectResponse(url=redirect_url)

    # 4. Set HttpOnly Cookie (Best Practice for persistence)
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="none",
        secure=True, 
        max_age=3600   # 1 hour
    )
    
    return response


@app.get("/auth/login/google", tags=["proxy"])
async def google_oauth_login(request: Request, response: Response):
    user_management_url = "http://34.139.134.144:8002/auth/login/google"

    async with AsyncClient() as client:
        try:
            resp = await client.get(
                user_management_url,
                params=request.query_params,
                cookies=request.cookies, 
                timeout=10.0
            )
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code

    for key, value in resp.headers.items():
        if key.lower() not in ["content-length", "content-encoding"]:
            response.headers[key] = value

    return resp.content


@app.get("/auth/me", tags=["auth"])
async def auth_me(request: Request):
    headers = _get_auth_headers(request)
    
    if not headers:
        raise HTTPException(401, "Missing Authorization credentials")

    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/auth/me",
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, resp.text)

    return resp.json()


@app.post("/auth/logout", status_code=204, tags=["auth"])
async def auth_logout(request: Request, response: Response):
    response.delete_cookie("access_token")
    return response


@app.delete("/api/reviews/{review_id}", tags=["composite"])
async def delete_review_proxy(review_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.delete(
                f"{REVIEWS_SERVICE_URL}/review/{review_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(502, f"Downstream unreachable: {e}")

    response.status_code = resp.status_code
    if resp.status_code == 404:
        raise HTTPException(404, "Review not found")

    return None


@app.post("/composite/ratings/batch", tags=["composite"])
async def get_batch_ratings(payload: dict = Body(...)):
    """
    Input:  { "ids": ["uuid-1", "uuid-2"] }
    Output: { "uuid-1": 4.5, "uuid-2": 3.2 }
    """
    spot_ids = payload.get("ids", [])
    if not spot_ids:
        return {}

    async with AsyncClient() as client:
        tasks = [
            client.get(f"{REVIEWS_SERVICE_URL}/ratings/{sid}/average", timeout=5.0)
            for sid in spot_ids
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    results = {}
    for sid, resp in zip(spot_ids, responses):
        if not isinstance(resp, Exception) and resp.status_code == 200:
            data = resp.json().get("data", {})
            results[sid] = data.get("average_rating", 0) or 0
            
    return results




@app.get("/proxy/user/health", tags=["internal"])
async def proxy_user_health():
    """
    Server-side health proxy to avoid cors nonsense
    """
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/health",
                timeout=5.0,
            )
        except RequestError:
            return Response(
                content=json.dumps({
                    "status": "unreachable",
                    "service": "User Service",
                }),
                status_code=503,
                media_type="application/json",
            )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "application/json"),
    )
from __future__ import annotations

import os
import asyncio
import json
import httpx
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Body, Response, Request
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from httpx import AsyncClient, HTTPStatusError, RequestError
from uuid import uuid4 # 

from google.cloud import pubsub_v1

# ---------- Config ----------

COMPOSITE_SERVICE_NAME = "Composite Gateway"

USER_SERVICE_URL = "http://34.139.134.144:8002"
SPOT_SERVICE_URL = "https://spot-management-642518168067.us-east1.run.app"
REVIEWS_SERVICE_URL = "https://reviews-api-c73xxvyjwq-ue.a.run.app"

DEFAULT_TIMEOUT = 20.0  # seconds

GCP_PROJECT_ID = "study-spot-nyc"
PUBSUB_TOPIC_ID = "composite-events"

# ---------- Pub/Sub setup ----------

publisher = pubsub_v1.PublisherClient()
TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)


def emit_event(event_type: str, data: dict) -> None:
    """
    Fire-and-forget helper to publish a small JSON event to Pub/Sub.

    This is intentionally simple: we don't wait on the future or handle retries,
    since for the project we just need to demonstrate event emission.
    """
    envelope = {
        "event_type": event_type,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    message_bytes = json.dumps(envelope).encode("utf-8")
    try:
        publisher.publish(TOPIC_PATH, message_bytes)
    except Exception as exc:  # noqa: BLE001
        # We *never* fail the main request just because Pub/Sub failed.
        print(f"[WARN] Failed to publish event to Pub/Sub: {exc}")


# ---------- FastAPI app ----------

app = FastAPI(
    title="Composite Gateway Service",
    description=(
        "Aggregates Spot, User, and Reviews microservices into a unified API.\n\n"
        "Key features:\n"
        "- /composite/spots/{id}/full → spot + reviews + users (parallel fan-out)\n"
        "- FK validation in POST /api/reviews (composite write)\n"
        "- Async geocode 202 flow via POST /api/spots/{id}/geocode + GET /api/tasks/{task_id}\n"
        "- Pub/Sub events on review creation (Cloud Run function listener)\n"
    ),
    version="1.0.0",
)

# ---------- CORS ----------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in prod, lock this to your frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------


async def _get_json(client: AsyncClient, url: str):
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


async def _safe_get_reviews(client: AsyncClient, url: str):
    """
    GET reviews for a spot.
    If reviews service returns 404, treat as "no reviews yet" → [].
    """
    resp = await client.get(url, timeout=DEFAULT_TIMEOUT)

    if resp.status_code == 404:
        return []

    resp.raise_for_status()
    return resp.json()


# ---------- Basic endpoints ----------


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


# ---------- Composite endpoint ----------


# Replace the existing get_spot_full function with this one

@app.get("/composite/spots/{spot_id}/full", tags=["composite"])
async def get_spot_full(spot_id: str):
    """
    Fan-out read:
    - Spot service:    GET /studyspots/{spot_id}
    - Reviews service: GET /reviews/{spot_id}
    - Reviews service: GET /ratings/{spot_id}
    - Reviews service: GET /ratings/{spot_id}/average
    - User service:    GET /users
    """
    async with AsyncClient() as client:
        try:
            # 1. Launch all requests in parallel
            results = await asyncio.gather(
                client.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{REVIEWS_SERVICE_URL}/reviews/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{REVIEWS_SERVICE_URL}/ratings/{spot_id}", timeout=DEFAULT_TIMEOUT),
                client.get(f"{REVIEWS_SERVICE_URL}/ratings/{spot_id}/average", timeout=DEFAULT_TIMEOUT),
                client.get(f"{USER_SERVICE_URL}/users", timeout=DEFAULT_TIMEOUT),
                return_exceptions=True
            )

            spot_res, review_res, rating_res, avg_res, user_res = results

            # 2. Extract Data Safely
            spot = spot_res.json().get("data", spot_res.json()) if not isinstance(spot_res, Exception) and spot_res.status_code == 200 else None
            users = user_res.json() if not isinstance(user_res, Exception) and user_res.status_code == 200 else []
            
            # Helper to unwrap "data" key if present (Handles your Review API format)
            def unwrap_list(response):
                if isinstance(response, Exception) or response.status_code != 200:
                    return []
                json_data = response.json()
                # If API returns [ { "data": {...} }, ... ] -> return [ {...}, ... ]
                if isinstance(json_data, list):
                    return [item["data"] if "data" in item else item for item in json_data]
                return json_data

            reviews = unwrap_list(review_res)
            ratings = unwrap_list(rating_res)

            # Rating Summary
            rating_summary = None
            if not isinstance(avg_res, Exception) and avg_res.status_code == 200:
                rating_summary = avg_res.json().get("data")

            # 3. MERGE LOGIC: Attach star ratings to reviews
            # Now that we've unwrapped the data, this logic works!
            for r in reviews:
                r_user_id = r.get("user_id")
                
                # Find matching rating for this user
                user_rating = next((item for item in ratings if item.get("user_id") == r_user_id), None)
                
                if user_rating:
                    r["rating"] = user_rating.get("rating")

        except Exception as e:
            print(f"Composite Error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return {
        "spot": spot,
        "reviews": reviews,
        "users": users,
        "rating_summary": rating_summary
    }
# ---------- Proxy endpoints ----------


@app.get("/api/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot_api(spot_id: str, response: Response):
    """Proxy for the /api/spots/{spot_id} route."""
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}

# -------------------------------------------------------------
# *** NEW ROUTE ADDED HERE TO FIX FRONTEND 404 ***
# -------------------------------------------------------------

@app.get("/spots/{spot_id}", tags=["proxy"])
async def proxy_get_spot_simple(spot_id: str, response: Response):
    """
    Simple proxy for the frontend to fetch spot details via /spots/{spot_id}.
    This route handles the likely path requested by the frontend when clicking 'Details'.
    It proxies the request to the Spot Management Service's /studyspots endpoint.
    """
    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}", timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    etag = resp.headers.get("etag")
    if etag:
        response.headers["ETag"] = etag

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}
        
# -------------------------------------------------------------


@app.get("/api/reviews", tags=["proxy"])
async def proxy_list_reviews(
    spot_id: Optional[str] = None,
    response: Response = None,
):

    if spot_id:
        url = f"{REVIEWS_SERVICE_URL}/reviews/{spot_id}"
    else:
        url = f"{REVIEWS_SERVICE_URL}/reviews" 

    async with AsyncClient() as client:
        resp = await client.get(url, timeout=DEFAULT_TIMEOUT)

    if response is not None:
        response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


# Add this to main.py to handle the list view!
@app.get("/api/spots", tags=["proxy"])
async def proxy_list_spots(
    response: Response,
    page: int = 1,
    page_size: int = 10,
    city: Optional[str] = None,
    open_now: Optional[bool] = None
):
    """
    Proxies the list/search endpoint to the Spot Service.
    Matches the frontend call: GET /api/spots?page=1...
    """
    params = {"page": page, "page_size": page_size}
    if city:
        params["city"] = city
    if open_now is not None:
        params["open_now"] = str(open_now).lower()

    async with AsyncClient() as client:
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/studyspots", 
            params=params,
            timeout=DEFAULT_TIMEOUT
        )

    response.status_code = resp.status_code
    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/api/users/{user_id}", tags=["proxy"])
async def proxy_get_user(user_id: str, response: Response):
    async with AsyncClient() as client:
        try:
            resp = await client.get(
                f"{USER_SERVICE_URL}/users/{user_id}",
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestError as e:
            raise HTTPException(status_code=502, detail=f"User service error: {e}")

    response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


# ---------- Review w/ FK validation + Pub/Sub event ----------


@app.post("/api/reviews", tags=["composite"])
async def create_review_with_fk_check(
    payload: dict = Body(...),
    response: Response = None,
):
    """
    Composite FK-validation for creating a review AND a rating.
    """
    spot_id = payload.get("spot_id")
    user_id = payload.get("user_id")
    rating_val = payload.get("rating")
    comment_val = payload.get("comment") or payload.get("review") or ""

    if not spot_id or not user_id:
        raise HTTPException(status_code=400, detail="spot_id and user_id required")

    async with AsyncClient() as client:
        # 1) Parallel FK checks (Check if Spot and User exist)
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

        if spot_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Spot not found")
        if user_resp.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found")

        # 2) Prepare Payloads
        # We must generate IDs here because the Reviews Service expects them in the body
        current_time = datetime.now(timezone.utc).isoformat()
        
        # Payload for the Review (Text)
        review_payload = {
            "id": str(uuid4()),
            "postDate": current_time,
            "review": comment_val,
        }

        # Payload for the Rating (Stars)
        rating_payload = {
            "id": str(uuid4()),
            "postDate": current_time,
            "rating": int(rating_val) if rating_val else 0
        }

        # 3) Parallel Writes to Reviews Service
        # We create tasks to send both the text and the stars
        tasks = []
        
        # A: Send Review Text
        tasks.append(client.post(
            f"{REVIEWS_SERVICE_URL}/review/{spot_id}/user/{user_id}",
            json=review_payload,
            timeout=DEFAULT_TIMEOUT,
        ))

        # B: Send Rating (if it exists)
        if rating_val is not None:
            tasks.append(client.post(
                f"{REVIEWS_SERVICE_URL}/rating/{spot_id}/user/{user_id}",
                json=rating_payload,
                timeout=DEFAULT_TIMEOUT,
            ))

        # Execute both
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for errors in results
        for res in results:
            if isinstance(res, Exception):
                print(f"[Composite] Error saving review/rating: {res}")
                raise HTTPException(status_code=500, detail=f"Downstream error: {res}")
            elif hasattr(res, 'status_code') and res.status_code >= 400:
                 print(f"[Composite] Downstream error: {res.status_code} - {res.text}")
                 raise HTTPException(status_code=res.status_code, detail=f"Review service failed: {res.text}")

    # 4) Emit Pub/Sub event
    emit_event(
        "review_created",
        {
            "spot_id": spot_id,
            "user_id": user_id,
            "rating": rating_val,
            "comment": comment_val,
        },
    )

    if response: 
        response.status_code = 201

    # Return a combined success message
    return {
        "message": "Review and rating created", 
        "review_id": review_payload["id"],
        "rating": rating_val
    }


# ---------- Async geocode endpoints ----------


@app.post("/api/spots/{spot_id}/geocode", tags=["async"])
async def start_geocode_job(spot_id: str, response: Response):
    async with AsyncClient() as client:
        resp = await client.post(
            f"{SPOT_SERVICE_URL}/studyspots/{spot_id}/geocode",
            timeout=DEFAULT_TIMEOUT,
        )

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
        resp = await client.get(
            f"{SPOT_SERVICE_URL}/jobs/{task_id}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code

    try:
        return resp.json()
    except ValueError:
        return {"detail": resp.text}


@app.get("/auth/callback/google", tags=["proxy"])
async def google_oauth_callback(request: Request):
    print("[Composite] OAuth Callback hit. Query params:", request.query_params)

    FRONTEND_URL = "https://nyc-study-spots-frontend.storage.googleapis.com/index.html"
    
    # ⚠️ NOTE: Hardcoding IPs is risky. If the VM restarts, this IP might change.
    user_management_url = "http://34.139.134.144:8002/auth/callback/google"

    # Convert request cookies to a standard dict to ensure compatibility
    cookies = dict(request.cookies)

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                user_management_url,
                params=request.query_params,
                cookies=cookies,
                timeout=10.0  # Increased timeout for safety
            )
        except httpx.RequestError as exc:
            print(f"[Composite] ❌ Connection failed to User Management: {exc}")
            raise HTTPException(status_code=502, detail=f"Could not connect to User Service: {exc}")

    print(f"[Composite] VM Response Status: {resp.status_code}")
    
    # Handle cases where the VM returns an error (like 400 or 500)
    if resp.status_code != 200:
        print(f"[Composite] ❌ User Service returned error: {resp.text}")
        raise HTTPException(status_code=resp.status_code, detail="Login failed in User Service")

    try:
        data = resp.json()
        session_id = data.get("session_id")
    except Exception as e:
        print(f"[Composite] JSON Decode Error: {e}")
        session_id = None

    if not session_id:
        print("[Composite] ❌ CRITICAL: No session_id received from VM!")
        # Optional: Redirect to an error page instead of crashing
        return RedirectResponse(url=f"{FRONTEND_URL}#/error?msg=LoginFailed")

    print(f"[Composite] ✅ Session ID received: {session_id[:10]}...")

    redirect_url = f"{FRONTEND_URL}#/callback?session_id={session_id}"
    return RedirectResponse(url=redirect_url)

@app.get("/auth/login/google", tags=["proxy"])
async def google_oauth_login(request: Request, response: Response):
    user_management_url = "http://34.139.134.144:8002/auth/login/google" 
    async with httpx.AsyncClient() as client:
        # 1. Capture the Redirect + Set-Cookie from VM
        resp = await client.get(
            user_management_url, 
            params=request.query_params, 
            follow_redirects=False
        )
        
        response.status_code = resp.status_code
        
        # 2. CAREFULLY copy headers to ensure Set-Cookie survives
        for key, value in resp.headers.items():
            if key.lower() not in ["content-length", "content-encoding"]:
                response.headers[key] = value
                
        return resp.content

#if __name__ == "__main__":
    #import uvicorn

    #port = int(os.environ.get("FASTAPIPORT", 8000))
    #uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)



from fastapi import Header

# composite.py

@app.get("/auth/me", tags=["auth"])
async def auth_me(authorization: str = Header(None)):
    """
    Proxy /auth/me to the User Management service.
    TRANSLATION: Frontend sends 'Authorization' -> We send 'auth' to VM.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{USER_SERVICE_URL}/auth/me",
            # ✅ FIX: Map 'Authorization' to 'auth'
            headers={"auth": authorization}, 
            timeout=DEFAULT_TIMEOUT,
        )

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()


@app.post("/auth/logout", status_code=204, tags=["auth"])
async def auth_logout(authorization: str = Header(None)):
    """
    Proxy /auth/logout to the User Management service.
    TRANSLATION: Frontend sends 'Authorization' -> We send 'auth' to VM.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{USER_SERVICE_URL}/auth/logout",
            # ✅ FIX: Map 'Authorization' to 'auth'
            headers={"auth": authorization},
            timeout=DEFAULT_TIMEOUT,
        )

    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return Response(status_code=204)




@app.delete("/api/reviews/{review_id}", tags=["composite"])
async def delete_review_proxy(review_id: str, response: Response):
    """
    Proxy DELETE to the Reviews Service.
    Only deletes the text review (reviews table). 
    Ratings (stars) remain as they have a separate ID not linked here.
    """
    async with AsyncClient() as client:
        # Call the microservice: DELETE /review/{reviewId}
        resp = await client.delete(
            f"{REVIEWS_SERVICE_URL}/review/{review_id}",
            timeout=DEFAULT_TIMEOUT,
        )

    response.status_code = resp.status_code
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Review not found")
        
    return None
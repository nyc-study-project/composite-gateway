import os
import sys

# Ensure composite-gateway project root is on sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import respx
import httpx
from fastapi.testclient import TestClient

from main import app, SPOT_SERVICE_URL, REVIEWS_SERVICE_URL, USER_SERVICE_URL

client = TestClient(app)


def test_fanout_ok():
    spot_id = "abc"

    with respx.mock:
        # **FIX:** Changed /spots/{spot_id} to /studyspots/{spot_id}
        respx.get(f"{SPOT_SERVICE_URL}/studyspots/{spot_id}").mock(
            return_value=httpx.Response(200, json={"id": spot_id, "name": "Cafe"})
        )
        respx.get(f"{REVIEWS_SERVICE_URL}/reviews?spot_id={spot_id}").mock(
            return_value=httpx.Response(
                200, json=[{"id": "r1", "rating": 5, "spot_id": spot_id}]
            )
        )
        respx.get(f"{USER_SERVICE_URL}/users").mock(
            return_value=httpx.Response(200, json=[{"id": "u1"}])
        )

        r = client.get(f"/composite/spots/{spot_id}/full")

    assert r.status_code == 200
    data = r.json()
    assert data["spot"]["id"] == spot_id
    assert data["reviews"][0]["id"] == "r1"
    assert data["users"][0]["id"] == "u1"


def test_fk_check_review_user_missing():
    with respx.mock:
        # **FIX:** Changed /spots/s1 to /studyspots/s1
        respx.get(f"{SPOT_SERVICE_URL}/studyspots/s1").mock(
            return_value=httpx.Response(200, json={"id": "s1"})
        )
        respx.get(f"{USER_SERVICE_URL}/users/u-missing").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )

        r = client.post(
            "/api/reviews",
            json={"spot_id": "s1", "user_id": "u-missing", "rating": 4},
        )

    assert r.status_code == 404
    assert "User not found" in r.text

#  Composite Gateway Service

The **Composite Gateway** is the central orchestrator for the NYC Study Spots system.  
It aggregates data from the **User-Management**, **Spot-Management**, and **Reviews-Ratings** microservices into unified API responses.  
This allows clients (like the React web app) to fetch complete study spot information with a single request.

---

##  Architecture Overview

**Purpose:**  
- Combine and normalize responses from the individual microservices.  
- Act as a single entry point for frontend requests.  
- Handle service discovery, response caching (future), and authentication forwarding.

**Upstream Services**
| Service | Description | Example URL |
|----------|--------------|-------------|
| User-Management | Auth & user profiles | `http://localhost:8001` |
| Spot-Management | Study spot data | `http://localhost:8002` |
| Reviews-Ratings | Reviews & ratings | `http://localhost:8003` |

---

##  Tech Stack

- **Language:** Python 3.11  
- **Framework:** [FastAPI](https://fastapi.tiangolo.com/)  
- **HTTP Client:** [httpx](https://www.python-httpx.org/)  
- **Testing:** pytest + FastAPI TestClient  
- **CI/CD:** GitHub Actions (Python 3.11, auto-tests on every push/PR)  

---

##  Running Locally

```bash
# 1. Clone the repo
git clone https://github.com/nyc-study-project/composite-gateway.git
cd composite-gateway

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run locally
uvicorn main:app --host 0.0.0.0 --port 8004 --reload

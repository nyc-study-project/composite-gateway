
# **Composite Gateway Microservice**

**NYC Study Spots – Sprint 2**
**Authors:** Pranav / Avi
**Service:** `composite-gateway`
**Platform:** Google Cloud Run

---

## **1. Overview**

The Composite Gateway is the **front-facing API aggregator** for the NYC Study Spots microservices.
It provides a **single unified endpoint** for clients while internally delegating work to:

* **Spot Management Service** (Cloud Run)
* **Reviews Service** (Cloud Compute VM)
* **User Management Service** (Cloud SQL + Compute)

This service performs:

* Parallel fan-out calls
* Foreign-key validation
* Proxy routing
* Asynchronous 202 Accepted workflows
* Task polling
* Unified OpenAPI documentation

This is the required “Composite Microservice” for Sprint 2.

---

## **2. Architecture**

### **Internal Structure**

The gateway exposes **four classes of endpoints**:

1. **Health / Internal**

   * Used to check service uptime and environment.

2. **Proxy Endpoints**

   * Forward requests directly to atomic services (Spot, Reviews, Users).
   * Preserve method, body, and headers when possible.

3. **Composite Endpoints**

   * Combine data from multiple microservices using parallel fan-out via `asyncio.gather()`.

4. **Async Workflow Endpoints**

   * Start a long-running task in Spot service (geocoding).
   * Immediately return `202 Accepted` + `job_id`.
   * Expose a polling endpoint to check task status.

---

## **3. Environment Variables**

These **must be set in Cloud Run** under *Variables & Secrets → Environment Variables*:

| Variable              | Description                      | Example                                                 |
| --------------------- | -------------------------------- | ------------------------------------------------------- |
| `USER_SERVICE_URL`    | Base URL of Users microservice   | `https://users-…run.app`                                |
| `SPOT_SERVICE_URL`    | Base URL of Spots microservice   | `https://spot-management-642518168067.us-east1.run.app` |
| `REVIEWS_SERVICE_URL` | Base URL of Reviews microservice | `http://10.128.0.12:8000` (internal VM IP)              |
| `FASTAPIPORT`         | Port for local dev               | `8000`                                                  |

For safety, **these values are already deployed in Cloud Run** for you.

---

## **4. Deployment Instructions**

### **Build and deploy from VM / local**

```bash
gcloud builds submit --tag gcr.io/study-spot-nyc/composite-gateway
gcloud run deploy composite-gateway \
  --image gcr.io/study-spot-nyc/composite-gateway \
  --region us-east1 \
  --allow-unauthenticated \
  --set-env-vars USER_SERVICE_URL=... \
  --set-env-vars SPOT_SERVICE_URL=... \
  --set-env-vars REVIEWS_SERVICE_URL=...
```

Service URL (LIVE):
**[https://composite-gateway-642518168067.us-east1.run.app](https://composite-gateway-642518168067.us-east1.run.app)**

---

## **5. Endpoints**

### --------------------------------------------------------

#  **1. Internal Endpoints**

### **GET /health**

Returns basic service status.

**Example Output**

```json
{
  "status": "ok",
  "service": "Composite Gateway",
  "timestamp": "2025-11-15T21:17:21.870612+00:00"
}
```

---

### --------------------------------------------------------

#  **2. Proxy Endpoints**

These forward directly to atomic services.

### **GET /api/spots/{spot_id}**

Proxies request to:

```
GET {SPOT_SERVICE_URL}/studyspots/{spot_id}
```

---

### **GET /api/users/{user_id}**

Proxies request to:

```
GET {USER_SERVICE_URL}/users/{user_id}
```

---

### **GET /api/reviews?spot_id=123**

Proxies request to:

```
GET {REVIEWS_SERVICE_URL}/reviews?spot_id=123
```

---

### --------------------------------------------------------

#  **3. Composite Endpoints**

### **GET /composite/spots/{spot_id}/full**

This fans out **in parallel** to:

* Spot service
* Reviews service
* Users service

Using:

```python
asyncio.gather(spot_task, reviews_task, users_task)
```

**Returns unified JSON with:**

```json
{
  "spot": {},
  "reviews": [],
  "users": []
}
```

---

### --------------------------------------------------------

#  **4. Async Operations**

### **POST /api/spots/{spot_id}/geocode**

Starts the long-running geocode job inside Spot microservice.

Composite Gateway transforms:

* Original location header
* Response formatting

### **Returns (202 Accepted):**

```json
{
  "message": "Geocoding started.",
  "job_id": "8986a8da-874a-41ad-99e1-f8ba22011eeb"
}
```

---

### **GET /api/tasks/{task_id}**

Polls job status by forwarding to:

```
GET {SPOT_SERVICE_URL}/tasks/{task_id}
```

**Example response**

```json
{
  "job_id": "...",
  "status": "completed",
  "updated_coordinates": { ... }
}
```

---

## **6. Foreign Key Validation (Reviews)**

### **POST /api/reviews**

Before creating a review, Composite Gateway:

1. Checks that the referenced `spot_id` **exists**
2. Checks that the referenced `user_id` **exists**
3. Only then forwards the POST to Reviews service

If invalid:

```json
{
  "detail": "Spot not found"
}
```

This enforces referential integrity at the API layer.

---

## **7. Running Locally**

### Install deps:

```bash
pip install -r requirements.txt
```

### Run service:

```bash
uvicorn main:app --reload --port 8000
```

---

## **8. Testing Endpoints**

### Get Spot via Composite:

```bash
curl "https://composite-gateway-642518168067.us-east1.run.app/api/spots/<spot_id>"
```

### Start geocode:

```bash
curl -i -X POST \
  "https://composite-gateway-642518168067.us-east1.run.app/api/spots/<spot_id>/geocode"
```

### Poll geocode:

```bash
curl "https://composite-gateway-642518168067.us-east1.run.app/api/tasks/<job_id>"
```

---

## **9. Status of Sprint 2 Requirements**

| Requirement         | Status |
| ------------------- | ------ |
| Repo Setup          | ✔ Done |
| Parallel Fan-out    | ✔ Done |
| FK Validation       | ✔ Done |
| Async geocode (202) | ✔ Done |
| Polling Endpoint    | ✔ Done |
| Docs                | ✔ Done |

Everything for Composite Gateway is now complete.

✅ A separate CONTRIBUTING.md
Just say the word.

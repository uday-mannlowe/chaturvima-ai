"""
main.py — ChaturVima Report Generator API (entry point)

All logic lives in sub-packages:
  core/       — config, rate_limiter, worker_pool
  models/     — schemas (JobStatus, ReportJob, ReportQueue)
  services/   — frappe_client, report_renderer, report_storage
  api/        — html_report_routes, employee_routes, json_report_routes
"""
from typing import Any, Dict

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from core.config import Config
from core.rate_limiter import RateLimiter
from core.worker_pool import WorkerPool
from models.schemas import ReportQueue
from services.report_storage import (
    cleanup_old_per_type_files,
    initialize_report_storage,
    storage_backend_name,
)

from api.html_report_routes import router as html_router
from api.json_report_routes import router as json_router
from api.employee_routes import setup_routes as make_employee_router

# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="ChaturVima Report Generator API",
    description="Production-ready async API for generating behavioral diagnostic reports",
    version="2.0.0",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────

class DynamicCORSMiddleware(BaseHTTPMiddleware):
    """
    Reflects the incoming Origin back as the allowed origin.
    Avoids the allow_origins=['*'] + credentials conflict while still being
    open to all origins (dev/staging/prod without extra config).
    """
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "")
        if request.method == "OPTIONS" and origin:
            response = Response(status_code=200)
            response.headers["Access-Control-Allow-Origin"]   = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Methods"]  = "GET, POST, PUT, DELETE, OPTIONS, PATCH"
            response.headers["Access-Control-Allow-Headers"]  = "Authorization, Content-Type, Accept, X-Requested-With"
            response.headers["Access-Control-Max-Age"]        = "600"
            return response
        response = await call_next(request)
        if origin:
            response.headers["Access-Control-Allow-Origin"]      = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
        return response

app.add_middleware(DynamicCORSMiddleware)

# ─── Static files ─────────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=Config.TEMPLATE_DIR), name="static")
# Vercel-style proxy setups often expose backend under /api.
# Mount static there as well so absolute logo URLs resolve consistently.
app.mount("/api/static", StaticFiles(directory=Config.TEMPLATE_DIR), name="api-static")

# ─── Services ─────────────────────────────────────────────────────────────────

report_queue = ReportQueue(max_size=Config.MAX_QUEUE_SIZE)
rate_limiter = RateLimiter(rate_per_minute=Config.GROQ_RATE_LIMIT_PER_MINUTE)
worker_pool  = WorkerPool(queue=report_queue, rate_limiter=rate_limiter, num_workers=Config.MAX_CONCURRENT_GENERATIONS)

# ─── Routers ──────────────────────────────────────────────────────────────────

app.include_router(html_router)
app.include_router(json_router)
app.include_router(make_employee_router(report_queue))

# ─── Lifecycle ────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    print("\n" + "="*60)
    print("🚀 CHATURVIMA API STARTING")
    print("="*60)
    print(f"Workers:     {Config.MAX_CONCURRENT_GENERATIONS}")
    print(f"Queue Size:  {Config.MAX_QUEUE_SIZE}")
    print(f"Rate Limit:  {Config.GROQ_RATE_LIMIT_PER_MINUTE}/min")
    print(f"Storage:     {storage_backend_name()}")
    print("Frappe auth: dynamic-only mode (runtime token/key-secret required per request)")
    print("="*60 + "\n")
    initialize_report_storage()
    cleanup_old_per_type_files()
    await worker_pool.start()


@app.on_event("shutdown")
async def shutdown_event():
    print("\n" + "="*60)
    print("🛑 CHATURVIMA API SHUTTING DOWN")
    print("="*60 + "\n")
    await worker_pool.stop()


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    return {
        "status": "healthy",
        "workers": Config.MAX_CONCURRENT_GENERATIONS,
        "queue": report_queue.get_queue_stats(),
    }


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=False, workers=1)


"""
Improved ChaturVima Report Generator API with Production-Ready Concurrency
"""
import asyncio
import os
import html as html_lib
import json
import time
from typing import Any, Dict, List, Union, Optional, Tuple, AsyncGenerator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from fastapi import FastAPI, Body, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape
import uvicorn

from generate_groq import generate_text_report, resolve_input_data

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Centralized configuration with environment variable support"""
    
    # Concurrency settings
    MAX_CONCURRENT_GENERATIONS = int(os.getenv("MAX_CONCURRENT_GENERATIONS", "5"))
    MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "100"))
    
    # Groq API settings
    GROQ_RATE_LIMIT_PER_MINUTE = int(os.getenv("GROQ_RATE_LIMIT_PER_MINUTE", "30"))
    GROQ_TIMEOUT_SECONDS = int(os.getenv("GROQ_TIMEOUT_SECONDS", "120"))
    
    # Template settings
    TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "html")
    DEFAULT_TEMPLATE_NAME = os.getenv("REPORT_TEMPLATE", "report_wrapper.html")
    
    # Performance settings
    REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "300"))
    ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"


# ============================================================================
# TASK QUEUE & JOB MANAGEMENT
# ============================================================================

class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ReportJob:
    """Represents a single report generation job"""
    job_id: str
    payload: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[str] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error
        }


class ReportQueue:
    """Thread-safe async queue for report generation jobs"""
    
    def __init__(self, max_size: int = 100):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self.jobs: Dict[str, ReportJob] = {}
        self.job_counter = 0
        self._lock = asyncio.Lock()
    
    async def add_job(self, payload: Dict[str, Any]) -> str:
        """Add a new job to the queue"""
        async with self._lock:
            self.job_counter += 1
            job_id = f"job_{int(time.time())}_{self.job_counter}"
            
            job = ReportJob(job_id=job_id, payload=payload)
            self.jobs[job_id] = job
            
            try:
                await self.queue.put(job)
                return job_id
            except asyncio.QueueFull:
                job.status = JobStatus.FAILED
                job.error = "Queue is full. Please try again later."
                raise HTTPException(status_code=503, detail=job.error)
    
    async def get_job(self) -> ReportJob:
        """Get next job from queue"""
        return await self.queue.get()
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job"""
        async with self._lock:
            job = self.jobs.get(job_id)
            return job.to_dict() if job else None
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        statuses = {}
        for job in self.jobs.values():
            statuses[job.status.value] = statuses.get(job.status.value, 0) + 1
        
        return {
            "queue_size": self.queue.qsize(),
            "total_jobs": len(self.jobs),
            "status_breakdown": statuses
        }


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    """Token bucket rate limiter for API calls"""
    
    def __init__(self, rate_per_minute: int):
        self.rate_per_minute = rate_per_minute
        self.tokens = rate_per_minute
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire a token, waiting if necessary"""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            self.tokens = min(
                self.rate_per_minute,
                self.tokens + (elapsed * self.rate_per_minute / 60)
            )
            self.last_update = now
            
            if self.tokens < 1:
                # Calculate wait time
                wait_time = (1 - self.tokens) * 60 / self.rate_per_minute
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


# ============================================================================
# WORKER POOL
# ============================================================================

class WorkerPool:
    """Pool of workers to process report generation jobs"""
    
    def __init__(
        self,
        queue: ReportQueue,
        rate_limiter: RateLimiter,
        num_workers: int = 5
    ):
        self.queue = queue
        self.rate_limiter = rate_limiter
        self.num_workers = num_workers
        self.workers: List[asyncio.Task] = []
        self.running = False
    
    async def _worker(self, worker_id: int):
        """Worker coroutine that processes jobs from the queue"""
        print(f"🔧 Worker {worker_id} started")
        
        while self.running:
            try:
                # Get job from queue with timeout
                job = await asyncio.wait_for(
                    self.queue.get_job(),
                    timeout=1.0
                )
                
                print(f"👷 Worker {worker_id} processing job {job.job_id}")
                
                # Update job status
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now()
                
                try:
                    # Rate limiting
                    await self.rate_limiter.acquire()
                    
                    # Generate report
                    data = resolve_input_data(job.payload)
                    report = await asyncio.wait_for(
                        asyncio.to_thread(generate_text_report, data),
                        timeout=Config.GROQ_TIMEOUT_SECONDS
                    )
                    
                    # Update job with result
                    job.status = JobStatus.COMPLETED
                    job.result = report
                    job.completed_at = datetime.now()
                    
                    duration = (job.completed_at - job.started_at).total_seconds()
                    print(f"✅ Worker {worker_id} completed job {job.job_id} in {duration:.2f}s")
                    
                except asyncio.TimeoutError:
                    job.status = JobStatus.FAILED
                    job.error = "Report generation timed out"
                    job.completed_at = datetime.now()
                    print(f"⏱️ Worker {worker_id} timeout on job {job.job_id}")
                    
                except Exception as exc:
                    job.status = JobStatus.FAILED
                    job.error = str(exc)
                    job.completed_at = datetime.now()
                    print(f"❌ Worker {worker_id} error on job {job.job_id}: {exc}")
                
            except asyncio.TimeoutError:
                # Queue is empty, continue waiting
                continue
            except Exception as exc:
                print(f"⚠️ Worker {worker_id} unexpected error: {exc}")
                await asyncio.sleep(1)
        
        print(f"🛑 Worker {worker_id} stopped")
    
    async def start(self):
        """Start all workers"""
        self.running = True
        self.workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.num_workers)
        ]
        print(f"🚀 Started {self.num_workers} workers")
    
    async def stop(self):
        """Stop all workers"""
        self.running = False
        await asyncio.gather(*self.workers, return_exceptions=True)
        print("🛑 All workers stopped")


# ============================================================================
# METRICS & MONITORING
# ============================================================================

class Metrics:
    """Simple metrics collector"""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_processing_time = 0.0
        self._lock = asyncio.Lock()
    
    async def record_request(self, success: bool, processing_time: float):
        async with self._lock:
            self.total_requests += 1
            if success:
                self.successful_requests += 1
            else:
                self.failed_requests += 1
            self.total_processing_time += processing_time
    
    def get_stats(self) -> Dict[str, Any]:
        avg_time = (
            self.total_processing_time / self.total_requests
            if self.total_requests > 0
            else 0
        )
        success_rate = (
            (self.successful_requests / self.total_requests * 100)
            if self.total_requests > 0
            else 0
        )
        
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "average_processing_time_seconds": round(avg_time, 2),
            "success_rate_percent": round(success_rate, 2)
        }


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="ChaturVima Report Generator API",
    description="Production-ready psychological report generation system",
    version="2.0.0"
)

# Global instances
report_queue = ReportQueue(max_size=Config.MAX_QUEUE_SIZE)
rate_limiter = RateLimiter(Config.GROQ_RATE_LIMIT_PER_MINUTE)
worker_pool = WorkerPool(
    queue=report_queue,
    rate_limiter=rate_limiter,
    num_workers=Config.MAX_CONCURRENT_GENERATIONS
)
metrics = Metrics()

# Template environment
template_env = Environment(
    loader=FileSystemLoader(Config.TEMPLATE_DIR),
    autoescape=select_autoescape(["html"])
)


# ============================================================================
# STARTUP & SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Start worker pool on application startup"""
    await worker_pool.start()
    print("✨ ChaturVima API is ready!")


@app.on_event("shutdown")
async def shutdown_event():
    """Stop worker pool on application shutdown"""
    await worker_pool.stop()
    print("👋 ChaturVima API shutdown complete")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _report_text_to_html(report_text: str) -> str:
    """Convert plain text report to HTML"""
    lines = report_text.splitlines()
    blocks: List[str] = []
    buffer: List[str] = []

    for line in lines:
        if not line.strip():
            if buffer:
                blocks.append("<p>" + "<br>".join(buffer) + "</p>")
                buffer = []
            continue
        buffer.append(html_lib.escape(line))

    if buffer:
        blocks.append("<p>" + "<br>".join(buffer) + "</p>")

    return "\n".join(blocks)


def _build_title(data: Dict[str, Any]) -> str:
    """Build report title from data"""
    dimension = data.get("dimension", "Report")
    employee = data.get("employee_name")
    if employee:
        return f"ChaturVima {dimension} Report - {employee}"
    return f"ChaturVima {dimension} Report"


def _render_report_html(
    report_text: str,
    data: Dict[str, Any],
    template_name: Optional[str] = None
) -> str:
    """Render report as HTML using template"""
    name = template_name or Config.DEFAULT_TEMPLATE_NAME
    report_html = _report_text_to_html(report_text)
    title = _build_title(data)
    
    try:
        template = template_env.get_template(name)
        return template.render(title=title, report_html=report_html)
    except Exception:
        # Fallback to basic HTML
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html_lib.escape(title)}</title>
</head>
<body>
  <main>
    <h1>{html_lib.escape(title)}</h1>
    {report_html}
  </main>
</body>
</html>"""


# ============================================================================
# API ENDPOINTS - ASYNC (Submit and Poll)
# ============================================================================

@app.post("/submit")
async def submit_job(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Submit a report generation job to the queue.
    Returns job_id for status polling.
    
    This is the recommended approach for handling multiple concurrent requests.
    """
    try:
        job_id = await report_queue.add_job(payload)
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": "Job submitted successfully. Use /status/{job_id} to check progress."
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/status/{job_id}")
async def get_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get the status of a submitted job.
    Poll this endpoint to check if the report is ready.
    """
    status = await report_queue.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@app.get("/result/{job_id}")
async def get_job_result(job_id: str) -> Dict[str, Any]:
    """
    Get the result of a completed job.
    Returns the generated report if the job is completed.
    """
    job = report_queue.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status == JobStatus.PENDING:
        raise HTTPException(status_code=202, detail="Job is still pending")
    elif job.status == JobStatus.PROCESSING:
        raise HTTPException(status_code=202, detail="Job is being processed")
    elif job.status == JobStatus.FAILED:
        raise HTTPException(status_code=500, detail=job.error)
    elif job.status == JobStatus.COMPLETED:
        return {
            "job_id": job_id,
            "status": "completed",
            "dimension": resolve_input_data(job.payload).get("dimension"),
            "report": job.result
        }
    else:
        raise HTTPException(status_code=400, detail=f"Unknown status: {job.status}")


@app.get("/result/{job_id}/html", response_class=HTMLResponse)
async def get_job_result_html(job_id: str) -> HTMLResponse:
    """
    Get the result of a completed job as HTML.
    """
    job = report_queue.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=202,
            detail=f"Job is {job.status.value}. Please try again later."
        )
    
    data = resolve_input_data(job.payload)
    template_name = job.payload.get("template")
    html_doc = _render_report_html(job.result, data, template_name=template_name)
    return HTMLResponse(content=html_doc)


# ============================================================================
# API ENDPOINTS - BATCH ASYNC
# ============================================================================

@app.post("/submit/batch")
async def submit_batch_jobs(
    payload: Union[List[Dict[str, Any]], Dict[str, Any]] = Body(...)
) -> Dict[str, Any]:
    """
    Submit multiple report generation jobs at once.
    Returns list of job_ids for polling.
    
    Accepts either:
    - List of job payloads: [{"dimension": "1D", ...}, ...]
    - Object with items: {"items": [{"dimension": "1D", ...}, ...]}
    """
    # Extract items
    if isinstance(payload, dict) and "items" in payload:
        items = payload["items"]
    elif isinstance(payload, list):
        items = payload
    else:
        raise HTTPException(
            status_code=400,
            detail="Payload must be a list or {'items': [...]}"
        )
    
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="'items' must be a list")
    
    # Submit all jobs
    job_ids = []
    for item in items:
        try:
            job_id = await report_queue.add_job(item)
            job_ids.append({"job_id": job_id, "status": "submitted"})
        except HTTPException as exc:
            job_ids.append({"error": exc.detail, "status": "failed"})
    
    return {
        "total_submitted": len(job_ids),
        "jobs": job_ids
    }


# ============================================================================
# API ENDPOINTS - SYNCHRONOUS (For backward compatibility)
# ============================================================================

@app.post("/generate")
async def generate_one(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Synchronously generate a single report (waits for completion).
    
    ⚠️ Not recommended for high-concurrency scenarios.
    Use /submit + /status instead.
    """
    start_time = time.time()
    
    try:
        # Submit job
        job_id = await report_queue.add_job(payload)
        
        # Poll until complete
        while True:
            job = report_queue.jobs.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            
            if job.status == JobStatus.COMPLETED:
                await metrics.record_request(True, time.time() - start_time)
                return {
                    "dimension": resolve_input_data(job.payload).get("dimension"),
                    "report": job.result
                }
            elif job.status == JobStatus.FAILED:
                await metrics.record_request(False, time.time() - start_time)
                raise HTTPException(status_code=500, detail=job.error)
            
            # Wait before polling again
            await asyncio.sleep(0.5)
            
    except HTTPException:
        raise
    except Exception as exc:
        await metrics.record_request(False, time.time() - start_time)
        raise HTTPException(status_code=500, detail=str(exc))


# ============================================================================
# MONITORING ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    return {
        "status": "healthy",
        "workers": Config.MAX_CONCURRENT_GENERATIONS,
        "queue": report_queue.get_queue_stats()
    }


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    """Get system metrics"""
    if not Config.ENABLE_METRICS:
        raise HTTPException(status_code=404, detail="Metrics disabled")
    
    return {
        "queue": report_queue.get_queue_stats(),
        "performance": metrics.get_stats(),
        "config": {
            "max_workers": Config.MAX_CONCURRENT_GENERATIONS,
            "max_queue_size": Config.MAX_QUEUE_SIZE,
            "rate_limit_per_minute": Config.GROQ_RATE_LIMIT_PER_MINUTE
        }
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "api_server_improved:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1  # Important: Use 1 worker to share queue state
    )

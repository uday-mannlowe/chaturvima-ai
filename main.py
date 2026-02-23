"""
Improved ChaturVima Report Generator API with Multi-Report Support
"""
import asyncio
import os
import html as html_lib
import re
import json
import time
from typing import Any, Dict, List, Union, Optional, Tuple, AsyncGenerator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from fastapi import FastAPI, Body, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, Response
from jinja2 import Environment, FileSystemLoader, select_autoescape
import uvicorn

try:
    from weasyprint import HTML
except Exception:
    HTML = None

from generate_groq import (
    generate_text_report, 
    generate_multi_reports,
    generate_structured_report,
    generate_multi_reports_structured,
    generate_structured_report_by_dimension,
    resolve_input_data,
    normalize_dimension,
    DEFAULT_REPORT_TYPE_BY_DIMENSION,
    REPORT_TYPE_MAP,
    REPORT_TITLE_MAP
)

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
    """Represents a single report generation job (can contain multiple reports)"""
    job_id: str
    payload: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Union[str, Dict[str, Any]]] = None  # ✅ Can be single report or dict of reports
    error: Optional[str] = None
    multi_report: bool = False  # ✅ NEW: Track if this is a multi-report job
    structured: bool = False  # ✅ NEW: Track if this job returns structured output
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "multi_report": self.multi_report,
            "structured": self.structured
        }


class ReportQueue:
    """Thread-safe async queue for report generation jobs"""
    
    def __init__(self, max_size: int = 100):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self.jobs: Dict[str, ReportJob] = {}
        self.job_counter = 0
        self._lock = asyncio.Lock()
    
    async def add_job(self, payload: Dict[str, Any], multi_report: bool = False, structured: bool = False) -> str:
        """Add a new job to the queue"""
        async with self._lock:
            self.job_counter += 1
            job_id = f"job_{int(time.time())}_{self.job_counter}"
            
            job = ReportJob(job_id=job_id, payload=payload, multi_report=multi_report, structured=structured)
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
                
                print(f"👷 Worker {worker_id} processing job {job.job_id} (multi_report={job.multi_report})")
                
                # Update job status
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now()
                
                try:
                    # Rate limiting
                    await self.rate_limiter.acquire()
                    
                    # Resolve data
                    data = resolve_input_data(job.payload)
                    
                    # ✅ NEW: Check if multi-report generation
                    if job.multi_report:
                        # Generate multiple reports
                        print(f"📊 Generating multiple reports for dimension {data['dimension']} (structured={job.structured})")
                        if job.structured:
                            reports = await asyncio.wait_for(
                                asyncio.to_thread(generate_multi_reports_structured, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 5  # More time for section-wise generation
                            )
                        else:
                            reports = await asyncio.wait_for(
                                asyncio.to_thread(generate_multi_reports, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3
                            )
                        job.result = reports
                    else:
                        # Generate single report (legacy or structured)
                        print(f"📄 Generating single report for dimension {data['dimension']} (structured={job.structured})")
                        if job.structured:
                            report = await asyncio.wait_for(
                                asyncio.to_thread(generate_structured_report_by_dimension, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3
                            )
                        else:
                            report = await asyncio.wait_for(
                                asyncio.to_thread(generate_text_report, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS
                            )
                        job.result = report
                    
                    # Update job with result
                    job.status = JobStatus.COMPLETED
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
                # Queue get timeout - continue waiting
                continue
            except Exception as exc:
                print(f"⚠️ Worker {worker_id} unexpected error: {exc}")
    
    async def start(self):
        """Start all worker tasks"""
        if self.running:
            print("⚠️ Worker pool already running")
            return
        
        self.running = True
        self.workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.num_workers)
        ]
        print(f"✅ Started {self.num_workers} workers")
    
    async def stop(self):
        """Stop all worker tasks gracefully"""
        if not self.running:
            return
        
        print("🛑 Stopping worker pool...")
        self.running = False
        
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        print("✅ Worker pool stopped")


# ============================================================================
# METRICS
# ============================================================================

class Metrics:
    """Simple metrics collector"""
    
    def __init__(self):
        self.requests_total = 0
        self.requests_success = 0
        self.requests_failed = 0
        self.total_duration = 0.0
        self._lock = asyncio.Lock()
    
    async def record_request(self, success: bool, duration: float):
        async with self._lock:
            self.requests_total += 1
            if success:
                self.requests_success += 1
            else:
                self.requests_failed += 1
            self.total_duration += duration
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            "requests_total": self.requests_total,
            "requests_success": self.requests_success,
            "requests_failed": self.requests_failed,
            "success_rate": (
                self.requests_success / self.requests_total
                if self.requests_total > 0
                else 0
            ),
            "avg_duration_seconds": (
                self.total_duration / self.requests_total
                if self.requests_total > 0
                else 0
            )
        }


# ============================================================================
# INITIALIZE APP & SERVICES
# ============================================================================

app = FastAPI(
    title="ChaturVima Report Generator API",
    description="Production-ready async API for generating behavioral diagnostic reports with multi-report support",
    version="2.0.0"
)

# Global instances
report_queue = ReportQueue(max_size=Config.MAX_QUEUE_SIZE)
rate_limiter = RateLimiter(rate_per_minute=Config.GROQ_RATE_LIMIT_PER_MINUTE)
worker_pool = WorkerPool(
    queue=report_queue,
    rate_limiter=rate_limiter,
    num_workers=Config.MAX_CONCURRENT_GENERATIONS
)
metrics = Metrics()


# ============================================================================
# LIFECYCLE EVENTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Start worker pool on application startup"""
    print("\n" + "="*60)
    print("🚀 CHATURVIMA API STARTING")
    print("="*60)
    print(f"Workers: {Config.MAX_CONCURRENT_GENERATIONS}")
    print(f"Queue Size: {Config.MAX_QUEUE_SIZE}")
    print(f"Rate Limit: {Config.GROQ_RATE_LIMIT_PER_MINUTE}/min")
    print("="*60 + "\n")
    
    await worker_pool.start()


@app.on_event("shutdown")
async def shutdown_event():
    """Stop worker pool on application shutdown"""
    print("\n" + "="*60)
    print("🛑 CHATURVIMA API SHUTTING DOWN")
    print("="*60 + "\n")
    
    await worker_pool.stop()


# ============================================================================
# HTML TEMPLATE RENDERING
# ============================================================================

def _text_to_paragraphs(text: str) -> List[str]:
    if not text:
        return []
    parts = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    if len(parts) <= 1:
        parts = [p.strip() for p in text.splitlines() if p.strip()]
    return parts


def _normalize_single_report(report: Any, report_type: str, data: dict) -> Dict[str, Any]:
    if isinstance(report, dict) and "sections" in report:
        sections = []
        for section in report.get("sections", []):
            paragraphs = section.get("paragraphs") or _text_to_paragraphs(section.get("text", ""))
            sections.append({
                "title": section.get("title", "Section"),
                "paragraphs": paragraphs
            })
        return {
            "title": report.get("title") or REPORT_TITLE_MAP.get(report_type, "Report"),
            "report_type": report_type,
            "sections": sections,
        }

    if isinstance(report, str):
        return {
            "title": REPORT_TITLE_MAP.get(report_type, "Report"),
            "report_type": report_type,
            "sections": [{
                "title": "Report",
                "paragraphs": _text_to_paragraphs(report)
            }],
        }

    return {
        "title": REPORT_TITLE_MAP.get(report_type, "Report"),
        "report_type": report_type,
        "sections": [{
            "title": "Report",
            "paragraphs": _text_to_paragraphs(str(report))
        }],
    }


def _normalize_reports(report: Union[str, Dict[str, Any]], data: dict) -> List[Dict[str, Any]]:
    if isinstance(report, dict):
        # Structured single report
        if "sections" in report:
            report_type = report.get("report_type") or DEFAULT_REPORT_TYPE_BY_DIMENSION.get(data.get("dimension"), "employee")
            return [_normalize_single_report(report, report_type, data)]

        # Dict of report_type -> report
        reports = []
        for report_type, report_obj in report.items():
            reports.append(_normalize_single_report(report_obj, report_type, data))
        return reports

    # Plain text single report
    report_type = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(data.get("dimension"), "employee")
    return [_normalize_single_report(report, report_type, data)]

def _render_report_html(
    report: Union[str, Dict[str, str]], 
    data: dict, 
    template_name: Optional[str] = None
) -> str:
    """
    Render report(s) as HTML using Jinja2 template.
    
    Args:
        report: Single report string or dict of {report_type: report_text}
        data: Input data
        template_name: Optional custom template
    """
    try:
        env = Environment(
            loader=FileSystemLoader(Config.TEMPLATE_DIR),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template(template_name or Config.DEFAULT_TEMPLATE_NAME)
        
        report_sections = _normalize_reports(report, data)
        return template.render(
            title="ChaturVima Report",
            report_sections=report_sections,
            multi_report=len(report_sections) > 1,
            data=data,
            dimension=data.get("dimension"),
            generated_at=datetime.now().isoformat()
        )
    except Exception as e:
        print(f"⚠️ Template rendering failed: {e}")
        # Fallback: simple HTML wrapper
        report_sections = _normalize_reports(report, data)
        html_parts = ["<html><body>"]
        for report_obj in report_sections:
            html_parts.append(f"<h1>{html_lib.escape(report_obj.get('title', 'Report'))}</h1>")
            for section in report_obj.get("sections", []):
                html_parts.append(f"<h2>{html_lib.escape(section.get('title', 'Section'))}</h2>")
                for paragraph in section.get("paragraphs", []):
                    html_parts.append(f"<p>{html_lib.escape(paragraph)}</p>")
            html_parts.append("<hr>")
        html_parts.append("</body></html>")
        return "".join(html_parts)


def _render_pdf_from_html(html_doc: str) -> bytes:
    if HTML is None:
        raise RuntimeError("PDF rendering is not available. Install weasyprint to enable HTML to PDF conversion.")
    return HTML(string=html_doc).write_pdf()


# ============================================================================
# API ENDPOINTS - ASYNC JOB QUEUE
# ============================================================================

@app.post("/submit")
async def submit_job(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Submit a report generation job to the queue.
    Returns job_id for status polling.
    
    ✅ NEW: Supports multi_report flag to generate multiple reports based on dimension
    
    Request body:
    {
        "dimension": "2D",
        "multi_report": true,  // Optional: generate multiple reports
        "structured": true,    // Optional: section-wise structured output
        "data": {...}
    }
    """
    try:
        # Import normalize_dimension function
        from generate_groq import normalize_dimension
        
        # Normalize and validate dimension
        try:
            original_dim = payload["dimension"]
            normalized_dim = normalize_dimension(original_dim)
            
            # ✅ Force normalized dimension in payload
            payload["dimension"] = normalized_dim
            
            print(f"📥 Job submission: dimension '{original_dim}' → '{normalized_dim}'")
            
        except ValueError as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid dimension: {str(e)}"
            )
        
        # ✅ NEW: Check if multi-report requested
        multi_report = payload.pop("multi_report", False)
        structured = payload.pop("structured", False)
        
        # Submit job with validated payload
        job_id = await report_queue.add_job(payload, multi_report=multi_report, structured=structured)
        
        return {
            "job_id": job_id,
            "dimension": normalized_dim,
            "multi_report": multi_report,
            "structured": structured,
            "status": "submitted",
            "message": f"Job submitted successfully for {normalized_dim} report{'s' if multi_report else ''}. Use /status/{job_id} to check progress."
        }
        
    except HTTPException:
        raise
    except Exception as exc:
        print(f"❌ Job submission error: {exc}")
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
    Returns the generated report(s) if the job is completed.
    
    ✅ NEW: Returns single report or multiple reports based on job type
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
        data = resolve_input_data(job.payload)
        
        if job.multi_report:
            # Return multiple reports
            return {
                "job_id": job_id,
                "status": "completed",
                "dimension": data.get("dimension"),
                "multi_report": True,
                "structured": job.structured,
                "reports": job.result  # Dict of {report_type: report_text}
            }
        else:
            # Return single report (legacy)
            return {
                "job_id": job_id,
                "status": "completed",
                "dimension": data.get("dimension"),
                "multi_report": False,
                "structured": job.structured,
                "report": job.result
            }
    else:
        raise HTTPException(status_code=400, detail=f"Unknown status: {job.status}")


@app.get("/result/{job_id}/html", response_class=HTMLResponse)
async def get_job_result_html(job_id: str) -> HTMLResponse:
    """
    Get the result of a completed job as HTML.
    
    ✅ NEW: Renders multiple reports if multi_report job
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


@app.get("/result/{job_id}/pdf")
async def get_job_result_pdf(job_id: str) -> Response:
    """
    Get the result of a completed job as a PDF.
    Requires weasyprint to be installed on the server.
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

    try:
        pdf_bytes = _render_pdf_from_html(html_doc)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    return Response(content=pdf_bytes, media_type="application/pdf")


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
    
    ✅ NEW: Each job can have multi_report flag
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
            multi_report = item.pop("multi_report", False)
            structured = item.pop("structured", False)
            job_id = await report_queue.add_job(item, multi_report=multi_report, structured=structured)
            job_ids.append({
                "job_id": job_id, 
                "status": "submitted",
                "multi_report": multi_report,
                "structured": structured
            })
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
    
    ✅ NEW: Supports multi_report flag
    ✅ NEW: Supports structured flag for section-wise output
    """
    start_time = time.time()
    
    try:
        # Check if multi-report requested
        multi_report = payload.pop("multi_report", False)
        structured = payload.pop("structured", False)
        
        # Submit job
        job_id = await report_queue.add_job(payload, multi_report=multi_report, structured=structured)
        
        # Poll until complete
        while True:
            job = report_queue.jobs.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            
            if job.status == JobStatus.COMPLETED:
                await metrics.record_request(True, time.time() - start_time)
                data = resolve_input_data(job.payload)
                
                if multi_report:
                    return {
                        "dimension": data.get("dimension"),
                        "multi_report": True,
                        "structured": structured,
                        "reports": job.result
                    }
                else:
                    return {
                        "dimension": data.get("dimension"),
                        "multi_report": False,
                        "structured": structured,
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
# ✅ NEW: DIMENSION INFO ENDPOINT
# ============================================================================

@app.get("/dimensions")
async def get_dimension_info() -> Dict[str, Any]:
    """
    Get information about supported dimensions and their report types.
    """
    return {
        "supported_dimensions": list(REPORT_TYPE_MAP.keys()),
        "dimension_report_mapping": REPORT_TYPE_MAP,
        "report_titles": REPORT_TITLE_MAP
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5000,
        reload=False,
        workers=1  # Important: Use 1 worker to share queue state
    )

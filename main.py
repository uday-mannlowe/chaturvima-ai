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

import httpx
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
    # ✅ NEW: fast parallel JSON generation (one dedicated model per report type)
    generate_multi_reports_json,
    generate_report_as_json,
    MODEL_BY_REPORT_TYPE_DEDICATED,
    resolve_input_data,
    normalize_dimension,
    map_frappe_to_nd,
    DEFAULT_REPORT_TYPE_BY_DIMENSION,
    REPORT_TYPE_MAP,
    REPORT_TITLE_MAP,
    detect_dimension,
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

    # Frappe employee report settings
    FRAPPE_BASE_URL = os.getenv(
        "FRAPPE_BASE_URL",
        "https://cvdev.m.frappe.cloud/api/method/"
        "chaturvima_api.api.dashboard.get_employee_weighted_assessment_summary"
    )
    HTML_DATA_DIR = os.path.join(os.path.dirname(__file__), "html_data")

    # Frappe auth – Option A: API key + secret (preferred)
    FRAPPE_API_KEY    = os.getenv("FRAPPE_API_KEY", "")
    FRAPPE_API_SECRET = os.getenv("FRAPPE_API_SECRET", "")

    # Frappe auth – Option B: username + password (fallback)
    FRAPPE_USERNAME = os.getenv("FRAPPE_USERNAME", "")
    FRAPPE_PASSWORD = os.getenv("FRAPPE_PASSWORD", "")


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
    result: Optional[Union[str, Dict[str, Any]]] = None  # Can be single report or dict of reports
    error: Optional[str] = None
    multi_report: bool = False       # Generate all reports for the dimension
    structured: bool = False         # Section-wise structured output
    employee_report: bool = False    # Frappe employee report flow
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "multi_report": self.multi_report,
            "structured": self.structured,
            "employee_report": self.employee_report,
        }


class ReportQueue:
    """Thread-safe async queue for report generation jobs"""
    
    def __init__(self, max_size: int = 100):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self.jobs: Dict[str, ReportJob] = {}
        self.job_counter = 0
        self._lock = asyncio.Lock()
    
    async def add_job(
        self,
        payload: Dict[str, Any],
        multi_report: bool = False,
        structured: bool = False,
        employee_report: bool = False,
    ) -> str:
        """Add a new job to the queue"""
        async with self._lock:
            self.job_counter += 1
            job_id = f"job_{int(time.time())}_{self.job_counter}"
            
            job = ReportJob(
                job_id=job_id,
                payload=payload,
                multi_report=multi_report,
                structured=structured,
                employee_report=employee_report,
            )
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

                    # ── BRANCH 1: Frappe employee report flow ──────────────────
                    if job.employee_report:
                        employee_id = job.payload["employee"]
                        print(f"🌐 Worker {worker_id}: fetching Frappe data for {employee_id}")

                        async with httpx.AsyncClient(timeout=30) as client:
                            frappe_url = f"{Config.FRAPPE_BASE_URL}?employee={employee_id}"
                            resp = await client.get(frappe_url, headers=_frappe_headers())
                            resp.raise_for_status()
                            frappe_data = resp.json()

                        if "message" not in frappe_data:
                            raise ValueError(
                                f"Unexpected Frappe response structure: {list(frappe_data.keys())}"
                            )

                        # Map Frappe JSON → ND input (dimension auto-detected)
                        nd_data = map_frappe_to_nd(employee_id, frappe_data)
                        dimension = nd_data["dimension"]
                        print(f"📐 Worker {worker_id}: dimension detected = {dimension}")

                        # ── NEW FAST PATH ────────────────────────────────────────────────
                        # generate_multi_reports_json fires ONE dedicated model
                        # per report type ALL IN PARALLEL:
                        #   1D → MODEL_1D
                        #   2D → MODEL_1D + MODEL_2D  (simultaneously)
                        #   3D → MODEL_1D + MODEL_2D + MODEL_3D  (simultaneously)
                        #   4D → all 4 models at once
                        # Each model produces a complete structured JSON report
                        # in ONE call instead of 10-16 section calls.
                        # Result shape: {"dimension": "2D", "reports": {"employee": {...}, "boss": {...}}}
                        # ─────────────────────────────────────────────────────────────
                        multi_json_result = await asyncio.wait_for(
                            asyncio.to_thread(generate_multi_reports_json, nd_data),
                            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                        )
                        # Unwrap to the same shape the rest of the worker expects
                        reports_payload = multi_json_result.get("reports", {})
                        print(
                            f"📦 Worker {worker_id}: received {len(reports_payload)} JSON report(s) — "
                            + ", ".join(
                                f"{rt} ({r.get('word_count', 0)} words, model={r.get('model_used', '?')})"
                                for rt, r in reports_payload.items()
                            )
                        )

                        # ── Build structured JSON payload ─────────────────────
                        msg = frappe_data.get("message", frappe_data)
                        employee_name = (
                            msg.get("employee_name")
                            or msg.get("employee_full_name")
                            or msg.get("employee")
                            or employee_id
                        )
                        designation = (
                            msg.get("designation")
                            or msg.get("role")
                            or msg.get("employee_role")
                            or "Employee"
                        )
                        questionnaires = msg.get("questionnaires_considered", [])
                        dimension_label = {
                            "1D": "1D - Individual Assessment",
                            "2D": "2D - Employee-Boss Relationship",
                            "3D": "3D - Team Assessment",
                            "4D": "4D - Organisational Assessment",
                        }.get(dimension, dimension)

                        stage_scores = []
                        for st in msg.get("stages", []):
                            try:
                                score = float(st.get("score", 0))
                            except (TypeError, ValueError):
                                score = 0.0
                            try:
                                pct = float(st.get("percentage", 0))
                            except (TypeError, ValueError):
                                pct = 0.0
                            stage_scores.append({
                                "stage": str(st.get("stage", "-")),
                                "score": f"{score:.2f}",
                                "percentage": f"{pct:.1f}",
                            })

                        # Normalise reports payload into a clean list of dicts
                        report_sections_list = []
                        if isinstance(reports_payload, dict):
                            for rtype, robj in reports_payload.items():
                                if isinstance(robj, dict) and "sections" in robj:
                                    clean_sections = []
                                    for sec in robj.get("sections", []):
                                        paras = sec.get("paragraphs") or _text_to_paragraphs(sec.get("text", ""))
                                        clean_sections.append({
                                            "id": sec.get("id", ""),
                                            "title": sec.get("title", ""),
                                            "paragraphs": paras,
                                        })
                                    report_sections_list.append({
                                        "title": robj.get("title") or REPORT_TITLE_MAP.get(rtype, rtype),
                                        "report_type": rtype,
                                        "sections": clean_sections,
                                    })

                        json_payload = {
                            "status": "ok",
                            "header": {
                                "employee_id": employee_id,
                                "employee_name": employee_name,
                                "designation": designation,
                                "report_type": f"{dimension_label} Growth Report",
                                "dimension_label": dimension_label,
                                "dominant_stage": str(msg.get("dominant_stage", "-")),
                                "dominant_sub_stage": str(msg.get("dominant_sub_stage", "-")),
                                "questionnaire_text": ", ".join(str(q) for q in questionnaires) if questionnaires else "-",
                                "generated_date": datetime.now().strftime("%d %B %Y"),
                                "stage_scores": stage_scores,
                            },
                            "reports": report_sections_list,
                        }

                        # ── Save JSON only (no HTML generation) ──────────────
                        # HTML rendering is done by the frontend using the JSON.
                        os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)

                        # Determine which report types were generated
                        if isinstance(reports_payload, dict):
                            generated_types = list(reports_payload.keys())
                        else:
                            generated_types = [DEFAULT_REPORT_TYPE_BY_DIMENSION.get(dimension, "employee")]

                        # ── Save ONE combined JSON file only ──────────────────
                        # Shape: { status, header, reports: [ {report_type, title, sections}, ... ] }
                        # For 1D → reports has 1 item
                        # For 2D → reports has 2 items  (employee + boss)
                        # For 3D → reports has 3 items  (employee + boss + team)
                        # For 4D → reports has 4 items
                        # Frontend fetches this ONE file and renders all tabs from it.
                        combined_path = os.path.join(
                            Config.HTML_DATA_DIR, f"{employee_id}.json"
                        )
                        with open(combined_path, "w", encoding="utf-8") as f:
                            json.dump(json_payload, f, ensure_ascii=False, indent=2)
                        print(f"💾 Worker {worker_id}: saved {combined_path}")
                        print(
                            f"📊 Reports inside: "
                            + ", ".join(
                                f"{r['report_type']} ({len(r.get('sections', []))} sections)"
                                for r in json_payload.get("reports", [])
                            )
                        )

                        job.result = json_payload  # return full JSON payload

                    # ── BRANCH 2: Standard multi-report flow ──────────────────
                    elif job.multi_report:
                        data = resolve_input_data(job.payload)
                        print(f"📊 Worker {worker_id}: multi-report dim={data['dimension']} structured={job.structured}")
                        if job.structured:
                            reports = await asyncio.wait_for(
                                asyncio.to_thread(generate_multi_reports_structured, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 5,
                            )
                        else:
                            reports = await asyncio.wait_for(
                                asyncio.to_thread(generate_multi_reports, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                            )
                        job.result = reports

                    # ── BRANCH 3: Standard single-report flow ─────────────────
                    else:
                        data = resolve_input_data(job.payload)
                        print(f"📄 Worker {worker_id}: single report dim={data['dimension']} structured={job.structured}")
                        if job.structured:
                            report = await asyncio.wait_for(
                                asyncio.to_thread(generate_structured_report_by_dimension, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                            )
                        else:
                            report = await asyncio.wait_for(
                                asyncio.to_thread(generate_text_report, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS,
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

    # Frappe auth check
    if Config.FRAPPE_API_KEY and Config.FRAPPE_API_SECRET:
        print(f"🔑 Frappe auth: API key+secret (✓ configured)")
    elif Config.FRAPPE_USERNAME and Config.FRAPPE_PASSWORD:
        print(f"🔑 Frappe auth: username+password (✓ configured)")
    else:
        print("⚠️  Frappe auth: NO CREDENTIALS SET – employee report endpoints will return 403")
        print("   Set FRAPPE_API_KEY + FRAPPE_API_SECRET in .env to fix this.")

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
        "report_titles": REPORT_TITLE_MAP,
        "model_per_report_type": MODEL_BY_REPORT_TYPE_DEDICATED,
    }


# ============================================================================
# ✅ NEW: FAST JSON ENDPOINT — calls generate_multi_reports_json directly
# Frontend hits this instead of the old submit → poll → result flow.
# ============================================================================

@app.post("/generate/json", tags=["Fast JSON"])
async def generate_json_reports(
    payload: Dict[str, Any] = Body(
        ...,
        examples={
            "2D_frappe": {
                "summary": "2D report from Frappe employee ID",
                "value": {"employee": "HR-EMP-00031"},
            },
            "2D_direct": {
                "summary": "2D report from direct input data",
                "value": {
                    "dimension": "2D",
                    "data": {"behavioral_stage": {"stage": "Honeymoon"}},
                },
            },
        },
    )
) -> Dict[str, Any]:
    """
    **Fastest endpoint — recommended for frontend use.**

    Fires one dedicated LLM per report type, all in parallel, each returning
    complete structured JSON in a single call.

    | Dimension | Models fired simultaneously       | Typical time |
    |-----------|-----------------------------------|--------------|
    | 1D        | MODEL_1D                          | ~10-20s      |
    | 2D        | MODEL_1D + MODEL_2D               | ~15-25s      |
    | 3D        | MODEL_1D + MODEL_2D + MODEL_3D    | ~15-30s      |
    | 4D        | MODEL_1D + MODEL_2D + MODEL_3D + MODEL_4D | ~20-35s |

    ### Response shape
    ```json
    {
      "dimension": "2D",
      "reports": {
        "employee": {
          "title": "Individual Self-Assessment Report",
          "report_type": "employee",
          "model_used": "llama-3.1-8b-instant",
          "sections": [
            { "id": "purpose", "title": "Purpose ...", "paragraphs": ["...", "..."] },
            ...
          ],
          "word_count": 3200
        },
        "boss": { ... }
      }
    }
    ```
    """
    import time as _time
    start = _time.time()

    try:
        # ── Frappe flow (employee ID given) ──────────────────────────────────
        if "employee" in payload and "dimension" not in payload:
            employee_id = str(payload["employee"]).strip()
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{Config.FRAPPE_BASE_URL}?employee={employee_id}",
                    headers=_frappe_headers(),
                )
                resp.raise_for_status()
                frappe_data = resp.json()
            nd_data = map_frappe_to_nd(employee_id, frappe_data)
        else:
            # ── Direct data flow ────────────────────────────────────────────
            nd_data = resolve_input_data(payload)

        # Fire parallel JSON generation
        result = await asyncio.wait_for(
            asyncio.to_thread(generate_multi_reports_json, nd_data),
            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
        )

        elapsed = round(_time.time() - start, 2)
        result["elapsed_seconds"] = elapsed
        result["model_map"] = MODEL_BY_REPORT_TYPE_DEDICATED
        print(f"✅ /generate/json completed in {elapsed}s")
        return result

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Report generation timed out.")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ============================================================================
# EMPLOYEE REPORT – FRAPPE AUTH HELPER
# ============================================================================

def _frappe_headers() -> dict:
    """
    Build HTTP headers for Frappe Cloud API calls.

    Priority:
      1. API key + secret  (FRAPPE_API_KEY + FRAPPE_API_SECRET in .env)
         Header format:  Authorization: token <api_key>:<api_secret>
      2. Username + password basic auth (FRAPPE_USERNAME + FRAPPE_PASSWORD)
         Header format:  Authorization: Basic <base64(user:pass)>
      3. No auth (will 403 on protected endpoints – useful only for local dev)
    """
    import base64
    if Config.FRAPPE_API_KEY and Config.FRAPPE_API_SECRET:
        token = f"{Config.FRAPPE_API_KEY}:{Config.FRAPPE_API_SECRET}"
        return {
            "Authorization": f"token {token}",
            "Content-Type": "application/json",
        }
    if Config.FRAPPE_USERNAME and Config.FRAPPE_PASSWORD:
        creds = base64.b64encode(
            f"{Config.FRAPPE_USERNAME}:{Config.FRAPPE_PASSWORD}".encode()
        ).decode()
        return {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        }
    # No credentials – log a clear warning so it’s obvious in the console
    print(
        "⚠️  FRAPPE_AUTH: no credentials set in .env. "
        "Set FRAPPE_API_KEY + FRAPPE_API_SECRET (or FRAPPE_USERNAME + FRAPPE_PASSWORD). "
        "Requests to protected endpoints will return 403."
    )
    return {"Content-Type": "application/json"}


# ============================================================================
# EMPLOYEE REPORT – HTML RENDERER
# ============================================================================

def _render_employee_report_html(
    employee_id: str,
    dimension: str,
    report_payload: Union[str, Dict[str, Any]],
    nd_data: dict,
    frappe_data: dict,
    report_type: str = "employee",
) -> str:
    """
    Render a single report type using the correct per-type template.
    Templates:
      employee → html/employee_report.html
      boss     → html/boss_report.html
      others   → html/index.html (fallback)
    """

    msg = frappe_data.get("message", frappe_data)
    dominant_stage = str(msg.get("dominant_stage", "-"))
    dominant_sub_stage = str(msg.get("dominant_sub_stage", "-"))
    questionnaires = msg.get("questionnaires_considered", [])
    generated_date = datetime.now().strftime("%d %B %Y")

    employee_name = (
        msg.get("employee_name")
        or msg.get("employee_full_name")
        or msg.get("employee")
        or employee_id
    )
    designation = (
        msg.get("designation")
        or msg.get("role")
        or msg.get("employee_role")
        or "Employee"
    )

    dimension_label = {
        "1D": "1D - Individual Assessment",
        "2D": "2D - Employee-Boss Relationship",
        "3D": "3D - Team Assessment",
        "4D": "4D - Organisational Assessment",
    }.get(dimension, str(dimension))

    report_type = f"{dimension_label} Growth Report"
    report_subtitle = f"ChaturVima {dimension_label} Diagnostic Report"
    questionnaire_text = ", ".join(str(q) for q in questionnaires) if questionnaires else "-"
    # Filter to only this report type's sections (not all reports combined)
    all_reports = _normalize_reports(report_payload, nd_data)
    if isinstance(report_payload, dict) and not isinstance(list(report_payload.values())[0] if report_payload else None, str):
        # Multi-report dict: find matching report type
        matching = [r for r in all_reports if r.get("report_type") == report_type]
        report_sections_for_type = matching if matching else all_reports
    else:
        report_sections_for_type = all_reports

    # Flatten sections from all matching reports into a single list for this template
    report_sections = []
    for rpt in report_sections_for_type:
        for sec in rpt.get("sections", []):
            report_sections.append(sec)

    stage_rows: List[Dict[str, str]] = []
    for st in msg.get("stages", []):
        try:
            score = float(st.get("score", 0))
        except (TypeError, ValueError):
            score = 0.0
        try:
            percentage = float(st.get("percentage", 0))
        except (TypeError, ValueError):
            percentage = 0.0
        stage_rows.append({
            "stage": str(st.get("stage", "-")),
            "score": f"{score:.2f}",
            "percentage": f"{percentage:.1f}",
        })

    # Build per-type subtitle
    type_subtitle_map = {
        "employee": f"ChaturVima {dimension_label} – Individual Assessment Report",
        "boss": f"ChaturVima {dimension_label} – Employee–Manager Relationship Report",
        "team": f"ChaturVima {dimension_label} – Team Assessment Report",
        "organization": f"ChaturVima {dimension_label} – Organisational Assessment Report",
    }
    per_type_subtitle = type_subtitle_map.get(report_type, report_subtitle)

    context = {
        "page_title": f"ChaturVima Growth Report - {employee_name} ({report_type.title()})",
        "report_heading": "ChaturVima Diagnostic Report",
        "report_subtitle": per_type_subtitle,
        "employee_name": employee_name,
        "designation": designation,
        "report_type": f"{dimension_label} Growth Report",
        "employee_id": employee_id,
        "dimension_label": dimension_label,
        "generated_date": generated_date,
        "dominant_stage": dominant_stage,
        "dominant_sub_stage": dominant_sub_stage,
        "questionnaire_text": questionnaire_text,
        "stage_rows": stage_rows,
        "report_sections": report_sections,
    }

    TEMPLATE_MAP = {
        "employee": "employee_report.html",
        "boss": "boss_report.html",
    }
    template_name = TEMPLATE_MAP.get(report_type, "index.html")

    try:
        env = Environment(
            loader=FileSystemLoader(Config.TEMPLATE_DIR),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template(template_name)
        return template.render(**context)
    except Exception as exc:
        print(f"⚠️ Employee template rendering failed ({template_name}): {exc}")
        return _render_report_html(report_payload, nd_data)

# ============================================================================
# EMPLOYEE REPORT - API ENDPOINTS
# ============================================================================

@app.get(
    "/debug-frappe/{employee_id}",
    tags=["Employee Report"],
    summary="🔍 Debug – preview Frappe data & dimension detection (no LLM call)",
)
async def debug_frappe(employee_id: str) -> Dict[str, Any]:
    """
    **Use this first when testing.**

    Hits the Frappe API and shows you:
    - Raw `questionnaires_considered` list
    - Which dimension would be auto-detected from that list
    - Dominant stage and sub-stage

    Does **NOT** call the LLM or generate a report.
    """
    frappe_url = f"{Config.FRAPPE_BASE_URL}?employee={employee_id}"
    auth_headers = _frappe_headers()
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(frappe_url, headers=auth_headers)
            resp.raise_for_status()
            frappe_data = resp.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                502,
                f"Frappe returned {exc.response.status_code}. "
                f"If 403: check FRAPPE_API_KEY + FRAPPE_API_SECRET in .env. "
                f"Response: {exc.response.text[:300]}"
            )
        except Exception as exc:
            raise HTTPException(502, f"Cannot reach Frappe API: {exc}")

    if "message" not in frappe_data:
        raise HTTPException(502, f"Unexpected Frappe structure: {list(frappe_data.keys())}")

    msg = frappe_data["message"]
    questionnaires: List[str] = msg.get("questionnaires_considered", [])
    from generate_groq import detect_dimension
    dimension = detect_dimension(questionnaires)

    return {
        "employee": employee_id,
        "questionnaires_considered": questionnaires,
        "questionnaire_count": len(questionnaires),
        "dimension_detected": dimension,
        "dimension_rule": "1 questionnaire=1D, 2=2D, 3=3D, 4=4D",
        "dominant_stage": msg.get("dominant_stage", "—"),
        "dominant_sub_stage": msg.get("dominant_sub_stage", "—"),
        "frappe_raw": frappe_data,
    }


@app.post(
    "/generate-employee-report",
    tags=["Employee Report"],
    summary="⚡ Submit employee report job (async, uses worker queue)",
)
async def generate_employee_report(
    payload: Dict[str, Any] = Body(
        ...,
        examples={
            "basic": {
                "summary": "Generate report (uses cache if exists)",
                "value": {"employee": "HR-EMP-00031"},
            },
            "force": {
                "summary": "Force regeneration (ignore cache)",
                "value": {"employee": "HR-EMP-00031", "force_regenerate": True},
            },
        },
    )
) -> Dict[str, Any]:
    """
    **Main endpoint – called by the frontend when the user clicks "Generate Report".**

    Internally submits a job to the **existing worker queue** so it respects
    all concurrency limits and rate limiting already in place.

    ### Request body
    ```json
    { "employee": "HR-EMP-00031" }                      // serves cache if present
    { "employee": "HR-EMP-00031", "force_regenerate": true }  // always regenerates
    ```

    ### Response
    Returns a `job_id`. Poll `GET /status/{job_id}` to check progress,
    then call `GET /employee-report/{employee_id}` to get the cached HTML.

    ### Dimension auto-detection (no `dimension` field needed)
    ```
    questionnaires_considered length → dimension
    1 item   → 1D
    2 items  → 2D
    3 items  → 3D
    4 items  → 4D
    ```
    """
    employee_id = str(payload.get("employee", "")).strip()
    if not employee_id:
        raise HTTPException(400, "'employee' field is required.")

    force_regenerate = bool(payload.get("force_regenerate", False))

    # Serve from cache immediately – no LLM call, no queue
    if not force_regenerate:
        os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)
        combined_path = os.path.join(Config.HTML_DATA_DIR, f"{employee_id}.json")
        if os.path.exists(combined_path):
            print(f"📄 Cache hit – returning immediately for {employee_id}")
            return {
                "job_id": None,
                "status": "cached",
                "employee": employee_id,
                "message": "Cached report available.",
                "report_url": f"/report/{employee_id}",
            }

    # Submit to worker queue
    job_id = await report_queue.add_job(
        payload={"employee": employee_id},
        employee_report=True,
    )

    return {
        "job_id": job_id,
        "status": "submitted",
        "employee": employee_id,
        "message": (
            f"Report generation queued. "
            f"Poll GET /status/{job_id} until status=completed, "
            f"then fetch GET /report/{employee_id} to get all reports."
        ),
        "poll_url": f"/status/{job_id}",
        "report_url": f"/report/{employee_id}",
    }


# ============================================================================
# SINGLE REPORT ENDPOINT — returns ALL reports for an employee in one call
# This is the ONLY endpoint the frontend needs.
#
# Flow:
#   1. Frontend calls GET /report/{employee_id}
#   2. If cached → returns JSON immediately
#   3. If not cached → returns 404 with instructions to generate first
#
# JSON shape returned:
# {
#   "status": "ok",
#   "header": { employee_id, employee_name, dimension_label, stage_scores, ... },
#   "reports": [
#     { "report_type": "employee", "title": "...", "sections": [...] },  ← 1D
#     { "report_type": "boss",     "title": "...", "sections": [...] },  ← 2D
#     { "report_type": "team",     "title": "...", "sections": [...] },  ← 3D
#     { "report_type": "organization", ...}                              ← 4D
#   ]
# }
# reports[] will have 1, 2, 3, or 4 items depending on dimension.
# ============================================================================

@app.get(
    "/report/{employee_id}",
    tags=["Employee Report"],
    summary="📦 Get all reports for an employee (single endpoint for frontend)",
)
async def get_employee_all_reports(employee_id: str) -> Dict[str, Any]:
    """
    **The only endpoint the frontend needs.**

    Returns all generated reports for this employee in a single JSON response.
    The `reports` array contains 1–4 items depending on the employee's dimension:
    - 1D → `[employee]`
    - 2D → `[employee, boss]`
    - 3D → `[employee, boss, team]`
    - 4D → `[employee, boss, team, organization]`

    Call `POST /generate-employee-report` first if no report exists yet.
    """
    combined_path = os.path.join(Config.HTML_DATA_DIR, f"{employee_id}.json")
    if not os.path.exists(combined_path):
        raise HTTPException(
            404,
            detail={
                "error": f"No report found for employee '{employee_id}'.",
                "action": "Call POST /generate-employee-report to generate the report first.",
                "generate_url": "/generate-employee-report",
                "body": {"employee": employee_id},
            }
        )
    with open(combined_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================================
# STARTUP CLEANUP — remove old per-type files from previous code versions
# ============================================================================

def _cleanup_old_per_type_files() -> None:
    """Remove {employee_id}_{rtype}.json files where a combined file already exists."""
    import glob
    data_dir = Config.HTML_DATA_DIR
    if not os.path.isdir(data_dir):
        return
    for rtype in ("employee", "boss", "team", "organization"):
        for old_file in glob.glob(os.path.join(data_dir, f"*_{rtype}.json")):
            base   = os.path.basename(old_file)
            emp_id = base.replace(f"_{rtype}.json", "")
            if os.path.exists(os.path.join(data_dir, f"{emp_id}.json")):
                os.remove(old_file)
                print(f"🧹 Cleaned up old file: {old_file}")

_cleanup_old_per_type_files()


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
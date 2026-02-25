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
from fastapi.staticfiles import StaticFiles
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

    MAX_CONCURRENT_GENERATIONS = int(os.getenv("MAX_CONCURRENT_GENERATIONS", "5"))
    MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "100"))

    GROQ_RATE_LIMIT_PER_MINUTE = int(os.getenv("GROQ_RATE_LIMIT_PER_MINUTE", "30"))
    GROQ_TIMEOUT_SECONDS = int(os.getenv("GROQ_TIMEOUT_SECONDS", "120"))

    TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "html")
    DEFAULT_TEMPLATE_NAME = os.getenv("REPORT_TEMPLATE", "report_wrapper.html")

    REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "300"))
    ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"

    FRAPPE_BASE_URL = os.getenv(
        "FRAPPE_BASE_URL",
        "https://cvdev.m.frappe.cloud/api/method/"
        "chaturvima_api.api.dashboard.get_employee_weighted_assessment_summary"
    )
    HTML_DATA_DIR = os.path.join(os.path.dirname(__file__), "html_data")


    FRAPPE_API_KEY    = os.getenv("FRAPPE_API_KEY", "")
    FRAPPE_API_SECRET = os.getenv("FRAPPE_API_SECRET", "")
    FRAPPE_USERNAME   = os.getenv("FRAPPE_USERNAME", "")
    FRAPPE_PASSWORD   = os.getenv("FRAPPE_PASSWORD", "")

    # Single Jinja2 template used for ALL dimensions
    REPORT_TEMPLATE_NAME = "report_template.html"


# ============================================================================
# TASK QUEUE & JOB MANAGEMENT
# ============================================================================

class JobStatus(str, Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"


@dataclass
class ReportJob:
    job_id: str
    payload: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Union[str, Dict[str, Any]]] = None
    error: Optional[str] = None
    multi_report: bool = False
    structured: bool = False
    employee_report: bool = False

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
        return await self.queue.get()

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            job = self.jobs.get(job_id)
            return job.to_dict() if job else None

    def get_queue_stats(self) -> Dict[str, Any]:
        statuses = {}
        for job in self.jobs.values():
            statuses[job.status.value] = statuses.get(job.status.value, 0) + 1
        return {
            "queue_size": self.queue.qsize(),
            "total_jobs": len(self.jobs),
            "status_breakdown": statuses,
        }


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.rate_per_minute = rate_per_minute
        self.tokens = rate_per_minute
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(
                self.rate_per_minute,
                self.tokens + (elapsed * self.rate_per_minute / 60)
            )
            self.last_update = now
            if self.tokens < 1:
                wait_time = (1 - self.tokens) * 60 / self.rate_per_minute
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


# ============================================================================
# WORKER POOL
# ============================================================================

class WorkerPool:
    def __init__(self, queue: ReportQueue, rate_limiter: RateLimiter, num_workers: int = 5):
        self.queue = queue
        self.rate_limiter = rate_limiter
        self.num_workers = num_workers
        self.workers: List[asyncio.Task] = []
        self.running = False

    async def _worker(self, worker_id: int):
        print(f"🔧 Worker {worker_id} started")
        while self.running:
            try:
                job = await asyncio.wait_for(self.queue.get_job(), timeout=1.0)
                print(f"👷 Worker {worker_id} processing job {job.job_id}")
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now()
                try:
                    await self.rate_limiter.acquire()

                    # ── BRANCH 1: Frappe employee report ──────────────────────
                    if job.employee_report:
                        employee_id = job.payload["employee"]
                        cycle_name = str(job.payload.get("cycle_name", "")).strip() or None
                        frappe_params = _frappe_query_params(employee_id, cycle_name)
                        if cycle_name:
                            print(
                                f"🌐 Worker {worker_id}: fetching Frappe data for {employee_id} "
                                f"(cycle_name={cycle_name})"
                            )
                        else:
                            print(f"🌐 Worker {worker_id}: fetching Frappe data for {employee_id}")

                        async with httpx.AsyncClient(timeout=30) as client:
                            try:
                                resp = await client.get(
                                    Config.FRAPPE_BASE_URL,
                                    params=frappe_params,
                                    headers=_frappe_headers(),
                                )
                                resp.raise_for_status()
                                frappe_data = resp.json()
                            except httpx.HTTPStatusError as exc:
                                body_snippet = (exc.response.text or "").strip().replace("\n", " ")[:300]
                                raise RuntimeError(
                                    f"Frappe {exc.response.status_code} for params={frappe_params}. "
                                    f"Response: {body_snippet}"
                                ) from exc

                        if "message" not in frappe_data:
                            raise ValueError(f"Unexpected Frappe response: {list(frappe_data.keys())}")

                        nd_data = map_frappe_to_nd(employee_id, frappe_data)
                        dimension = nd_data["dimension"]
                        print(f"📐 Worker {worker_id}: dimension={dimension}")

                        multi_json_result = await asyncio.wait_for(
                            asyncio.to_thread(generate_multi_reports_json, nd_data),
                            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                        )
                        reports_payload = multi_json_result.get("reports", {})

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
                                "cycle_name": cycle_name or "",
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

                        # Save JSON (no HTML — HTML is rendered on demand per endpoint)
                        os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)
                        combined_path = os.path.join(Config.HTML_DATA_DIR, f"{employee_id}.json")
                        with open(combined_path, "w", encoding="utf-8") as f:
                            json.dump(json_payload, f, ensure_ascii=False, indent=2)
                        print(f"💾 Worker {worker_id}: saved {combined_path}")

                        job.result = json_payload

                    # ── BRANCH 2: Standard multi-report ──────────────────────
                    elif job.multi_report:
                        data = resolve_input_data(job.payload)
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

                    # ── BRANCH 3: Standard single-report ─────────────────────
                    else:
                        data = resolve_input_data(job.payload)
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
                continue
            except Exception as exc:
                print(f"⚠️ Worker {worker_id} unexpected error: {exc}")

    async def start(self):
        if self.running:
            return
        self.running = True
        self.workers = [asyncio.create_task(self._worker(i)) for i in range(self.num_workers)]
        print(f"✅ Started {self.num_workers} workers")

    async def stop(self):
        if not self.running:
            return
        print("🛑 Stopping worker pool...")
        self.running = False
        await asyncio.gather(*self.workers, return_exceptions=True)
        print("✅ Worker pool stopped")


# ============================================================================
# METRICS
# ============================================================================

class Metrics:
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
                self.requests_success / self.requests_total if self.requests_total > 0 else 0
            ),
            "avg_duration_seconds": (
                self.total_duration / self.requests_total if self.requests_total > 0 else 0
            ),
        }


# ============================================================================
# INITIALIZE APP & SERVICES
# ============================================================================

app = FastAPI(
    title="ChaturVima Report Generator API",
    description="Production-ready async API for generating behavioral diagnostic reports",
    version="2.0.0",
)

# Serve the logo image at /static/chatur-logo.png
app.mount(
    "/static",
    StaticFiles(directory=Config.TEMPLATE_DIR),
    name="static",
)

report_queue  = ReportQueue(max_size=Config.MAX_QUEUE_SIZE)
rate_limiter  = RateLimiter(rate_per_minute=Config.GROQ_RATE_LIMIT_PER_MINUTE)
worker_pool   = WorkerPool(queue=report_queue, rate_limiter=rate_limiter, num_workers=Config.MAX_CONCURRENT_GENERATIONS)
metrics       = Metrics()


# ============================================================================
# LIFECYCLE EVENTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    print("\n" + "="*60)
    print("🚀 CHATURVIMA API STARTING")
    print("="*60)
    print(f"Workers: {Config.MAX_CONCURRENT_GENERATIONS}")
    print(f"Queue Size: {Config.MAX_QUEUE_SIZE}")
    print(f"Rate Limit: {Config.GROQ_RATE_LIMIT_PER_MINUTE}/min")
    if Config.FRAPPE_API_KEY and Config.FRAPPE_API_SECRET:
        print("🔑 Frappe auth: API key+secret (✓)")
    elif Config.FRAPPE_USERNAME and Config.FRAPPE_PASSWORD:
        print("🔑 Frappe auth: username+password (✓)")
    else:
        print("⚠️  Frappe auth: NO CREDENTIALS – employee endpoints will 403")
    print("="*60 + "\n")
    await worker_pool.start()


@app.on_event("shutdown")
async def shutdown_event():
    print("\n" + "="*60)
    print("🛑 CHATURVIMA API SHUTTING DOWN")
    print("="*60 + "\n")
    await worker_pool.stop()


# ============================================================================
# HELPERS
# ============================================================================

def _text_to_paragraphs(text: str) -> List[str]:
    if not text:
        return []
    parts = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    if len(parts) <= 1:
        parts = [p.strip() for p in text.splitlines() if p.strip()]
    return parts


def _frappe_headers() -> dict:
    import base64
    if Config.FRAPPE_API_KEY and Config.FRAPPE_API_SECRET:
        return {
            "Authorization": f"token {Config.FRAPPE_API_KEY}:{Config.FRAPPE_API_SECRET}",
            "Content-Type": "application/json",
        }
    if Config.FRAPPE_USERNAME and Config.FRAPPE_PASSWORD:
        creds = base64.b64encode(
            f"{Config.FRAPPE_USERNAME}:{Config.FRAPPE_PASSWORD}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    print("⚠️  FRAPPE_AUTH: no credentials – requests to protected endpoints will 403")
    return {"Content-Type": "application/json"}


def _frappe_query_params(employee_id: str, cycle_name: Optional[str] = None) -> Dict[str, str]:
    params = {"employee": employee_id}
    normalized_cycle = (cycle_name or "").strip()
    if normalized_cycle:
        params["cycle_name"] = normalized_cycle
    return params


def _normalize_single_report(report: Any, report_type: str, data: dict) -> Dict[str, Any]:
    if isinstance(report, dict) and "sections" in report:
        sections = []
        for section in report.get("sections", []):
            paragraphs = section.get("paragraphs") or _text_to_paragraphs(section.get("text", ""))
            sections.append({
                "id": section.get("id", ""),
                "title": section.get("title", "Section"),
                "paragraphs": paragraphs,
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
            "sections": [{"id": "", "title": "Report", "paragraphs": _text_to_paragraphs(report)}],
        }
    return {
        "title": REPORT_TITLE_MAP.get(report_type, "Report"),
        "report_type": report_type,
        "sections": [{"id": "", "title": "Report", "paragraphs": _text_to_paragraphs(str(report))}],
    }


def _normalize_reports(report: Union[str, Dict[str, Any]], data: dict) -> List[Dict[str, Any]]:
    if isinstance(report, dict):
        if "sections" in report:
            report_type = report.get("report_type") or DEFAULT_REPORT_TYPE_BY_DIMENSION.get(data.get("dimension"), "employee")
            return [_normalize_single_report(report, report_type, data)]
        reports = []
        for report_type, report_obj in report.items():
            reports.append(_normalize_single_report(report_obj, report_type, data))
        return reports
    report_type = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(data.get("dimension"), "employee")
    return [_normalize_single_report(report, report_type, data)]


def _render_report_html(report: Union[str, Dict[str, str]], data: dict, template_name: Optional[str] = None) -> str:
    try:
        env = Environment(
            loader=FileSystemLoader(Config.TEMPLATE_DIR),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template(template_name or Config.DEFAULT_TEMPLATE_NAME)
        report_sections = _normalize_reports(report, data)
        return template.render(
            title="ChaturVima Report",
            report_sections=report_sections,
            multi_report=len(report_sections) > 1,
            data=data,
            dimension=data.get("dimension"),
            generated_at=datetime.now().isoformat(),
        )
    except Exception as e:
        print(f"⚠️ Template rendering failed: {e}")
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
        raise RuntimeError("PDF rendering unavailable. Install weasyprint.")
    return HTML(string=html_doc).write_pdf()


# ============================================================================
# HTML REPORT RENDERING — single unified template for all dimensions
# ============================================================================

def _render_html_report(json_payload: Dict[str, Any]) -> str:
    """
    Render the stored JSON payload into HTML using the single unified
    Jinja2 template (report_template.html).

    The template receives:
        header  — dict with employee_name, designation, dimension_label,
                  dominant_stage, dominant_sub_stage, questionnaire_text,
                  generated_date, stage_scores
        reports — list of { report_type, title, sections: [{id, title, paragraphs}] }

    All dimensions use the SAME template. For 2D/3D/4D the reports list
    simply has more items (boss, team, organization), which the template
    renders as additional blocks separated by a visual divider.
    """
    try:
        env = Environment(
            loader=FileSystemLoader(Config.TEMPLATE_DIR),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template(Config.REPORT_TEMPLATE_NAME)
        return template.render(
            header=json_payload.get("header", {}),
            reports=json_payload.get("reports", []),
        )
    except Exception as exc:
        raise RuntimeError(f"Template rendering failed: {exc}") from exc


def _load_employee_json(employee_id: str) -> Dict[str, Any]:
    """Load the stored JSON for an employee, or raise 404."""
    path = os.path.join(Config.HTML_DATA_DIR, f"{employee_id}.json")
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail={
                "error": f"No report found for '{employee_id}'.",
                "action": "Call POST /generate-employee-report first.",
                "body": {"employee": employee_id},
            },
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _filter_reports_for_dimension(json_payload: Dict[str, Any], report_types: List[str]) -> Dict[str, Any]:
    """
    Return a copy of the payload with only the requested report_types included.
    Used so /1d endpoint shows only the 'employee' report even if a 4D JSON exists.
    """
    filtered = {k: v for k, v in json_payload.items() if k != "reports"}
    filtered["reports"] = [
        r for r in json_payload.get("reports", [])
        if r.get("report_type") in report_types
    ]
    return filtered


# ============================================================================
# HTML REPORT ENDPOINTS
# Dimension endpoints read the stored JSON and render HTML on the fly.
# One single template is used for all dimensions.
#
#   GET /html-report/{employee_id}/1d  →  shows employee report only
#   GET /html-report/{employee_id}/2d  →  shows boss report only
#   GET /html-report/{employee_id}/3d  →  shows team report only
#   GET /html-report/{employee_id}/4d  →  shows organization report only
# ============================================================================

_DIMENSION_REPORT_TYPES = {
    "1d": ["employee"],
    "2d": ["boss"],
    "3d": ["team"],
    "4d": ["organization"],
}


@app.get(
    "/html-report/{employee_id}/1d",
    response_class=HTMLResponse,
    tags=["HTML Reports"],
    summary="📄 1D – Individual HTML Report",
)
async def html_report_1d(employee_id: str) -> HTMLResponse:
    """
    Serve the **1D Individual** HTML report for the employee.

    Reads the stored JSON (`html_data/{employee_id}.json`) and renders it
    using the single unified Jinja2 template. Only the `employee` report
    section is included.

    Call `POST /generate-employee-report` first if no JSON exists yet.
    """
    payload = _load_employee_json(employee_id)
    filtered = _filter_reports_for_dimension(payload, _DIMENSION_REPORT_TYPES["1d"])
    return HTMLResponse(content=_render_html_report(filtered))


@app.get(
    "/html-report/{employee_id}/2d",
    response_class=HTMLResponse,
    tags=["HTML Reports"],
    summary="📄 2D – Manager HTML Report",
)
async def html_report_2d(employee_id: str) -> HTMLResponse:
    """
    Serve the **2D Employee–Manager** HTML report.

    Includes only the `boss` report section.
    """
    payload = _load_employee_json(employee_id)
    filtered = _filter_reports_for_dimension(payload, _DIMENSION_REPORT_TYPES["2d"])
    return HTMLResponse(content=_render_html_report(filtered))


@app.get(
    "/html-report/{employee_id}/3d",
    response_class=HTMLResponse,
    tags=["HTML Reports"],
    summary="📄 3D – Team HTML Report",
)
async def html_report_3d(employee_id: str) -> HTMLResponse:
    """
    Serve the **3D Team** HTML report.

    Includes only the `team` report section.
    """
    payload = _load_employee_json(employee_id)
    filtered = _filter_reports_for_dimension(payload, _DIMENSION_REPORT_TYPES["3d"])
    return HTMLResponse(content=_render_html_report(filtered))


@app.get(
    "/html-report/{employee_id}/4d",
    response_class=HTMLResponse,
    tags=["HTML Reports"],
    summary="📄 4D – Full Organisational HTML Report",
)
async def html_report_4d(employee_id: str) -> HTMLResponse:
    """
    Serve the **4D Organisational** HTML report.

    Includes only the `organization` report section.
    """
    payload = _load_employee_json(employee_id)
    filtered = _filter_reports_for_dimension(payload, _DIMENSION_REPORT_TYPES["4d"])
    return HTMLResponse(content=_render_html_report(filtered))


# ============================================================================
# API ENDPOINTS — ASYNC JOB QUEUE
# ============================================================================

@app.post("/submit")
async def submit_job(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Submit a report generation job. Returns job_id for polling."""
    try:
        from generate_groq import normalize_dimension
        try:
            original_dim = payload["dimension"]
            normalized_dim = normalize_dimension(original_dim)
            payload["dimension"] = normalized_dim
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid dimension: {str(e)}")

        multi_report = payload.pop("multi_report", False)
        structured = payload.pop("structured", False)
        job_id = await report_queue.add_job(payload, multi_report=multi_report, structured=structured)
        return {
            "job_id": job_id,
            "dimension": normalized_dim,
            "multi_report": multi_report,
            "structured": structured,
            "status": "submitted",
            "message": f"Use /status/{job_id} to check progress.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/status/{job_id}")
async def get_job_status(job_id: str) -> Dict[str, Any]:
    status = await report_queue.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@app.get("/result/{job_id}")
async def get_job_result(job_id: str) -> Dict[str, Any]:
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
            return {"job_id": job_id, "status": "completed", "dimension": data.get("dimension"), "multi_report": True, "reports": job.result}
        else:
            return {"job_id": job_id, "status": "completed", "dimension": data.get("dimension"), "multi_report": False, "report": job.result}
    raise HTTPException(status_code=400, detail=f"Unknown status: {job.status}")


@app.get("/result/{job_id}/html", response_class=HTMLResponse)
async def get_job_result_html(job_id: str) -> HTMLResponse:
    job = report_queue.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.COMPLETED:
        raise HTTPException(status_code=202, detail=f"Job is {job.status.value}.")
    data = resolve_input_data(job.payload)
    template_name = job.payload.get("template")
    html_doc = _render_report_html(job.result, data, template_name=template_name)
    return HTMLResponse(content=html_doc)


@app.get("/result/{job_id}/pdf")
async def get_job_result_pdf(job_id: str) -> Response:
    job = report_queue.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.COMPLETED:
        raise HTTPException(status_code=202, detail=f"Job is {job.status.value}.")
    data = resolve_input_data(job.payload)
    template_name = job.payload.get("template")
    html_doc = _render_report_html(job.result, data, template_name=template_name)
    try:
        pdf_bytes = _render_pdf_from_html(html_doc)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return Response(content=pdf_bytes, media_type="application/pdf")


# ============================================================================
# API ENDPOINTS — BATCH ASYNC
# ============================================================================

@app.post("/submit/batch")
async def submit_batch_jobs(payload: Union[List[Dict[str, Any]], Dict[str, Any]] = Body(...)) -> Dict[str, Any]:
    if isinstance(payload, dict) and "items" in payload:
        items = payload["items"]
    elif isinstance(payload, list):
        items = payload
    else:
        raise HTTPException(status_code=400, detail="Payload must be a list or {'items': [...]}")
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="'items' must be a list")
    job_ids = []
    for item in items:
        try:
            multi_report = item.pop("multi_report", False)
            structured = item.pop("structured", False)
            job_id = await report_queue.add_job(item, multi_report=multi_report, structured=structured)
            job_ids.append({"job_id": job_id, "status": "submitted", "multi_report": multi_report, "structured": structured})
        except HTTPException as exc:
            job_ids.append({"error": exc.detail, "status": "failed"})
    return {"total_submitted": len(job_ids), "jobs": job_ids}


# ============================================================================
# API ENDPOINTS — SYNCHRONOUS
# ============================================================================

@app.post("/generate")
async def generate_one(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    start_time = time.time()
    try:
        multi_report = payload.pop("multi_report", False)
        structured = payload.pop("structured", False)
        job_id = await report_queue.add_job(payload, multi_report=multi_report, structured=structured)
        while True:
            job = report_queue.jobs.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            if job.status == JobStatus.COMPLETED:
                await metrics.record_request(True, time.time() - start_time)
                data = resolve_input_data(job.payload)
                if multi_report:
                    return {"dimension": data.get("dimension"), "multi_report": True, "structured": structured, "reports": job.result}
                else:
                    return {"dimension": data.get("dimension"), "multi_report": False, "structured": structured, "report": job.result}
            elif job.status == JobStatus.FAILED:
                await metrics.record_request(False, time.time() - start_time)
                raise HTTPException(status_code=500, detail=job.error)
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
    return {"status": "healthy", "workers": Config.MAX_CONCURRENT_GENERATIONS, "queue": report_queue.get_queue_stats()}


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    if not Config.ENABLE_METRICS:
        raise HTTPException(status_code=404, detail="Metrics disabled")
    return {
        "queue": report_queue.get_queue_stats(),
        "performance": metrics.get_stats(),
        "config": {
            "max_workers": Config.MAX_CONCURRENT_GENERATIONS,
            "max_queue_size": Config.MAX_QUEUE_SIZE,
            "rate_limit_per_minute": Config.GROQ_RATE_LIMIT_PER_MINUTE,
        },
    }


@app.get("/dimensions")
async def get_dimension_info() -> Dict[str, Any]:
    return {
        "supported_dimensions": list(REPORT_TYPE_MAP.keys()),
        "dimension_report_mapping": REPORT_TYPE_MAP,
        "report_titles": REPORT_TITLE_MAP,
        "model_per_report_type": MODEL_BY_REPORT_TYPE_DEDICATED,
    }


# ============================================================================
# FAST JSON ENDPOINT
# ============================================================================

@app.post("/generate/json", tags=["Fast JSON"])
async def generate_json_reports(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """Fastest endpoint — fires one dedicated LLM per report type, all in parallel."""
    import time as _time
    start = _time.time()
    try:
        if "employee" in payload and "dimension" not in payload:
            employee_id = str(payload["employee"]).strip()
            cycle_name = str(payload.get("cycle_name", "")).strip() or None
            frappe_params = _frappe_query_params(employee_id, cycle_name)
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.get(
                        Config.FRAPPE_BASE_URL,
                        params=frappe_params,
                        headers=_frappe_headers(),
                    )
                    resp.raise_for_status()
                    frappe_data = resp.json()
                except httpx.HTTPStatusError as exc:
                    detail = (exc.response.text or "").strip().replace("\n", " ")[:300]
                    raise HTTPException(
                        status_code=502,
                        detail=(
                            f"Frappe returned {exc.response.status_code} for params={frappe_params}. "
                            f"Response: {detail}"
                        ),
                    ) from exc
            nd_data = map_frappe_to_nd(employee_id, frappe_data)
        else:
            nd_data = resolve_input_data(payload)

        result = await asyncio.wait_for(
            asyncio.to_thread(generate_multi_reports_json, nd_data),
            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
        )
        elapsed = round(_time.time() - start, 2)
        result["elapsed_seconds"] = elapsed
        result["model_map"] = MODEL_BY_REPORT_TYPE_DEDICATED
        return result
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Report generation timed out.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ============================================================================
# EMPLOYEE REPORT — FRAPPE ENDPOINTS
# ============================================================================

@app.get("/debug-frappe/{employee_id}", tags=["Employee Report"])
async def debug_frappe(employee_id: str, cycle_name: Optional[str] = None) -> Dict[str, Any]:
    """Debug endpoint — preview Frappe data and dimension detection (no LLM)."""
    frappe_params = _frappe_query_params(employee_id, cycle_name)
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                Config.FRAPPE_BASE_URL,
                params=frappe_params,
                headers=_frappe_headers(),
            )
            resp.raise_for_status()
            frappe_data = resp.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                502,
                f"Frappe returned {exc.response.status_code} for params={frappe_params}: {exc.response.text[:300]}",
            )
        except Exception as exc:
            raise HTTPException(502, f"Cannot reach Frappe: {exc}")

    if "message" not in frappe_data:
        raise HTTPException(502, f"Unexpected Frappe structure: {list(frappe_data.keys())}")

    msg = frappe_data["message"]
    questionnaires: List[str] = msg.get("questionnaires_considered", [])
    dimension = detect_dimension(questionnaires)
    return {
        "employee": employee_id,
        "cycle_name": cycle_name,
        "questionnaires_considered": questionnaires,
        "questionnaire_count": len(questionnaires),
        "dimension_detected": dimension,
        "dominant_stage": msg.get("dominant_stage", "—"),
        "dominant_sub_stage": msg.get("dominant_sub_stage", "—"),
        "frappe_raw": frappe_data,
    }


@app.post("/generate-employee-report", tags=["Employee Report"])
async def generate_employee_report(
    payload: Dict[str, Any] = Body(
        ...,
        examples={
            "basic": {"summary": "Generate report", "value": {"employee": "HR-EMP-00031"}},
            "with_cycle": {
                "summary": "Generate with cycle name",
                "value": {"employee": "HR-EMP-00031", "cycle_name": "Assessment Cycle - 0442"},
            },
            "force": {"summary": "Force regenerate", "value": {"employee": "HR-EMP-00031", "force_regenerate": True}},
        },
    )
) -> Dict[str, Any]:
    """
    Submit employee report job. Stores result as JSON in html_data/.
    Poll /status/{job_id}, then fetch HTML via /html-report/{employee_id}/1d (or 2d/3d/4d).
    """
    employee_id = str(payload.get("employee", "")).strip()
    if not employee_id:
        raise HTTPException(400, "'employee' field is required.")
    cycle_name = str(payload.get("cycle_name", "")).strip() or None

    force_regenerate = bool(payload.get("force_regenerate", False))

    if not force_regenerate:
        os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)
        combined_path = os.path.join(Config.HTML_DATA_DIR, f"{employee_id}.json")
        if os.path.exists(combined_path):
            with open(combined_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            cached_cycle_name = str(cached.get("header", {}).get("cycle_name", "")).strip() or None
            if cycle_name and cached_cycle_name != cycle_name:
                cached = None
            if cached is not None:
                # Detect dimension from cached JSON
                dim_label = cached.get("header", {}).get("dimension_label", "1D")
                return {
                    "job_id": None,
                    "status": "cached",
                    "employee": employee_id,
                    "cycle_name": cached_cycle_name,
                    "message": "Cached report available.",
                    "report_urls": {
                        "1d": f"/html-report/{employee_id}/1d",
                        "2d": f"/html-report/{employee_id}/2d",
                        "3d": f"/html-report/{employee_id}/3d",
                        "4d": f"/html-report/{employee_id}/4d",
                    },
                }

    job_payload = {"employee": employee_id}
    if cycle_name:
        job_payload["cycle_name"] = cycle_name
    job_id = await report_queue.add_job(
        payload=job_payload,
        employee_report=True,
    )
    return {
        "job_id": job_id,
        "status": "submitted",
        "employee": employee_id,
        "cycle_name": cycle_name,
        "message": f"Poll GET /status/{job_id} until completed, then fetch HTML.",
        "poll_url": f"/status/{job_id}",
        "report_urls": {
            "1d": f"/html-report/{employee_id}/1d",
            "2d": f"/html-report/{employee_id}/2d",
            "3d": f"/html-report/{employee_id}/3d",
            "4d": f"/html-report/{employee_id}/4d",
        },
    }


@app.get("/report/{employee_id}", tags=["Employee Report"])
async def get_employee_all_reports(employee_id: str) -> Dict[str, Any]:
    """Return the stored JSON for an employee (all reports combined)."""
    return _load_employee_json(employee_id)


# ============================================================================
# STARTUP CLEANUP
# ============================================================================

def _cleanup_old_per_type_files() -> None:
    import glob
    data_dir = Config.HTML_DATA_DIR
    if not os.path.isdir(data_dir):
        return
    for rtype in ("employee", "boss", "team", "organization"):
        for old_file in glob.glob(os.path.join(data_dir, f"*_{rtype}.json")):
            base = os.path.basename(old_file)
            emp_id = base.replace(f"_{rtype}.json", "")
            if os.path.exists(os.path.join(data_dir, f"{emp_id}.json")):
                os.remove(old_file)
                print(f"🧹 Cleaned up old file: {old_file}")

_cleanup_old_per_type_files()


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=False, workers=1)

"""
Improved ChaturVima Report Generator API with Multi-Report Support
"""
import asyncio
import os
import html as html_lib
import re
import json
import time
import hashlib
from typing import Any, Dict, List, Union, Optional, Tuple, AsyncGenerator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from urllib.parse import quote
import httpx
from fastapi import FastAPI, Body, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from dotenv import load_dotenv
import uvicorn


# Load local .env values when present (especially useful in local and Docker runs).
load_dotenv()

try:
    from weasyprint import HTML
    WEASYPRINT_IMPORT_ERROR: Optional[str] = None
except Exception as exc:
    HTML = None
    WEASYPRINT_IMPORT_ERROR = str(exc)

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
    FRAPPE_RESOURCE_BASE_URL = os.getenv(
        "FRAPPE_RESOURCE_BASE_URL",
        "https://cvdev.m.frappe.cloud/api/resource",
    )
    HTML_DATA_DIR = os.path.join(os.path.dirname(__file__), "html_data")


    FRAPPE_API_KEY    = os.getenv("FRAPPE_API_KEY", "")
    FRAPPE_API_SECRET = os.getenv("FRAPPE_API_SECRET", "")
    FRAPPE_USERNAME   = os.getenv("FRAPPE_USERNAME", "")
    FRAPPE_PASSWORD   = os.getenv("FRAPPE_PASSWORD", "")

    CORS_ALLOWED_ORIGINS = [
        origin.strip()
        for origin in os.getenv(
            "CORS_ALLOWED_ORIGINS",
            "http://localhost:5173,http://127.0.0.1:5173",
        ).split(",")
        if origin.strip()
    ]
    CORS_ALLOWED_ORIGIN_REGEX = (
        os.getenv(
            "CORS_ALLOWED_ORIGIN_REGEX",
            r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
        ).strip()
        or None
    )
    CORS_ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", "false").lower() == "true"
    CORS_ALLOW_METHODS = ["*"]
    CORS_ALLOW_HEADERS = ["*"]

    # Browsers reject credentialed CORS responses when allow-origin is wildcard.
    # If credentials are enabled and '*' is configured, switch to regex matching.
    if CORS_ALLOW_CREDENTIALS and "*" in CORS_ALLOWED_ORIGINS:
        CORS_ALLOWED_ORIGINS = [origin for origin in CORS_ALLOWED_ORIGINS if origin != "*"]
        if not CORS_ALLOWED_ORIGIN_REGEX:
            CORS_ALLOWED_ORIGIN_REGEX = r"^https?://.*$"

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
                        requested_cycle_name = _normalize_optional_str(job.payload.get("cycle_name"))
                        requested_submission_id = _normalize_optional_str(job.payload.get("submission_id"))
                        frappe_params = _frappe_query_params(
                            employee_id,
                            cycle_name=requested_cycle_name,
                            submission_id=requested_submission_id,
                        )
                        if requested_submission_id:
                            print(
                                f"🌐 Worker {worker_id}: fetching Frappe data for {employee_id} "
                                f"(submission_id={requested_submission_id})"
                            )
                        elif requested_cycle_name:
                            print(
                                f"🌐 Worker {worker_id}: fetching Frappe data for {employee_id} "
                                f"(cycle_name={requested_cycle_name})"
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

                        msg = frappe_data.get("message", frappe_data)
                        nd_data = map_frappe_to_nd(employee_id, frappe_data)
                        dimension = nd_data["dimension"]
                        print(f"📐 Worker {worker_id}: dimension={dimension}")
                        questionnaires = msg.get("questionnaires_considered", [])
                        single_questionnaire = len(questionnaires) == 1
                        primary_report_type = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(dimension)

                        swot_doc: Optional[Dict[str, Any]] = None
                        dominant_sub_stage = _normalize_optional_str(msg.get("dominant_sub_stage"))
                        if dimension == "1D" and dominant_sub_stage:
                            swot_doc = await _fetch_frappe_swot_doc(dominant_sub_stage)
                            if swot_doc:
                                print(
                                    f"🧩 Worker {worker_id}: SWOT source found "
                                    f"for sub_stage='{dominant_sub_stage}'"
                                )
                            else:
                                print(
                                    f"⚠️ Worker {worker_id}: SWOT not found for "
                                    f"sub_stage='{dominant_sub_stage}', using default stage mapping"
                                )

                        multi_json_result = await asyncio.wait_for(
                            asyncio.to_thread(generate_multi_reports_json, nd_data),
                            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                        )
                        reports_payload = multi_json_result.get("reports", {})
                        if (
                            single_questionnaire
                            and isinstance(reports_payload, dict)
                            and primary_report_type
                            and primary_report_type in reports_payload
                        ):
                            reports_payload = {primary_report_type: reports_payload[primary_report_type]}
                            print(
                                f"🧭 Worker {worker_id}: single questionnaire submission -> "
                                f"keeping only '{primary_report_type}' report"
                            )
                        if dimension == "1D" and swot_doc:
                            replaced = _apply_1d_swot_override_to_reports_payload(reports_payload, swot_doc)
                            if replaced:
                                print(
                                    f"✅ Worker {worker_id}: 1D SWOT section injected directly from Frappe"
                                )

                        submission_id = _extract_submission_id(msg, requested_submission_id)
                        cycle_name = _extract_cycle_name(msg, requested_cycle_name)
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
                                        section_id = sec.get("id", "")
                                        section_title = sec.get("title", "")
                                        clean_section = {
                                            "id": sec.get("id", ""),
                                            "title": sec.get("title", ""),
                                            "paragraphs": paras,
                                        }
                                        existing_swot_lists = sec.get("swot_lists")
                                        if isinstance(existing_swot_lists, dict):
                                            clean_section["swot_lists"] = existing_swot_lists
                                        elif _is_swot_section(section_id, section_title):
                                            clean_section["swot_lists"] = _build_swot_lists_from_section_paragraphs(paras)
                                        clean_sections.append(clean_section)
                                    report_sections_list.append({
                                        "title": robj.get("title") or REPORT_TITLE_MAP.get(rtype, rtype),
                                        "report_type": rtype,
                                        "sections": clean_sections,
                                    })

                        json_payload = {
                            "status": "ok",
                            "header": {
                                "employee_id": employee_id,
                                "submission_id": submission_id or "",
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
                        combined_path = _build_employee_report_path(
                            employee_id,
                            submission_id=submission_id,
                            cycle_name=cycle_name,
                        )
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

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=Config.CORS_ALLOWED_ORIGINS,
#     allow_origin_regex=Config.CORS_ALLOWED_ORIGIN_REGEX,
#     allow_credentials=Config.CORS_ALLOW_CREDENTIALS,
#     allow_methods=Config.CORS_ALLOW_METHODS,
#     allow_headers=Config.CORS_ALLOW_HEADERS,
# )

app.add_middleware(     
    CORSMiddleware,     
    allow_origins=["*"],     
    allow_credentials=True,     
    allow_methods=["*"],     
    allow_headers=["*"]
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
    print(f"CORS Origins: {', '.join(Config.CORS_ALLOWED_ORIGINS)}")
    print(f"CORS Origin Regex: {Config.CORS_ALLOWED_ORIGIN_REGEX or '-'}")
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


def _split_numbered_items(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []

    compact = re.sub(r"\s+", " ", raw).strip()
    matches = list(re.finditer(r"(^|\s)(\d{1,2}[.)])\s+", compact))
    if matches:
        items: List[str] = []
        for idx, match in enumerate(matches):
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(compact)
            chunk = compact[start:end].strip(" ;,-")
            if chunk:
                items.append(chunk)
        if items:
            return items

    line_items = [ln.strip(" -•\t") for ln in re.split(r"(?:\r?\n)+|•", raw) if ln.strip(" -•\t")]
    if len(line_items) > 1:
        return line_items

    return [compact]


def _is_swot_section(section_id: Any, section_title: Any) -> bool:
    sec_id = str(section_id or "").strip().lower()
    sec_title = str(section_title or "").strip().lower()
    return sec_id == "swot" or "swot" in sec_title


def _build_swot_lists_from_section_paragraphs(paragraphs: List[str]) -> Dict[str, List[str]]:
    def para_at(index: int) -> str:
        if 0 <= index < len(paragraphs):
            return str(paragraphs[index] or "")
        return ""

    strengths_para = para_at(0)
    weaknesses_para = para_at(1)
    opportunities_para = para_at(2)
    threats_para = para_at(3)

    return {
        "strengths": _split_numbered_items(strengths_para) if strengths_para.strip() else [],
        "weaknesses": _split_numbered_items(weaknesses_para) if weaknesses_para.strip() else [],
        "opportunities": _split_numbered_items(opportunities_para) if opportunities_para.strip() else [],
        "threats": _split_numbered_items(threats_para) if threats_para.strip() else [],
    }


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


def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _optional_payload_str(payload: Dict[str, Any], key: str) -> Optional[str]:
    """
    Return an optional string field from JSON payload.
    If provided, it must be a string.
    """
    if key not in payload or payload.get(key) is None:
        return None
    value = payload.get(key)
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail=f"'{key}' must be a string.")
    text = value.strip()
    return text or None


def _required_query_submission_id(value: str) -> str:
    normalized = _normalize_optional_str(value)
    if not normalized:
        raise HTTPException(
            status_code=400,
            detail="'submission_id' query parameter is required and cannot be empty.",
        )
    return normalized


def _questionnaire_text_to_list(value: Any) -> List[str]:
    text = _normalize_optional_str(value)
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _extract_dimension_code(value: Any) -> Optional[str]:
    text = _normalize_optional_str(value)
    if not text:
        return None
    m = re.search(r"\b([1-4]D)\b", text.upper())
    return m.group(1).upper() if m else None


def _extract_submission_id(message: Dict[str, Any], fallback: Optional[str] = None) -> Optional[str]:
    for key in (
        "submission_id",
        "assessment_submission_id",
        "employee_submission_id",
        "submission",
        "assessment_id",
    ):
        candidate = _normalize_optional_str(message.get(key))
        if candidate:
            return candidate
    return _normalize_optional_str(fallback)


def _extract_cycle_name(message: Dict[str, Any], fallback: Optional[str] = None) -> Optional[str]:
    for key in ("cycle_name", "assessment_cycle", "assessment_cycle_name"):
        candidate = _normalize_optional_str(message.get(key))
        if candidate:
            return candidate
    return _normalize_optional_str(fallback)


def _frappe_query_params(
    employee_id: str,
    cycle_name: Optional[str] = None,
    submission_id: Optional[str] = None,
) -> Dict[str, str]:
    params = {"employee": employee_id}
    normalized_cycle = _normalize_optional_str(cycle_name)
    normalized_submission = _normalize_optional_str(submission_id)
    if normalized_cycle:
        params["cycle_name"] = normalized_cycle
    if normalized_submission:
        params["submission_id"] = normalized_submission
    return params


def _frappe_swot_doc_url(docname: str) -> str:
    encoded = quote(docname, safe="")
    return f"{Config.FRAPPE_RESOURCE_BASE_URL}/SWOT%20Analysis/{encoded}"


def _collect_child_row_texts(rows: Any) -> List[str]:
    if not isinstance(rows, list):
        return []
    values: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            text = _normalize_optional_str(
                row.get("description")
                or row.get("desription")  # Frappe child-table typo in Threat rows
                or row.get("recommendations_description")
                or row.get("value")
                or row.get("title")
            )
        else:
            text = _normalize_optional_str(row)
        if text:
            values.append(text)
    return values


def _extract_swot_lists(swot_doc: Dict[str, Any]) -> Dict[str, List[str]]:
    return {
        "strengths": _collect_child_row_texts(swot_doc.get("strength") or swot_doc.get("strengths")),
        "weaknesses": _collect_child_row_texts(swot_doc.get("weakness") or swot_doc.get("weaknesses")),
        "opportunities": _collect_child_row_texts(swot_doc.get("opportunity") or swot_doc.get("opportunities")),
        "threats": _collect_child_row_texts(swot_doc.get("threat") or swot_doc.get("threats")),
    }


def _build_swot_items(texts: List[str], category_label: str) -> List[Dict[str, str]]:
    return [
        {
            "area": f"{category_label} {idx}",
            "description": text,
            "context": "Fetched from SWOT Analysis doctype.",
            "impact": "",
        }
        for idx, text in enumerate(texts, start=1)
    ]


def _map_frappe_swot_doc(swot_doc: Dict[str, Any]) -> Dict[str, Any]:
    swot_lists = _extract_swot_lists(swot_doc)
    strengths = swot_lists["strengths"]
    weaknesses = swot_lists["weaknesses"]
    opportunities = swot_lists["opportunities"]
    threats = swot_lists["threats"]

    rec_rows = swot_doc.get("reccomendation") or swot_doc.get("recommendation") or swot_doc.get("recommendations")
    principles: List[str] = []
    recommended_actions: List[Dict[str, str]] = []

    strategic_intro = _normalize_optional_str(swot_doc.get("strategic_recommendations"))
    if strategic_intro:
        principles.append(strategic_intro)

    if isinstance(rec_rows, list):
        for idx, row in enumerate(rec_rows, start=1):
            if not isinstance(row, dict):
                continue
            title = _normalize_optional_str(row.get("recommendations_title") or row.get("title"))
            description = _normalize_optional_str(
                row.get("recommendations_description")
                or row.get("description")
                or row.get("desription")
            )
            if title:
                principles.append(title)
            if title or description:
                recommended_actions.append(
                    {
                        "focus_area": title or f"Action {idx}",
                        "recommendation": description or title or f"Action {idx}",
                        "priority": "Medium",
                        "time_horizon": "Short Term",
                    }
                )

    framework_name = _normalize_optional_str(swot_doc.get("sub_stage")) or "SWOT Strategic Recommendations"
    recommendation_framework = {
        "framework_name": framework_name,
        "principles": principles,
        "recommended_actions": recommended_actions,
    }

    return {
        "individual_swot": {
            "strengths": _build_swot_items(strengths, "Strength"),
            "weaknesses": _build_swot_items(weaknesses, "Weakness"),
            "opportunities": _build_swot_items(opportunities, "Opportunity"),
            "threats": _build_swot_items(threats, "Threat"),
        },
        "recommendation_framework": recommendation_framework,
    }


def _format_swot_paragraph(texts: List[str], fallback: str) -> str:
    if not texts:
        return fallback
    return " ".join(f"{idx}. {txt}" for idx, txt in enumerate(texts, start=1))


def _build_swot_paragraphs_from_doc(swot_doc: Dict[str, Any]) -> List[str]:
    swot_lists = _extract_swot_lists(swot_doc)
    strengths = swot_lists["strengths"]
    weaknesses = swot_lists["weaknesses"]
    opportunities = swot_lists["opportunities"]
    threats = swot_lists["threats"]
    return [
        _format_swot_paragraph(strengths, "Strengths not available."),
        _format_swot_paragraph(weaknesses, "Weaknesses not available."),
        _format_swot_paragraph(opportunities, "Opportunities not available."),
        _format_swot_paragraph(threats, "Threats not available."),
    ]


def _apply_1d_swot_override_to_reports_payload(
    reports_payload: Any,
    swot_doc: Optional[Dict[str, Any]],
) -> bool:
    if not isinstance(reports_payload, dict) or not isinstance(swot_doc, dict):
        return False

    employee_report = reports_payload.get("employee")
    if not isinstance(employee_report, dict):
        return False

    sections = employee_report.get("sections")
    if not isinstance(sections, list):
        return False

    swot_lists = _extract_swot_lists(swot_doc)
    paragraphs = _build_swot_paragraphs_from_doc(swot_doc)
    swot_section = None
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        sec_id = str(sec.get("id", "")).strip().lower()
        sec_title = str(sec.get("title", "")).strip().lower()
        if sec_id == "swot" or "swot" in sec_title:
            swot_section = sec
            break

    if swot_section is None:
        swot_section = {
            "id": "swot",
            "title": "Individual SWOT Analysis",
            "paragraphs": paragraphs,
        }
        sections.append(swot_section)
    else:
        swot_section["id"] = "swot"
        swot_section["title"] = swot_section.get("title") or "Individual SWOT Analysis"
        swot_section["paragraphs"] = paragraphs

    swot_section["swot_lists"] = swot_lists
    swot_section["source"] = "frappe_swot"
    swot_section["sub_stage"] = _normalize_optional_str(swot_doc.get("sub_stage") or swot_doc.get("name")) or ""
    return True


async def _fetch_frappe_swot_doc(sub_stage: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized_sub_stage = _normalize_optional_str(sub_stage)
    if not normalized_sub_stage:
        return None

    headers = _frappe_headers()
    async with httpx.AsyncClient(timeout=30) as client:
        # Fast path: docname matches dominant_sub_stage exactly.
        direct_url = _frappe_swot_doc_url(normalized_sub_stage)
        try:
            direct_resp = await client.get(direct_url, headers=headers)
            if direct_resp.status_code == 200:
                direct_payload = direct_resp.json()
                direct_data = direct_payload.get("data", direct_payload)
                if isinstance(direct_data, dict):
                    return direct_data
            elif direct_resp.status_code not in (404,):
                direct_resp.raise_for_status()
        except Exception as exc:
            print(f"⚠️ SWOT direct lookup failed for '{normalized_sub_stage}': {exc}")

        # Fallback path: resolve docname by filter, then fetch full doc.
        list_url = f"{Config.FRAPPE_RESOURCE_BASE_URL}/SWOT%20Analysis"
        filter_candidates = (
            [["sub_stage", "=", normalized_sub_stage]],
            [["name", "=", normalized_sub_stage]],
        )
        for filters in filter_candidates:
            try:
                list_resp = await client.get(
                    list_url,
                    headers=headers,
                    params={
                        "filters": json.dumps(filters),
                        "fields": json.dumps(["name"]),
                        "limit_page_length": "1",
                    },
                )
                list_resp.raise_for_status()
                rows = list_resp.json().get("data", [])
                if not isinstance(rows, list) or not rows:
                    continue
                row0 = rows[0] if isinstance(rows[0], dict) else {}
                doc_name = _normalize_optional_str(row0.get("name"))
                if not doc_name:
                    continue
                doc_resp = await client.get(_frappe_swot_doc_url(doc_name), headers=headers)
                doc_resp.raise_for_status()
                payload = doc_resp.json()
                doc = payload.get("data", payload)
                if isinstance(doc, dict):
                    return doc
            except Exception as exc:
                print(f"⚠️ SWOT filter lookup failed for '{normalized_sub_stage}' with {filters}: {exc}")
                continue

    return None


def _sanitize_filename_token(value: str, fallback: str) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip())
    token = token.strip("._-")
    return token or fallback


def _build_employee_report_path(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    employee_token = _sanitize_filename_token(employee_id, "employee")
    normalized_submission = _normalize_optional_str(submission_id)
    normalized_cycle = _normalize_optional_str(cycle_name)
    if normalized_submission:
        key_token = _sanitize_filename_token(normalized_submission, "submission")
        key_hash = hashlib.sha1(normalized_submission.encode("utf-8")).hexdigest()[:10]
        filename = f"{employee_token}__submission_{key_token}_{key_hash}.json"
    elif normalized_cycle:
        key_token = _sanitize_filename_token(normalized_cycle, "cycle")
        key_hash = hashlib.sha1(normalized_cycle.encode("utf-8")).hexdigest()[:10]
        filename = f"{employee_token}__cycle_{key_token}_{key_hash}.json"
    else:
        filename = f"{employee_token}.json"
    return os.path.join(Config.HTML_DATA_DIR, filename)


def _append_identity_query(
    url: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    normalized_submission = _normalize_optional_str(submission_id)
    normalized_cycle = _normalize_optional_str(cycle_name)
    query_parts = []
    if normalized_submission:
        query_parts.append(f"submission_id={quote(normalized_submission, safe='')}")
    if normalized_cycle:
        query_parts.append(f"cycle_name={quote(normalized_cycle, safe='')}")
    if not query_parts:
        return url
    return f"{url}?{'&'.join(query_parts)}"


def _build_report_urls(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, str]:
    report_url = _append_identity_query(
        f"/html-report/{employee_id}",
        submission_id=submission_id,
        cycle_name=cycle_name,
    )
    return {
        "report": report_url
    }


def _build_auto_download_pdf_url(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> str:
    base_url = f"/html-report/{employee_id}/pdf"
    return _append_identity_query(base_url, submission_id=submission_id, cycle_name=cycle_name)


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
        detail = "PDF rendering unavailable. Install weasyprint."
        if WEASYPRINT_IMPORT_ERROR:
            detail = f"{detail} Import error: {WEASYPRINT_IMPORT_ERROR}"
        raise RuntimeError(detail)
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


def _load_employee_json(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Load the stored JSON for an employee (and optional submission/cycle), or raise 404."""
    requested_submission = _normalize_optional_str(submission_id)
    requested_cycle = _normalize_optional_str(cycle_name)
    direct_path = _build_employee_report_path(
        employee_id,
        submission_id=requested_submission,
        cycle_name=requested_cycle,
    )
    latest_alias_path = _build_employee_report_path(employee_id)

    candidate_paths: List[str] = []
    if requested_submission or requested_cycle:
        candidate_paths.append(direct_path)
    candidate_paths.append(latest_alias_path)

    seen = set()
    for path in candidate_paths:
        if path in seen:
            continue
        seen.add(path)
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        header = payload.get("header", {})
        payload_submission = _normalize_optional_str(header.get("submission_id"))
        payload_cycle = _normalize_optional_str(header.get("cycle_name"))
        if requested_submission and payload_submission != requested_submission:
            continue
        if requested_cycle and payload_cycle != requested_cycle:
            continue
        return payload

    # Fallback search for matching cycle/submission in all stored files for this employee.
    import glob
    employee_token = _sanitize_filename_token(employee_id, "employee")
    pattern = os.path.join(Config.HTML_DATA_DIR, f"{employee_token}__*.json")
    matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]
    if requested_submission or requested_cycle:
        for path in sorted(matches, key=os.path.getmtime, reverse=True):
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            header = payload.get("header", {})
            payload_submission = _normalize_optional_str(header.get("submission_id"))
            payload_cycle = _normalize_optional_str(header.get("cycle_name"))
            if requested_submission and payload_submission != requested_submission:
                continue
            if requested_cycle and payload_cycle != requested_cycle:
                continue
            return payload
    elif matches:
        latest_path = max(matches, key=os.path.getmtime)
        with open(latest_path, "r", encoding="utf-8") as f:
            return json.load(f)

    detail = {
        "error": f"No report found for '{employee_id}'.",
        "action": "Call POST /generate-employee-report first.",
        "body": {"employee": employee_id},
    }
    if requested_submission:
        detail["error"] = (
            f"No report found for '{employee_id}' with submission_id '{requested_submission}'."
        )
        detail["body"]["submission_id"] = requested_submission
    if requested_cycle:
        detail["error"] = (
            f"No report found for '{employee_id}' with cycle_name '{requested_cycle}'."
            if not requested_submission
            else detail["error"]
        )
        detail["body"]["cycle_name"] = requested_cycle
    raise HTTPException(status_code=404, detail=detail)


def _filter_reports_for_dimension(json_payload: Dict[str, Any], dimension_key: str) -> Dict[str, Any]:
    """
    Return a copy of the payload with only the requested report_type included,
    and endpoint-specific header metadata for display.
    """
    view = _DIMENSION_VIEW_CONFIG.get(dimension_key.lower())
    if not view:
        return json_payload

    report_types = view["report_types"]
    filtered = {k: v for k, v in json_payload.items() if k != "reports"}

    header = dict(json_payload.get("header", {}))
    header["report_heading"] = view["heading"]
    header["dimension_label"] = view["dimension_label"]
    header["report_type"] = f"{view['dimension_label']} Growth Report"
    filtered["header"] = header

    filtered["reports"] = [
        r for r in json_payload.get("reports", [])
        if r.get("report_type") in report_types
    ]
    return filtered


def _ensure_dimension_report_available(
    employee_id: str,
    dimension_key: str,
    payload: Dict[str, Any],
    filtered_payload: Dict[str, Any],
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> None:
    if filtered_payload.get("reports"):
        return

    header = payload.get("header", {})
    detected_dim = _extract_dimension_code(
        header.get("dimension_label") or header.get("report_type")
    )
    available_dims = []
    for rep in payload.get("reports", []):
        rep_type = str(rep.get("report_type", "")).strip().lower()
        if rep_type == "employee":
            available_dims.append("1d")
        elif rep_type == "boss":
            available_dims.append("2d")
        elif rep_type == "team":
            available_dims.append("3d")
        elif rep_type == "organization":
            available_dims.append("4d")
    available_dims = sorted(set(available_dims))

    detail: Dict[str, Any] = {
        "error": f"No {dimension_key.upper()} report available for this selection.",
        "requested_dimension": dimension_key.upper(),
        "detected_dimension": f"{detected_dim}" if detected_dim else None,
        "questionnaire_text": header.get("questionnaire_text"),
        "available_dimensions": available_dims,
        "suggested_auto_url": _append_identity_query(
            f"/html-report/{employee_id}",
            submission_id=submission_id,
            cycle_name=cycle_name,
        ),
    }

    raise HTTPException(status_code=404, detail=detail)


def _with_download_link(
    json_payload: Dict[str, Any],
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    augmented = dict(json_payload)
    header = dict(json_payload.get("header", {}))
    normalized_submission = _normalize_optional_str(submission_id)
    normalized_cycle = _normalize_optional_str(cycle_name)
    if normalized_submission and not _normalize_optional_str(header.get("submission_id")):
        header["submission_id"] = normalized_submission
    if normalized_cycle and not _normalize_optional_str(header.get("cycle_name")):
        header["cycle_name"] = normalized_cycle
    header["download_pdf_url"] = _build_auto_download_pdf_url(
        employee_id,
        submission_id=normalized_submission or _normalize_optional_str(header.get("submission_id")),
        cycle_name=normalized_cycle or _normalize_optional_str(header.get("cycle_name")),
    )
    augmented["header"] = header
    return augmented


# ============================================================================
# HTML REPORT ENDPOINTS
# Dimension endpoints read the stored JSON and render HTML on the fly.
# One single template is used for all dimensions.
#
#   GET /html-report/{employee_id}      → auto-detect and show matching report
# ============================================================================

_DIMENSION_VIEW_CONFIG = {
    "1d": {
        "report_types": ["employee"],
        "heading": "Employee Personal Insights",
        "dimension_label": "1D - Individual Assessment",
    },
    "2d": {
        "report_types": ["boss"],
        "heading": "Employee-Boss Relationship Insights",
        "dimension_label": "2D - Employee-Boss Relationship",
    },
    "3d": {
        "report_types": ["team"],
        "heading": "Employee-Boss-Department Context Insights",
        "dimension_label": "3D - Employee-Boss-Department Context",
    },
    "4d": {
        "report_types": ["organization"],
        "heading": "Organisational Insights",
        "dimension_label": "4D - Organisational Assessment",
    },
}


_REPORT_TYPE_TO_DIMENSION = {
    "employee": "1d",
    "boss": "2d",
    "team": "3d",
    "organization": "4d",
}

_DIMENSION_PRIORITY = {"1d": 1, "2d": 2, "3d": 3, "4d": 4}


def _infer_dimension_key_from_payload(json_payload: Dict[str, Any]) -> str:
    header = json_payload.get("header", {})
    header_dim = _extract_dimension_code(
        header.get("dimension_label") or header.get("report_type")
    )
    if header_dim:
        dim_key = header_dim.lower()
        if dim_key in _DIMENSION_VIEW_CONFIG:
            required_types = set(_DIMENSION_VIEW_CONFIG[dim_key]["report_types"])
            if any(
                str(rep.get("report_type", "")).strip().lower() in required_types
                for rep in json_payload.get("reports", [])
                if isinstance(rep, dict)
            ):
                return dim_key

    available_dims: List[str] = []
    for rep in json_payload.get("reports", []):
        if not isinstance(rep, dict):
            continue
        rep_type = str(rep.get("report_type", "")).strip().lower()
        mapped = _REPORT_TYPE_TO_DIMENSION.get(rep_type)
        if mapped:
            available_dims.append(mapped)

    if available_dims:
        return max(available_dims, key=lambda d: _DIMENSION_PRIORITY.get(d, 0))

    return "1d"


@app.get(
    "/html-report/{employee_id}",
    response_class=HTMLResponse,
    tags=["HTML Reports"],
    summary="📄 Auto Dimension HTML Report",
)
async def html_report_auto(
    employee_id: str,
    submission_id: str = Query(..., description="Submission identifier"),
    cycle_name: Optional[str] = None,
) -> HTMLResponse:
    """
    Serve a single HTML report endpoint.
    The backend auto-detects dimension from stored payload and renders the
    matching report (1D/2D/3D/4D).
    """
    normalized_submission = _required_query_submission_id(submission_id)
    payload = _load_employee_json(
        employee_id,
        submission_id=normalized_submission,
        cycle_name=cycle_name,
    )
    dim_key = _infer_dimension_key_from_payload(payload)
    payload = _with_download_link(payload, employee_id, normalized_submission, cycle_name)
    filtered = _filter_reports_for_dimension(payload, dim_key)
    _ensure_dimension_report_available(
        employee_id,
        dim_key,
        payload,
        filtered,
        submission_id=normalized_submission,
        cycle_name=cycle_name,
    )
    header = dict(filtered.get("header", {}))
    header["download_pdf_url"] = _build_auto_download_pdf_url(
        employee_id,
        submission_id=normalized_submission,
        cycle_name=_normalize_optional_str(cycle_name) or _normalize_optional_str(header.get("cycle_name")),
    )
    filtered["header"] = header
    return HTMLResponse(content=_render_html_report(filtered))


@app.get(
    "/html-report/{employee_id}/pdf",
    tags=["HTML Reports"],
    summary="📥 Auto Dimension PDF Download",
)
async def html_report_auto_pdf(
    employee_id: str,
    submission_id: str = Query(..., description="Submission identifier"),
    cycle_name: Optional[str] = None,
) -> Response:
    normalized_submission = _required_query_submission_id(submission_id)
    payload = _load_employee_json(
        employee_id,
        submission_id=normalized_submission,
        cycle_name=cycle_name,
    )
    dim_key = _infer_dimension_key_from_payload(payload)
    payload = _with_download_link(payload, employee_id, normalized_submission, cycle_name)
    filtered = _filter_reports_for_dimension(payload, dim_key)
    _ensure_dimension_report_available(
        employee_id,
        dim_key,
        payload,
        filtered,
        submission_id=normalized_submission,
        cycle_name=cycle_name,
    )
    html_doc = _render_html_report(filtered)
    try:
        pdf_bytes = _render_pdf_from_html(html_doc)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    header = filtered.get("header", {})
    identity_key = (
        normalized_submission
        or _normalize_optional_str(header.get("submission_id"))
        or _normalize_optional_str(cycle_name)
        or _normalize_optional_str(header.get("cycle_name"))
        or "latest"
    )
    safe_employee = _sanitize_filename_token(employee_id, "employee")
    safe_identity = _sanitize_filename_token(identity_key, "latest")
    filename = f"{safe_employee}_{dim_key}_{safe_identity}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
            cycle_name = _normalize_optional_str(payload.get("cycle_name"))
            submission_id = _optional_payload_str(payload, "submission_id")
            frappe_params = _frappe_query_params(
                employee_id,
                cycle_name=cycle_name,
                submission_id=submission_id,
            )
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
            swot_doc: Optional[Dict[str, Any]] = None
            msg = frappe_data.get("message", frappe_data)
            questionnaires = msg.get("questionnaires_considered", [])
            single_questionnaire = len(questionnaires) == 1
            if nd_data.get("dimension") == "1D":
                dominant_sub_stage = _normalize_optional_str(msg.get("dominant_sub_stage"))
                if dominant_sub_stage:
                    swot_doc = await _fetch_frappe_swot_doc(dominant_sub_stage)
        else:
            nd_data = resolve_input_data(payload)
            swot_doc = None
            single_questionnaire = False

        result = await asyncio.wait_for(
            asyncio.to_thread(generate_multi_reports_json, nd_data),
            timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
        )
        if single_questionnaire:
            primary_report_type = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(nd_data.get("dimension"))
            reports_map = result.get("reports", {})
            if (
                primary_report_type
                and isinstance(reports_map, dict)
                and primary_report_type in reports_map
            ):
                result["reports"] = {primary_report_type: reports_map[primary_report_type]}
        if nd_data.get("dimension") == "1D" and swot_doc:
            _apply_1d_swot_override_to_reports_payload(result.get("reports", {}), swot_doc)
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
async def debug_frappe(
    employee_id: str,
    cycle_name: Optional[str] = None,
    submission_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Debug endpoint — preview Frappe data and dimension detection (no LLM)."""
    normalized_cycle = _normalize_optional_str(cycle_name)
    normalized_submission = _normalize_optional_str(submission_id)
    frappe_params = _frappe_query_params(
        employee_id,
        cycle_name=normalized_cycle,
        submission_id=normalized_submission,
    )
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
    detected_submission = _extract_submission_id(msg, normalized_submission)
    detected_cycle = _extract_cycle_name(msg, normalized_cycle)
    return {
        "employee": employee_id,
        "submission_id": detected_submission,
        "cycle_name": detected_cycle,
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
            "with_submission": {
                "summary": "Generate with submission id",
                "value": {"employee": "HR-EMP-00031", "submission_id": "SUB-000442"},
            },
            "force": {"summary": "Force regenerate", "value": {"employee": "HR-EMP-00031", "force_regenerate": True}},
        },
    )
) -> Dict[str, Any]:
    """
    Submit employee report job. Stores result as JSON in html_data/.
    Poll /status/{job_id}, then fetch HTML via /html-report/{employee_id}.
    """
    employee_id = str(payload.get("employee", "")).strip()
    if not employee_id:
        raise HTTPException(400, "'employee' field is required.")
    submission_id = _optional_payload_str(payload, "submission_id")
    cycle_name = _normalize_optional_str(payload.get("cycle_name"))

    force_regenerate = bool(payload.get("force_regenerate", False))

    if not force_regenerate:
        os.makedirs(Config.HTML_DATA_DIR, exist_ok=True)
        try:
            cached = _load_employee_json(
                employee_id,
                submission_id=submission_id,
                cycle_name=cycle_name,
            )
            cached_header = cached.get("header", {})
            cached_submission_id = _normalize_optional_str(cached_header.get("submission_id"))
            cached_cycle_name = _normalize_optional_str(cached_header.get("cycle_name"))

            # Guard against legacy cache built with old count-only dimension detection.
            cached_questionnaires = _questionnaire_text_to_list(cached_header.get("questionnaire_text"))
            expected_dim = detect_dimension(cached_questionnaires) if cached_questionnaires else None
            cached_dim = (
                _extract_dimension_code(cached_header.get("dimension_label"))
                or _extract_dimension_code(cached_header.get("report_type"))
            )
            dimension_mismatch = bool(expected_dim and cached_dim and expected_dim != cached_dim)
            single_questionnaire = len(cached_questionnaires) == 1
            expected_primary_report = (
                DEFAULT_REPORT_TYPE_BY_DIMENSION.get(expected_dim)
                if expected_dim
                else None
            )
            cached_report_types = [
                str(r.get("report_type", "")).strip().lower()
                for r in cached.get("reports", [])
                if isinstance(r, dict)
            ]
            report_scope_mismatch = bool(
                submission_id
                and single_questionnaire
                and expected_primary_report
                and (
                    len(cached_report_types) != 1
                    or cached_report_types[0] != expected_primary_report
                )
            )

            if dimension_mismatch and submission_id:
                print(
                    f"⚠️ Cached report dimension mismatch for submission_id={submission_id}: "
                    f"cached={cached_dim}, expected={expected_dim}. Regenerating."
                )
            elif report_scope_mismatch:
                print(
                    f"⚠️ Cached report scope mismatch for submission_id={submission_id}: "
                    f"reports={cached_report_types}, expected=['{expected_primary_report}']. Regenerating."
                )
            else:
                cached_urls = _build_report_urls(
                    employee_id,
                    submission_id=cached_submission_id or submission_id,
                    cycle_name=cached_cycle_name or cycle_name,
                )
                return {
                    "job_id": None,
                    "status": "cached",
                    "employee": employee_id,
                    "submission_id": cached_submission_id,
                    "cycle_name": cached_cycle_name,
                    "message": "Cached report available.",
                    "report_url": cached_urls.get("report"),
                    "report_urls": cached_urls,
                }
        except HTTPException as exc:
            if exc.status_code != 404:
                raise

    job_payload = {"employee": employee_id}
    if submission_id:
        job_payload["submission_id"] = submission_id
    if cycle_name:
        job_payload["cycle_name"] = cycle_name
    job_id = await report_queue.add_job(
        payload=job_payload,
        employee_report=True,
    )
    report_urls = _build_report_urls(
        employee_id,
        submission_id=submission_id,
        cycle_name=cycle_name,
    )
    return {
        "job_id": job_id,
        "status": "submitted",
        "employee": employee_id,
        "submission_id": submission_id,
        "cycle_name": cycle_name,
        "message": f"Poll GET /status/{job_id} until completed, then fetch HTML.",
        "poll_url": f"/status/{job_id}",
        "report_url": report_urls.get("report"),
        "report_urls": report_urls,
    }


@app.get("/report/{employee_id}", tags=["Employee Report"])
async def get_employee_all_reports(
    employee_id: str,
    submission_id: Optional[str] = None,
    cycle_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Return the stored JSON for an employee (all reports combined)."""
    return _load_employee_json(employee_id, submission_id=submission_id, cycle_name=cycle_name)


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

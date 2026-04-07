"""
core/worker_pool.py
Async worker pool that drains the ReportQueue and calls generation functions.
"""
import asyncio
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from core.config import Config
from core.rate_limiter import RateLimiter
from models.schemas import JobStatus, ReportJob, ReportQueue
from services.frappe_client import (
    extract_full_swot_doc,
    fetch_frappe_swot_doc,
    frappe_headers,
    frappe_query_params,
)
from services.report_renderer import (
    build_swot_lists_from_section_paragraphs,
    is_swot_section,
    text_to_paragraphs,
)
from services.report_storage import save_employee_json

# LLM generation imports (from generate_groq.py / future llm package)
from generate_groq import (
    DEFAULT_REPORT_TYPE_BY_DIMENSION,
    MODEL_BY_REPORT_TYPE_DEDICATED,
    REPORT_TITLE_MAP,
    generate_multi_reports,
    generate_multi_reports_json,
    generate_multi_reports_structured,
    generate_primary_report_json,
    generate_structured_report_by_dimension,
    generate_text_report,
    map_frappe_to_nd,
    resolve_input_data,
)


def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _generate_swot_via_llm(behavioral_stage: Dict[str, Any], report_type: str = "employee") -> Dict[str, Any]:
    """
    Generate SWOT via a lean LLM call.
    Works for all report types (employee/boss/team/organization).

    report_type controls the framing:
      employee     → Individual SWOT from the employee's perspective
      boss         → Dyadic SWOT for the employee-boss relationship
      team         → Collective SWOT for the team/department
      organization → Cumulative SWOT across all dimensions
    """
    from groq import Groq
    import os, json as _json, re as _re

    stage     = behavioral_stage.get("stage", "")
    sub_stage = behavioral_stage.get("sub_stage", "") or behavioral_stage.get("sub_stage_definition", "")
    definition = behavioral_stage.get("sub_stage_definition", "")

    # Context and SWOT title vary by report type
    _context_map = {
        "employee": (
            "an individual employee's personal growth and development",
            "Individual SWOT",
        ),
        "boss": (
            "the employee-boss working relationship and its dynamics",
            "Dyadic SWOT (Employee-Boss Relationship)",
        ),
        "team": (
            "the team's collective performance, dynamics, and collaboration",
            "Collective SWOT (Team/Department)",
        ),
        "organization": (
            "the organization's alignment, culture, and strategic performance",
            "Cumulative SWOT (Organizational)",
        ),
    }
    context_desc, swot_label = _context_map.get(report_type, _context_map["employee"])

    prompt = f"""You are a senior behavioral coach working within the ChaturVima framework.

Generate a {swot_label} analysis for the following context:

Stage: {stage}
Sub-Stage: {sub_stage}
Definition: {definition}
Focus: {context_desc}

Return ONLY a valid JSON object with EXACTLY this structure (no markdown fences, no extra keys):
{{
  "strengths": ["point 1", "point 2", "point 3", "point 4"],
  "weaknesses": ["point 1", "point 2", "point 3", "point 4"],
  "opportunities": ["point 1", "point 2", "point 3", "point 4"],
  "threats": ["point 1", "point 2", "point 3", "point 4"],
  "recommendations": [
    {{"recommendations_title": "Title 1", "recommendations_description": "2-3 sentence description."}},
    {{"recommendations_title": "Title 2", "recommendations_description": "2-3 sentence description."}},
    {{"recommendations_title": "Title 3", "recommendations_description": "2-3 sentence description."}}
  ],
  "actionable_steps": [
    {{"description": "Concrete step 1"}},
    {{"description": "Concrete step 2"}},
    {{"description": "Concrete step 3"}},
    {{"description": "Concrete step 4"}},
    {{"description": "Concrete step 5"}}
  ],
  "strategic_recommendations": "One paragraph of strategic guidance."
}}

RULES:
- Each strength/weakness/opportunity/threat must be a single clear sentence.
- Frame the analysis from the perspective of: {context_desc}.
- Ground all points in the specific stage and sub-stage characteristics.
- Do NOT wrap in markdown code fences."""

    from generate_groq import GLOBAL_MODEL_FALLBACKS, MODEL_NAME, _dedupe_models, _is_rate_limited_error
    groq_key = os.getenv("GROQ_API_KEY", "")
    fallback_chain = _dedupe_models(
        [os.getenv("GROQ_MODEL_1D", MODEL_NAME)] + GLOBAL_MODEL_FALLBACKS + [MODEL_NAME]
    )

    raw = ""
    for model in fallback_chain:
        try:
            client = Groq(api_key=groq_key)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=1500,
            )
            raw = resp.choices[0].message.content.strip()
            break
        except Exception as exc:
            if _is_rate_limited_error(str(exc)):
                continue
            print(f"⚠️  SWOT LLM model '{model}' error: {exc}")

    if not raw:
        return {
            "sub_stage": sub_stage, "source": "llm_generated",
            "strengths": [], "weaknesses": [], "opportunities": [], "threats": [],
            "recommendations": [], "actionable_steps": [], "strategic_recommendations": "",
        }

    cleaned = _re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()
    try:
        parsed = _json.loads(cleaned)
    except Exception:
        match = _re.search(r"\{.*\}", cleaned, _re.DOTALL)
        try:
            parsed = _json.loads(match.group()) if match else {}
        except Exception:
            parsed = {}

    return {
        "sub_stage": sub_stage,
        "source": "llm_generated",
        "strengths":     [{"description": s} for s in parsed.get("strengths",     [])],
        "weaknesses":    [{"description": s} for s in parsed.get("weaknesses",    [])],
        "opportunities": [{"description": s} for s in parsed.get("opportunities", [])],
        "threats":       [{"description": s} for s in parsed.get("threats",       [])],
        "recommendations":  parsed.get("recommendations",  []),
        "actionable_steps": parsed.get("actionable_steps", []),
        "strategic_recommendations": parsed.get("strategic_recommendations", ""),
    }


def _extract_submission_id(message: Dict[str, Any], fallback: Optional[str] = None) -> Optional[str]:
    for key in ("submission_id", "assessment_submission_id", "employee_submission_id", "submission", "assessment_id"):
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


def _apply_1d_swot_override(reports_payload: Any, swot_doc: Optional[Dict[str, Any]]) -> bool:
    """Inject SWOT data from Frappe into the employee report sections in-place."""
    from services.frappe_client import extract_swot_lists

    if not isinstance(reports_payload, dict) or not isinstance(swot_doc, dict):
        return False
    employee_report = reports_payload.get("employee")
    if not isinstance(employee_report, dict):
        return False
    sections = employee_report.get("sections")
    if not isinstance(sections, list):
        return False

    swot_lists = extract_swot_lists(swot_doc)

    # Build paragraph lines from the structured lists
    def _fmt(texts: List[str], fallback: str) -> str:
        return " ".join(f"{i}. {t}" for i, t in enumerate(texts, 1)) if texts else fallback

    paragraphs = [
        _fmt(swot_lists["strengths"],     "Strengths not available."),
        _fmt(swot_lists["weaknesses"],    "Weaknesses not available."),
        _fmt(swot_lists["opportunities"], "Opportunities not available."),
        _fmt(swot_lists["threat"],      "threat not available."),
    ]

    swot_section = next(
        (s for s in sections if isinstance(s, dict) and
         (str(s.get("id", "")).strip().lower() == "swot" or "swot" in str(s.get("title", "")).lower())),
        None,
    )
    if swot_section is None:
        swot_section = {"id": "swot", "title": "Individual SWOT Analysis", "paragraphs": paragraphs}
        sections.append(swot_section)
    else:
        swot_section["id"] = "swot"
        swot_section.setdefault("title", "Individual SWOT Analysis")
        swot_section["paragraphs"] = paragraphs

    swot_section["swot_lists"] = swot_lists
    swot_section["source"] = "frappe_swot"
    swot_section["sub_stage"] = _normalize_optional_str(swot_doc.get("sub_stage") or swot_doc.get("name")) or ""
    return True


_SWOT_KEYS = ("strengths", "weaknesses", "opportunities", "threats")
_SWOT_LABELS = {
    "strengths": "Strengths",
    "weaknesses": "Weaknesses",
    "opportunities": "Opportunities",
    "threats": "Threats",
}
_SWOT_SECTION_TITLES = {
    "employee": "Individual SWOT Analysis",
    "boss": "Dyadic SWOT Analysis",
    "team": "Collective SWOT Analysis",
    "organization": "Cumulative SWOT Overlay",
}


def _normalize_swot_lists(raw_swot: Any) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {key: [] for key in _SWOT_KEYS}
    if not isinstance(raw_swot, dict):
        return normalized

    for key in _SWOT_KEYS:
        values = raw_swot.get(key, [])
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list):
            continue

        clean_values: List[str] = []
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            # Remove heading prefixes when they appear in item text.
            text = re.sub(
                r"^(strengths?|weaknesses?|opportunities?|threat?)\s*[:\-]\s*",
                "",
                text,
                flags=re.IGNORECASE,
            ).strip()
            if text:
                clean_values.append(text)
        normalized[key] = clean_values
    return normalized


def _fill_missing_swot_lists(swot_lists: Dict[str, List[str]]) -> None:
    for key in _SWOT_KEYS:
        if not swot_lists.get(key):
            label = _SWOT_LABELS[key]
            swot_lists[key] = [f"{label} not explicitly available in this report output."]


def _swot_lists_to_paragraphs(swot_lists: Dict[str, List[str]]) -> List[str]:
    paragraphs: List[str] = []
    for key in _SWOT_KEYS:
        items = swot_lists.get(key, [])
        line = " ".join(f"{idx}. {item}" for idx, item in enumerate(items, 1))
        paragraphs.append(line or f"{_SWOT_LABELS[key]} not available.")
    return paragraphs


def _ensure_swot_section(clean_sections: List[Dict[str, Any]], report_type: str) -> None:
    swot_index: Optional[int] = None
    for i, section in enumerate(clean_sections):
        if not isinstance(section, dict):
            continue
        if is_swot_section(section.get("id", ""), section.get("title", "")):
            swot_index = i
            break

    if swot_index is None:
        swot_lists = {key: [] for key in _SWOT_KEYS}
        _fill_missing_swot_lists(swot_lists)
        clean_sections.append({
            "id": "swot",
            "title": _SWOT_SECTION_TITLES.get(report_type, "SWOT Analysis"),
            "paragraphs": _swot_lists_to_paragraphs(swot_lists),
            "swot_lists": swot_lists,
        })
        return

    swot_section = clean_sections[swot_index]
    swot_section["id"] = "swot"
    swot_section["title"] = swot_section.get("title") or _SWOT_SECTION_TITLES.get(report_type, "SWOT Analysis")

    existing_swot = swot_section.get("swot_lists")
    if isinstance(existing_swot, dict):
        swot_lists = _normalize_swot_lists(existing_swot)
    else:
        swot_lists = build_swot_lists_from_section_paragraphs(swot_section.get("paragraphs") or [])
        swot_lists = _normalize_swot_lists(swot_lists)

    _fill_missing_swot_lists(swot_lists)
    swot_section["swot_lists"] = swot_lists
    swot_section["paragraphs"] = _swot_lists_to_paragraphs(swot_lists)


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
                job: ReportJob = await asyncio.wait_for(self.queue.get_job(), timeout=1.0)
                print(f"👷 Worker {worker_id} processing job {job.job_id}")
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now()
                try:
                    await self.rate_limiter.acquire()

                    # ── BRANCH 1: Frappe employee report ──────────────────────
                    if job.employee_report:
                        await self._process_employee_report(job, worker_id)

                    # ── BRANCH 2: Standard multi-report ──────────────────────
                    elif job.multi_report:
                        data = resolve_input_data(job.payload)
                        if job.structured:
                            result = await asyncio.wait_for(
                                asyncio.to_thread(generate_multi_reports_structured, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 5,
                            )
                        else:
                            result = await asyncio.wait_for(
                                asyncio.to_thread(generate_multi_reports, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                            )
                        job.result = result

                    # ── BRANCH 3: Standard single-report ─────────────────────
                    else:
                        data = resolve_input_data(job.payload)
                        if job.structured:
                            result = await asyncio.wait_for(
                                asyncio.to_thread(generate_structured_report_by_dimension, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                            )
                        else:
                            result = await asyncio.wait_for(
                                asyncio.to_thread(generate_text_report, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS,
                            )
                        job.result = result

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

    async def _process_employee_report(self, job: ReportJob, worker_id: int):
        employee_id = job.payload["employee"]
        requested_cycle = _normalize_optional_str(job.payload.get("cycle_name"))
        requested_submission = _normalize_optional_str(job.payload.get("submission_id"))

        frappe_params = frappe_query_params(employee_id, cycle_name=requested_cycle, submission_id=requested_submission)
        runtime_auth = _normalize_optional_str(job.payload.get("_frappe_auth")) or _normalize_optional_str(
            job.payload.get("_user_auth")
        )
        headers = frappe_headers(explicit_auth=runtime_auth)
        print(f"🌐 Worker {worker_id}: fetching Frappe data for {employee_id}")

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(Config.FRAPPE_BASE_URL, params=frappe_params, headers=headers)
                resp.raise_for_status()
                frappe_data = resp.json()
            except httpx.HTTPStatusError as exc:
                body_snippet = (exc.response.text or "").strip().replace("\n", " ")[:300]
                raise RuntimeError(
                    f"Frappe {exc.response.status_code} for params={frappe_params}. Response: {body_snippet}"
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
            # Pass the user's own token so SWOT fetch uses their identity, not the admin key
            swot_doc = await fetch_frappe_swot_doc(dominant_sub_stage, user_auth=runtime_auth or "")
            status = "found" if swot_doc else "not found"
            print(f"🧩 Worker {worker_id}: SWOT doc {status} for sub_stage='{dominant_sub_stage}'")

        # ── SWOT handling for 1D ──────────────────────────────────────────
        # ALWAYS strip the large hardcoded fallback SWOT from nd_data before LLM.
        # These dicts (~3-5k tokens) are never needed by the LLM.
        #
        # Two cases:
        #   A) Frappe SWOT doc found  → hold it for verbatim post-inject (no LLM).
        #   B) No Frappe SWOT doc     → LLM generates SWOT from behavioral stage data
        #                               via a separate lean call after the main report.
        nd_data.pop("individual_swot", None)
        nd_data.pop("recommendation_framework", None)

        full_swot: Optional[Dict[str, Any]] = None
        if swot_doc:
            full_swot = extract_full_swot_doc(swot_doc)
            print(f"✅ Worker {worker_id}: Frappe SWOT held for post-inject (NOT sent to LLM)")
        else:
            print(f"⚠️  Worker {worker_id}: no Frappe SWOT for sub_stage='{dominant_sub_stage}' — will generate via LLM post-report")

        if single_questionnaire and primary_report_type:
            print(f"🧭 Worker {worker_id}: single questionnaire -> generating only '{primary_report_type}' report")
            result = await asyncio.wait_for(
                asyncio.to_thread(generate_primary_report_json, nd_data),
                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
            )
            reports_payload = result.get("reports", {})
        else:
            result = await asyncio.wait_for(
                asyncio.to_thread(generate_multi_reports_json, nd_data),
                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
            )
            reports_payload = result.get("reports", {})

        submission_id = _extract_submission_id(msg, requested_submission)
        cycle_name = _extract_cycle_name(msg, requested_cycle)
        employee_name = (
            msg.get("employee_name") or msg.get("employee_full_name")
            or msg.get("employee") or employee_id
        )
        designation = msg.get("designation") or msg.get("role") or msg.get("employee_role") or "Employee"
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
            try:
                final_value = float(st.get("final_value", score))
            except (TypeError, ValueError):
                final_value = score
            try:
                final_pct = float(st.get("final_percentage", pct))
            except (TypeError, ValueError):
                final_pct = pct
            stage_scores.append({
                "stage": str(st.get("stage", "-")),
                # Keep legacy keys for backward compatibility while switching
                # report display to final_value/final_percentage.
                "score": f"{final_value:.2f}",
                "percentage": f"{final_pct:.1f}",
                "final_value": f"{final_value:.2f}",
                "final_percentage": f"{final_pct:.1f}",
            })

        report_sections_list = []
        if isinstance(reports_payload, dict):
            for rtype, robj in reports_payload.items():
                if isinstance(robj, dict) and "sections" in robj:
                    clean_sections = []
                    for sec in robj.get("sections", []):
                        paras = sec.get("paragraphs") or text_to_paragraphs(sec.get("text", ""))
                        section_id = sec.get("id", "")
                        section_title = sec.get("title", "")
                        clean_sec: Dict[str, Any] = {
                            "id": section_id,
                            "title": section_title,
                            "paragraphs": paras,
                        }
                        existing_swot = sec.get("swot_lists")
                        if isinstance(existing_swot, dict):
                            clean_sec["swot_lists"] = existing_swot
                        elif is_swot_section(section_id, section_title):
                            clean_sec["swot_lists"] = build_swot_lists_from_section_paragraphs(paras)
                        clean_sections.append(clean_sec)
                    _ensure_swot_section(clean_sections, rtype)

                    # ── SWOT inject for ALL report types ──────────────────────────────
                    # 1D employee:  use Frappe SWOT if available, else generate via LLM
                    # 2D boss:      always generate via LLM (no Frappe SWOT for boss)
                    # 3D team:      always generate via LLM
                    # 4D org:       always generate via LLM
                    #
                    # In all cases we also check whether the LLM already put real SWOT
                    # content in (i.e. swot_lists has items). If it did, skip inject.
                    swot_to_inject: Optional[Dict[str, Any]] = None

                    if rtype == "employee":
                        # 1D: Frappe SWOT if found, else LLM
                        if full_swot is not None:
                            swot_to_inject = full_swot
                        else:
                            # Check if LLM already generated real content
                            existing_sec = next(
                                (s for s in clean_sections if is_swot_section(s.get("id", ""), s.get("title", ""))),
                                None,
                            )
                            existing_lists = existing_sec.get("swot_lists", {}) if existing_sec else {}
                            has_real_content = any(
                                existing_lists.get(k) and
                                not str(existing_lists[k][0]).lower().endswith("not explicitly available in this report output.")
                                for k in ("strengths", "weaknesses", "opportunities", "threats")
                                if existing_lists.get(k)
                            )
                            if not has_real_content:
                                print(f"🤖 Worker {worker_id}: generating {rtype} SWOT via LLM (sub_stage='{dominant_sub_stage}')")
                                swot_to_inject = await asyncio.to_thread(
                                    _generate_swot_via_llm,
                                    nd_data.get("behavioral_stage", {}),
                                    rtype,
                                )
                    else:
                        # 2D/3D/4D: always check if LLM already gave real SWOT content
                        existing_sec = next(
                            (s for s in clean_sections if is_swot_section(s.get("id", ""), s.get("title", ""))),
                            None,
                        )
                        existing_lists = existing_sec.get("swot_lists", {}) if existing_sec else {}
                        has_real_content = any(
                            existing_lists.get(k) and
                            not str(existing_lists[k][0]).lower().endswith("not explicitly available in this report output.")
                            for k in ("strengths", "weaknesses", "opportunities", "threats")
                            if existing_lists.get(k)
                        )
                        if not has_real_content:
                            print(f"🤖 Worker {worker_id}: generating {rtype} SWOT via LLM (stage='{nd_data.get('behavioral_stage', {}).get('stage', '')}')")
                            swot_to_inject = await asyncio.to_thread(
                                _generate_swot_via_llm,
                                nd_data.get("behavioral_stage", {}),
                                rtype,
                            )

                    if swot_to_inject:
                        for sec in clean_sections:
                            if is_swot_section(sec.get("id", ""), sec.get("title", "")):
                                def _row_text(r: Dict[str, Any]) -> str:
                                    return (
                                        r.get("description")
                                        or r.get("desription")   # Frappe Threat typo
                                        or r.get("recommendations_description")
                                        or r.get("value")
                                        or r.get("title")
                                        or ""
                                    ).strip()
                                sec["swot_lists"] = {
                                    "strengths":     [_row_text(r) for r in swot_to_inject.get("strengths",     []) if _row_text(r)],
                                    "weaknesses":    [_row_text(r) for r in swot_to_inject.get("weaknesses",    []) if _row_text(r)],
                                    "opportunities": [_row_text(r) for r in swot_to_inject.get("opportunities", []) if _row_text(r)],
                                    "threats":       [_row_text(r) for r in swot_to_inject.get("threats",       []) if _row_text(r)],
                                }
                                sec["recommendations"]  = swot_to_inject.get("recommendations",  [])
                                sec["actionable_steps"] = swot_to_inject.get("actionable_steps", [])
                                sec["strategic_recommendations"] = swot_to_inject.get("strategic_recommendations", "")
                                sec["source"]    = swot_to_inject.get("source", "llm_generated")
                                sec["sub_stage"] = swot_to_inject.get("sub_stage", "")
                                sec["paragraphs"] = _swot_lists_to_paragraphs(sec["swot_lists"])
                                print(f"✅ Worker {worker_id}: SWOT injected into '{rtype}' (source={sec['source']})")
                                break

                    report_sections_list.append({
                        "title": robj.get("title") or REPORT_TITLE_MAP.get(rtype, rtype),
                        "report_type": rtype,
                        "sections": clean_sections,
                    })

        from datetime import datetime as _dt
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
                "generated_date": _dt.now().strftime("%d %B %Y"),
                "stage_scores": stage_scores,
            },
            "reports": report_sections_list,
        }

        path = save_employee_json(json_payload, employee_id, submission_id=submission_id, cycle_name=cycle_name)
        print(f"💾 Worker {worker_id}: saved {path}")
        job.result = json_payload

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


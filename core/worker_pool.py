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

from generate_groq import (
    DEFAULT_REPORT_TYPE_BY_DIMENSION,
    MODEL_BY_REPORT_TYPE_DEDICATED,
    REPORT_TITLE_MAP,
    _rename_stage_for_display,
    generate_report_as_json,
    generate_structured_report_by_dimension,
    generate_text_report,
    map_frappe_to_nd,
    rag_lock,
    resolve_input_data,
    retrieve_rag_context,
)


def _normalize_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _generate_swot_via_llm(behavioral_stage: Dict[str, Any], report_type: str = "employee") -> Dict[str, Any]:
    import os, json as _json, re as _re

    stage      = behavioral_stage.get("stage", "")
    sub_stage  = behavioral_stage.get("sub_stage", "") or behavioral_stage.get("sub_stage_definition", "")
    definition = behavioral_stage.get("sub_stage_definition", "")

    _context_map = {
        "employee":     ("an individual employee's personal growth and development",          "Individual SWOT"),
        "boss":         ("the employee-boss working relationship and its dynamics",           "Dyadic SWOT (Employee-Boss Relationship)"),
        "team":         ("the team's collective performance, dynamics, and collaboration",    "Collective SWOT (Team/Department)"),
        "organization": ("the organization's alignment, culture, and strategic performance", "Cumulative SWOT (Organizational)"),
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

    from generate_groq import (
        GLOBAL_MODEL_FALLBACKS,
        MODEL_NAME,
        _create_groq_chat_completion,
        _filter_allowed_models,
        _is_rate_limited_error,
        create_groq_client,
    )
    fallback_chain = _filter_allowed_models(
        [os.getenv("GROQ_MODEL_1D", MODEL_NAME)] + GLOBAL_MODEL_FALLBACKS + [MODEL_NAME]
    )

    raw = ""
    for model in fallback_chain:
        try:
            client = create_groq_client()
            resp = _create_groq_chat_completion(
                client,
                request_label=f"swot:{report_type} [{model}]",
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
            print(f"SWOT LLM model '{model}' error: {exc}")

    if not raw:
        return {
            "sub_stage": sub_stage, "source": "llm_generated",
            "strengths": [], "weaknesses": [], "opportunities": [],
            "threat": [], "threats": [],
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

    parsed_threats = parsed.get("threat", parsed.get("threats", []))
    threat_rows = [{"description": s} for s in parsed_threats]
    return {
        "sub_stage": sub_stage, "source": "llm_generated",
        "strengths":     [{"description": s} for s in parsed.get("strengths",     [])],
        "weaknesses":    [{"description": s} for s in parsed.get("weaknesses",    [])],
        "opportunities": [{"description": s} for s in parsed.get("opportunities", [])],
        "threat": threat_rows, "threats": threat_rows,
        "recommendations":           parsed.get("recommendations",           []),
        "actionable_steps":          parsed.get("actionable_steps",          []),
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


def _build_stage_default_swot(nd_data: Dict[str, Any], dominant_sub_stage: Optional[str] = None) -> Optional[Dict[str, Any]]:
    raw_swot = nd_data.get("individual_swot")
    if not isinstance(raw_swot, dict):
        return None
    rec_fw      = nd_data.get("recommendation_framework")
    rec_actions = rec_fw.get("recommended_actions", []) if isinstance(rec_fw, dict) else []
    principles  = rec_fw.get("principles", [])           if isinstance(rec_fw, dict) else []

    recommendations: List[Dict[str, Any]] = []
    actionable_steps: List[Dict[str, Any]] = []
    if isinstance(rec_actions, list):
        for idx, row in enumerate(rec_actions, start=1):
            if not isinstance(row, dict):
                continue
            title       = str(row.get("focus_area") or row.get("title") or f"Action {idx}").strip()
            description = str(row.get("recommendation") or row.get("description") or "").strip()
            if title or description:
                recommendations.append({"recommendations_title": title, "recommendations_description": description or title})
                actionable_steps.append({"description": description or title})

    strategic_recommendations = ""
    if isinstance(principles, list):
        strategic_recommendations = " ".join(str(p).strip() for p in principles if str(p).strip())

    return {
        "sub_stage": _normalize_optional_str(dominant_sub_stage) or "",
        "source": "stage_default_swot",
        "strengths":     raw_swot.get("strengths",     []),
        "weaknesses":    raw_swot.get("weaknesses",    []),
        "opportunities": raw_swot.get("opportunities", []),
        "threat": raw_swot.get("threat", raw_swot.get("threats", [])),
        "recommendations":           recommendations,
        "actionable_steps":          actionable_steps,
        "strategic_recommendations": strategic_recommendations,
    }


def _swot_row_text(row: Any) -> str:
    if isinstance(row, dict):
        return (
            row.get("description") or row.get("desription")
            or row.get("recommendations_description")
            or row.get("value") or row.get("title") or ""
        ).strip()
    return str(row or "").strip()


def _has_non_empty_swot_rows(rows: Any) -> bool:
    return isinstance(rows, list) and any(_swot_row_text(r) for r in rows)


def _apply_1d_swot_override(reports_payload: Any, swot_doc: Optional[Dict[str, Any]]) -> bool:
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

    def _fmt(texts: List[str], fallback: str) -> str:
        return " ".join(f"{i}. {t}" for i, t in enumerate(texts, 1)) if texts else fallback

    paragraphs = [
        _fmt(swot_lists["strengths"],     "Strengths not available."),
        _fmt(swot_lists["weaknesses"],    "Weaknesses not available."),
        _fmt(swot_lists["opportunities"], "Opportunities not available."),
        _fmt(swot_lists["threat"],        "Threats not available."),
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
    swot_section["source"]     = "frappe_swot"
    swot_section["sub_stage"]  = _normalize_optional_str(swot_doc.get("sub_stage") or swot_doc.get("name")) or ""
    return True


_SWOT_KEYS = ("strengths", "weaknesses", "opportunities", "threat")
_SWOT_LABELS = {
    "strengths": "Strengths", "weaknesses": "Weaknesses",
    "opportunities": "Opportunities", "threat": "Threats",
}
_SWOT_SECTION_TITLES = {
    "employee": "Individual SWOT Analysis", "boss": "Dyadic SWOT Analysis",
    "team": "Collective SWOT Analysis",     "organization": "Cumulative SWOT Overlay",
}
_RECOMMENDATION_SECTION_TITLES = {
    "employee": "Recommendations", "boss": "Recommendations",
    "team": "Recommendations",     "organization": "Recommendations",
}
_ACTION_SECTION_ID_PRIORITY = ("action_plan", "next_steps", "joint_recommendations", "boss_recommendations", "recommendations")
_ACTION_SECTION_TITLE_HINTS = ("action navigator", "action plan", "next steps", "development path", "recommendation", "intervention")


def _split_swot_item_into_points(text: str) -> List[str]:
    text = str(text or "").strip()
    if not text:
        return []
    if text.startswith('[') and text.endswith(']'):
        inner = text[1:-1].strip()
        if (inner.startswith('"') and inner.endswith('"')) or (inner.startswith("'") and inner.endswith("'")):
            inner = inner[1:-1].strip()
        text = inner.strip()
    if not text:
        return []
    quad_pat = re.compile(r'(strengths?|weaknesses?|opportunities?|threats?)\s*[:\-]', re.IGNORECASE)
    quad_matches = list(quad_pat.finditer(text))
    if len(quad_matches) >= 2:
        text = text[:quad_matches[1].start()].strip()
    text = re.sub(r'^(strengths?|weaknesses?|opportunities?|threats?)\s*[:\-]\s*', '', text, flags=re.IGNORECASE).strip()
    if not text:
        return []
    numbered = re.split(r'(?<=\S)\s+(?=\d+\.\s+[^\d])', text)
    if len(numbered) < 2:
        numbered = re.split(r'(?<=\S)\s+(?=\d+\)\s)', text)
    if len(numbered) >= 2:
        clean = []
        for part in numbered:
            part = re.sub(r'^\s*\d+[.)\]]\s*', '', part).strip()
            if part and len(part) > 15:
                clean.append(part)
        if len(clean) >= 2:
            return clean
    if any(ch in text for ch in ('\u2022', '\u25e6', '\u25b8', '\u00b7')):
        parts = re.split(r'\s*[\u2022\u25e6\u25b8\u00b7]\s*', text)
        clean = [p.strip() for p in parts if p.strip() and len(p.strip()) > 15]
        if len(clean) >= 2:
            return clean
    return [text] if text else []


def _normalize_swot_lists(raw_swot: Any) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {key: [] for key in _SWOT_KEYS}
    if not isinstance(raw_swot, dict):
        return normalized
    if "threat" not in raw_swot and "threats" in raw_swot:
        raw_swot = dict(raw_swot)
        raw_swot["threat"] = raw_swot.get("threats", [])

    _quad_intro = re.compile(
        r'^(there are|one|another|additionally|furthermore|the|these)?\s*'
        r'(strengths?|weaknesses?|blind.?spots?|opportunities?|threats?)'
        r'\s*(of|for|to|include|in|is|are|:)', re.IGNORECASE,
    )
    _quad_kw = [
        (re.compile(r'\bopportunit', re.IGNORECASE), "opportunities"),
        (re.compile(r'\bthreat',     re.IGNORECASE), "threat"),
        (re.compile(r'\bweakness|\bblind.?spot', re.IGNORECASE), "weaknesses"),
        (re.compile(r'\bstrength',   re.IGNORECASE), "strengths"),
    ]

    def _detect(text: str) -> Optional[str]:
        m = _quad_intro.match(text.strip())
        if m:
            w = m.group(2).lower()
            if 'opportunit' in w: return 'opportunities'
            if 'threat'     in w: return 'threat'
            if 'weakness' in w or 'blind' in w: return 'weaknesses'
            if 'strength'   in w: return 'strengths'
        for pat, k in _quad_kw:
            if pat.search(text):
                return k
        return None

    for key in _SWOT_KEYS:
        values = raw_swot.get(key, [])
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list):
            continue
        for value in values:
            text = str(value or "").strip()
            if text.startswith('[') and text.endswith(']'):
                inner = text[1:-1].strip()
                if (inner.startswith('"') and inner.endswith('"')) or (inner.startswith("'") and inner.endswith("'")):
                    inner = inner[1:-1].strip()
                text = inner
            if not text:
                continue
            for point in _split_swot_item_into_points(text):
                detected_key = _detect(point)
                target = detected_key if detected_key else key
                normalized[target].append(point)
    return normalized


def _fill_missing_swot_lists(swot_lists: Dict[str, List[str]]) -> None:
    for key in _SWOT_KEYS:
        if not swot_lists.get(key):
            swot_lists[key] = [f"{_SWOT_LABELS[key]} not explicitly available in this report output."]


def _swot_lists_to_paragraphs(swot_lists: Dict[str, List[str]]) -> List[str]:
    paragraphs: List[str] = []
    for key in _SWOT_KEYS:
        items = swot_lists.get(key, [])
        line  = " ".join(f"{idx}. {item}" for idx, item in enumerate(items, 1))
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
    swot_section["id"]    = "swot"
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


def _inject_actionable_into_action_section(clean_sections: List[Dict[str, Any]], swot_payload: Dict[str, Any]) -> bool:
    if not isinstance(swot_payload, dict):
        return False
    raw_steps  = swot_payload.get("actionable_steps", [])
    if not isinstance(raw_steps, list):
        return False
    step_texts = [_swot_row_text(step) for step in raw_steps if _swot_row_text(step)]
    if not step_texts:
        return False

    target_idx: Optional[int] = None
    for preferred_id in _ACTION_SECTION_ID_PRIORITY:
        for idx, section in enumerate(clean_sections):
            if not isinstance(section, dict):
                continue
            if str(section.get("id", "")).strip().lower() == preferred_id:
                target_idx = idx
                break
        if target_idx is not None:
            break
    if target_idx is None:
        for idx, section in enumerate(clean_sections):
            if not isinstance(section, dict):
                continue
            title = str(section.get("title", "")).strip().lower()
            if any(hint in title for hint in _ACTION_SECTION_TITLE_HINTS):
                target_idx = idx
                break
    if target_idx is None:
        return False

    target = clean_sections[target_idx]
    existing_rows  = target.get("actionable_steps", [])
    existing_texts = [_swot_row_text(r) for r in existing_rows if _swot_row_text(r)] if isinstance(existing_rows, list) else []
    merged = list(existing_texts)
    for text in step_texts:
        if text not in merged:
            merged.append(text)
    target["actionable_steps"] = [{"description": text} for text in merged]

    action_line = "Actionable Steps: " + " ".join(f"{i}. {t}" for i, t in enumerate(merged, 1))
    paras = target.get("paragraphs", [])
    if not isinstance(paras, list):
        paras = text_to_paragraphs(str(paras))
    replaced = False
    for i, para in enumerate(paras):
        if str(para).strip().lower().startswith("actionable steps:"):
            paras[i] = action_line
            replaced = True
            break
    if not replaced:
        paras.append(action_line)
    target["paragraphs"] = paras
    return True


def _inject_recommendations_section(clean_sections: List[Dict[str, Any]], swot_payload: Dict[str, Any], report_type: str) -> bool:
    if not isinstance(swot_payload, dict):
        return False
    rec_rows  = swot_payload.get("recommendations", [])
    strategic = str(swot_payload.get("strategic_recommendations", "") or "").strip()

    rec_paragraphs: List[str] = []
    normalized_recs: List[Dict[str, str]] = []
    if isinstance(rec_rows, list):
        idx = 1
        for row in rec_rows:
            if isinstance(row, dict):
                title = str(row.get("recommendations_title") or row.get("title") or "").strip()
                desc  = str(row.get("recommendations_description") or row.get("description") or row.get("desription") or row.get("value") or "").strip()
                if not title and not desc:
                    continue
                line = f"{idx}. {title}: {desc}" if title and desc else f"{idx}. {title or desc}"
                rec_paragraphs.append(line)
                normalized_recs.append({"recommendations_title": title or f"Recommendation {idx}", "recommendations_description": desc or title or ""})
                idx += 1
            else:
                text = _swot_row_text(row)
                if text:
                    rec_paragraphs.append(f"{idx}. {text}")
                    normalized_recs.append({"recommendations_title": f"Recommendation {idx}", "recommendations_description": text})
                    idx += 1

    if strategic:
        rec_paragraphs.append(f"Strategic Recommendations: {strategic}")
    if not rec_paragraphs:
        return False

    title = _RECOMMENDATION_SECTION_TITLES.get(report_type, "Recommendations")
    target_idx: Optional[int] = None
    for idx, sec in enumerate(clean_sections):
        if not isinstance(sec, dict):
            continue
        sec_id    = str(sec.get("id", "")).strip().lower()
        sec_title = str(sec.get("title", "")).strip().lower()
        if sec_id == "recommendations" or sec_title == title.lower():
            target_idx = idx
            break

    section_payload = {
        "id": "recommendations", "title": title,
        "paragraphs": rec_paragraphs, "recommendations": normalized_recs,
        "strategic_recommendations": strategic,
        "source":    swot_payload.get("source", ""),
        "sub_stage": swot_payload.get("sub_stage", ""),
    }

    if target_idx is not None:
        clean_sections[target_idx] = section_payload
        return True

    swot_idx: Optional[int] = None
    for idx, sec in enumerate(clean_sections):
        if not isinstance(sec, dict):
            continue
        if is_swot_section(sec.get("id", ""), sec.get("title", "")):
            swot_idx = idx
            break

    if swot_idx is not None:
        clean_sections.insert(swot_idx + 1, section_payload)
    else:
        clean_sections.append(section_payload)
    return True


class WorkerPool:
    def __init__(self, queue: ReportQueue, rate_limiter: RateLimiter, num_workers: int = 1):
        self.queue        = queue
        self.rate_limiter = rate_limiter
        self.num_workers  = num_workers
        self.workers: List[asyncio.Task] = []
        self.running = False

    async def _worker(self, worker_id: int):
        print(f"Worker {worker_id} started")
        while self.running:
            try:
                job: ReportJob = await asyncio.wait_for(self.queue.get_job(), timeout=1.0)
                print(f"Worker {worker_id} processing job {job.job_id}")
                job.status     = JobStatus.PROCESSING
                job.started_at = datetime.now()
                try:
                    await self.rate_limiter.acquire()

                    if job.employee_report:
                        await self._process_employee_report(job, worker_id)
                    elif job.multi_report:
                        data = resolve_input_data(job.payload)

                        if job.structured:
                            result = await asyncio.wait_for(
                                asyncio.to_thread(generate_structured_report_by_dimension, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 5,
                            )
                        else:
                            result = await asyncio.wait_for(
                                asyncio.to_thread(generate_text_report, data),
                                timeout=Config.GROQ_TIMEOUT_SECONDS * 3,
                            )

                        job.result = result
                    else:
                        data = resolve_input_data(job.payload)
                        if job.structured:
                            result = await asyncio.wait_for(asyncio.to_thread(generate_structured_report_by_dimension, data), timeout=Config.GROQ_TIMEOUT_SECONDS * 3)
                        else:
                            result = await asyncio.wait_for(asyncio.to_thread(generate_text_report, data), timeout=Config.GROQ_TIMEOUT_SECONDS)
                        job.result = result

                    job.status       = JobStatus.COMPLETED
                    job.completed_at = datetime.now()
                    duration = (job.completed_at - job.started_at).total_seconds()
                    print(f"Worker {worker_id} completed job {job.job_id} in {duration:.2f}s")

                except asyncio.TimeoutError:
                    job.status       = JobStatus.FAILED
                    job.error        = "Report generation timed out"
                    job.completed_at = datetime.now()
                    print(f"Worker {worker_id} timeout on job {job.job_id}")
                except Exception as exc:
                    job.status       = JobStatus.FAILED
                    job.error        = str(exc)
                    job.completed_at = datetime.now()
                    print(f"Worker {worker_id} error on job {job.job_id}: {exc}")

            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                print(f"Worker {worker_id} unexpected error: {exc}")

    async def _process_employee_report(self, job: ReportJob, worker_id: int):
        employee_id          = job.payload["employee"]
        requested_cycle      = _normalize_optional_str(job.payload.get("cycle_name"))
        requested_submission = _normalize_optional_str(job.payload.get("submission_id"))

        frappe_params = frappe_query_params(employee_id, cycle_name=requested_cycle, submission_id=requested_submission)
        runtime_auth  = _normalize_optional_str(job.payload.get("_frappe_auth")) or _normalize_optional_str(job.payload.get("_user_auth"))
        headers       = frappe_headers(explicit_auth=runtime_auth)
        print(f"Worker {worker_id}: fetching Frappe data for {employee_id}")

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(Config.FRAPPE_BASE_URL, params=frappe_params, headers=headers)
                resp.raise_for_status()
                frappe_data = resp.json()
            except httpx.HTTPStatusError as exc:
                body_snippet = (exc.response.text or "").strip().replace("\n", " ")[:300]
                raise RuntimeError(f"Frappe {exc.response.status_code} for params={frappe_params}. Response: {body_snippet}") from exc

        if "message" not in frappe_data:
            raise ValueError(f"Unexpected Frappe response: {list(frappe_data.keys())}")

        msg       = frappe_data.get("message", frappe_data)
        nd_data   = map_frappe_to_nd(employee_id, frappe_data)
        dimension = nd_data["dimension"]
        print(f"Worker {worker_id}: dimension={dimension}")

        questionnaires       = msg.get("questionnaires_considered", [])
        single_questionnaire = len(questionnaires) == 1
        primary_report_type  = DEFAULT_REPORT_TYPE_BY_DIMENSION.get(dimension)

        swot_doc: Optional[Dict[str, Any]] = None
        dominant_sub_stage = _normalize_optional_str(msg.get("dominant_sub_stage"))
        if dimension == "1D" and dominant_sub_stage:
            swot_doc = await fetch_frappe_swot_doc(dominant_sub_stage, user_auth=runtime_auth or "")
            status   = "found" if swot_doc else "not found"
            print(f"Worker {worker_id}: SWOT doc {status} for sub_stage='{dominant_sub_stage}'")

        stage_default_swot: Optional[Dict[str, Any]] = None
        if dimension == "1D":
            stage_default_swot = _build_stage_default_swot(nd_data, dominant_sub_stage)

        # Strip large hardcoded SWOT/recommendation dicts — never sent to LLM for 1D.
        # Frappe SWOT quadrant lists are injected AFTER the LLM finishes.
        nd_data.pop("individual_swot",         None)
        nd_data.pop("recommendation_framework", None)

        full_swot: Optional[Dict[str, Any]] = None
        if swot_doc:
            full_swot = extract_full_swot_doc(swot_doc)
            print(f"Worker {worker_id}: Frappe SWOT held for post-inject (NOT sent to LLM)")
        elif stage_default_swot:
            full_swot = stage_default_swot
            print(f"Worker {worker_id}: using stage-default SWOT fallback for sub_stage='{dominant_sub_stage}'")
        else:
            if dimension == "1D":
                print(f"Worker {worker_id}: no Frappe SWOT for sub_stage='{dominant_sub_stage}' -- will generate via LLM post-report")
            else:
                print(f"Worker {worker_id}: {dimension} flow uses LLM-generated SWOT (Frappe SWOT lookup skipped)")

        # 1D: if SWOT quadrant lists exist but action/recommendation fields are missing, fill via LLM
        if dimension == "1D" and full_swot is not None:
            missing_actionable      = not _has_non_empty_swot_rows(full_swot.get("actionable_steps",   []))
            missing_recommendations = not _has_non_empty_swot_rows(full_swot.get("recommendations",    []))
            missing_strategic       = not str(full_swot.get("strategic_recommendations", "") or "").strip()

            if missing_actionable or missing_recommendations or missing_strategic:
                print(f"Worker {worker_id}: filling missing 1D SWOT guidance via LLM")
                llm_guidance = await asyncio.to_thread(_generate_swot_via_llm, nd_data.get("behavioral_stage", {}), "employee")
                if missing_recommendations:
                    full_swot["recommendations"] = llm_guidance.get("recommendations", [])
                if missing_actionable:
                    full_swot["actionable_steps"] = llm_guidance.get("actionable_steps", [])
                if missing_strategic:
                    full_swot["strategic_recommendations"] = llm_guidance.get("strategic_recommendations", "")
                base_source = str(full_swot.get("source", "") or "").strip()
                full_swot["source"] = f"{base_source}+llm_guidance_fill" if base_source else "llm_guidance_fill"

        if single_questionnaire and primary_report_type:
            print(f"Worker {worker_id}: single questionnaire -> generating only '{primary_report_type}' report")
            print(f"SINGLE REPORT GENERATION -- {dimension} -> {primary_report_type} [fast-json]")
            with rag_lock:
                rag_context = retrieve_rag_context(nd_data)
            result_report = await asyncio.wait_for(
                asyncio.to_thread(generate_report_as_json, nd_data, primary_report_type, rag_context),
                timeout=Config.GROQ_TIMEOUT_SECONDS * 5,
            )
            reports_payload = {primary_report_type: result_report}
        else:
            print(f"SINGLE REPORT GENERATION -- {dimension} [fast-json]")
            if not primary_report_type:
                raise ValueError(f"Unsupported dimension: {dimension}")
            with rag_lock:
                rag_context = retrieve_rag_context(nd_data)
            result = await asyncio.wait_for(
                asyncio.to_thread(generate_report_as_json, nd_data, primary_report_type, rag_context),
                timeout=Config.GROQ_TIMEOUT_SECONDS * 5,
            )
            result_report_type = (
                result.get("report_type")
                if isinstance(result, dict)
                else primary_report_type
            ) or primary_report_type or dimension.lower()
            reports_payload = {result_report_type: result}

        submission_id   = _extract_submission_id(msg, requested_submission)
        cycle_name      = _extract_cycle_name(msg, requested_cycle)
        employee_name   = msg.get("employee_name") or msg.get("employee_full_name") or msg.get("employee") or employee_id
        designation     = msg.get("designation") or msg.get("role") or msg.get("employee_role") or "Employee"
        dimension_label = {
            "1D": "1D - Individual Assessment",
            "2D": "2D - Employee-Boss Relationship",
            "3D": "3D - Team Assessment",
            "4D": "4D - Organisational Assessment",
        }.get(dimension, dimension)

        stage_scores = []
        for st in msg.get("stages", []):
            try:    score       = float(st.get("score",       0))
            except: score       = 0.0
            try:    pct         = float(st.get("percentage",  0))
            except: pct         = 0.0
            try:    final_value = float(st.get("final_value", score))
            except: final_value = score
            try:    final_pct   = float(st.get("final_percentage", pct))
            except: final_pct   = pct
            stage_scores.append({
                "stage":            _rename_stage_for_display(str(st.get("stage", "-"))),
                "score":            f"{final_value:.2f}",
                "percentage":       f"{final_pct:.1f}",
                "final_value":      f"{final_value:.2f}",
                "final_percentage": f"{final_pct:.1f}",
            })

        report_sections_list = []
        if isinstance(reports_payload, dict):
            for rtype, robj in reports_payload.items():
                if not (isinstance(robj, dict) and "sections" in robj):
                    continue

                clean_sections: List[Dict[str, Any]] = []
                for sec in robj.get("sections", []):
                    paras         = sec.get("paragraphs") or text_to_paragraphs(sec.get("text", ""))
                    section_id    = sec.get("id",    "")
                    section_title = sec.get("title", "")
                    clean_sec: Dict[str, Any] = {"id": section_id, "title": section_title, "paragraphs": paras}
                    existing_swot = sec.get("swot_lists")
                    if isinstance(existing_swot, dict):
                        clean_sec["swot_lists"] = existing_swot
                    elif is_swot_section(section_id, section_title):
                        clean_sec["swot_lists"] = build_swot_lists_from_section_paragraphs(paras)
                    clean_sections.append(clean_sec)
                _ensure_swot_section(clean_sections, rtype)

                # ── Determine SWOT data to inject ────────────────────────────────
                swot_to_inject: Optional[Dict[str, Any]] = None

                if rtype == "employee":
                    # 1D: Frappe SWOT quadrant lists if available, else LLM
                    if full_swot is not None:
                        swot_to_inject = full_swot
                    else:
                        existing_sec   = next((s for s in clean_sections if is_swot_section(s.get("id", ""), s.get("title", ""))), None)
                        existing_lists = existing_sec.get("swot_lists", {}) if existing_sec else {}
                        has_real = any(
                            existing_lists.get(k) and not str(existing_lists[k][0]).lower().endswith("not explicitly available in this report output.")
                            for k in ("strengths", "weaknesses", "opportunities", "threat", "threats")
                            if existing_lists.get(k)
                        )
                        if not has_real:
                            print(f"Worker {worker_id}: generating {rtype} SWOT via LLM")
                            swot_to_inject = await asyncio.to_thread(_generate_swot_via_llm, nd_data.get("behavioral_stage", {}), rtype)
                else:
                    existing_sec   = next((s for s in clean_sections if is_swot_section(s.get("id", ""), s.get("title", ""))), None)
                    existing_lists = existing_sec.get("swot_lists", {}) if existing_sec else {}
                    has_real = any(
                        existing_lists.get(k) and not str(existing_lists[k][0]).lower().endswith("not explicitly available in this report output.")
                        for k in ("strengths", "weaknesses", "opportunities", "threat", "threats")
                        if existing_lists.get(k)
                    )
                    if not has_real:
                        print(f"Worker {worker_id}: generating {rtype} SWOT via LLM")
                        swot_to_inject = await asyncio.to_thread(_generate_swot_via_llm, nd_data.get("behavioral_stage", {}), rtype)

                if swot_to_inject:
                    for sec in clean_sections:
                        if is_swot_section(sec.get("id", ""), sec.get("title", "")):
                            threat_rows = swot_to_inject.get("threat") or swot_to_inject.get("threats", [])
                            sec["swot_lists"] = {
                                "strengths":     [_swot_row_text(r) for r in swot_to_inject.get("strengths",     []) if _swot_row_text(r)],
                                "weaknesses":    [_swot_row_text(r) for r in swot_to_inject.get("weaknesses",    []) if _swot_row_text(r)],
                                "opportunities": [_swot_row_text(r) for r in swot_to_inject.get("opportunities", []) if _swot_row_text(r)],
                                "threat":        [_swot_row_text(r) for r in threat_rows                         if _swot_row_text(r)],
                            }
                            # ── 1D employee: inject ONLY the four SWOT quadrant lists ──
                            # Recommendations (section 9) and Action Navigator (section 10)
                            # were generated by generate_structured_report() with full
                            # word targets, week-wise format, and score-specific content.
                            # Do NOT overwrite them with the short API data here.
                            #
                            # 2D/3D/4D: inject recommendations/actionable from swot_to_inject
                            # because those report types do not have dedicated LLM sections.
                            if rtype != "employee":
                                sec["recommendations"]           = swot_to_inject.get("recommendations",           [])
                                sec["actionable_steps"]          = swot_to_inject.get("actionable_steps",          [])
                                sec["strategic_recommendations"] = swot_to_inject.get("strategic_recommendations", "")

                            sec["source"]    = swot_to_inject.get("source", "llm_generated")
                            sec["sub_stage"] = swot_to_inject.get("sub_stage", "")
                            sec["paragraphs"] = _swot_lists_to_paragraphs(sec["swot_lists"])
                            print(f"Worker {worker_id}: SWOT quadrants injected into '{rtype}' (source={sec['source']})")
                            break

                    # 2D/3D/4D only: push actionable steps and recommendations
                    # into their dedicated sections.
                    # 1D: LLM already generated these with week-wise format -- do not overwrite.
                    if rtype != "employee":
                        if _inject_actionable_into_action_section(clean_sections, swot_to_inject):
                            print(f"Worker {worker_id}: actionable steps added for '{rtype}'")
                        if _inject_recommendations_section(clean_sections, swot_to_inject, rtype):
                            print(f"Worker {worker_id}: recommendations section updated for '{rtype}'")

                report_sections_list.append({
                    "title":       robj.get("title") or REPORT_TITLE_MAP.get(rtype, rtype),
                    "report_type": rtype,
                    "sections":    clean_sections,
                })

        from datetime import datetime as _dt
        json_payload = {
            "status": "ok",
            "header": {
                "employee_id":        employee_id,
                "submission_id":      submission_id or "",
                "cycle_name":         cycle_name or "",
                "employee_name":      employee_name,
                "designation":        designation,
                "report_type":        f"{dimension_label} Growth Report",
                "dimension_label":    dimension_label,
                "dominant_stage":     _rename_stage_for_display(str(msg.get("dominant_stage",     "-"))),
                "dominant_sub_stage": _rename_stage_for_display(str(msg.get("dominant_sub_stage", "-"))),
                "questionnaire_text": ", ".join(str(q) for q in questionnaires) if questionnaires else "-",
                "generated_date":     _dt.now().strftime("%d %B %Y"),
                "stage_scores":       stage_scores,
            },
            "reports": report_sections_list,
        }

        path = save_employee_json(json_payload, employee_id, submission_id=submission_id, cycle_name=cycle_name)
        print(f"Worker {worker_id}: saved {path}")
        job.result = json_payload

    async def start(self):
        if self.running:
            return
        self.running = True
        self.workers = [asyncio.create_task(self._worker(i)) for i in range(self.num_workers)]
        print(f"Started {self.num_workers} workers")

    async def stop(self):
        if not self.running:
            return
        print("Stopping worker pool...")
        self.running = False
        await asyncio.gather(*self.workers, return_exceptions=True)
        print("Worker pool stopped")
